package engine

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/vortexkv/vortexkv/internal/resp"
)

// DefaultWasmTimeout limits execution duration of a Wasm call
const DefaultWasmTimeout = 5 * time.Second

// WasmFunction stores compiled WebAssembly module metadata.
type WasmFunction struct {
	Name      string
	Bytecode  []byte
	Compiled  wazero.CompiledModule
	LoadedAt  time.Time
	CallCount int64
}

// WasmManager manages compiled WebAssembly functions and host bindings.
type WasmManager struct {
	mu        sync.RWMutex
	runtime   wazero.Runtime
	functions map[string]*WasmFunction
}

// NewWasmManager initializes a pure-Go WebAssembly runtime.
func NewWasmManager() (*WasmManager, error) {
	ctx := context.Background()
	r := wazero.NewRuntime(ctx)

	wm := &WasmManager{
		runtime:   r,
		functions: make(map[string]*WasmFunction),
	}

	return wm, nil
}

// RegisterHostFunctions exports keyspace manipulation functions to the Wasm environment.
func (wm *WasmManager) registerHostFunctions(eng *Engine) error {
	ctx := context.Background()

	// Check if already registered
	if wm.runtime.Module("vortex") != nil {
		return nil
	}

	_, err := wm.runtime.NewHostModuleBuilder("vortex").
		NewFunctionBuilder().
		WithFunc(func(ctx context.Context, mod api.Module, keyPtr, keyLen, outPtr, outMax uint32) uint32 {
			mem := mod.Memory()
			keyBytes, ok := mem.Read(keyPtr, keyLen)
			if !ok {
				return 0
			}
			key := string(keyBytes)

			entry, found := eng.Keyspace.Get(key)
			if !found || entry.Type != TypeString {
				return 0
			}

			valStr, isStr := entry.Value.(string)
			if !isStr {
				return 0
			}

			valBytes := []byte(valStr)
			toWrite := uint32(len(valBytes))
			if toWrite > outMax {
				toWrite = outMax
			}

			if ok := mem.Write(outPtr, valBytes[:toWrite]); !ok {
				return 0
			}
			return toWrite
		}).
		Export("vortex_get").
		NewFunctionBuilder().
		WithFunc(func(ctx context.Context, mod api.Module, keyPtr, keyLen, valPtr, valLen uint32) uint32 {
			mem := mod.Memory()
			keyBytes, ok1 := mem.Read(keyPtr, keyLen)
			valBytes, ok2 := mem.Read(valPtr, valLen)
			if !ok1 || !ok2 {
				return 1 // Error
			}

			eng.Keyspace.Set(string(keyBytes), &Entry{
				Type:  TypeString,
				Value: string(valBytes),
			})
			return 0 // Success
		}).
		Export("vortex_set").
		NewFunctionBuilder().
		WithFunc(func(ctx context.Context, mod api.Module, keyPtr, keyLen uint32) uint32 {
			mem := mod.Memory()
			keyBytes, ok := mem.Read(keyPtr, keyLen)
			if !ok {
				return 1
			}
			eng.Keyspace.Delete(string(keyBytes))
			return 0
		}).
		Export("vortex_del").
		NewFunctionBuilder().
		WithFunc(func(ctx context.Context, mod api.Module, msgPtr, msgLen uint32) {
			mem := mod.Memory()
			msgBytes, ok := mem.Read(msgPtr, msgLen)
			if ok {
				log.Printf("[VortexKV Wasm] %s", string(msgBytes))
			}
		}).
		Export("vortex_log").
		Instantiate(ctx)

	return err
}

// HandleCommand dispatches WASM subcommands: LOAD, CALL, LIST, DELETE.
func (wm *WasmManager) HandleCommand(eng *Engine, args []string) resp.Value {
	if len(args) == 0 {
		return resp.Error("ERR wrong number of arguments for 'wasm' command")
	}

	// Ensure host functions are registered
	_ = wm.registerHostFunctions(eng)

	sub := strings.ToUpper(args[0])
	switch sub {
	case "LOAD":
		if len(args) < 3 {
			return resp.Error("ERR wrong number of arguments for 'wasm load' command")
		}
		funcName := args[1]
		payload := args[2]

		var bytecode []byte
		var err error
		if len(payload) > 8 && (strings.HasPrefix(payload, "0061736d") || strings.HasPrefix(payload, "0x")) {
			// Hex encoded wasm bytecode
			cleanHex := strings.TrimPrefix(payload, "0x")
			bytecode, err = hex.DecodeString(cleanHex)
			if err != nil {
				return resp.Error(fmt.Sprintf("ERR invalid hex bytecode: %v", err))
			}
		} else {
			bytecode = []byte(payload)
		}

		ctx := context.Background()
		compiled, err := wm.runtime.CompileModule(ctx, bytecode)
		if err != nil {
			return resp.Error(fmt.Sprintf("ERR failed to compile Wasm module: %v", err))
		}

		wm.mu.Lock()
		wm.functions[funcName] = &WasmFunction{
			Name:     funcName,
			Bytecode: bytecode,
			Compiled: compiled,
			LoadedAt: time.Now(),
		}
		wm.mu.Unlock()

		return resp.SimpleString("OK")

	case "CALL":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'wasm call' command")
		}
		funcName := args[1]

		wm.mu.RLock()
		fn, ok := wm.functions[funcName]
		wm.mu.RUnlock()

		if !ok {
			return resp.Error(fmt.Sprintf("ERR Wasm function '%s' not found", funcName))
		}

		atomic.AddInt64(&fn.CallCount, 1)

		ctx, cancel := context.WithTimeout(context.Background(), DefaultWasmTimeout)
		defer cancel()

		mod, err := wm.runtime.InstantiateModule(ctx, fn.Compiled, wazero.NewModuleConfig().WithName(""))
		if err != nil {
			return resp.Error(fmt.Sprintf("ERR failed to instantiate Wasm module: %v", err))
		}
		defer mod.Close(ctx)

		// Search for entrypoint function: run, execute, or the function name itself
		entrypoint := mod.ExportedFunction("run")
		if entrypoint == nil {
			entrypoint = mod.ExportedFunction("execute")
		}
		if entrypoint == nil {
			entrypoint = mod.ExportedFunction(funcName)
		}
		if entrypoint == nil {
			return resp.Error("ERR no exported 'run', 'execute', or named entrypoint in Wasm module")
		}

		// Convert call arguments to uint64 params if numeric
		callParams := make([]uint64, 0, len(args)-2)
		for _, a := range args[2:] {
			if n, err := strconv.ParseInt(a, 10, 64); err == nil {
				callParams = append(callParams, uint64(n))
			} else {
				callParams = append(callParams, 0)
			}
		}

		results, err := entrypoint.Call(ctx, callParams...)
		if err != nil {
			return resp.Error(fmt.Sprintf("ERR Wasm execution failed: %v", err))
		}

		if len(results) == 0 {
			return resp.Null()
		}

		// Return integer or simple string
		return resp.Integer(int64(results[0]))

	case "LIST":
		wm.mu.RLock()
		defer wm.mu.RUnlock()

		vals := make([]resp.Value, 0, len(wm.functions))
		for _, fn := range wm.functions {
			item := []resp.Value{
				resp.BulkString("name"), resp.BulkString(fn.Name),
				resp.BulkString("bytes"), resp.Integer(int64(len(fn.Bytecode))),
				resp.BulkString("calls"), resp.Integer(atomic.LoadInt64(&fn.CallCount)),
				resp.BulkString("loaded_at"), resp.Integer(fn.LoadedAt.Unix()),
			}
			vals = append(vals, resp.Array(item))
		}
		return resp.Array(vals)

	case "DELETE":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'wasm delete' command")
		}
		funcName := args[1]

		wm.mu.Lock()
		defer wm.mu.Unlock()

		if _, ok := wm.functions[funcName]; !ok {
			return resp.Integer(0)
		}
		delete(wm.functions, funcName)
		return resp.Integer(1)

	default:
		return resp.Error(fmt.Sprintf("ERR unknown WASM subcommand '%s'", sub))
	}
}
