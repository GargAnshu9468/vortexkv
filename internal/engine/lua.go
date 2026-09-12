package engine

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	lua "github.com/yuin/gopher-lua"
	"github.com/vortexkv/vortexkv/internal/resp"
)

// DefaultLuaTimeout defines maximum execution duration before terminating a script
const DefaultLuaTimeout = 5 * time.Second

// LuaManager handles execution of Lua 5.1 scripts, script caching, and command bridging.
type LuaManager struct {
	mu           sync.RWMutex
	execMu       sync.Mutex // Ensures atomic execution of scripts
	scripts      map[string]string
	activeCancel context.CancelFunc
}

// NewLuaManager creates an instance of LuaManager.
func NewLuaManager() *LuaManager {
	return &LuaManager{
		scripts: make(map[string]string),
	}
}

// ComputeSHA1 computes hex-encoded SHA1 of a script.
func ComputeSHA1(script string) string {
	h := sha1.New()
	h.Write([]byte(script))
	return hex.EncodeToString(h.Sum(nil))
}

// Load stores a script into the cache and returns its SHA1 hash.
func (lm *LuaManager) Load(script string) string {
	sha := strings.ToLower(ComputeSHA1(script))
	lm.mu.Lock()
	lm.scripts[sha] = script
	lm.mu.Unlock()
	return sha
}

// Get retrieves a script by its SHA1 hash.
func (lm *LuaManager) Get(sha string) (string, bool) {
	lm.mu.RLock()
	s, ok := lm.scripts[strings.ToLower(sha)]
	lm.mu.RUnlock()
	return s, ok
}

// Flush clears all cached scripts.
func (lm *LuaManager) Flush() {
	lm.mu.Lock()
	lm.scripts = make(map[string]string)
	lm.mu.Unlock()
}

// Exists checks which of the given SHA1 hashes exist in cache.
func (lm *LuaManager) Exists(shas []string) []int64 {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	res := make([]int64, len(shas))
	for i, sha := range shas {
		if _, ok := lm.scripts[strings.ToLower(sha)]; ok {
			res[i] = 1
		} else {
			res[i] = 0
		}
	}
	return res
}

// Kill cancels the currently running script if any.
func (lm *LuaManager) Kill() bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if lm.activeCancel != nil {
		lm.activeCancel()
		lm.activeCancel = nil
		return true
	}
	return false
}

// Eval executes a script given args: [script, numkeys, key1, ..., keyN, arg1, ..., argM]
func (lm *LuaManager) Eval(eng *Engine, args []string) resp.Value {
	if len(args) < 2 {
		return resp.Error("ERR wrong number of arguments for 'eval' command")
	}

	script := args[0]
	numKeys, err := strconv.Atoi(args[1])
	if err != nil || numKeys < 0 {
		return resp.Error("ERR value is not an integer or out of range")
	}

	if len(args) < 2+numKeys {
		return resp.Error("ERR Number of keys can't be greater than number of args")
	}

	keys := args[2 : 2+numKeys]
	argv := args[2+numKeys:]

	// Cache script automatically on EVAL
	lm.Load(script)

	return lm.runScript(eng, script, keys, argv)
}

// EvalSha executes a cached script by SHA1 hash.
func (lm *LuaManager) EvalSha(eng *Engine, args []string) resp.Value {
	if len(args) < 2 {
		return resp.Error("ERR wrong number of arguments for 'evalsha' command")
	}

	sha := strings.ToLower(args[0])
	script, ok := lm.Get(sha)
	if !ok {
		return resp.Error("NOSCRIPT No matching script. Please use EVAL.")
	}

	numKeys, err := strconv.Atoi(args[1])
	if err != nil || numKeys < 0 {
		return resp.Error("ERR value is not an integer or out of range")
	}

	if len(args) < 2+numKeys {
		return resp.Error("ERR Number of keys can't be greater than number of args")
	}

	keys := args[2 : 2+numKeys]
	argv := args[2+numKeys:]

	return lm.runScript(eng, script, keys, argv)
}

// ScriptCommand handles SCRIPT subcommands (LOAD, EXISTS, FLUSH, KILL).
func (lm *LuaManager) ScriptCommand(args []string) resp.Value {
	if len(args) == 0 {
		return resp.Error("ERR wrong number of arguments for 'script' command")
	}

	sub := strings.ToUpper(args[0])
	switch sub {
	case "LOAD":
		if len(args) != 2 {
			return resp.Error("ERR wrong number of arguments for 'script|load' command")
		}
		sha := lm.Load(args[1])
		return resp.BulkString(sha)

	case "EXISTS":
		if len(args) < 2 {
			return resp.Error("ERR wrong number of arguments for 'script|exists' command")
		}
		shas := args[1:]
		flags := lm.Exists(shas)
		vals := make([]resp.Value, len(flags))
		for i, f := range flags {
			vals[i] = resp.Integer(f)
		}
		return resp.Array(vals)

	case "FLUSH":
		lm.Flush()
		return resp.SimpleString("OK")

	case "KILL":
		if lm.Kill() {
			return resp.SimpleString("OK")
		}
		return resp.Error("NOTBUSY No scripts in execution.")

	default:
		return resp.Error(fmt.Sprintf("ERR Unknown SCRIPT subcommand or wrong # of args for '%s'", sub))
	}
}

func (lm *LuaManager) runScript(eng *Engine, script string, keys []string, argv []string) resp.Value {
	// Atomic script execution lock
	lm.execMu.Lock()
	defer lm.execMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), DefaultLuaTimeout)
	defer cancel()

	lm.mu.Lock()
	lm.activeCancel = cancel
	lm.mu.Unlock()

	defer func() {
		lm.mu.Lock()
		lm.activeCancel = nil
		lm.mu.Unlock()
	}()

	L := lua.NewState()
	defer L.Close()
	L.SetContext(ctx)

	// Set KEYS table
	keysTable := L.NewTable()
	for i, k := range keys {
		keysTable.RawSetInt(i+1, lua.LString(k))
	}
	L.SetGlobal("KEYS", keysTable)

	// Set ARGV table
	argvTable := L.NewTable()
	for i, a := range argv {
		argvTable.RawSetInt(i+1, lua.LString(a))
	}
	L.SetGlobal("ARGV", argvTable)

	// Register `redis` global table
	redisTable := L.NewTable()
	L.SetField(redisTable, "call", L.NewFunction(func(ls *lua.LState) int {
		return lm.luaRedisCall(ls, eng, false)
	}))
	L.SetField(redisTable, "pcall", L.NewFunction(func(ls *lua.LState) int {
		return lm.luaRedisCall(ls, eng, true)
	}))
	L.SetField(redisTable, "sha1hex", L.NewFunction(func(ls *lua.LState) int {
		str := ls.CheckString(1)
		ls.Push(lua.LString(ComputeSHA1(str)))
		return 1
	}))
	L.SetField(redisTable, "status_reply", L.NewFunction(func(ls *lua.LState) int {
		str := ls.CheckString(1)
		t := ls.NewTable()
		ls.SetField(t, "ok", lua.LString(str))
		ls.Push(t)
		return 1
	}))
	L.SetField(redisTable, "error_reply", L.NewFunction(func(ls *lua.LState) int {
		str := ls.CheckString(1)
		t := ls.NewTable()
		ls.SetField(t, "err", lua.LString(str))
		ls.Push(t)
		return 1
	}))
	L.SetGlobal("redis", redisTable)

	// Execute Lua script
	if err := L.DoString(script); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return resp.Error("ERR Script execution timed out.")
		}
		// Clean Lua error message (strip file/line if present)
		errMsg := err.Error()
		if idx := strings.Index(errMsg, ": "); idx != -1 {
			// keep the user-facing portion
			errMsg = errMsg[idx+2:]
		}
		return resp.Error(fmt.Sprintf("ERR Error running script: %s", errMsg))
	}

	// Fetch return value
	top := L.GetTop()
	if top == 0 {
		return resp.Null()
	}

	return lm.luaToResp(L, L.Get(top))
}

func (lm *LuaManager) luaRedisCall(ls *lua.LState, eng *Engine, isPcall bool) int {
	nArgs := ls.GetTop()
	if nArgs < 1 {
		if isPcall {
			t := ls.NewTable()
			ls.SetField(t, "err", lua.LString("ERR wrong number of arguments for redis.call()"))
			ls.Push(t)
			return 1
		}
		ls.RaiseError("ERR wrong number of arguments for redis.call()")
		return 0
	}

	cmdArgs := make([]string, nArgs)
	for i := 1; i <= nArgs; i++ {
		cmdArgs[i-1] = ls.Get(i).String()
	}

	// Execute internal command with empty connID (authorized server mode)
	res := eng.ExecuteCommand("", cmdArgs)

	if res.Type == resp.ErrorPrefix {
		if isPcall {
			t := ls.NewTable()
			ls.SetField(t, "err", lua.LString(res.Str))
			ls.Push(t)
			return 1
		}
		ls.RaiseError("%s", res.Str)
		return 0
	}

	ls.Push(lm.respToLua(ls, res))
	return 1
}

func (lm *LuaManager) respToLua(ls *lua.LState, v resp.Value) lua.LValue {
	if v.Null {
		return lua.LFalse // Redis Lua 5.1 converts nil bulk reply to boolean false
	}

	switch v.Type {
	case resp.SimpleStringPrefix:
		t := ls.NewTable()
		ls.SetField(t, "ok", lua.LString(v.Str))
		return t
	case resp.ErrorPrefix:
		t := ls.NewTable()
		ls.SetField(t, "err", lua.LString(v.Str))
		return t
	case resp.IntegerPrefix:
		return lua.LNumber(float64(v.Num))
	case resp.BulkStringPrefix:
		return lua.LString(string(v.Bulk))
	case resp.ArrayPrefix:
		t := ls.NewTable()
		for i, item := range v.Array {
			t.RawSetInt(i+1, lm.respToLua(ls, item))
		}
		return t
	case resp.BooleanPrefix:
		if v.Bool {
			return lua.LTrue
		}
		return lua.LFalse
	default:
		return lua.LNil
	}
}

func (lm *LuaManager) luaToResp(ls *lua.LState, lv lua.LValue) resp.Value {
	switch lv.Type() {
	case lua.LTNil:
		return resp.Null()
	case lua.LTBool:
		if lv == lua.LTrue {
			return resp.Integer(1)
		}
		return resp.Null()
	case lua.LTNumber:
		return resp.Integer(int64(lv.(lua.LNumber)))
	case lua.LTString:
		return resp.BulkString(lv.String())
	case lua.LTTable:
		t := lv.(*lua.LTable)
		// Check for {err = "..."}
		errVal := ls.GetField(t, "err")
		if errVal.Type() == lua.LTString {
			return resp.Error(errVal.String())
		}
		// Check for {ok = "..."}
		okVal := ls.GetField(t, "ok")
		if okVal.Type() == lua.LTString {
			return resp.SimpleString(okVal.String())
		}

		// Array-like table
		arrLen := t.Len()
		items := make([]resp.Value, 0, arrLen)
		for i := 1; ; i++ {
			elem := t.RawGetInt(i)
			if elem == lua.LNil {
				break
			}
			items = append(items, lm.luaToResp(ls, elem))
		}
		return resp.Array(items)
	default:
		return resp.BulkString(lv.String())
	}
}
