package engine

import (
	"testing"

	"github.com/GargAnshu9468/vortexkv/internal/persistence"
	"github.com/GargAnshu9468/vortexkv/internal/resp"
)

func TestWasmLoadAndCallConstant(t *testing.T) {
	eng, err := NewEngine("", persistence.FsyncNo)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	// Wasm module returning constant 42 from exported "run" function
	// WAT:
	// (module
	//   (func (export "run") (result i32)
	//     i32.const 42
	//   )
	// )
	wasmHex := "0061736d010000000105016000017f030201000707010372756e00000a06010400412a0b"

	// 1. WASM LOAD
	res := eng.ExecuteCommand("test_client", []string{"WASM", "LOAD", "answer", wasmHex})
	if res.Type != resp.SimpleStringPrefix || res.Str != "OK" {
		t.Fatalf("Expected OK from WASM LOAD, got: %v", res)
	}

	// 2. WASM CALL
	callRes := eng.ExecuteCommand("test_client", []string{"WASM", "CALL", "answer"})
	if callRes.Type != resp.IntegerPrefix || callRes.Num != 42 {
		t.Fatalf("Expected 42 from WASM CALL, got: %v", callRes)
	}

	// 3. WASM LIST
	listRes := eng.ExecuteCommand("test_client", []string{"WASM", "LIST"})
	if listRes.Type != resp.ArrayPrefix || len(listRes.Array) == 0 {
		t.Fatalf("Expected array from WASM LIST, got: %v", listRes)
	}

	// 4. WASM DELETE
	delRes := eng.ExecuteCommand("test_client", []string{"WASM", "DELETE", "answer"})
	if delRes.Type != resp.IntegerPrefix || delRes.Num != 1 {
		t.Fatalf("Expected 1 from WASM DELETE, got: %v", delRes)
	}

	// 5. WASM CALL after delete should fail
	callFail := eng.ExecuteCommand("test_client", []string{"WASM", "CALL", "answer"})
	if callFail.Type != resp.ErrorPrefix {
		t.Fatalf("Expected error calling deleted function, got: %v", callFail)
	}
}

func TestWasmAddTwoNumbers(t *testing.T) {
	eng, err := NewEngine("", persistence.FsyncNo)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	// Wasm module: run(a, b) -> a + b
	// WAT:
	// (module
	//   (func (export "run") (param i32 i32) (result i32)
	//     local.get 0
	//     local.get 1
	//     i32.add
	//   )
	// )
	wasmHex := "0061736d0100000001070160027f7f017f030201000707010372756e00000a09010700200020016a0b"

	res := eng.ExecuteCommand("test_client", []string{"WASM", "LOAD", "add", wasmHex})
	if res.Type != resp.SimpleStringPrefix || res.Str != "OK" {
		t.Fatalf("Failed to load Wasm: %v", res)
	}

	callRes := eng.ExecuteCommand("test_client", []string{"WASM", "CALL", "add", "100", "250"})
	if callRes.Type != resp.IntegerPrefix || callRes.Num != 350 {
		t.Fatalf("Expected 350, got: %v", callRes)
	}
}
