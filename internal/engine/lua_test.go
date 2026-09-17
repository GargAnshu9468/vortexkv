package engine

import (
	"testing"

	"github.com/GargAnshu9468/vortexkv/internal/persistence"
	"github.com/GargAnshu9468/vortexkv/internal/resp"
)

func TestLuaEvalBasicTypes(t *testing.T) {
	eng, err := NewEngine("", persistence.FsyncNo)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	// 1. String return
	res := eng.ExecuteCommand("test_conn", []string{"EVAL", "return 'hello vortex'", "0"})
	if res.Type != resp.BulkStringPrefix || string(res.Bulk) != "hello vortex" {
		t.Fatalf("Expected bulk string 'hello vortex', got: %v", res)
	}

	// 2. Number return
	res = eng.ExecuteCommand("test_conn", []string{"EVAL", "return 100 + 42", "0"})
	if res.Type != resp.IntegerPrefix || res.Num != 142 {
		t.Fatalf("Expected integer 142, got: %v", res)
	}

	// 3. Nil return
	res = eng.ExecuteCommand("test_conn", []string{"EVAL", "return nil", "0"})
	if !res.Null {
		t.Fatalf("Expected null reply, got: %v", res)
	}

	// 4. Boolean return (true -> 1, false -> null)
	res = eng.ExecuteCommand("test_conn", []string{"EVAL", "return true", "0"})
	if res.Type != resp.IntegerPrefix || res.Num != 1 {
		t.Fatalf("Expected 1 for true, got: %v", res)
	}

	res = eng.ExecuteCommand("test_conn", []string{"EVAL", "return false", "0"})
	if !res.Null {
		t.Fatalf("Expected null for false, got: %v", res)
	}

	// 5. Array table return
	res = eng.ExecuteCommand("test_conn", []string{"EVAL", "return {'item1', 99, 'item3'}", "0"})
	if res.Type != resp.ArrayPrefix || len(res.Array) != 3 {
		t.Fatalf("Expected 3-element array, got: %v", res)
	}
	if string(res.Array[0].Bulk) != "item1" || res.Array[1].Num != 99 || string(res.Array[2].Bulk) != "item3" {
		t.Fatalf("Unexpected array elements: %v", res.Array)
	}
}

func TestLuaEvalKeysAndArgv(t *testing.T) {
	eng, err := NewEngine("", persistence.FsyncNo)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	script := `return {KEYS[1], KEYS[2], ARGV[1], ARGV[2]}`
	res := eng.ExecuteCommand("test_conn", []string{"EVAL", script, "2", "k1", "k2", "v1", "v2"})
	if res.Type != resp.ArrayPrefix || len(res.Array) != 4 {
		t.Fatalf("Expected 4-element array, got: %v", res)
	}
	if string(res.Array[0].Bulk) != "k1" || string(res.Array[1].Bulk) != "k2" ||
		string(res.Array[2].Bulk) != "v1" || string(res.Array[3].Bulk) != "v2" {
		t.Fatalf("Unexpected KEYS/ARGV mapping: %v", res.Array)
	}
}

func TestLuaRedisCallAndPcall(t *testing.T) {
	eng, err := NewEngine("", persistence.FsyncNo)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	// 1. SET and GET inside Lua
	script := `
		redis.call("SET", KEYS[1], ARGV[1])
		local val = redis.call("GET", KEYS[1])
		return val
	`
	res := eng.ExecuteCommand("test_conn", []string{"EVAL", script, "1", "user:100", "Anshu"})
	if res.Type != resp.BulkStringPrefix || string(res.Bulk) != "Anshu" {
		t.Fatalf("Expected 'Anshu', got: %v", res)
	}

	// Verify key was really set in engine keyspace
	getRes := eng.ExecuteCommand("test_conn", []string{"GET", "user:100"})
	if string(getRes.Bulk) != "Anshu" {
		t.Fatalf("Expected key user:100 to be Anshu, got: %v", getRes)
	}

	// 2. Multi-step atomic counter with threshold limit
	rateLimitScript := `
		local current = redis.call("GET", KEYS[1])
		if not current then
			redis.call("SET", KEYS[1], "1")
			return 1
		end
		local num = tonumber(current)
		if num >= 3 then
			return -1
		end
		redis.call("INCR", KEYS[1])
		return num + 1
	`
	// Call 1 -> 1
	r1 := eng.ExecuteCommand("test_conn", []string{"EVAL", rateLimitScript, "1", "rate:ip"})
	if r1.Num != 1 {
		t.Fatalf("Call 1 expected 1, got %v", r1)
	}
	// Call 2 -> 2
	r2 := eng.ExecuteCommand("test_conn", []string{"EVAL", rateLimitScript, "1", "rate:ip"})
	if r2.Num != 2 {
		t.Fatalf("Call 2 expected 2, got %v", r2)
	}
	// Call 3 -> 3
	r3 := eng.ExecuteCommand("test_conn", []string{"EVAL", rateLimitScript, "1", "rate:ip"})
	if r3.Num != 3 {
		t.Fatalf("Call 3 expected 3, got %v", r3)
	}
	// Call 4 -> -1 (rejected by limit)
	r4 := eng.ExecuteCommand("test_conn", []string{"EVAL", rateLimitScript, "1", "rate:ip"})
	if r4.Num != -1 {
		t.Fatalf("Call 4 expected -1, got %v", r4)
	}

	// 3. redis.pcall error trap
	pcallScript := `
		local res = redis.pcall("UNKNOWNCOMMAND_XYZ")
		if res.err then
			return "trapped error: " .. res.err
		end
		return "should not reach"
	`
	pRes := eng.ExecuteCommand("test_conn", []string{"EVAL", pcallScript, "0"})
	if pRes.Type != resp.BulkStringPrefix || len(pRes.Bulk) == 0 {
		t.Fatalf("Expected trapped error string, got %v", pRes)
	}
}

func TestLuaScriptLifecycle(t *testing.T) {
	eng, err := NewEngine("", persistence.FsyncNo)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	script := `return redis.call("INCR", KEYS[1])`

	// 1. SCRIPT LOAD
	loadRes := eng.ExecuteCommand("test_conn", []string{"SCRIPT", "LOAD", script})
	if loadRes.Type != resp.BulkStringPrefix {
		t.Fatalf("Expected SHA1 bulk string, got %v", loadRes)
	}
	sha := string(loadRes.Bulk)

	// 2. SCRIPT EXISTS
	existsRes := eng.ExecuteCommand("test_conn", []string{"SCRIPT", "EXISTS", sha, "nonexistentsha123"})
	if existsRes.Type != resp.ArrayPrefix || len(existsRes.Array) != 2 {
		t.Fatalf("Expected 2 items in SCRIPT EXISTS, got %v", existsRes)
	}
	if existsRes.Array[0].Num != 1 || existsRes.Array[1].Num != 0 {
		t.Fatalf("Expected [1, 0], got [%d, %d]", existsRes.Array[0].Num, existsRes.Array[1].Num)
	}

	// 3. EVALSHA execution
	evalShaRes := eng.ExecuteCommand("test_conn", []string{"EVALSHA", sha, "1", "counter:sha"})
	if evalShaRes.Type != resp.IntegerPrefix || evalShaRes.Num != 1 {
		t.Fatalf("Expected 1 from EVALSHA, got %v", evalShaRes)
	}

	// 4. SCRIPT FLUSH
	flushRes := eng.ExecuteCommand("test_conn", []string{"SCRIPT", "FLUSH"})
	if flushRes.Type != resp.SimpleStringPrefix || flushRes.Str != "OK" {
		t.Fatalf("Expected OK from SCRIPT FLUSH, got %v", flushRes)
	}

	// 5. EVALSHA after flush should fail with NOSCRIPT
	evalShaFail := eng.ExecuteCommand("test_conn", []string{"EVALSHA", sha, "1", "counter:sha"})
	if evalShaFail.Type != resp.ErrorPrefix || evalShaFail.Str != "NOSCRIPT No matching script. Please use EVAL." {
		t.Fatalf("Expected NOSCRIPT error, got %v", evalShaFail)
	}
}

func TestLuaTimeoutProtection(t *testing.T) {
	eng, err := NewEngine("", persistence.FsyncNo)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}

	// Override timeout for fast testing
	// Run an infinite loop that would hang forever without timeout
	script := `
		local count = 0
		while true do
			count = count + 1
		end
		return count
	`

	// Let's test that cancel works
	go func() {
		for eng.Lua == nil {
		}
		// Give it a tiny slice to start, then kill
		eng.Lua.Kill()
	}()

	res := eng.ExecuteCommand("test_conn", []string{"EVAL", script, "0"})
	// Should return an error and not hang
	if res.Type != resp.ErrorPrefix {
		t.Fatalf("Expected error for infinite loop, got %v", res)
	}
}
