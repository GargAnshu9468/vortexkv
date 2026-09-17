package engine

import (
	"strings"
	"testing"
	"time"

	"github.com/GargAnshu9468/vortexkv/internal/resp"
)

func TestEngineCoreCommands(t *testing.T) {
	eng, err := NewEngine("", "")
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer eng.Close()

	// 1. PING
	res := eng.ExecuteCommand("c1", []string{"PING"})
	if res.Str != "PONG" {
		t.Fatalf("Expected PONG, got %v", res)
	}

	// 2. SET and GET
	eng.ExecuteCommand("c1", []string{"SET", "framework", "vortexkv"})
	res = eng.ExecuteCommand("c1", []string{"GET", "framework"})
	if string(res.Bulk) != "vortexkv" {
		t.Fatalf("Expected vortexkv, got %s", string(res.Bulk))
	}

	// 3. INCR
	res = eng.ExecuteCommand("c1", []string{"INCR", "counter"})
	if res.Num != 1 {
		t.Fatalf("Expected 1, got %d", res.Num)
	}
	res = eng.ExecuteCommand("c1", []string{"INCRBY", "counter", "41"})
	if res.Num != 42 {
		t.Fatalf("Expected 42, got %d", res.Num)
	}

	// 4. Hash Commands
	eng.ExecuteCommand("c1", []string{"HSET", "user:1", "name", "Nova", "role", "admin"})
	res = eng.ExecuteCommand("c1", []string{"HGET", "user:1", "name"})
	if string(res.Bulk) != "Nova" {
		t.Fatalf("Expected Nova, got %s", string(res.Bulk))
	}
	res = eng.ExecuteCommand("c1", []string{"HLEN", "user:1"})
	if res.Num != 2 {
		t.Fatalf("Expected HLEN 2, got %d", res.Num)
	}

	// 5. List Commands
	eng.ExecuteCommand("c1", []string{"RPUSH", "queue", "job1", "job2"})
	eng.ExecuteCommand("c1", []string{"LPUSH", "queue", "job0"})
	res = eng.ExecuteCommand("c1", []string{"LRANGE", "queue", "0", "-1"})
	if len(res.Array) != 3 || string(res.Array[0].Bulk) != "job0" {
		t.Fatalf("Unexpected lrange output: %v", res)
	}

	// 6. ZSet Commands
	eng.ExecuteCommand("c1", []string{"ZADD", "rankings", "100", "p1", "200", "p2", "150", "p3"})
	res = eng.ExecuteCommand("c1", []string{"ZRANGE", "rankings", "0", "-1"})
	if len(res.Array) != 3 || string(res.Array[0].Bulk) != "p1" || string(res.Array[1].Bulk) != "p3" || string(res.Array[2].Bulk) != "p2" {
		t.Fatalf("Unexpected zrange output: %v", res)
	}

	// 7. Vector Search (Next-Gen AI Primitives)
	eng.ExecuteCommand("c1", []string{"VADD", "emb_space", "doc_tech", "0.9", "0.1", "0.0"})
	eng.ExecuteCommand("c1", []string{"VADD", "emb_space", "doc_bio", "0.0", "0.8", "0.2"})
	res = eng.ExecuteCommand("c1", []string{"VSEARCH", "emb_space", "1", "cosine", "0.95", "0.05", "0.0"})
	if len(res.Array) != 1 || string(res.Array[0].Array[0].Bulk) != "doc_tech" {
		t.Fatalf("Unexpected vector search result: %v", res)
	}

	// 8. Stream Commands
	res = eng.ExecuteCommand("c1", []string{"XADD", "events", "*", "sensor", "temp", "val", "23.5"})
	if res.Type != resp.BulkStringPrefix {
		t.Fatalf("Expected bulk string ID from XADD, got %v", res)
	}
	res = eng.ExecuteCommand("c1", []string{"XLEN", "events"})
	if res.Num != 1 {
		t.Fatalf("Expected stream len 1, got %d", res.Num)
	}

	// 9. TTL & Expiration
	eng.ExecuteCommand("c1", []string{"SET", "temp_key", "ephemeral", "PX", "50"})
	res = eng.ExecuteCommand("c1", []string{"GET", "temp_key"})
	if string(res.Bulk) != "ephemeral" {
		t.Fatalf("Expected ephemeral, got %s", string(res.Bulk))
	}
	time.Sleep(60 * time.Millisecond)
	res = eng.ExecuteCommand("c1", []string{"GET", "temp_key"})
	if !res.Null {
		t.Fatalf("Expected expired key to return null, got %v", res)
	}
}

func TestAuthAndSecurity(t *testing.T) {
	eng, err := NewEngine("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	eng.Password = "SuperSecret123"

	// 1. Without AUTH, command should fail with NOAUTH
	res := eng.ExecuteCommand("clientA", []string{"SET", "secure_key", "val"})
	if res.Type != resp.ErrorPrefix || !strings.Contains(res.Str, "NOAUTH") {
		t.Fatalf("Expected NOAUTH error, got: %v", res)
	}

	// 2. Invalid password should fail
	res = eng.ExecuteCommand("clientA", []string{"AUTH", "wrong_password"})
	if res.Type != resp.ErrorPrefix || !strings.Contains(res.Str, "ERR invalid password") {
		t.Fatalf("Expected invalid password error, got: %v", res)
	}

	// 3. Valid password should succeed
	res = eng.ExecuteCommand("clientA", []string{"AUTH", "SuperSecret123"})
	if res.Str != "OK" {
		t.Fatalf("Expected OK from AUTH, got: %v", res)
	}

	// 4. Now command succeeds
	res = eng.ExecuteCommand("clientA", []string{"SET", "secure_key", "val"})
	if res.Str != "OK" {
		t.Fatalf("Expected OK from SET after AUTH, got: %v", res)
	}
}

func TestTransactionsMultiExec(t *testing.T) {
	eng, err := NewEngine("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// 1. Start transaction
	res := eng.ExecuteCommand("clientTx", []string{"MULTI"})
	if res.Str != "OK" {
		t.Fatalf("Expected OK from MULTI, got: %v", res)
	}

	// 2. Queue commands
	res1 := eng.ExecuteCommand("clientTx", []string{"SET", "tx_key", "100"})
	res2 := eng.ExecuteCommand("clientTx", []string{"INCR", "tx_key"})
	if res1.Str != "QUEUED" || res2.Str != "QUEUED" {
		t.Fatalf("Expected QUEUED, got %v, %v", res1, res2)
	}

	// 3. Commit transaction
	execRes := eng.ExecuteCommand("clientTx", []string{"EXEC"})
	if execRes.Type != resp.ArrayPrefix || len(execRes.Array) != 2 {
		t.Fatalf("Expected array of 2 results from EXEC, got: %v", execRes)
	}
	if execRes.Array[0].Str != "OK" || execRes.Array[1].Num != 101 {
		t.Fatalf("Unexpected EXEC results: %+v", execRes.Array)
	}

	// 4. Test DISCARD
	eng.ExecuteCommand("clientTx", []string{"MULTI"})
	eng.ExecuteCommand("clientTx", []string{"SET", "discarded", "foo"})
	discardRes := eng.ExecuteCommand("clientTx", []string{"DISCARD"})
	if discardRes.Str != "OK" {
		t.Fatalf("Expected OK from DISCARD, got: %v", discardRes)
	}
	checkRes := eng.ExecuteCommand("clientTx", []string{"GET", "discarded"})
	if !checkRes.Null {
		t.Fatalf("Expected discarded key to be nil, got: %v", checkRes)
	}
}

func TestCredentialRedactionInSlowLog(t *testing.T) {
	eng, err := NewEngine("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	eng.Password = "SuperSecretPassword99"

	// Record a slow AUTH command
	eng.Telemetry.RecordCommand("AUTH", 15000, []string{"AUTH", "SuperSecretPassword99"})

	snap := eng.Telemetry.GetSnapshot(0)
	if len(snap.RecentSlowLogs) == 0 {
		t.Fatal("Expected slowlog entry to be recorded")
	}

	slowEntry := snap.RecentSlowLogs[len(snap.RecentSlowLogs)-1]
	for _, arg := range slowEntry.Command[1:] {
		if strings.Contains(arg, "SuperSecretPassword99") {
			t.Fatalf("Security breach! Plaintext password found in slowlog: %v", slowEntry.Command)
		}
		if arg != "[REDACTED]" {
			t.Fatalf("Expected password to be [REDACTED], got %s", arg)
		}
	}
}

func TestClientSessionCleanup(t *testing.T) {
	eng, err := NewEngine("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	session := eng.GetClientSession("client-test-123")
	if session == nil {
		t.Fatal("Expected session to be created")
	}

	eng.ClearClientSession("client-test-123")

	eng.muClients.RLock()
	_, exists := eng.clientSessions["client-test-123"]
	eng.muClients.RUnlock()

	if exists {
		t.Fatal("Expected client session to be deleted after ClearClientSession")
	}
}

