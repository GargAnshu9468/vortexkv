package engine

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/GargAnshu9468/vortexkv/internal/persistence"
	"github.com/GargAnshu9468/vortexkv/internal/resp"
)

func TestRDBEngineE2E(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vortex_rdb_e2e_*")
	if err != nil {
		t.Fatalf("Failed to create tmp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	rdbPath := filepath.Join(tmpDir, "dump.rdb")

	// 1. Start primary engine
	eng1, err := NewEngine("", persistence.FsyncNo, rdbPath)
	if err != nil {
		t.Fatalf("Failed to initialize primary engine: %v", err)
	}

	// 2. Populate entries across all data types
	eng1.ExecuteCommand("c1", []string{"SET", "site", "vortexkv.io"})
	eng1.ExecuteCommand("c1", []string{"HSET", "user:admin", "name", "Neo", "role", "root"})
	eng1.ExecuteCommand("c1", []string{"RPUSH", "queue:tasks", "task_alpha", "task_beta"})
	eng1.ExecuteCommand("c1", []string{"SADD", "skills", "golang", "redis", "ai"})
	eng1.ExecuteCommand("c1", []string{"ZADD", "leaderboard", "1500", "player_one", "2200", "player_two"})
	eng1.ExecuteCommand("c1", []string{"XADD", "events:telemetry", "*", "device", "iot_42", "temp", "24.5"})
	eng1.ExecuteCommand("c1", []string{"VADD", "embeddings", "vec_1", "0.1", "0.2", "0.3", "0.4"})

	// 3. Test synchronous SAVE
	saveRes := eng1.ExecuteCommand("c1", []string{"SAVE"})
	if saveRes.Type != resp.SimpleStringPrefix || saveRes.Str != "OK" {
		t.Fatalf("SAVE failed: %v", saveRes)
	}

	// Verify file was written
	stat, err := os.Stat(rdbPath)
	if err != nil || stat.Size() == 0 {
		t.Fatalf("Expected non-empty RDB file after SAVE: %v", err)
	}

	// 4. Test LASTSAVE
	lastSaveRes := eng1.ExecuteCommand("c1", []string{"LASTSAVE"})
	if lastSaveRes.Type != resp.IntegerPrefix || lastSaveRes.Num == 0 {
		t.Fatalf("LASTSAVE returned invalid timestamp: %v", lastSaveRes)
	}

	// 5. Test BGSAVE
	bgRes := eng1.ExecuteCommand("c1", []string{"BGSAVE"})
	if bgRes.Type != resp.SimpleStringPrefix || bgRes.Str != "Background saving started" {
		t.Fatalf("BGSAVE failed: %v", bgRes)
	}

	// Wait for background save to complete
	time.Sleep(50 * time.Millisecond)

	// 6. Start secondary engine loading from the generated dump.rdb
	eng2, err := NewEngine("", persistence.FsyncNo, rdbPath)
	if err != nil {
		t.Fatalf("Failed to initialize secondary engine from RDB: %v", err)
	}

	// 7. Verify all data types were fully restored
	// String
	sRes := eng2.ExecuteCommand("c2", []string{"GET", "site"})
	if sRes.Type != resp.BulkStringPrefix || string(sRes.Bulk) != "vortexkv.io" {
		t.Fatalf("Restored String mismatch: %v", sRes)
	}

	// Hash
	hRes := eng2.ExecuteCommand("c2", []string{"HGET", "user:admin", "name"})
	if hRes.Type != resp.BulkStringPrefix || string(hRes.Bulk) != "Neo" {
		t.Fatalf("Restored Hash mismatch: %v", hRes)
	}

	// List
	lRes := eng2.ExecuteCommand("c2", []string{"LRANGE", "queue:tasks", "0", "-1"})
	if lRes.Type != resp.ArrayPrefix || len(lRes.Array) != 2 {
		t.Fatalf("Restored List mismatch: %v", lRes)
	}

	// Set
	setRes := eng2.ExecuteCommand("c2", []string{"SCARD", "skills"})
	if setRes.Type != resp.IntegerPrefix || setRes.Num != 3 {
		t.Fatalf("Restored Set cardinality mismatch: %v", setRes)
	}

	// ZSet
	zRes := eng2.ExecuteCommand("c2", []string{"ZRANGE", "leaderboard", "0", "-1"})
	if zRes.Type != resp.ArrayPrefix || len(zRes.Array) != 2 || string(zRes.Array[1].Bulk) != "player_two" {
		t.Fatalf("Restored ZSet mismatch: %v", zRes)
	}

	// Stream
	stRes := eng2.ExecuteCommand("c2", []string{"XLEN", "events:telemetry"})
	if stRes.Type != resp.IntegerPrefix || stRes.Num != 1 {
		t.Fatalf("Restored Stream length mismatch: %v", stRes)
	}

	// Vector
	vRes := eng2.ExecuteCommand("c2", []string{"VINFO", "embeddings"})
	if vRes.Type != resp.ArrayPrefix || len(vRes.Array) < 2 {
		t.Fatalf("Restored Vector index mismatch: %v", vRes)
	}
	for i := 0; i < len(vRes.Array); i += 2 {
		if string(vRes.Array[i].Bulk) == "count" && string(vRes.Array[i+1].Bulk) != "1" {
			t.Fatalf("Restored Vector count mismatch: %s", string(vRes.Array[i+1].Bulk))
		}
	}
}
