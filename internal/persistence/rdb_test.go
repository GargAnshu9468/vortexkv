package persistence

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestCRC64(t *testing.T) {
	// Reference vector: "123456789" with Redis CRC64-Jones polynomial
	data := []byte("123456789")
	crc := ChecksumCRC64(data)
	expected := uint64(0xcf228cf2176e85ed)
	if crc != expected {
		t.Fatalf("CRC64 mismatch: got %016x, expected %016x", crc, expected)
	}
}

func TestRDBSaveAndLoadAllTypes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vortex_rdb_test_*")
	if err != nil {
		t.Fatalf("Failed to create tmp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	rdbPath := filepath.Join(tmpDir, "dump.rdb")

	now := time.Now().UnixMilli()
	futureExpire := now + 100000

	entries := []RDBEntry{
		{Key: "str1", Type: RDBTypeString, ExpiresAt: 0, Value: "hello world"},
		{Key: "str2", Type: RDBTypeString, ExpiresAt: futureExpire, Value: "expiring soon"},
		{Key: "list1", Type: RDBTypeList, ExpiresAt: 0, Value: []string{"a", "b", "c"}},
		{Key: "set1", Type: RDBTypeSet, ExpiresAt: 0, Value: []string{"red", "green", "blue"}},
		{Key: "hash1", Type: RDBTypeHash, ExpiresAt: 0, Value: map[string]string{"f1": "v1", "f2": "v2"}},
		{Key: "zset1", Type: RDBTypeZSet, ExpiresAt: 0, Value: []ZSetItem{
			{Score: 10.5, Member: "first"},
			{Score: 99.0, Member: "second"},
		}},
		{Key: "stream1", Type: RDBTypeStream, ExpiresAt: 0, Value: []StreamItem{
			{ID: "1000-0", Fields: map[string]string{"sensor": "temp", "val": "22.5"}},
		}},
		{Key: "vec1", Type: RDBTypeVector, ExpiresAt: 0, Value: VectorItem{
			Dim: 3,
			Vectors: map[string][]float32{
				"v1": {0.1, 0.2, 0.3},
			},
		}},
	}

	aux := map[string]string{
		"redis-ver": "1.0.0-PROD",
		"ctime":     "1726148000",
	}

	// 1. Save to RDB
	if err := SaveRDB(rdbPath, entries, aux); err != nil {
		t.Fatalf("SaveRDB failed: %v", err)
	}

	// Verify file exists and has size
	stat, err := os.Stat(rdbPath)
	if err != nil || stat.Size() == 0 {
		t.Fatalf("RDB file missing or empty: %v", err)
	}

	// 2. Load from RDB
	restored := make(map[string]RDBEntry)
	loadedAux, err := LoadRDB(rdbPath, func(entry RDBEntry) error {
		restored[entry.Key] = entry
		return nil
	})
	if err != nil {
		t.Fatalf("LoadRDB failed: %v", err)
	}

	// Verify Aux
	if loadedAux["redis-ver"] != "1.0.0-PROD" {
		t.Fatalf("Aux redis-ver mismatch: %v", loadedAux)
	}

	// Verify restored entries count
	if len(restored) != len(entries) {
		t.Fatalf("Expected %d restored entries, got %d", len(entries), len(restored))
	}

	// Verify str1
	if s, ok := restored["str1"].Value.(string); !ok || s != "hello world" {
		t.Fatalf("Restored str1 mismatch: %v", restored["str1"])
	}

	// Verify str2 expiration
	if restored["str2"].ExpiresAt != futureExpire {
		t.Fatalf("Restored str2 TTL mismatch: %d vs %d", restored["str2"].ExpiresAt, futureExpire)
	}

	// Verify zset1
	zsetItems, ok := restored["zset1"].Value.([]ZSetItem)
	if !ok || len(zsetItems) != 2 || zsetItems[0].Score != 10.5 {
		t.Fatalf("Restored zset1 mismatch: %v", restored["zset1"])
	}

	// Verify vector1
	vecItem, ok := restored["vec1"].Value.(VectorItem)
	if !ok || vecItem.Dim != 3 || len(vecItem.Vectors["v1"]) != 3 {
		t.Fatalf("Restored vec1 mismatch: %v", restored["vec1"])
	}
}

func TestRDBChecksumMismatch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vortex_rdb_corrupt_*")
	if err != nil {
		t.Fatalf("Failed to create tmp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	rdbPath := filepath.Join(tmpDir, "dump.rdb")
	entries := []RDBEntry{
		{Key: "foo", Type: RDBTypeString, Value: "bar"},
	}

	if err := SaveRDB(rdbPath, entries, nil); err != nil {
		t.Fatalf("SaveRDB failed: %v", err)
	}

	// Corrupt a byte in the middle of the file
	data, err := os.ReadFile(rdbPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	data[len(data)/2] ^= 0xFF
	if err := os.WriteFile(rdbPath, data, 0600); err != nil {
		t.Fatalf("Failed to overwrite file: %v", err)
	}

	// Load should fail with ErrCRCMismatch
	_, err = LoadRDB(rdbPath, nil)
	if err != ErrCRCMismatch {
		t.Fatalf("Expected ErrCRCMismatch, got %v", err)
	}
}

func TestRDBManagerBgSave(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vortex_rdb_mgr_*")
	if err != nil {
		t.Fatalf("Failed to create tmp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	rdbPath := filepath.Join(tmpDir, "dump.rdb")
	mgr := NewRDBManager(rdbPath)

	entries := []RDBEntry{
		{Key: "alpha", Type: RDBTypeString, Value: "beta"},
	}

	var wg sync.WaitGroup
	wg.Add(1)

	err = mgr.BgSave(func() ([]RDBEntry, map[string]string) {
		return entries, map[string]string{"version": "1.0"}
	}, func(err error) {
		defer wg.Done()
		if err != nil {
			t.Errorf("BgSave failed: %v", err)
		}
	})
	if err != nil {
		t.Fatalf("Failed to trigger BgSave: %v", err)
	}

	// Concurrent BgSave should be rejected
	err = mgr.BgSave(func() ([]RDBEntry, map[string]string) { return nil, nil }, nil)
	if err != ErrBgSaveInProgress {
		t.Fatalf("Expected ErrBgSaveInProgress, got %v", err)
	}

	wg.Wait()

	if mgr.LastBgSaveStatus() != "ok" {
		t.Fatalf("Expected ok bg save status, got %s", mgr.LastBgSaveStatus())
	}
	if mgr.LastSave() == 0 {
		t.Fatalf("LastSave timestamp should be non-zero")
	}
}
