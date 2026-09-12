package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vortexkv/vortexkv/internal/cluster"
	"github.com/vortexkv/vortexkv/internal/persistence"
	"github.com/vortexkv/vortexkv/internal/resp"
)

func TestClusterEngineE2E(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vortex_cluster_e2e_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	confA := filepath.Join(tmpDir, "nodes_a.conf")
	confB := filepath.Join(tmpDir, "nodes_b.conf")

	// 1. Initialize Engine A on port 7379
	engA, err := NewEngine("", persistence.FsyncNo, "")
	if err != nil {
		t.Fatalf("failed to init engA: %v", err)
	}
	defer engA.Close()
	engA.Cluster = cluster.NewClusterManager("127.0.0.1", 7379, 17379, confA)

	// 2. Initialize Engine B on port 7381
	engB, err := NewEngine("", persistence.FsyncNo, "")
	if err != nil {
		t.Fatalf("failed to init engB: %v", err)
	}
	defer engB.Close()
	engB.Cluster = cluster.NewClusterManager("127.0.0.1", 7381, 17381, confB)

	// Assign slots 0..8191 to Node A, 8192..16383 to Node B
	var slotsA, slotsB []uint16
	for s := uint16(0); s < 8192; s++ {
		slotsA = append(slotsA, s)
	}
	for s := uint16(8192); s < 16384; s++ {
		slotsB = append(slotsB, s)
	}

	if err := engA.Cluster.AddSlots(slotsA...); err != nil {
		t.Fatalf("engA failed to add slots: %v", err)
	}
	if err := engB.Cluster.AddSlots(slotsB...); err != nil {
		t.Fatalf("engB failed to add slots: %v", err)
	}

	// Cross-register nodes into each other's cluster topology
	engA.Cluster.AddNode(engB.Cluster.Self)
	_ = engA.Cluster.AssignSlotsToNode(engB.Cluster.Self.ID, slotsB...)

	engB.Cluster.AddNode(engA.Cluster.Self)
	_ = engB.Cluster.AssignSlotsToNode(engA.Cluster.Self.ID, slotsA...)

	// Test CLUSTER KEYSLOT
	res := engA.ExecuteCommand("c1", []string{"CLUSTER", "KEYSLOT", "foo"})
	if res.Type != resp.IntegerPrefix || res.Num != 12182 {
		t.Fatalf("expected CLUSTER KEYSLOT foo to be 12182, got %+v", res)
	}

	// Test CLUSTER MYID
	res = engA.ExecuteCommand("c1", []string{"CLUSTER", "MYID"})
	if res.Type != resp.BulkStringPrefix || string(res.Bulk) != engA.Cluster.Self.ID {
		t.Fatalf("expected CLUSTER MYID to return %s, got %s", engA.Cluster.Self.ID, string(res.Bulk))
	}

	// Test CLUSTER INFO
	res = engA.ExecuteCommand("c1", []string{"CLUSTER", "INFO"})
	if res.Type != resp.BulkStringPrefix || !strings.Contains(string(res.Bulk), "cluster_state:ok") {
		t.Fatalf("expected CLUSTER INFO cluster_state:ok, got %s", string(res.Bulk))
	}

	// Test CLUSTER NODES
	res = engA.ExecuteCommand("c1", []string{"CLUSTER", "NODES"})
	if res.Type != resp.BulkStringPrefix || !strings.Contains(string(res.Bulk), "myself,master") {
		t.Fatalf("expected CLUSTER NODES to contain myself,master, got %s", string(res.Bulk))
	}

	// Test CLUSTER SLOTS
	res = engA.ExecuteCommand("c1", []string{"CLUSTER", "SLOTS"})
	if res.Type != resp.ArrayPrefix || len(res.Array) != 2 {
		t.Fatalf("expected CLUSTER SLOTS to return 2 range blocks, got %d", len(res.Array))
	}

	// Slot 12182 ('foo') belongs to Node B (range 8192..16383).
	// When client sends SET foo "bar" to Node A, Node A MUST respond with -MOVED 12182 127.0.0.1:7381
	res = engA.ExecuteCommand("c1", []string{"SET", "foo", "bar"})
	if res.Type != resp.ErrorPrefix || !strings.HasPrefix(res.Str, "MOVED 12182 127.0.0.1:7381") {
		t.Fatalf("expected -MOVED 12182 127.0.0.1:7381, got %+v", res)
	}

	// When sent directly to Node B, it succeeds!
	res = engB.ExecuteCommand("c1", []string{"SET", "foo", "bar"})
	if res.Type != resp.SimpleStringPrefix || res.Str != "OK" {
		t.Fatalf("expected SET foo to succeed on Node B, got %+v", res)
	}

	// Find a key belonging to Node A (slot < 8192)
	var keyA string
	for i := 0; i < 10000; i++ {
		k := fmt.Sprintf("key:%d", i)
		if cluster.KeySlot(k) < 8192 {
			keyA = k
			break
		}
	}
	if keyA == "" {
		t.Fatalf("failed to find key in slot < 8192")
	}

	// Node A should successfully execute SET keyA
	res = engA.ExecuteCommand("c1", []string{"SET", keyA, "valA"})
	if res.Type != resp.SimpleStringPrefix || res.Str != "OK" {
		t.Fatalf("expected SET keyA to succeed on Node A, got %+v", res)
	}

	// Node B should redirect GET keyA to Node A
	res = engB.ExecuteCommand("c1", []string{"GET", keyA})
	expectedRedirect := fmt.Sprintf("MOVED %d 127.0.0.1:7379", cluster.KeySlot(keyA))
	if res.Type != resp.ErrorPrefix || !strings.HasPrefix(res.Str, expectedRedirect) {
		t.Fatalf("expected -%s, got %+v", expectedRedirect, res)
	}

	// Test CROSSSLOT validation
	res = engA.ExecuteCommand("c1", []string{"MGET", keyA, "foo"})
	if res.Type != resp.ErrorPrefix || !strings.Contains(res.Str, "CROSSSLOT") {
		t.Fatalf("expected CROSSSLOT error for cross-slot keys, got %+v", res)
	}

	// Test Hash Tags co-location
	tagKey1 := "{orders:1}:item1"
	tagKey2 := "{orders:1}:item2"
	if cluster.KeySlot(tagKey1) != cluster.KeySlot(tagKey2) {
		t.Fatalf("hash tagged keys must have identical slot")
	}

	// Multi-key with matching hash tag should not trigger CROSSSLOT
	targetEng := engA
	if cluster.KeySlot(tagKey1) >= 8192 {
		targetEng = engB
	}
	_ = targetEng.ExecuteCommand("c1", []string{"SET", tagKey1, "val1"})
	_ = targetEng.ExecuteCommand("c1", []string{"SET", tagKey2, "val2"})
	res = targetEng.ExecuteCommand("c1", []string{"MGET", tagKey1, tagKey2})
	if res.Type != resp.ArrayPrefix || len(res.Array) != 2 {
		t.Fatalf("expected MGET on hash-tagged keys to succeed, got %+v", res)
	}

	// Test CLUSTER COUNTKEYSINSLOT & GETKEYSINSLOT
	slot := cluster.KeySlot(keyA)
	res = engA.ExecuteCommand("c1", []string{"CLUSTER", "COUNTKEYSINSLOT", fmt.Sprintf("%d", slot)})
	if res.Type != resp.IntegerPrefix || res.Num < 1 {
		t.Fatalf("expected COUNTKEYSINSLOT to be >= 1, got %+v", res)
	}

	res = engA.ExecuteCommand("c1", []string{"CLUSTER", "GETKEYSINSLOT", fmt.Sprintf("%d", slot), "10"})
	if res.Type != resp.ArrayPrefix || len(res.Array) < 1 {
		t.Fatalf("expected GETKEYSINSLOT to return keys, got %+v", res)
	}

	// Test CLUSTER SAVECONFIG
	res = engA.ExecuteCommand("c1", []string{"CLUSTER", "SAVECONFIG"})
	if res.Type != resp.SimpleStringPrefix || res.Str != "OK" {
		t.Fatalf("expected SAVECONFIG to return OK, got %+v", res)
	}
}
