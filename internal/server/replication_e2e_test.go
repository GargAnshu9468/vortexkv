package server

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/GargAnshu9468/vortexkv/internal/engine"
	"github.com/GargAnshu9468/vortexkv/internal/resp"
)

func TestMasterReplicaE2E(t *testing.T) {
	// 1. Setup Master
	masterEng, err := engine.NewEngine("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer masterEng.Close()

	lMaster, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	masterAddr := lMaster.Addr().String()
	_ = lMaster.Close()

	masterSrv := NewTCPServer(masterAddr, masterEng)
	if err := masterSrv.Start(); err != nil {
		t.Fatal(err)
	}
	defer masterSrv.Stop()

	// Populate Master with initial data
	masterEng.ExecuteCommand("", []string{"SET", "alpha", "100"})
	masterEng.ExecuteCommand("", []string{"HSET", "user:1", "name", "Alice", "role", "admin"})
	masterEng.ExecuteCommand("", []string{"RPUSH", "tasks", "task1", "task2"})
	masterEng.ExecuteCommand("", []string{"SADD", "tags", "go", "redis", "vortex"})

	// 2. Setup Replica
	replicaEng, err := engine.NewEngine("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer replicaEng.Close()

	lReplica, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	replicaAddr := lReplica.Addr().String()
	_ = lReplica.Close()

	replicaSrv := NewTCPServer(replicaAddr, replicaEng)
	if err := replicaSrv.Start(); err != nil {
		t.Fatal(err)
	}
	defer replicaSrv.Stop()

	// Parse master host/port
	host, portStr, _ := net.SplitHostPort(masterAddr)

	replicaPortStr := replicaAddr[strings.LastIndex(replicaAddr, ":")+1:]
	replicaPort, _ := strconv.Atoi(replicaPortStr)
	replicaEng.Replication.ListeningPort = replicaPort
	replicaEng.Replication.ReadOnly = true

	// Command replica to connect to master via REPLICAOF
	repRes := replicaEng.ExecuteCommand("", []string{"REPLICAOF", host, portStr})
	if repRes.Type != resp.SimpleStringPrefix || repRes.Str != "OK" {
		t.Fatalf("Failed to execute REPLICAOF on replica: %v", repRes)
	}

	// Wait up to 3s for sync to complete
	deadline := time.Now().Add(3 * time.Second)
	synced := false
	for time.Now().Before(deadline) {
		res := replicaEng.ExecuteCommand("", []string{"GET", "alpha"})
		if res.Type == resp.BulkStringPrefix && string(res.Bulk) == "100" {
			synced = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !synced {
		t.Fatalf("Replica did not synchronize initial key 'alpha' within timeout")
	}

	// Verify other data structures synced
	hres := replicaEng.ExecuteCommand("", []string{"HGET", "user:1", "name"})
	if string(hres.Bulk) != "Alice" {
		t.Fatalf("Expected HGET user:1 name == Alice, got %s", string(hres.Bulk))
	}

	lres := replicaEng.ExecuteCommand("", []string{"LLEN", "tasks"})
	if lres.Num != 2 {
		t.Fatalf("Expected LLEN tasks == 2, got %d", lres.Num)
	}

	// 3. Verify Read-Only Guard on replica
	writeRes := replicaEng.ExecuteCommand("", []string{"SET", "illegal", "write"})
	if writeRes.Type != resp.ErrorPrefix || !strings.Contains(writeRes.Str, "READONLY") {
		t.Fatalf("Expected READONLY error on replica write, got: %v", writeRes)
	}

	// 4. Test Live Replication Stream: Master writes a new key
	masterEng.ExecuteCommand("", []string{"SET", "live_stream_key", "streaming_value"})

	streamSynced := false
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		res := replicaEng.ExecuteCommand("", []string{"GET", "live_stream_key"})
		if res.Type == resp.BulkStringPrefix && string(res.Bulk) == "streaming_value" {
			streamSynced = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !streamSynced {
		t.Fatalf("Replica did not receive live streamed write within timeout")
	}

	// 5. Test Dynamic Failover / Promotion: REPLICAOF NO ONE
	promoteRes := replicaEng.ExecuteCommand("", []string{"REPLICAOF", "NO", "ONE"})
	if promoteRes.Type != resp.SimpleStringPrefix || promoteRes.Str != "OK" {
		t.Fatalf("Expected OK on REPLICAOF NO ONE, got: %v", promoteRes)
	}

	// Verify promoted replica can now accept writes!
	writableRes := replicaEng.ExecuteCommand("", []string{"SET", "promoted_key", "now_writable"})
	if writableRes.Type != resp.SimpleStringPrefix || writableRes.Str != "OK" {
		t.Fatalf("Expected OK on write after promotion, got: %v", writableRes)
	}

	checkPromoted := replicaEng.ExecuteCommand("", []string{"GET", "promoted_key"})
	if string(checkPromoted.Bulk) != "now_writable" {
		t.Fatalf("Expected promoted_key == now_writable, got: %s", string(checkPromoted.Bulk))
	}

	// Verify ROLE command reports master
	roleRes := replicaEng.ExecuteCommand("", []string{"ROLE"})
	if roleRes.Type != resp.ArrayPrefix || len(roleRes.Array) < 1 || string(roleRes.Array[0].Bulk) != "master" {
		t.Fatalf("Expected ROLE master after promotion, got: %v", roleRes)
	}
	fmt.Println("MasterReplicaE2E test passed successfully!")
}
