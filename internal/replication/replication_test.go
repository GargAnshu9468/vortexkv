package replication

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/GargAnshu9468/vortexkv/internal/resp"
)

func TestReplicationLifecycleAndSync(t *testing.T) {
	// 1. Setup simulated master storage
	masterStore := make(map[string]string)
	masterStore["init:1"] = "alpha"

	dumpFunc := func() [][]string {
		return [][]string{
			{"SET", "init:1", masterStore["init:1"]},
		}
	}

	// 2. Setup Master replication manager
	masterMgr := NewReplicationManager(
		7379,
		nil,
		dumpFunc,
	)

	// Spin up master mock TCP listener for replication
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	masterAddr := listener.Addr().String()
	parts := strings.Split(masterAddr, ":")
	masterHost := parts[0]
	var masterPort int
	fmt.Sscanf(parts[1], "%d", &masterPort)

	// Master listener goroutine
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				reader := resp.NewReader(c)
				writer := resp.NewWriter(c)
				for {
					args, err := reader.ReadCommand()
					if err != nil {
						return
					}
					cmd := strings.ToUpper(args[0])
					switch cmd {
					case "PING":
						_ = writer.WritePong()
						_ = writer.Flush()
					case "REPLCONF":
						_ = writer.WriteOK()
						_ = writer.Flush()
					case "PSYNC":
						_ = masterMgr.HandlePSync(c, c.RemoteAddr().String(), 7381)
						return
					}
				}
			}(conn)
		}
	}()

	// 3. Setup Replica replication manager and local storage
	var storeMu sync.RWMutex
	replicaStore := make(map[string]string)
	applyOnReplica := func(args []string) {
		cmd := strings.ToUpper(args[0])
		if cmd == "SET" && len(args) >= 3 {
			storeMu.Lock()
			replicaStore[args[1]] = args[2]
			storeMu.Unlock()
		}
	}

	getReplicaVal := func(k string) string {
		storeMu.RLock()
		defer storeMu.RUnlock()
		return replicaStore[k]
	}

	replicaMgr := NewReplicationManager(7381, applyOnReplica, nil)
	replicaMgr.ConnectToMaster(masterHost, masterPort, "")

	// 4. Wait for full sync to establish
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if replicaMgr.GetSyncState() == "online" && getReplicaVal("init:1") == "alpha" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if getReplicaVal("init:1") != "alpha" {
		t.Fatalf("Expected replica to receive initial dumped key 'init:1'='alpha', got: %v", replicaStore)
	}

	// 5. Test Live Command Streaming from Master
	masterMgr.Broadcast([]string{"SET", "live:key", "stream_success"})

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if getReplicaVal("live:key") == "stream_success" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if getReplicaVal("live:key") != "stream_success" {
		t.Fatalf("Expected replica to receive live streamed key 'live:key', got: %v", replicaStore)
	}

	// 6. Test Read-Only Guard
	if !replicaMgr.ReadOnly {
		t.Fatalf("Expected replica to be in read-only mode")
	}

	// 7. Test Promotion to Master
	replicaMgr.PromoteToMaster()
	if replicaMgr.Role != RoleMaster {
		t.Fatalf("Expected role to be master after promotion, got: %s", replicaMgr.Role)
	}
	if replicaMgr.ReadOnly {
		t.Fatalf("Expected replica read-only flag to be false after promotion")
	}

	// 8. Test INFO Replication Formatting
	masterInfo := masterMgr.GenerateReplicationInfo()
	if !strings.Contains(masterInfo, "role:master") || !strings.Contains(masterInfo, "connected_slaves:1") {
		t.Fatalf("Unexpected master replication info:\n%s", masterInfo)
	}

	// 9. Test GetStatus()
	status := masterMgr.GetStatus()
	if status.Role != "master" || status.ConnectedSlaves != 1 || len(status.Slaves) != 1 {
		t.Fatalf("Unexpected master GetStatus(): %+v", status)
	}
	if status.Slaves[0].ListeningPort != 7381 {
		t.Fatalf("Expected slave listening port 7381, got %d", status.Slaves[0].ListeningPort)
	}
}
