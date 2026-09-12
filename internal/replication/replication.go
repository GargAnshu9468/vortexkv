package replication

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vortexkv/vortexkv/internal/resp"
)

type Role string

const (
	RoleMaster  Role = "master"
	RoleReplica Role = "slave"
)

type ReplicaClient struct {
	ID            string
	RemoteAddr    string
	ListeningPort int
	State         string // "syncing", "online"
	Writer        *resp.Writer
	Closer        io.Closer
	CmdChan       chan []byte
	Offset        int64
	ConnectedAt   time.Time
}

type ReplicationManager struct {
	mu             sync.RWMutex
	Role           Role
	MasterReplID   string
	MasterOffset   atomic.Int64
	MasterHost     string
	MasterPort     int
	MasterAuth     string
	ReadOnly       bool
	ListeningPort  int

	// Master side state
	replicas       map[string]*ReplicaClient
	replicaCounter atomic.Int64

	// Replica side state
	masterConn     net.Conn
	stopReplica    chan struct{}
	syncState      string // "disconnected", "connecting", "syncing", "online"
	lastMasterPing time.Time

	// Callbacks
	applyCmdFunc   func(args []string)
	dumpKeysFunc   func() [][]string
}

func NewReplicationManager(listeningPort int, applyCmd func(args []string), dumpKeys func() [][]string) *ReplicationManager {
	replID := generateReplID()
	return &ReplicationManager{
		Role:          RoleMaster,
		MasterReplID:  replID,
		ReadOnly:      false,
		ListeningPort: listeningPort,
		replicas:      make(map[string]*ReplicaClient),
		syncState:     "disconnected",
		applyCmdFunc:  applyCmd,
		dumpKeysFunc:  dumpKeys,
	}
}

func generateReplID() string {
	b := make([]byte, 20)
	if _, err := rand.Read(b); err != nil {
		return "0000000000000000000000000000000000000000"
	}
	return hex.EncodeToString(b)
}

// Broadcast sends a mutating command to all connected replicas
func (rm *ReplicationManager) Broadcast(args []string) {
	rm.mu.RLock()
	if rm.Role != RoleMaster || len(rm.replicas) == 0 {
		rm.mu.RUnlock()
		return
	}

	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	_ = w.WriteStringArray(args)
	_ = w.Flush()
	payload := buf.Bytes()
	payloadLen := int64(len(payload))
	rm.MasterOffset.Add(payloadLen)

	for _, rep := range rm.replicas {
		select {
		case rep.CmdChan <- payload:
		default:
			// If buffer is full, do not block master; client will catch up or disconnect
		}
	}
	rm.mu.RUnlock()
}

// HandlePSync executes full initial synchronization for an incoming replica connection
func (rm *ReplicationManager) HandlePSync(rwc io.ReadWriteCloser, remoteAddr string, listeningPort int) error {
	rm.mu.Lock()
	repID := fmt.Sprintf("replica_%d", rm.replicaCounter.Add(1))
	rep := &ReplicaClient{
		ID:            repID,
		RemoteAddr:    remoteAddr,
		ListeningPort: listeningPort,
		State:         "syncing",
		Writer:        resp.NewWriter(rwc),
		Closer:        rwc,
		CmdChan:       make(chan []byte, 1000),
		ConnectedAt:   time.Now(),
	}
	rm.replicas[repID] = rep
	currentOffset := rm.MasterOffset.Load()
	replID := rm.MasterReplID
	rm.mu.Unlock()

	defer func() {
		rm.mu.Lock()
		delete(rm.replicas, repID)
		rm.mu.Unlock()
		_ = rwc.Close()
		log.Printf("[VortexKV] 🔁 Replica %s (%s) disconnected from replication pool", repID, remoteAddr)
	}()

	// 1. Send FULLRESYNC header
	if err := rep.Writer.WriteSimpleString(fmt.Sprintf("FULLRESYNC %s %d", replID, currentOffset)); err != nil {
		return err
	}
	if err := rep.Writer.Flush(); err != nil {
		return err
	}

	// 2. Dump all existing keys as serialized commands to bring replica up to current state
	if rm.dumpKeysFunc != nil {
		dumpCmds := rm.dumpKeysFunc()
		for _, cmdArgs := range dumpCmds {
			if err := rep.Writer.WriteStringArray(cmdArgs); err != nil {
				return err
			}
		}
		if err := rep.Writer.Flush(); err != nil {
			return err
		}
	}

	// Mark state as online
	rm.mu.Lock()
	rep.State = "online"
	rm.mu.Unlock()
	log.Printf("[VortexKV] 🔁 Replica %s (%s, listening port %d) synced and ONLINE", repID, remoteAddr, listeningPort)

	// 3. Enter continuous streaming loop
	for {
		select {
		case payload, ok := <-rep.CmdChan:
			if !ok {
				return nil
			}
			if _, err := rwc.Write(payload); err != nil {
				return err
			}
		case <-time.After(10 * time.Second):
			// Send heartbeat PING periodically
			if err := rep.Writer.WriteStringArray([]string{"PING"}); err != nil {
				return err
			}
			if err := rep.Writer.Flush(); err != nil {
				return err
			}
		}
	}
}

// ConnectToMaster attaches this node to a remote master as a replica
func (rm *ReplicationManager) ConnectToMaster(host string, port int, auth string) {
	rm.mu.Lock()
	if rm.stopReplica != nil {
		close(rm.stopReplica)
	}
	rm.Role = RoleReplica
	rm.ReadOnly = true
	rm.MasterHost = host
	rm.MasterPort = port
	rm.MasterAuth = auth
	rm.syncState = "connecting"
	rm.stopReplica = make(chan struct{})
	stopCh := rm.stopReplica
	rm.mu.Unlock()

	go rm.replicaLoop(host, port, auth, stopCh)
}

func (rm *ReplicationManager) replicaLoop(host string, port int, auth string, stopCh chan struct{}) {
	backoff := 1 * time.Second

	for {
		select {
		case <-stopCh:
			return
		default:
		}

		addr := net.JoinHostPort(host, strconv.Itoa(port))
		log.Printf("[VortexKV] 🔁 Connecting to master at %s...", addr)

		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			log.Printf("[VortexKV] ⚠️ Failed to connect to master %s: %v. Retrying in %v...", addr, err, backoff)
			time.Sleep(backoff)
			if backoff < 10*time.Second {
				backoff *= 2
			}
			continue
		}

		backoff = 1 * time.Second
		rm.mu.Lock()
		rm.masterConn = conn
		rm.syncState = "handshake"
		rm.mu.Unlock()

		err = rm.performHandshakeAndSync(conn, auth, stopCh)
		_ = conn.Close()

		rm.mu.Lock()
		rm.syncState = "disconnected"
		rm.mu.Unlock()

		if err != nil {
			log.Printf("[VortexKV] ⚠️ Replication link disconnected: %v. Reconnecting...", err)
		}

		select {
		case <-stopCh:
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (rm *ReplicationManager) performHandshakeAndSync(conn net.Conn, auth string, stopCh chan struct{}) error {
	reader := resp.NewReader(conn)
	writer := resp.NewWriter(conn)

	// 1. Send AUTH if required
	if auth != "" {
		_ = writer.WriteStringArray([]string{"AUTH", auth})
		if err := writer.Flush(); err != nil {
			return err
		}
		val, err := reader.ReadValue()
		if err != nil {
			return err
		}
		if val.Type == resp.ErrorPrefix {
			return fmt.Errorf("master auth failed: %s", val.Str)
		}
	}

	// 2. Send PING
	_ = writer.WriteStringArray([]string{"PING"})
	if err := writer.Flush(); err != nil {
		return err
	}
	val, err := reader.ReadValue()
	if err != nil || (val.Str != "PONG" && val.Type != resp.SimpleStringPrefix) {
		return fmt.Errorf("master PING failed: %v", err)
	}

	// 3. Send REPLCONF listening-port
	portStr := strconv.Itoa(rm.ListeningPort)
	if rm.ListeningPort <= 0 {
		portStr = "7379"
	}
	_ = writer.WriteStringArray([]string{"REPLCONF", "listening-port", portStr})
	if err := writer.Flush(); err != nil {
		return err
	}
	val, err = reader.ReadValue()
	if err != nil || val.Str != "OK" {
		return fmt.Errorf("master REPLCONF listening-port failed: %v", err)
	}

	// 4. Send REPLCONF capa psync2
	_ = writer.WriteStringArray([]string{"REPLCONF", "capa", "psync2"})
	if err := writer.Flush(); err != nil {
		return err
	}
	val, err = reader.ReadValue()
	if err != nil || val.Str != "OK" {
		return fmt.Errorf("master REPLCONF capa failed: %v", err)
	}

	// 5. Send PSYNC ? -1
	_ = writer.WriteStringArray([]string{"PSYNC", "?", "-1"})
	if err := writer.Flush(); err != nil {
		return err
	}
	val, err = reader.ReadValue()
	if err != nil {
		return fmt.Errorf("master PSYNC failed: %v", err)
	}

	// Expect +FULLRESYNC <replid> <offset>
	if !strings.HasPrefix(val.Str, "FULLRESYNC") {
		return fmt.Errorf("unexpected PSYNC response: %s", val.Str)
	}

	parts := strings.Fields(val.Str)
	if len(parts) >= 3 {
		rm.mu.Lock()
		rm.MasterReplID = parts[1]
		if offset, err := strconv.ParseInt(parts[2], 10, 64); err == nil {
			rm.MasterOffset.Store(offset)
		}
		rm.syncState = "online"
		rm.lastMasterPing = time.Now()
		rm.mu.Unlock()
	}

	log.Printf("[VortexKV] 🚀 Full synchronization established with master %s:%d! Now streaming live updates.", rm.MasterHost, rm.MasterPort)

	// 6. Streaming loop: read continuous stream of commands from master
	for {
		select {
		case <-stopCh:
			return nil
		default:
		}

		cmdArgs, err := reader.ReadCommand()
		if err != nil {
			return err
		}

		if len(cmdArgs) > 0 {
			if strings.ToUpper(cmdArgs[0]) == "PING" {
				rm.mu.Lock()
				rm.lastMasterPing = time.Now()
				rm.mu.Unlock()
				continue
			}
			if rm.applyCmdFunc != nil {
				rm.applyCmdFunc(cmdArgs)
			}
		}
	}
}

// PromoteToMaster promotes a replica to an independent writable master node
func (rm *ReplicationManager) PromoteToMaster() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.Role == RoleMaster {
		return
	}

	if rm.stopReplica != nil {
		close(rm.stopReplica)
		rm.stopReplica = nil
	}
	if rm.masterConn != nil {
		_ = rm.masterConn.Close()
		rm.masterConn = nil
	}

	rm.Role = RoleMaster
	rm.ReadOnly = false
	rm.MasterHost = ""
	rm.MasterPort = 0
	rm.MasterAuth = ""
	rm.syncState = "disconnected"
	rm.MasterReplID = generateReplID()
	log.Println("[VortexKV] 👑 Promoted node to independent MASTER. Read-only guard removed.")
}

// GetConnectedReplicasCount returns the number of active replicas
func (rm *ReplicationManager) GetConnectedReplicasCount() int {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return len(rm.replicas)
}

// GenerateReplicationInfo returns standard Redis-compatible replication INFO string
func (rm *ReplicationManager) GenerateReplicationInfo() string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	var b strings.Builder
	b.WriteString("# Replication\r\n")
	fmt.Fprintf(&b, "role:%s\r\n", string(rm.Role))

	if rm.Role == RoleMaster {
		fmt.Fprintf(&b, "connected_slaves:%d\r\n", len(rm.replicas))
		idx := 0
		for _, r := range rm.replicas {
			fmt.Fprintf(&b, "slave%d:ip=%s,port=%d,state=%s,offset=%d,lag=0\r\n",
				idx, r.RemoteAddr, r.ListeningPort, r.State, r.Offset)
			idx++
		}
		fmt.Fprintf(&b, "master_replid:%s\r\n", rm.MasterReplID)
		fmt.Fprintf(&b, "master_repl_offset:%d\r\n", rm.MasterOffset.Load())
		b.WriteString("second_repl_offset:-1\r\n")
		b.WriteString("repl_backlog_active:1\r\n")
		b.WriteString("repl_backlog_size:1048576\r\n")
		fmt.Fprintf(&b, "repl_backlog_histlen:%d\r\n", rm.MasterOffset.Load())
	} else {
		fmt.Fprintf(&b, "master_host:%s\r\n", rm.MasterHost)
		fmt.Fprintf(&b, "master_port:%d\r\n", rm.MasterPort)
		fmt.Fprintf(&b, "master_link_status:%s\r\n", map[bool]string{true: "up", false: "down"}[rm.syncState == "online"])
		fmt.Fprintf(&b, "master_last_io_seconds_ago:%d\r\n", int(time.Since(rm.lastMasterPing).Seconds()))
		b.WriteString("master_sync_in_progress:0\r\n")
		fmt.Fprintf(&b, "slave_read_only:%d\r\n", map[bool]int{true: 1, false: 0}[rm.ReadOnly])
		fmt.Fprintf(&b, "slave_repl_offset:%d\r\n", rm.MasterOffset.Load())
	}

	return b.String()
}
