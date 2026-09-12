package cluster

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vortexkv/vortexkv/internal/resp"
)

// ClusterManager coordinates distributed node membership, slot routing,
// configuration persistence, and client redirection.
type ClusterManager struct {
	mu            sync.RWMutex
	Enabled       bool
	Self          *ClusterNode
	Nodes         map[string]*ClusterNode // Keyed by 40-char Node ID
	SlotMap       [16384]*ClusterNode     // Direct slot -> owning master node
	ConfigFile    string
	CurrentEpoch  uint64
	LastVoteEpoch uint64
	AuthPass      string

	// Cluster Bus & Gossip fields
	BusListener net.Listener
	BusRunning  bool
	busStop     chan struct{}
	NodeTimeout time.Duration
	OnPromote   func()
	failReports map[string]map[string]time.Time // targetNodeID -> reporterNodeID -> timestamp
}

// NewClusterManager instantiates a cluster manager for the local node.
func NewClusterManager(ip string, port, busPort int, configFile string) *ClusterManager {
	if ip == "" {
		ip = "127.0.0.1"
	}
	if port <= 0 {
		port = 7379
	}
	if busPort <= 0 {
		busPort = port + 10000
	}
	if configFile == "" {
		configFile = "nodes.conf"
	}

	self := NewNode("", ip, port, busPort, "master")

	cm := &ClusterManager{
		Enabled:      true,
		Self:         self,
		Nodes:        make(map[string]*ClusterNode),
		ConfigFile:   configFile,
		CurrentEpoch: 1,
		NodeTimeout:  DefaultNodeTimeout,
		failReports:  make(map[string]map[string]time.Time),
	}
	cm.Nodes[self.ID] = self

	// Attempt to load existing nodes.conf if present
	_ = cm.LoadConfig()

	return cm
}

// KeySlot returns the 14-bit hash slot for a key (0..16383).
func (cm *ClusterManager) KeySlot(key string) uint16 {
	return KeySlot(key)
}

// GetSlotOwner returns the node owning the given slot, or nil if unassigned.
func (cm *ClusterManager) GetSlotOwner(slot uint16) *ClusterNode {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if slot >= 16384 {
		return nil
	}
	return cm.SlotMap[slot]
}

// AddSlots assigns one or more hash slots to the local node.
func (cm *ClusterManager) AddSlots(slots ...uint16) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Validation phase: ensure no slots are already assigned
	for _, s := range slots {
		if s >= 16384 {
			return fmt.Errorf("ERR slot out of range: %d", s)
		}
		if owner := cm.SlotMap[s]; owner != nil && owner.ID != cm.Self.ID {
			return fmt.Errorf("ERR slot %d is already busy by node %s", s, owner.ID)
		}
	}

	// Assignment phase
	for _, s := range slots {
		cm.Self.Slots[s] = true
		cm.SlotMap[s] = cm.Self
	}

	_ = cm.saveConfigLocked()
	return nil
}

// AssignSlotsToNode assigns slots to an arbitrary known node.
func (cm *ClusterManager) AssignSlotsToNode(nodeID string, slots ...uint16) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	target, exists := cm.Nodes[nodeID]
	if !exists {
		return fmt.Errorf("ERR unknown node %s", nodeID)
	}

	for _, s := range slots {
		if s >= 16384 {
			return fmt.Errorf("ERR slot out of range: %d", s)
		}
		if owner := cm.SlotMap[s]; owner != nil && owner.ID != target.ID {
			owner.Slots[s] = false
		}
		target.Slots[s] = true
		cm.SlotMap[s] = target
	}

	_ = cm.saveConfigLocked()
	return nil
}

// DelSlots removes one or more hash slots from the local node.
func (cm *ClusterManager) DelSlots(slots ...uint16) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	for _, s := range slots {
		if s >= 16384 {
			return fmt.Errorf("ERR slot out of range: %d", s)
		}
		if owner := cm.SlotMap[s]; owner == nil || owner.ID != cm.Self.ID {
			return fmt.Errorf("ERR slot %d is not owned by this node", s)
		}
	}

	for _, s := range slots {
		cm.Self.Slots[s] = false
		cm.SlotMap[s] = nil
	}

	_ = cm.saveConfigLocked()
	return nil
}

// AddNode adds or updates a known cluster node.
func (cm *ClusterManager) AddNode(node *ClusterNode) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.Nodes[node.ID] = node
	if node.Role == "master" {
		for i, s := range node.Slots {
			if s {
				cm.SlotMap[i] = node
			}
		}
	}
	_ = cm.saveConfigLocked()
}

// RemoveNode removes a node by ID from the cluster.
func (cm *ClusterManager) RemoveNode(nodeID string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if nodeID == cm.Self.ID {
		return fmt.Errorf("ERR cannot remove self node from cluster")
	}

	target, exists := cm.Nodes[nodeID]
	if !exists {
		return fmt.Errorf("ERR unknown node %s", nodeID)
	}

	for i, s := range target.Slots {
		if s && cm.SlotMap[i] == target {
			cm.SlotMap[i] = nil
		}
	}
	delete(cm.Nodes, nodeID)

	_ = cm.saveConfigLocked()
	return nil
}

// Replicate configures the local node as a replica of another master node.
func (cm *ClusterManager) Replicate(masterID string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if masterID == cm.Self.ID {
		return fmt.Errorf("ERR cannot replicate self")
	}

	master, exists := cm.Nodes[masterID]
	if !exists {
		return fmt.Errorf("ERR unknown node %s", masterID)
	}
	if master.Role != "master" {
		return fmt.Errorf("ERR target node is not a master")
	}

	// Release any slots previously held by this node
	for i, s := range cm.Self.Slots {
		if s {
			cm.Self.Slots[i] = false
			if cm.SlotMap[i] == cm.Self {
				cm.SlotMap[i] = nil
			}
		}
	}

	cm.Self.Role = "slave"
	cm.Self.MasterID = masterID

	_ = cm.saveConfigLocked()
	return nil
}

// Meet contacts a peer node, exchanges node info, and registers it in the cluster.
func (cm *ClusterManager) Meet(peerIP string, peerPort int, peerBusPort int) (*ClusterNode, error) {
	if peerIP == "" {
		peerIP = "127.0.0.1"
	}
	if peerBusPort <= 0 {
		peerBusPort = peerPort + 10000
	}

	// Attempt active TCP handshake over peer's wire port
	peerID := ""
	var peerNodesText string
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", peerIP, peerPort), 1500*time.Millisecond)
	if err == nil {
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		reader := bufio.NewReader(conn)

		// 1. Authenticate if password configured
		if cm.AuthPass != "" {
			_, _ = fmt.Fprintf(conn, "*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(cm.AuthPass), cm.AuthPass)
			_, _ = reader.ReadString('\n')
		}

		// 2. Request CLUSTER NODES from peer to learn its ID, role, and slot map
		_, _ = fmt.Fprintf(conn, "*2\r\n$7\r\nCLUSTER\r\n$5\r\nNODES\r\n")
		line, readErr := reader.ReadString('\n')
		if readErr == nil {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(line, "$") {
				bodyLen, _ := strconv.Atoi(strings.TrimPrefix(line, "$"))
				if bodyLen > 0 {
					buf := make([]byte, bodyLen)
					_, _ = io.ReadFull(reader, buf)
					peerNodesText = string(buf)
					_, _ = reader.ReadString('\n') // CRLF
				}
			}
		}

		// 3. Reciprocally instruct peer to MEET back
		_, _ = fmt.Fprintf(conn, "*4\r\n$7\r\nCLUSTER\r\n$4\r\nMEET\r\n$%d\r\n%s\r\n$%d\r\n%d\r\n",
			len(cm.Self.IP), cm.Self.IP, len(strconv.Itoa(cm.Self.Port)), cm.Self.Port)
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()

	if peerNodesText != "" {
		cm.parseAndMergeNodesTextLocked(peerNodesText)
	}

	// Check if peer is now known
	for _, n := range cm.Nodes {
		if n.IP == peerIP && n.Port == peerPort {
			_ = cm.saveConfigLocked()
			return n, nil
		}
	}

	if peerID == "" {
		peerID = GenerateNodeID()
	}

	peerNode := NewNode(peerID, peerIP, peerPort, peerBusPort, "master")
	cm.Nodes[peerID] = peerNode

	_ = cm.saveConfigLocked()
	return peerNode, nil
}

// FormatNodes generates the standard Redis CLUSTER NODES table output.
func (cm *ClusterManager) FormatNodes() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	var lines []string

	// Place self first
	lines = append(lines, cm.Self.FormatNodeLine(true))

	// Sort peers by Node ID for deterministic output
	var peerIDs []string
	for id := range cm.Nodes {
		if id != cm.Self.ID {
			peerIDs = append(peerIDs, id)
		}
	}
	sort.Strings(peerIDs)

	for _, id := range peerIDs {
		lines = append(lines, cm.Nodes[id].FormatNodeLine(false))
	}

	return strings.Join(lines, "\n") + "\n"
}

// FormatSlots creates the RESP array representation for CLUSTER SLOTS.
// Format: [[start_slot, end_slot, [ip, port, id], [replica_ip, replica_port, id]...], ...]
func (cm *ClusterManager) FormatSlots() resp.Value {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	type slotRangeBlock struct {
		start  uint16
		end    uint16
		master *ClusterNode
	}

	var blocks []slotRangeBlock
	var currentBlock *slotRangeBlock

	for i := 0; i < 16384; i++ {
		owner := cm.SlotMap[i]
		if owner == nil {
			if currentBlock != nil {
				blocks = append(blocks, *currentBlock)
				currentBlock = nil
			}
			continue
		}

		if currentBlock == nil {
			currentBlock = &slotRangeBlock{
				start:  uint16(i),
				end:    uint16(i),
				master: owner,
			}
		} else if currentBlock.master.ID == owner.ID && currentBlock.end == uint16(i-1) {
			currentBlock.end = uint16(i)
		} else {
			blocks = append(blocks, *currentBlock)
			currentBlock = &slotRangeBlock{
				start:  uint16(i),
				end:    uint16(i),
				master: owner,
			}
		}
	}
	if currentBlock != nil {
		blocks = append(blocks, *currentBlock)
	}

	// Find replicas for each master
	masterReplicas := make(map[string][]*ClusterNode)
	for _, n := range cm.Nodes {
		if n.Role == "slave" && n.MasterID != "" && n.MasterID != "-" {
			masterReplicas[n.MasterID] = append(masterReplicas[n.MasterID], n)
		}
	}

	var results []resp.Value
	for _, b := range blocks {
		masterInfo := resp.Array([]resp.Value{
			resp.BulkString(b.master.IP),
			resp.Integer(int64(b.master.Port)),
			resp.BulkString(b.master.ID),
		})

		entry := []resp.Value{
			resp.Integer(int64(b.start)),
			resp.Integer(int64(b.end)),
			masterInfo,
		}

		// Append any replicas
		for _, rep := range masterReplicas[b.master.ID] {
			repInfo := resp.Array([]resp.Value{
				resp.BulkString(rep.IP),
				resp.Integer(int64(rep.Port)),
				resp.BulkString(rep.ID),
			})
			entry = append(entry, repInfo)
		}

		results = append(results, resp.Array(entry))
	}

	return resp.Array(results)
}

// FormatInfo generates the multi-line CLUSTER INFO output.
func (cm *ClusterManager) FormatInfo() string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	assignedSlots := 0
	for _, o := range cm.SlotMap {
		if o != nil {
			assignedSlots++
		}
	}

	masters := 0
	for _, n := range cm.Nodes {
		if n.Role == "master" {
			masters++
		}
	}

	state := "fail"
	if assignedSlots == 16384 {
		state = "ok"
	} else if assignedSlots > 0 {
		state = "ok" // Partial or test cluster
	}

	info := fmt.Sprintf("cluster_state:%s\r\n"+
		"cluster_slots_assigned:%d\r\n"+
		"cluster_slots_ok:%d\r\n"+
		"cluster_slots_pfail:0\r\n"+
		"cluster_slots_fail:0\r\n"+
		"cluster_known_nodes:%d\r\n"+
		"cluster_size:%d\r\n"+
		"cluster_current_epoch:%d\r\n"+
		"cluster_my_epoch:%d\r\n"+
		"cluster_stats_messages_ping_sent:%d\r\n"+
		"cluster_stats_messages_pong_received:%d\r\n"+
		"cluster_stats_messages_sent:0\r\n"+
		"cluster_stats_messages_received:0\r\n",
		state,
		assignedSlots,
		assignedSlots,
		len(cm.Nodes),
		masters,
		cm.CurrentEpoch,
		cm.Self.Epoch,
		cm.Self.PingSent,
		cm.Self.PongRecv,
	)

	return info
}

// SaveConfig persists the current cluster topology and epoch to ConfigFile atomically.
func (cm *ClusterManager) SaveConfig() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.saveConfigLocked()
}

func (cm *ClusterManager) saveConfigLocked() error {
	if cm.ConfigFile == "" {
		return nil
	}

	var sb strings.Builder
	sb.WriteString(cm.Self.FormatNodeLine(true))
	sb.WriteString("\n")

	var peerIDs []string
	for id := range cm.Nodes {
		if id != cm.Self.ID {
			peerIDs = append(peerIDs, id)
		}
	}
	sort.Strings(peerIDs)

	for _, id := range peerIDs {
		sb.WriteString(cm.Nodes[id].FormatNodeLine(false))
		sb.WriteString("\n")
	}

	sb.WriteString(fmt.Sprintf("vars currentEpoch %d lastVoteEpoch 0\n", cm.CurrentEpoch))

	tmpPath := cm.ConfigFile + ".tmp"
	err := os.WriteFile(tmpPath, []byte(sb.String()), 0644)
	if err != nil {
		return err
	}
	return os.Rename(tmpPath, cm.ConfigFile)
}

// LoadConfig parses an existing nodes.conf file to restore cluster topology.
func (cm *ClusterManager) LoadConfig() error {
	if cm.ConfigFile == "" {
		return nil
	}

	data, err := os.ReadFile(cm.ConfigFile)
	if err != nil {
		return err // file does not exist yet
	}

	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.parseAndMergeNodesTextLocked(string(data))
	return nil
}

func (cm *ClusterManager) parseAndMergeNodesTextLocked(text string) {
	lines := strings.Split(text, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "vars ") {
			// e.g. vars currentEpoch 1 lastVoteEpoch 0
			parts := strings.Fields(line)
			for i := 1; i < len(parts)-1; i += 2 {
				if parts[i] == "currentEpoch" {
					if ep, err := strconv.ParseUint(parts[i+1], 10, 64); err == nil {
						cm.CurrentEpoch = ep
					}
				}
			}
			continue
		}

		parts := strings.Fields(line)
		if len(parts) < 8 {
			continue
		}

		nodeID := parts[0]
		addr := parts[1]
		flags := parts[2]
		masterID := parts[3]
		epoch, _ := strconv.ParseUint(parts[6], 10, 64)
		linkState := parts[7]

		// Parse ip:port@cport
		ip := "127.0.0.1"
		port := 7379
		busPort := 17379
		if atIdx := strings.Index(addr, "@"); atIdx != -1 {
			bpStr := addr[atIdx+1:]
			addr = addr[:atIdx]
			if bp, err := strconv.Atoi(bpStr); err == nil {
				busPort = bp
			}
		}
		if colonIdx := strings.LastIndex(addr, ":"); colonIdx != -1 {
			ip = addr[:colonIdx]
			if p, err := strconv.Atoi(addr[colonIdx+1:]); err == nil {
				port = p
			}
		}

		isMyself := strings.Contains(flags, "myself")
		role := "master"
		if strings.Contains(flags, "slave") {
			role = "slave"
		}

		var node *ClusterNode
		if isMyself {
			if cm.Self.ID == "" || cm.Self.ID == nodeID {
				node = cm.Self
				node.ID = nodeID
				node.IP = ip
				node.Port = port
				node.BusPort = busPort
				node.Role = role
				node.MasterID = masterID
				node.Epoch = epoch
				node.LinkState = linkState
			} else {
				node = cm.Nodes[nodeID]
				if node == nil {
					node = NewNode(nodeID, ip, port, busPort, role)
					cm.Nodes[nodeID] = node
				}
				node.MasterID = masterID
				node.Epoch = epoch
				node.LinkState = linkState
			}
		} else {
			if nodeID == cm.Self.ID {
				node = cm.Self
			} else {
				node = cm.Nodes[nodeID]
				if node == nil {
					node = NewNode(nodeID, ip, port, busPort, role)
					cm.Nodes[nodeID] = node
				}
				node.MasterID = masterID
				node.Epoch = epoch
				node.LinkState = linkState
			}
		}

		// Parse slot ranges if present
		if len(parts) >= 9 && role == "master" && node != nil {
			slotSpec := strings.Join(parts[8:], " ")
			slots, err := ParseSlotRanges(slotSpec)
			if err == nil {
				for _, s := range slots {
					node.Slots[s] = true
					cm.SlotMap[s] = node
				}
			}
		}
	}
}
