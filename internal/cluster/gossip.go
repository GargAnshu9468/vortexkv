package cluster

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

// Default cluster timeouts and heartbeat intervals
const (
	DefaultNodeTimeout   = 2000 * time.Millisecond // Time without PONG to trigger PFAIL
	DefaultHeartbeatTick = 250 * time.Millisecond  // Frequency of gossip heartbeat
	DefaultFailoverDelay = 500 * time.Millisecond  // Stagger election start
)

// StartBus initiates the inter-node gossip TCP listener and background heartbeat loop.
func (cm *ClusterManager) StartBus() error {
	cm.mu.Lock()
	if cm.BusRunning {
		cm.mu.Unlock()
		return nil
	}

	addr := fmt.Sprintf("0.0.0.0:%d", cm.Self.BusPort)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		cm.mu.Unlock()
		return fmt.Errorf("failed to bind cluster bus on %s: %w", addr, err)
	}

	cm.BusListener = listener
	cm.BusRunning = true
	cm.busStop = make(chan struct{})
	if cm.NodeTimeout <= 0 {
		cm.NodeTimeout = DefaultNodeTimeout
	}
	if cm.failReports == nil {
		cm.failReports = make(map[string]map[string]time.Time)
	}
	cm.mu.Unlock()

	log.Printf("[VortexKV Cluster Bus] Listening for gossip heartbeats on %s", addr)

	// Accept loop for incoming gossip connections
	go cm.acceptBusConnections(listener)

	// Background ticker for outgoing heartbeats and failure audits
	go cm.gossipLoop()

	return nil
}

// StopBus halts the gossip bus listener and background worker routines.
func (cm *ClusterManager) StopBus() {
	cm.mu.Lock()
	if !cm.BusRunning {
		cm.mu.Unlock()
		return
	}
	cm.BusRunning = false
	if cm.busStop != nil {
		close(cm.busStop)
	}
	if cm.BusListener != nil {
		_ = cm.BusListener.Close()
	}
	cm.mu.Unlock()
}

func (cm *ClusterManager) acceptBusConnections(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-cm.busStop:
				return
			default:
				return
			}
		}
		go cm.handleBusConn(conn)
	}
}

func (cm *ClusterManager) handleBusConn(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))

	packet, err := ReadPacket(conn)
	if err != nil {
		return
	}

	respPacket := cm.ProcessPacket(packet)
	if respPacket != nil {
		_ = WritePacket(conn, respPacket)
	}
}

// ProcessPacket handles an incoming bus packet and returns an optional response packet.
func (cm *ClusterManager) ProcessPacket(p *BusPacket) *BusPacket {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	now := time.Now().UnixMilli()

	// 1. Update or register the sender node
	sender, exists := cm.Nodes[p.SenderID]
	if !exists {
		sender = NewNode(p.SenderID, p.SenderIP, int(p.Port), int(p.BusPort), "master")
		if (p.Flags & FlagSlave) != 0 {
			sender.Role = "slave"
		}
		cm.Nodes[p.SenderID] = sender
	}

	sender.PongRecv = now
	sender.PFail = false
	sender.Fail = false
	sender.LinkState = "connected"

	// Adopt sender epoch if strictly newer
	if p.Epoch > cm.CurrentEpoch {
		cm.CurrentEpoch = p.Epoch
	}

	// Synchronize slot map if sender is master with slots
	if (p.Flags & FlagMaster) != 0 {
		senderSlots := BitmapToSlots(p.Slots)
		for i := 0; i < 16384; i++ {
			if senderSlots[i] {
				currentOwner := cm.SlotMap[i]
				if currentOwner == nil || currentOwner.ID != sender.ID {
					if currentOwner != nil && currentOwner.ID == cm.Self.ID && cm.CurrentEpoch > p.Epoch {
						continue // Self has higher epoch precedence
					}
					if currentOwner != nil {
						currentOwner.Slots[i] = false
					}
					sender.Slots[i] = true
					cm.SlotMap[i] = sender
				}
			}
		}
	}

	// 2. Dispatch based on packet type
	switch p.Type {
	case TypePing:
		// Process gossip section
		cm.processGossipEntriesLocked(p.Gossip, p.SenderID)

		// Return Pong packet
		pong := cm.buildSelfPacketLocked(TypePong)
		return pong

	case TypePong:
		// Process gossip section
		cm.processGossipEntriesLocked(p.Gossip, p.SenderID)
		return nil

	case TypeFail:
		// Remote node broadcast that TargetNodeID is dead
		if target, ok := cm.Nodes[p.TargetNodeID]; ok && target.ID != cm.Self.ID {
			target.Fail = true
			target.PFail = false
			target.LinkState = "disconnected"
			log.Printf("[VortexKV Cluster Bus] FAIL received from %s: node %s marked FAIL", p.SenderID[:8], target.ID[:8])
		}
		return nil

	case TypeFailoverAuthReq:
		// A replica is requesting permission to elect itself master
		if cm.Self.Role == "master" {
			// Validate request: candidate must exist, candidate epoch must be >= currentEpoch,
			// and master hasn't voted for this epoch yet
			if p.ReqEpoch >= cm.CurrentEpoch && p.ReqEpoch > cm.LastVoteEpoch {
				// Verify target master is indeed in FAIL state
				if targetMaster, ok := cm.Nodes[p.TargetNodeID]; ok && targetMaster.Fail {
					cm.LastVoteEpoch = p.ReqEpoch
					cm.CurrentEpoch = p.ReqEpoch
					_ = cm.saveConfigLocked()

					ack := &BusPacket{
						Type:         TypeFailoverAuthAck,
						Epoch:        cm.CurrentEpoch,
						SenderID:     cm.Self.ID,
						SenderIP:     cm.Self.IP,
						Port:         uint16(cm.Self.Port),
						BusPort:      uint16(cm.Self.BusPort),
						TargetNodeID: p.SenderID,
						ReqEpoch:     p.ReqEpoch,
					}
					log.Printf("[VortexKV Cluster Bus] Granted failover vote to replica %s for epoch %d", p.SenderID[:8], p.ReqEpoch)
					return ack
				}
			}
		}
		return nil

	case TypeFailoverAuthAck:
		// Handled directly by election routine
		return nil
	}

	return nil
}

func (cm *ClusterManager) processGossipEntriesLocked(gossip []GossipEntry, reporterID string) {
	now := time.Now()
	for _, g := range gossip {
		if g.NodeID == cm.Self.ID {
			continue // Don't process rumors about ourselves
		}

		// If entry is reported with FlagFail, immediately record FAIL
		if (g.Flags & FlagFail) != 0 {
			if node, ok := cm.Nodes[g.NodeID]; ok {
				node.Fail = true
				node.PFail = false
				node.LinkState = "disconnected"
			}
			continue
		}

		// If entry is reported with FlagPFail, register failure report
		if (g.Flags & FlagPFail) != 0 {
			if cm.failReports[g.NodeID] == nil {
				cm.failReports[g.NodeID] = make(map[string]time.Time)
			}
			cm.failReports[g.NodeID][reporterID] = now
		}
	}
}

func (cm *ClusterManager) buildSelfPacketLocked(pktType uint8) *BusPacket {
	flags := uint16(0)
	if cm.Self.Role == "master" {
		flags |= FlagMaster
	} else {
		flags |= FlagSlave
	}

	p := &BusPacket{
		Type:     pktType,
		Epoch:    cm.CurrentEpoch,
		SenderID: cm.Self.ID,
		SenderIP: cm.Self.IP,
		Port:     uint16(cm.Self.Port),
		BusPort:  uint16(cm.Self.BusPort),
		Flags:    flags,
		Slots:    SlotsToBitmap(cm.Self.Slots),
	}

	// Pack gossip section: sample peer states
	var gossip []GossipEntry
	for _, n := range cm.Nodes {
		if n.ID == cm.Self.ID {
			continue
		}
		gFlags := uint16(0)
		if n.Role == "master" {
			gFlags |= FlagMaster
		} else {
			gFlags |= FlagSlave
		}
		if n.Fail {
			gFlags |= FlagFail
		} else if n.PFail {
			gFlags |= FlagPFail
		}

		gossip = append(gossip, GossipEntry{
			NodeID:   n.ID,
			IP:       n.IP,
			Port:     uint16(n.Port),
			BusPort:  uint16(n.BusPort),
			Flags:    gFlags,
			PingSent: n.PingSent,
			PongRecv: n.PongRecv,
		})
	}
	p.Gossip = gossip
	return p
}

func (cm *ClusterManager) gossipLoop() {
	ticker := time.NewTicker(DefaultHeartbeatTick)
	defer ticker.Stop()

	for {
		select {
		case <-cm.busStop:
			return
		case <-ticker.C:
			cm.heartbeatStep()
		}
	}
}

func (cm *ClusterManager) heartbeatStep() {
	cm.mu.Lock()
	now := time.Now().UnixMilli()

	// 1. Audit node health (PFAIL / FAIL detection)
	totalMasters := 0
	for _, n := range cm.Nodes {
		if n.Role == "master" {
			totalMasters++
		}
		if n.ID == cm.Self.ID {
			continue
		}

		timeSincePong := time.Duration(now-n.PongRecv) * time.Millisecond
		if timeSincePong > cm.NodeTimeout {
			if !n.PFail && !n.Fail {
				n.PFail = true
				n.LinkState = "disconnected"
				log.Printf("[VortexKV Cluster Bus] Node %s:%d (%s) timed out (%v). Marked PFAIL.", n.IP, n.Port, n.ID[:8], timeSincePong)

				if cm.failReports[n.ID] == nil {
					cm.failReports[n.ID] = make(map[string]time.Time)
				}
				cm.failReports[n.ID][cm.Self.ID] = time.Now()
			}
		}

		// Check if PFAIL should be elevated to FAIL:
		// If majority of masters observed PFAIL
		if n.PFail && !n.Fail && totalMasters > 0 {
			pfailReports := len(cm.failReports[n.ID])
			if pfailReports > (totalMasters / 2) {
				n.Fail = true
				n.PFail = false
				log.Printf("[VortexKV Cluster Bus] Node %s elevated to FAIL (confirmed by %d/%d masters). Broadcasting FAIL.", n.ID[:8], pfailReports, totalMasters)

				// Asynchronously broadcast FAIL to all nodes
				go cm.BroadcastFail(n.ID)
			}
		}
	}

	// 2. Check if self is a replica whose master has failed -> trigger automatic failover
	shouldFailover := false
	failedMasterID := ""
	if cm.Self.Role == "slave" && cm.Self.MasterID != "" && cm.Self.MasterID != "-" {
		masterNode := cm.Nodes[cm.Self.MasterID]
		if masterNode != nil && masterNode.Fail {
			shouldFailover = true
			failedMasterID = masterNode.ID
		}
	}

	// Prepare peers list to ping
	type peerInfo struct {
		id      string
		ip      string
		busPort int
	}
	var peers []peerInfo
	for _, n := range cm.Nodes {
		if n.ID != cm.Self.ID && !n.Fail {
			peers = append(peers, peerInfo{id: n.ID, ip: n.IP, busPort: n.BusPort})
		}
	}
	pingPacket := cm.buildSelfPacketLocked(TypePing)
	cm.mu.Unlock()

	// 3. Send PING to peers
	for _, p := range peers {
		go cm.sendPingToPeer(p.ip, p.busPort, pingPacket)
	}

	// 4. Trigger replica election if master is confirmed dead
	if shouldFailover {
		go cm.RunFailoverElection(failedMasterID)
	}
}

func (cm *ClusterManager) sendPingToPeer(ip string, busPort int, pkt *BusPacket) {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", ip, busPort), 400*time.Millisecond)
	if err != nil {
		return
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(600 * time.Millisecond))

	if err := WritePacket(conn, pkt); err != nil {
		return
	}

	resp, err := ReadPacket(conn)
	if err != nil {
		return
	}

	if resp.Type == TypePong {
		cm.ProcessPacket(resp)
	}
}

// BroadcastFail announces a node's confirmed FAIL status to all known nodes.
func (cm *ClusterManager) BroadcastFail(failedNodeID string) {
	cm.mu.RLock()
	failPkt := &BusPacket{
		Type:         TypeFail,
		Epoch:        cm.CurrentEpoch,
		SenderID:     cm.Self.ID,
		SenderIP:     cm.Self.IP,
		Port:         uint16(cm.Self.Port),
		BusPort:      uint16(cm.Self.BusPort),
		TargetNodeID: failedNodeID,
	}

	var targets []string
	for _, n := range cm.Nodes {
		if n.ID != cm.Self.ID && !n.Fail {
			targets = append(targets, fmt.Sprintf("%s:%d", n.IP, n.BusPort))
		}
	}
	cm.mu.RUnlock()

	for _, addr := range targets {
		go func(targetAddr string) {
			conn, err := net.DialTimeout("tcp", targetAddr, 500*time.Millisecond)
			if err == nil {
				defer conn.Close()
				_ = WritePacket(conn, failPkt)
			}
		}(addr)
	}
}

// RunFailoverElection coordinates replica promotion by soliciting votes from active masters.
func (cm *ClusterManager) RunFailoverElection(failedMasterID string) {
	cm.mu.Lock()
	// Stagger election start to reduce split-brain tie votes
	time.Sleep(DefaultFailoverDelay)

	// Verify conditions still hold
	if cm.Self.Role != "slave" {
		cm.mu.Unlock()
		return
	}

	cm.CurrentEpoch++
	electionEpoch := cm.CurrentEpoch
	candidateID := cm.Self.ID

	var masterBusAddrs []string
	totalMasters := 0
	for _, n := range cm.Nodes {
		if n.Role == "master" && !n.Fail {
			totalMasters++
			masterBusAddrs = append(masterBusAddrs, fmt.Sprintf("%s:%d", n.IP, n.BusPort))
		}
	}
	cm.mu.Unlock()

	log.Printf("[VortexKV Cluster Bus] Replica %s starting failover election for epoch %d (target master %s)", candidateID[:8], electionEpoch, failedMasterID[:8])

	req := &BusPacket{
		Type:         TypeFailoverAuthReq,
		Epoch:        electionEpoch,
		SenderID:     candidateID,
		SenderIP:     cm.Self.IP,
		Port:         uint16(cm.Self.Port),
		BusPort:      uint16(cm.Self.BusPort),
		TargetNodeID: failedMasterID,
		ReqEpoch:     electionEpoch,
	}

	var voteMu sync.Mutex
	votes := 1 // Replica votes for itself

	var wg sync.WaitGroup
	for _, addr := range masterBusAddrs {
		wg.Add(1)
		go func(target string) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", target, 800*time.Millisecond)
			if err != nil {
				return
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(1 * time.Second))

			if err := WritePacket(conn, req); err != nil {
				return
			}

			resp, err := ReadPacket(conn)
			if err != nil {
				return
			}

			if resp.Type == TypeFailoverAuthAck && resp.TargetNodeID == candidateID && resp.ReqEpoch == electionEpoch {
				voteMu.Lock()
				votes++
				voteMu.Unlock()
			}
		}(addr)
	}
	wg.Wait()

	// Evaluate election outcome: need majority of known active masters (including the candidate)
	needed := (totalMasters / 2) + 1
	if votes >= needed {
		cm.PromoteReplica(failedMasterID)
	} else {
		log.Printf("[VortexKV Cluster Bus] Replica %s election failed (%d/%d votes required)", candidateID[:8], votes, needed)
	}
}

// PromoteReplica completes the promotion: updates roles, takes over slots, and announces to the cluster.
func (cm *ClusterManager) PromoteReplica(failedMasterID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.Self.Role == "master" {
		return // Already promoted
	}

	log.Printf("[VortexKV Cluster Bus] Replica %s WON ELECTION! Promoting to MASTER for epoch %d.", cm.Self.ID[:8], cm.CurrentEpoch)

	cm.Self.Role = "master"
	cm.Self.MasterID = "-"

	// Take over all slots belonging to the failed master
	failedMaster := cm.Nodes[failedMasterID]
	claimedSlots := 0
	for i := 0; i < 16384; i++ {
		if failedMaster != nil && failedMaster.Slots[i] {
			failedMaster.Slots[i] = false
			cm.Self.Slots[i] = true
			cm.SlotMap[i] = cm.Self
			claimedSlots++
		} else if cm.SlotMap[i] != nil && cm.SlotMap[i].ID == failedMasterID {
			cm.Self.Slots[i] = true
			cm.SlotMap[i] = cm.Self
			claimedSlots++
		}
	}

	_ = cm.saveConfigLocked()

	// Execute external promotion callback (e.g. Replication.PromoteToMaster())
	if cm.OnPromote != nil {
		go cm.OnPromote()
	}

	log.Printf("[VortexKV Cluster Bus] Promotion complete. Claimed %d slots. Broadcasting updated state.", claimedSlots)

	// Broadcast PONG announcement to the cluster
	pong := cm.buildSelfPacketLocked(TypePong)
	for _, n := range cm.Nodes {
		if n.ID != cm.Self.ID && !n.Fail {
			go cm.sendPingToPeer(n.IP, n.BusPort, pong)
		}
	}
}
