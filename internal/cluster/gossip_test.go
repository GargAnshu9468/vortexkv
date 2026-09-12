package cluster

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"
)

func TestBusPacketSerialization(t *testing.T) {
	var slots [16384]bool
	slots[0] = true
	slots[100] = true
	slots[16383] = true

	pkt := &BusPacket{
		Type:         TypePing,
		Epoch:        42,
		SenderID:     "1234567890123456789012345678901234567890",
		SenderIP:     "192.168.1.100",
		Port:         7379,
		BusPort:      17379,
		Flags:        FlagMaster,
		Slots:        SlotsToBitmap(slots),
		TargetNodeID: "abcdefabcdefabcdefabcdefabcdefabcdefabcd",
		ReqEpoch:     42,
		Gossip: []GossipEntry{
			{
				NodeID:   "peer111111111111111111111111111111111111",
				IP:       "192.168.1.101",
				Port:     7380,
				BusPort:  17380,
				Flags:    FlagSlave | FlagPFail,
				PingSent: 12345678,
				PongRecv: 87654321,
			},
		},
	}

	var buf bytes.Buffer
	if err := WritePacket(&buf, pkt); err != nil {
		t.Fatalf("Failed to write packet: %v", err)
	}

	readPkt, err := ReadPacket(&buf)
	if err != nil {
		t.Fatalf("Failed to read packet: %v", err)
	}

	if readPkt.Type != pkt.Type || readPkt.Epoch != pkt.Epoch || readPkt.SenderID != pkt.SenderID {
		t.Fatalf("Header mismatch: got type=%d, epoch=%d, sender=%s", readPkt.Type, readPkt.Epoch, readPkt.SenderID)
	}

	readSlots := BitmapToSlots(readPkt.Slots)
	if !readSlots[0] || !readSlots[100] || !readSlots[16383] || readSlots[50] {
		t.Fatalf("Slot bitmap corruption")
	}

	if len(readPkt.Gossip) != 1 {
		t.Fatalf("Expected 1 gossip entry, got %d", len(readPkt.Gossip))
	}

	g := readPkt.Gossip[0]
	if g.NodeID != pkt.Gossip[0].NodeID || g.Flags != (FlagSlave|FlagPFail) {
		t.Fatalf("Gossip entry mismatch: %+v", g)
	}
}

func TestGossipHeartbeatAndPFail(t *testing.T) {
	cfg1 := "nodes_test_1.conf"
	cfg2 := "nodes_test_2.conf"
	defer os.Remove(cfg1)
	defer os.Remove(cfg2)

	cm1 := NewClusterManager("127.0.0.1", 27379, 37379, cfg1)
	cm2 := NewClusterManager("127.0.0.1", 27380, 37380, cfg2)

	cm1.NodeTimeout = 400 * time.Millisecond
	cm2.NodeTimeout = 400 * time.Millisecond

	if err := cm1.StartBus(); err != nil {
		t.Fatalf("Failed to start cm1 bus: %v", err)
	}
	defer cm1.StopBus()

	if err := cm2.StartBus(); err != nil {
		t.Fatalf("Failed to start cm2 bus: %v", err)
	}

	// Register nodes manually
	cm1.Nodes[cm2.Self.ID] = cm2.Self
	cm2.Nodes[cm1.Self.ID] = cm1.Self

	// Allow one or two heartbeat cycles
	time.Sleep(350 * time.Millisecond)

	// Now abruptly stop cm2 bus
	cm2.StopBus()

	// Wait past node timeout (400ms)
	time.Sleep(600 * time.Millisecond)

	// cm1 should have flagged cm2 as PFail
	cm1.mu.RLock()
	node2In1 := cm1.Nodes[cm2.Self.ID]
	cm1.mu.RUnlock()

	if node2In1 == nil {
		t.Fatalf("Expected node 2 to exist in cm1 nodes")
	}
	if !node2In1.PFail && !node2In1.Fail {
		t.Fatalf("Expected node 2 to be marked PFAIL or FAIL, got pfail=%v, fail=%v", node2In1.PFail, node2In1.Fail)
	}

	// FormatNodes should show fail? or fail
	line := node2In1.FormatNodeLine(false)
	if !strings.Contains(line, "fail") {
		t.Fatalf("Expected FormatNodeLine to contain 'fail', got: %s", line)
	}
}

func TestAutomaticFailoverElection(t *testing.T) {
	cfgMaster := "nodes_m.conf"
	cfgReplica := "nodes_r.conf"
	defer os.Remove(cfgMaster)
	defer os.Remove(cfgReplica)

	master := NewClusterManager("127.0.0.1", 28379, 38379, cfgMaster)
	replica := NewClusterManager("127.0.0.1", 28380, 38380, cfgReplica)

	// Assign slots 0-16383 to master
	for i := 0; i < 16384; i++ {
		master.Self.Slots[i] = true
		master.SlotMap[i] = master.Self
	}

	replica.Self.Role = "slave"
	replica.Self.MasterID = master.Self.ID

	// Link them
	master.Nodes[replica.Self.ID] = replica.Self
	replica.Nodes[master.Self.ID] = master.Self

	// Copy slots knowledge to replica
	for i := 0; i < 16384; i++ {
		replica.SlotMap[i] = master.Self
	}

	// Mark master as confirmed FAIL
	replica.mu.Lock()
	replica.Nodes[master.Self.ID].Fail = true
	replica.mu.Unlock()

	promotedCalled := false
	replica.OnPromote = func() {
		promotedCalled = true
	}

	// Trigger promotion
	replica.PromoteReplica(master.Self.ID)

	replica.mu.RLock()
	defer replica.mu.RUnlock()

	if replica.Self.Role != "master" {
		t.Fatalf("Expected replica role to become 'master', got %s", replica.Self.Role)
	}
	if replica.Self.MasterID != "-" {
		t.Fatalf("Expected master ID to be '-', got %s", replica.Self.MasterID)
	}
	if replica.Self.SlotCount() != 16384 {
		t.Fatalf("Expected replica to inherit all 16384 slots, got %d", replica.Self.SlotCount())
	}
	time.Sleep(50 * time.Millisecond)
	if !promotedCalled {
		t.Fatalf("Expected OnPromote callback to be called")
	}
}
