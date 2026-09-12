package cluster

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCRC16(t *testing.T) {
	// Reference test vector for CRC16-CCITT/XMODEM
	vec := []byte("123456789")
	val := CRC16(vec)
	if val != 0x31C3 {
		t.Fatalf("expected CRC16('123456789') to be 0x31C3 (12739), got 0x%X (%d)", val, val)
	}

	// Empty string produces 0
	if CRC16([]byte("")) != 0 {
		t.Fatalf("expected CRC16('') to be 0")
	}

	// Redis canonical slot test: 'foo' maps to slot 12182
	slotFoo := KeySlot("foo")
	if slotFoo != 12182 {
		t.Fatalf("expected KeySlot('foo') to be 12182, got %d", slotFoo)
	}
}

func TestHashTag(t *testing.T) {
	tests := []struct {
		key      string
		expected string
	}{
		{"user1000", "user1000"},
		{"{user1000}", "user1000"},
		{"{user1000}:profile", "user1000"},
		{"user1000:{profile}", "profile"},
		{"{user1000}:foo:{bar}", "user1000"},
		{"user:{}1000", "user:{}1000"},
		{"foo{{bar}}", "{bar"},
	}

	for _, tt := range tests {
		tag := ExtractHashTag(tt.key)
		if tag != tt.expected {
			t.Errorf("ExtractHashTag(%q) = %q, expected %q", tt.key, tag, tt.expected)
		}
	}

	// Hash tags must guarantee identical slot for co-located keys
	s1 := KeySlot("{user:100}:profile")
	s2 := KeySlot("{user:100}:orders")
	s3 := KeySlot("{user:100}:settings")
	if s1 != s2 || s2 != s3 {
		t.Fatalf("expected co-located keys to have identical slots, got %d, %d, %d", s1, s2, s3)
	}
}

func TestSlotAssignmentAndRanges(t *testing.T) {
	node := NewNode("node1", "127.0.0.1", 7379, 17379, "master")

	// Empty slots
	if node.SlotCount() != 0 {
		t.Fatalf("expected 0 slots, got %d", node.SlotCount())
	}
	if node.SlotRangesString() != "" {
		t.Fatalf("expected empty slot range string")
	}

	// Contiguous range 0-5460
	for i := uint16(0); i <= 5460; i++ {
		node.Slots[i] = true
	}
	// Discrete slots 6000 and 7000-7002
	node.Slots[6000] = true
	node.Slots[7000] = true
	node.Slots[7001] = true
	node.Slots[7002] = true

	ranges := node.SlotRangesString()
	expected := "0-5460 6000 7000-7002"
	if ranges != expected {
		t.Fatalf("expected slot ranges %q, got %q", expected, ranges)
	}

	// Parse back
	parsed, err := ParseSlotRanges(ranges)
	if err != nil {
		t.Fatalf("unexpected error parsing slot ranges: %v", err)
	}
	if len(parsed) != 5461+1+3 {
		t.Fatalf("expected %d parsed slots, got %d", 5461+1+3, len(parsed))
	}
}

func TestClusterManagerOperations(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "vortex_cluster_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	confPath := filepath.Join(tmpDir, "nodes.conf")
	cm := NewClusterManager("127.0.0.1", 7379, 17379, confPath)

	// Add slots 0 to 10
	var slots []uint16
	for s := uint16(0); s <= 10; s++ {
		slots = append(slots, s)
	}
	err = cm.AddSlots(slots...)
	if err != nil {
		t.Fatalf("failed to add slots: %v", err)
	}

	// Verify ownership
	for s := uint16(0); s <= 10; s++ {
		owner := cm.GetSlotOwner(s)
		if owner == nil || owner.ID != cm.Self.ID {
			t.Fatalf("slot %d owner expected to be %s", s, cm.Self.ID)
		}
	}
	if cm.GetSlotOwner(11) != nil {
		t.Fatalf("slot 11 should be unassigned")
	}

	// Introduce a second node
	peer := NewNode("peer-node-2", "127.0.0.1", 7381, 17381, "master")
	cm.AddNode(peer)

	// Assign slots 11..20 to peer
	var peerSlots []uint16
	for s := uint16(11); s <= 20; s++ {
		peerSlots = append(peerSlots, s)
	}
	err = cm.AssignSlotsToNode(peer.ID, peerSlots...)
	if err != nil {
		t.Fatalf("failed to assign slots to peer: %v", err)
	}

	if cm.GetSlotOwner(15).ID != peer.ID {
		t.Fatalf("slot 15 expected to be owned by peer %s", peer.ID)
	}

	// Delete slots from self
	err = cm.DelSlots(0, 1)
	if err != nil {
		t.Fatalf("failed to delete slots: %v", err)
	}
	if cm.GetSlotOwner(0) != nil {
		t.Fatalf("slot 0 should be nil after deletion")
	}

	// Verify CLUSTER NODES output
	nodeStr := cm.FormatNodes()
	if !strings.Contains(nodeStr, "myself,master") {
		t.Fatalf("CLUSTER NODES missing myself flag: %s", nodeStr)
	}
	if !strings.Contains(nodeStr, "peer-node-2") {
		t.Fatalf("CLUSTER NODES missing peer node: %s", nodeStr)
	}

	// Verify CLUSTER SLOTS output
	slotsResp := cm.FormatSlots()
	if len(slotsResp.Array) == 0 {
		t.Fatalf("CLUSTER SLOTS returned empty array")
	}

	// Verify CLUSTER INFO output
	infoStr := cm.FormatInfo()
	if !strings.Contains(infoStr, "cluster_state:ok") {
		t.Fatalf("CLUSTER INFO missing cluster_state: %s", infoStr)
	}

	// Verify config persistence
	err = cm.SaveConfig()
	if err != nil {
		t.Fatalf("failed to save cluster config: %v", err)
	}

	// Spin up new manager reading the saved config
	cm2 := NewClusterManager("127.0.0.1", 7379, 17379, confPath)
	if cm2.GetSlotOwner(15) == nil || cm2.GetSlotOwner(15).ID != peer.ID {
		t.Fatalf("cm2 failed to restore peer slot ownership from nodes.conf")
	}
}
