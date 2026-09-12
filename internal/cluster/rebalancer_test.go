package cluster

import (
	"os"
	"testing"
)

func TestComputeRebalancePlan_ThreeToFourMasters(t *testing.T) {
	// Initial state: 3 masters dividing 16,384 slots
	// Node 1: 5462 slots (0..5461)
	// Node 2: 5461 slots (5462..10922)
	// Node 3: 5461 slots (10923..16383)
	// Node 4: 0 slots (newly added)
	slots1 := make([]uint16, 5462)
	for i := 0; i < 5462; i++ {
		slots1[i] = uint16(i)
	}

	slots2 := make([]uint16, 5461)
	for i := 0; i < 5461; i++ {
		slots2[i] = uint16(5462 + i)
	}

	slots3 := make([]uint16, 5461)
	for i := 0; i < 5461; i++ {
		slots3[i] = uint16(10923 + i)
	}

	masters := []*MasterNodeInfo{
		{ID: "node111111111111111111111111111111111111", IP: "127.0.0.1", Port: 7379, Slots: slots1},
		{ID: "node222222222222222222222222222222222222", IP: "127.0.0.1", Port: 7381, Slots: slots2},
		{ID: "node333333333333333333333333333333333333", IP: "127.0.0.1", Port: 7383, Slots: slots3},
		{ID: "node444444444444444444444444444444444444", IP: "127.0.0.1", Port: 7385, Slots: []uint16{}},
	}

	plan, err := ComputeRebalancePlan(masters)
	if err != nil {
		t.Fatalf("Failed to compute rebalance plan: %v", err)
	}

	if plan.TotalSlotsToMove != 4096 {
		t.Fatalf("Expected 4096 slots to move to 4th node, got %d", plan.TotalSlotsToMove)
	}

	// Verify all 4 masters end up with 4096 slots
	for id, count := range plan.FinalCounts {
		if count != 4096 {
			t.Fatalf("Expected node %s to have 4096 slots, got %d", id, count)
		}
	}
}

func TestComputeRebalancePlan_AlreadyBalanced(t *testing.T) {
	slots1 := make([]uint16, 8192)
	slots2 := make([]uint16, 8192)
	for i := 0; i < 8192; i++ {
		slots1[i] = uint16(i)
		slots2[i] = uint16(8192 + i)
	}

	masters := []*MasterNodeInfo{
		{ID: "node1", IP: "127.0.0.1", Port: 7379, Slots: slots1},
		{ID: "node2", IP: "127.0.0.1", Port: 7381, Slots: slots2},
	}

	plan, err := ComputeRebalancePlan(masters)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if plan.TotalSlotsToMove != 0 {
		t.Fatalf("Expected 0 moves for already balanced cluster, got %d", plan.TotalSlotsToMove)
	}
}

func TestSetSlotOperations(t *testing.T) {
	cfg := "nodes_setslot_test.conf"
	defer os.Remove(cfg)
	cm := NewClusterManager("127.0.0.1", 7379, 17379, cfg)
	peer := NewNode("peer_node_123456789012345678901234567890", "127.0.0.1", 7381, 17381, "master")
	cm.Nodes[peer.ID] = peer

	// Assign slot 500 to self
	if err := cm.AddSlots(500); err != nil {
		t.Fatalf("Failed to add slot 500: %v", err)
	}
	if cm.GetSlotOwner(500).ID != cm.Self.ID {
		t.Fatalf("Self should own slot 500")
	}

	// Mark MIGRATING
	if err := cm.SetSlot(500, "MIGRATING", peer.ID); err != nil {
		t.Fatalf("Failed to set MIGRATING: %v", err)
	}
	if cm.MigratingSlots[500] != peer.ID {
		t.Fatalf("Slot 500 should be marked migrating to peer")
	}

	// Move ownership to peer via NODE
	if err := cm.SetSlot(500, "NODE", peer.ID); err != nil {
		t.Fatalf("Failed to set NODE: %v", err)
	}
	if cm.GetSlotOwner(500).ID != peer.ID {
		t.Fatalf("Peer should now own slot 500")
	}
	if _, migrating := cm.MigratingSlots[500]; migrating {
		t.Fatalf("Slot 500 should no longer be migrating")
	}
}
