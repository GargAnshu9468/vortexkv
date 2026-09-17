package operator

import (
	"strings"
	"testing"

	"github.com/GargAnshu9468/vortexkv/internal/cluster"
)

func TestCalculateTotalPods(t *testing.T) {
	spec1 := &VortexClusterSpec{Masters: 3, ReplicasPerMaster: 1}
	if pods := CalculateTotalPods(spec1); pods != 6 {
		t.Fatalf("Expected 6 pods for 3 masters + 1 replica each, got %d", pods)
	}

	spec2 := &VortexClusterSpec{Masters: 4, ReplicasPerMaster: 2}
	if pods := CalculateTotalPods(spec2); pods != 12 {
		t.Fatalf("Expected 12 pods for 4 masters + 2 replicas each, got %d", pods)
	}

	specStandalone := &VortexClusterSpec{Masters: 3, ReplicasPerMaster: 0}
	if pods := CalculateTotalPods(specStandalone); pods != 3 {
		t.Fatalf("Expected 3 pods for 3 masters + 0 replicas, got %d", pods)
	}
}

func TestPartitionSlots(t *testing.T) {
	// Test 3 masters partition
	partitions := PartitionSlots(3)
	if len(partitions) != 3 {
		t.Fatalf("Expected 3 partitions, got %d", len(partitions))
	}

	totalSlots := 0
	visited := make(map[uint16]bool)
	for i, p := range partitions {
		totalSlots += len(p)
		for _, s := range p {
			if visited[s] {
				t.Fatalf("Slot %d was duplicated in partition %d", s, i)
			}
			visited[s] = true
		}
	}

	if totalSlots != 16384 {
		t.Fatalf("Expected 16384 total slots across partitions, got %d", totalSlots)
	}

	// Test 4 masters partition (exactly 4096 each)
	p4 := PartitionSlots(4)
	for i, p := range p4 {
		if len(p) != 4096 {
			t.Fatalf("Partition %d expected 4096 slots, got %d", i, len(p))
		}
	}
}

func TestPlanBootstrap(t *testing.T) {
	vc := &VortexCluster{
		Metadata: ObjectMetadata{
			Name:      "vortex-prod",
			Namespace: "vortex-system",
		},
		Spec: VortexClusterSpec{
			Masters:           3,
			ReplicasPerMaster: 1,
		},
	}

	plan := PlanBootstrap(vc)
	if plan.TotalPods != 6 {
		t.Fatalf("Expected 6 total pods, got %d", plan.TotalPods)
	}
	if len(plan.Masters) != 3 {
		t.Fatalf("Expected 3 masters, got %d", len(plan.Masters))
	}
	if len(plan.Replicas) != 3 {
		t.Fatalf("Expected 3 replicas, got %d", len(plan.Replicas))
	}

	// Verify master pod naming and slot assignment
	for i, m := range plan.Masters {
		expectedName := "vortex-prod-" + string(rune('0'+i))
		if m.PodName != expectedName {
			t.Fatalf("Master %d expected name %s, got %s", i, expectedName, m.PodName)
		}
		if len(m.Slots) == 0 {
			t.Fatalf("Master %d has no slots assigned", i)
		}
	}

	// Verify replica pairings
	for i, r := range plan.Replicas {
		if r.Role != "slave" {
			t.Fatalf("Replica %d role should be 'slave'", i)
		}
		if r.MasterPod == "" {
			t.Fatalf("Replica %d has no master assigned", i)
		}
	}
}

func TestReconcileScaleOut(t *testing.T) {
	p3 := PartitionSlots(3)
	m1 := &cluster.MasterNodeInfo{ID: "m1", Slots: p3[0]}
	m2 := &cluster.MasterNodeInfo{ID: "m2", Slots: p3[1]}
	m3 := &cluster.MasterNodeInfo{ID: "m3", Slots: p3[2]}

	currentMasters := []*cluster.MasterNodeInfo{m1, m2, m3}

	plan, err := ReconcileScaleOut(currentMasters, 4)
	if err != nil {
		t.Fatalf("Failed to compute scale out: %v", err)
	}

	if plan.TotalSlotsToMove != 4096 {
		t.Fatalf("Expected 4096 slots moved to new 4th master, got %d", plan.TotalSlotsToMove)
	}
}

func TestManifestGeneration(t *testing.T) {
	ctrl := NewController("default")
	vc := &VortexCluster{
		Metadata: ObjectMetadata{
			Name: "analytics-cache",
		},
		Spec: VortexClusterSpec{
			Masters:           3,
			ReplicasPerMaster: 1,
			Image:             "vortexkv/vortexkv:latest",
			RequirePass:       "secret123",
			MaxMemory:         "4gb",
			Storage: StorageSpec{
				Size: "50Gi",
			},
		},
	}

	sts := ctrl.GenerateStatefulSetManifest(vc)
	if !strings.Contains(sts, "name: analytics-cache") {
		t.Fatalf("StatefulSet missing metadata name")
	}
	if !strings.Contains(sts, "replicas: 6") {
		t.Fatalf("StatefulSet missing replicas: 6")
	}
	if !strings.Contains(sts, "secret123") {
		t.Fatalf("StatefulSet missing requirepass argument")
	}
	if !strings.Contains(sts, "50Gi") {
		t.Fatalf("StatefulSet missing volume claim storage size")
	}

	svc := ctrl.GenerateHeadlessServiceManifest(vc)
	if !strings.Contains(svc, "clusterIP: None") {
		t.Fatalf("Headless service missing clusterIP: None")
	}
}
