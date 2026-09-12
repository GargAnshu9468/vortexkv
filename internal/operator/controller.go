package operator

import (
	"fmt"

	"github.com/vortexkv/vortexkv/internal/cluster"
)

// Controller orchestrates the lifecycle of VortexKV distributed clusters on Kubernetes.
type Controller struct {
	Namespace string
}

// NewController creates an instance of the operator reconciler.
func NewController(namespace string) *Controller {
	if namespace == "" {
		namespace = "default"
	}
	return &Controller{
		Namespace: namespace,
	}
}

// CalculateTotalPods returns the required pod count based on master and replica topology.
func CalculateTotalPods(spec *VortexClusterSpec) int {
	return spec.Masters * (1 + spec.ReplicasPerMaster)
}

// PartitionSlots divides 16,384 hash slots evenly across the specified number of master nodes.
func PartitionSlots(mastersCount int) [][]uint16 {
	if mastersCount <= 0 {
		return nil
	}

	result := make([][]uint16, mastersCount)
	base := 16384 / mastersCount
	remainder := 16384 % mastersCount

	currentSlot := uint16(0)
	for i := 0; i < mastersCount; i++ {
		count := base
		if i < remainder {
			count++
		}
		slots := make([]uint16, count)
		for j := 0; j < count; j++ {
			slots[j] = currentSlot
			currentSlot++
		}
		result[i] = slots
	}

	return result
}

// NodeRoleAssignment represents the topological role and slot ownership of a pod.
type NodeRoleAssignment struct {
	PodIndex   int
	PodName    string
	Role       string // "master" or "slave"
	MasterPod  string // Only set for replicas
	Slots      []uint16
	SlotRanges string
}

// BootstrapPlan defines the initial clustering blueprint for pods in the StatefulSet.
type BootstrapPlan struct {
	ClusterName string
	TotalPods   int
	Masters     []NodeRoleAssignment
	Replicas    []NodeRoleAssignment
}

// PlanBootstrap computes pod master/replica assignments and initial slot partitions.
func PlanBootstrap(vc *VortexCluster) *BootstrapPlan {
	totalPods := CalculateTotalPods(&vc.Spec)
	slotPartitions := PartitionSlots(vc.Spec.Masters)

	plan := &BootstrapPlan{
		ClusterName: vc.Metadata.Name,
		TotalPods:   totalPods,
		Masters:     make([]NodeRoleAssignment, vc.Spec.Masters),
		Replicas:    make([]NodeRoleAssignment, 0, totalPods-vc.Spec.Masters),
	}

	// First N pods are designated as Master nodes
	for i := 0; i < vc.Spec.Masters; i++ {
		podName := fmt.Sprintf("%s-%d", vc.Metadata.Name, i)
		slots := slotPartitions[i]
		slotRangeStr := ""
		if len(slots) > 0 {
			slotRangeStr = fmt.Sprintf("%d-%d", slots[0], slots[len(slots)-1])
		}

		plan.Masters[i] = NodeRoleAssignment{
			PodIndex:   i,
			PodName:    podName,
			Role:       "master",
			Slots:      slots,
			SlotRanges: slotRangeStr,
		}
	}

	// Remaining pods are assigned as replicas round-robin to masters
	replicaIdx := 0
	for i := vc.Spec.Masters; i < totalPods; i++ {
		podName := fmt.Sprintf("%s-%d", vc.Metadata.Name, i)
		assignedMaster := plan.Masters[replicaIdx%vc.Spec.Masters].PodName
		replicaIdx++

		plan.Replicas = append(plan.Replicas, NodeRoleAssignment{
			PodIndex:  i,
			PodName:   podName,
			Role:      "slave",
			MasterPod: assignedMaster,
		})
	}

	return plan
}

// ReconcileScaleOut computes a slot rebalance migration plan when masters count increases.
func ReconcileScaleOut(currentMasters []*cluster.MasterNodeInfo, desiredMastersCount int) (*cluster.RebalancePlan, error) {
	if desiredMastersCount <= len(currentMasters) {
		return nil, fmt.Errorf("desired masters (%d) must be greater than current (%d)", desiredMastersCount, len(currentMasters))
	}

	allMasters := make([]*cluster.MasterNodeInfo, desiredMastersCount)
	copy(allMasters, currentMasters)

	// Add new empty masters
	for i := len(currentMasters); i < desiredMastersCount; i++ {
		allMasters[i] = &cluster.MasterNodeInfo{
			ID:        fmt.Sprintf("new_master_%d", i),
			Slots:     []uint16{},
			SlotCount: 0,
		}
	}

	return cluster.ComputeRebalancePlan(allMasters)
}

// GenerateStatefulSetManifest produces the Kubernetes StatefulSet YAML for the cluster.
func (c *Controller) GenerateStatefulSetManifest(vc *VortexCluster) string {
	totalPods := CalculateTotalPods(&vc.Spec)
	image := vc.Spec.Image
	if image == "" {
		image = "vortexkv/vortexkv:latest"
	}
	maxMem := vc.Spec.MaxMemory
	if maxMem == "" {
		maxMem = "1gb"
	}
	storageSize := vc.Spec.Storage.Size
	if storageSize == "" {
		storageSize = "10Gi"
	}

	authArg := ""
	if vc.Spec.RequirePass != "" {
		authArg = fmt.Sprintf("          - -requirepass\n          - %s\n", vc.Spec.RequirePass)
	}

	return fmt.Sprintf(`apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: %s
  namespace: %s
  labels:
    app.kubernetes.io/name: vortexkv
    app.kubernetes.io/instance: %s
spec:
  serviceName: %s-headless
  replicas: %d
  selector:
    matchLabels:
      app.kubernetes.io/name: vortexkv
      app.kubernetes.io/instance: %s
  template:
    metadata:
      labels:
        app.kubernetes.io/name: vortexkv
        app.kubernetes.io/instance: %s
    spec:
      containers:
        - name: vortexkv
          image: %s
          ports:
            - name: client
              containerPort: 7379
            - name: web
              containerPort: 7380
            - name: bus
              containerPort: 17379
          command:
            - ./bin/vortex-server
          args:
            - -cluster-enabled
            - -cluster-config-file
            - /data/nodes.conf
            - -maxmemory
            - %s
%s            - -rdb
            - /data/dump.rdb
            - -aof
            - /data/vortex.aof
          volumeMounts:
            - name: data
              mountPath: /data
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: [ "ReadWriteOnce" ]
        resources:
          requests:
            storage: %s
`, vc.Metadata.Name, c.Namespace, vc.Metadata.Name, vc.Metadata.Name, totalPods,
		vc.Metadata.Name, vc.Metadata.Name, image, maxMem, authArg, storageSize)
}

// GenerateHeadlessServiceManifest produces the headless service for DNS resolution.
func (c *Controller) GenerateHeadlessServiceManifest(vc *VortexCluster) string {
	return fmt.Sprintf(`apiVersion: v1
kind: Service
metadata:
  name: %s-headless
  namespace: %s
  labels:
    app.kubernetes.io/name: vortexkv
    app.kubernetes.io/instance: %s
spec:
  clusterIP: None
  ports:
    - name: client
      port: 7379
    - name: bus
      port: 17379
    - name: web
      port: 7380
  selector:
    app.kubernetes.io/name: vortexkv
    app.kubernetes.io/instance: %s
`, vc.Metadata.Name, c.Namespace, vc.Metadata.Name, vc.Metadata.Name)
}
