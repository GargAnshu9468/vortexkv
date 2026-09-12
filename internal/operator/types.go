package operator

import "time"

// VortexClusterSpec defines the desired state of a distributed VortexKV cluster.
type VortexClusterSpec struct {
	Masters           int               `json:"masters"`
	ReplicasPerMaster int               `json:"replicasPerMaster"`
	Image             string            `json:"image,omitempty"`
	RequirePass       string            `json:"requirepass,omitempty"`
	MaxMemory         string            `json:"maxmemory,omitempty"`
	Storage           StorageSpec       `json:"storage,omitempty"`
	ServiceType       string            `json:"serviceType,omitempty"`
	NodeSelector      map[string]string `json:"nodeSelector,omitempty"`
}

// StorageSpec defines persistent disk volume requirements.
type StorageSpec struct {
	Size             string `json:"size,omitempty"`
	StorageClassName string `json:"storageClassName,omitempty"`
}

// VortexClusterStatus represents the observed state of the cluster.
type VortexClusterStatus struct {
	Phase         string    `json:"phase"` // "Pending", "Initializing", "Running", "Rebalancing", "Failed"
	ReadyNodes    int       `json:"readyNodes"`
	TotalNodes    int       `json:"totalNodes"`
	Masters       int       `json:"masters"`
	Replicas      int       `json:"replicas"`
	SlotsAssigned int       `json:"slotsAssigned"`
	ClusterState  string    `json:"clusterState"` // "ok" or "fail"
	LastUpdated   time.Time `json:"lastUpdated"`
}

// VortexCluster represents the custom resource object.
type VortexCluster struct {
	APIVersion string              `json:"apiVersion"`
	Kind       string              `json:"kind"`
	Metadata   ObjectMetadata      `json:"metadata"`
	Spec       VortexClusterSpec   `json:"spec"`
	Status     VortexClusterStatus `json:"status,omitempty"`
}

// ObjectMetadata represents standard Kubernetes resource metadata.
type ObjectMetadata struct {
	Name      string            `json:"name"`
	Namespace string            `json:"namespace"`
	Labels    map[string]string `json:"labels,omitempty"`
}
