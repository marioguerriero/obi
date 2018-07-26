package model

// MetricsSnapshot is the struct composing an any type cluster to save last snapshot about metrics
type MetricsSnapshot struct {
	PendingContainers int32
	AllocatedContainers int32
	PendingMemory int32
	AvailableMemory int32
	PendingVCores int32
}

// Scalable is the interface that must be implemented from a scalable cluster
type Scalable interface {
	Scale(int16, bool)
}

// ClusterBase is the base class for any type of cluster
type ClusterBase struct {
	Name string
	Nodes int16
	MetricsSnapshot
}


// NewClusterBase is the constructor of ClusterBase struct
// @param clusterName is the name of the cluster
// @param size is the number of nodes in the cluster
// return the pointer to the ClusterBase instance
func NewClusterBase(clusterName string, size int16) *ClusterBase {
	return &ClusterBase{
		clusterName,
		size,
		MetricsSnapshot{},
	}
}
