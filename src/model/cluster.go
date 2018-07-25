package model

// MetricsSnapshot is the struct composing an any type cluster to save last snapshot about metrics
type MetricsSnapshot struct {
	PendingContainers int16
	AllocatedContainers int16
	PendingMemory int16
	AvailableMemory int16
	PendingVCores int16
}

// Scalable is the interface that must be implemented from a scalable cluster
type Scalable interface {
	ScaleUp(int)
	ScaleDown(int)
}

// ClusterBase is the base class for any type of cluster
type ClusterBase struct {
	name string
	resourceManagerURI string
	MetricsSnapshot
}


// NewClusterBase is the constructor of ClusterBase struct
// @param clusterName is the name of the cluster
// @param rmYarnURL is the address and port which YARN Resource Manager listen to
// return the pointer to the ClusterBase instance
func NewClusterBase(clusterName string, rmYarnURL string) *ClusterBase {
	return &ClusterBase{
		clusterName,
		rmYarnURL,
		MetricsSnapshot{},
	}
}
