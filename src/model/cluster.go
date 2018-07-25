package model

// TO REVIEW
type MetricsSnapshot struct {
	PendingContainers int16
	AllocatedContainers int16
	PendingMemory int16
	AvailableMemory int16
	PendingVCores int16
}

// interface that must be implemented from a scalable cluster
type Scalable interface {
	ScaleUp(int)
	ScaleDown(int)
}

// base class for a any type of cluster
type ClusterBase struct {
	name string
	resourceManagerURI string
	MetricsSnapshot
}

/**
* Constructor of ClusterBase
* @param clusterName is the name of the cluster
* @param rmYarnURL is the address and port which YARN Resource Manager listen to
* return the pointer to the ClusterBase instance
 */
func NewClusterBase(clusterName string, rmYarnUrl string) *ClusterBase {
	return &ClusterBase{
		clusterName,
		rmYarnUrl,
		MetricsSnapshot{
			PendingContainers:   -1,
			AllocatedContainers: -1,
			PendingMemory:       -1,
			AvailableMemory:     -1,
			PendingVCores:       -1,
		},
	}
}
