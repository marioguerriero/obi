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
	status MetricsSnapshot
}
