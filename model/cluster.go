package model

type MetricsSnapshot struct {
	PendingContainer int16
	AllocatedContainer int16
	PendingMemory int16
	AvailableMemory int16
	PendingVCores int16

}

type Cluster interface {
	ScaleUp(int)
	ScaleDown(int)
}

type ClusterBase struct {
	name string
	resourceManagerURI string
	status MetricsSnapshot
}
