package model

type MetricsSnapshot struct {
	PendingContainer int
	PendingMemory int
}

type Cluster interface {
	ScaleUp()
	ScaleDown()
}

type ClusterBase struct {
	name string
	resourceManagerURI string
	status MetricsSnapshot
}
