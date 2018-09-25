package model

import (
	"obi/master/utils"
	"sync"
	"sync/atomic"
	"time"
)

// ClusterStatus defines the status of a cluster
type ClusterStatus int
const (
	// ClusterStatusRunning attached to a cluster when it is running
	ClusterStatusRunning = iota
	// ClusterStatusClosed attached to a cluster when it is closed
	ClusterStatusClosed  = iota
)

// ClusterStatusNames descriptive names for different cluster statuses
var ClusterStatusNames = map[ClusterStatus]string {
	ClusterStatusRunning: "running",
	ClusterStatusClosed: "closed",
}

// Scalable is the interface that must be implemented from a scalable cluster
type Scalable interface {
	Scale(nodes int32) bool
}

// ClusterBase is the base class for any type of cluster
type ClusterBase struct {
	Name          string
	WorkerNodes   int32
	Platform      string
	CreationTimestamp time.Time
	Cost float32
	Status ClusterStatus
	HeartbeatHost string
	HeartbeatPort int
	AssignedJobs  int32
	metrics       *utils.ConcurrentSlice // not available outside package to prevent race conditions, get and set must be used
	sync.Mutex
}

// ClusterBaseInterface defines the primitive methods that must be implemented for any type of cluster
type ClusterBaseInterface interface {
	GetName() string
	GetPlatform() string
	GetCreationTimestamp() time.Time
	GetCost() float32
	GetStatus() ClusterStatus
	SetStatus(ClusterStatus)
	SubmitJob(*Job) error
	GetMetricsWindow() *utils.ConcurrentSlice
	AddMetricsSnapshot(message HeartbeatMessage)
	AllocateResources() error
	FreeResources() error
	AddJob()
	RemoveJob()
	GetAssignedJobs() int32
}


// NewClusterBase is the constructor of ClusterBase struct
// @param clusterName is the name of the cluster
// @param size is the number of nodes in the cluster
// @param platform is the cloud service environment name
// return the pointer to the ClusterBase instance
func NewClusterBase(clusterName string, workers int32, platform string, hbHost string, hbPort int) *ClusterBase {
	return &ClusterBase{
		Name:          clusterName,
		WorkerNodes:   workers,
		Platform:      platform,
		CreationTimestamp: time.Now(),
		HeartbeatHost: hbHost,
		HeartbeatPort: hbPort,
		metrics:       utils.NewConcurrentSlice(6, true),
	}
}

// GetMetrics is the getter of status field inside ClusterBase
// thread-safe
func (c *ClusterBase) GetMetrics() *utils.ConcurrentSlice {
	return c.metrics
}

// SetMetrics is the setter of status field inside ClusterBase
// thread-safe
func (c *ClusterBase) SetMetrics(newStatus HeartbeatMessage) {
	c.metrics.Append(newStatus)
}

// AddJob increments the internal counter of running jobs
// thread-safe
func (c *ClusterBase) AddJob() {
	c.AssignedJobs = atomic.AddInt32(&c.AssignedJobs, 1)
}

// RemoveJob decrements the internal counter of running jobs
// thread-safe
func (c *ClusterBase) RemoveJob() {
	c.AssignedJobs = atomic.AddInt32(&c.AssignedJobs, -1)
}

// GetAssignedJobs return the assigned jobs count
// thread-safe
func (c *ClusterBase) GetAssignedJobs() int32 {
	return atomic.LoadInt32(&c.AssignedJobs)
}