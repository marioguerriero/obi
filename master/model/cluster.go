// Copyright 2018 Delivery Hero Germany
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
//     Unless required by applicable law or agreed to in writing, software
//     distributed under the License is distributed on an "AS IS" BASIS,
//     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//     See the License for the specific language governing permissions and
//     limitations under the License.

package model

import (
	"obi/master/utils"
	"sync"
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
	Jobs *utils.ConcurrentSlice
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
	AllocateResources(highPerformance bool) error
	FreeResources() error
	MonitorJobs()
	GetAllocatedJobSlots() int
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
		Jobs: 		   utils.NewConcurrentSlice(0, false),
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

