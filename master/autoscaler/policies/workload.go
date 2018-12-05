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

package policies

import (
							"obi/master/utils"
	"obi/master/model"
		"math"
	)


// WorkloadPolicy contains all useful state-variable to apply the policy
type WorkloadPolicy struct {
	scale float32
}

// NewWorkload is the constructor of the WorkloadPolicy struct
func NewWorkload(scale float32) *WorkloadPolicy {
	return &WorkloadPolicy{
		scale: scale,
	}
}

// Apply is the implementation of the Policy interface
func (p *WorkloadPolicy) Apply(metricsWindow *utils.ConcurrentSlice) int32 {
	var previousMetrics model.HeartbeatMessage
	var throughput float32
	var pendingGrowthRate float32
	var count int8

	for obj := range metricsWindow.Iter() {
		if obj.Value == nil {
			continue
		}

		hb := obj.Value.(model.HeartbeatMessage)

		if previousMetrics.ClusterName != "" {
			throughput += float32(hb.AggregateContainersReleased - previousMetrics.AggregateContainersReleased)
			if hb.PendingContainers > 0 {
				memoryContainer := hb.PendingMB / hb.PendingContainers
				containersWillConsumed := hb.AvailableMB / memoryContainer
				pendingGrowth := float32(hb.PendingContainers - containersWillConsumed - previousMetrics.PendingContainers)
				if pendingGrowth > 0 {
					pendingGrowthRate += pendingGrowth
				}
			}

			count++
		}
		previousMetrics = hb
	}

	if count > 0 {
		throughput /= float32(count)
		pendingGrowthRate /= float32(count)

		workerMemory := (previousMetrics.AvailableMB + previousMetrics.AllocatedMB) / previousMetrics.NumberOfNodes

		// compute the number of containers that fit in each node
		var containersPerNode int32
		if previousMetrics.AllocatedContainers > 0 {
			memoryContainer := previousMetrics.AllocatedMB / previousMetrics.AllocatedContainers
			containersPerNode = workerMemory / memoryContainer
		} else if previousMetrics.PendingContainers > 0 {
			memoryContainer := previousMetrics.PendingMB / previousMetrics.PendingContainers
			containersPerNode = workerMemory / memoryContainer
		} else {
			// unable to estimate the value - let's take the minimum
			containersPerNode = 2
		}

		if pendingGrowthRate == 0 && previousMetrics.AllocatedContainers > 0 {
			nodesUsed := math.Ceil(float64(previousMetrics.AllocatedContainers / containersPerNode))
			return int32(nodesUsed) - previousMetrics.NumberOfNodes
		}
		return int32((pendingGrowthRate - throughput) * (1 / float32(containersPerNode)) * p.scale)

	}

	return 0
}
