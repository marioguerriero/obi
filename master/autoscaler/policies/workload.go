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
func NewWorkload(scaleFactor float32) *WorkloadPolicy {
	return &WorkloadPolicy{
		scaleFactor,
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

		if hb.ClusterName != "" {
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
		if pendingGrowthRate == 0 && previousMetrics.AllocatedContainers > 0 {
			workerMemory := (previousMetrics.AvailableMB + previousMetrics.AllocatedMB) / previousMetrics.NumberOfNodes
			memoryContainer := previousMetrics.AllocatedMB / previousMetrics.AllocatedContainers
			containersPerNode := workerMemory / memoryContainer
			nodesUsed := math.Ceil(float64(previousMetrics.AllocatedContainers / containersPerNode))
			return int32(nodesUsed) - previousMetrics.NumberOfNodes
		}
		return int32((pendingGrowthRate - throughput) * p.scale)
	}
	return 0
}
