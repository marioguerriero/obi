package policies

import (
		"obi/master/utils"
	"obi/master/model"
	"fmt"
)


// WorkloadPolicy contains all useful state-variable to apply the policy
type WorkloadPolicy struct {
	scale float32
}

// NewWorkload is the constructor of the WorkloadPolicy struct
func NewWorkload(scaleFactor float32) *WorkloadPolicy {
	return &WorkloadPolicy{
		scale: scaleFactor,
	}
}

// Apply is the implementation of the Policy interface
func (p *WorkloadPolicy) Apply(metricsWindow *utils.ConcurrentSlice) int32 {
	var previousMetrics model.Metrics
	var throughput float32
	var pendingGrowthRate float32
	var count int8

	for obj := range metricsWindow.Iter() {
		if obj.Value == nil {
			continue
		}

		hb := obj.Value.(model.Metrics)

		if previousMetrics != (model.Metrics{}) {
			throughput += float32(hb.TotalContainersReleased - previousMetrics.TotalContainersReleased)

			if hb.PendingContainers > 0 {
				memoryContainer := hb.PendingMemory / hb.PendingContainers
				containersWillConsumed := hb.AvailableMemory / memoryContainer
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
		fmt.Printf("Throughput: %f\n", throughput)
		fmt.Printf("Pending rate: %f\n", pendingGrowthRate)
		return int32((pendingGrowthRate - throughput) * p.scale)
	}
	return 0
}
