package policies

import (
	"obi/master/utils"
	"obi/master/model"
	"fmt"
	"github.com/sirupsen/logrus"
)

var expCount int32

// Workload scales the cluster when the resource utilization is too high
func Workload(metricsWindow *utils.ConcurrentSlice) int32 {
	var previousMetrics model.Metrics
	var throughput float32
	var pendingGrowthRate float32
	var count int8

	logrus.Info("Applying workload-based policy")
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
		if throughput < pendingGrowthRate {
			// scale up
			if expCount <= 0 {
				expCount = 1
			} else {
				expCount = expCount << 1
			}
		} else if (pendingGrowthRate == 0) || (throughput > pendingGrowthRate) {
			// scale down
			if expCount >= 0 {
				expCount = -1
			} else {
				expCount = expCount << 1
			}
		} else {
			expCount = 0
		}
		if expCount == 64 || expCount < 0 {
			expCount = 0
		}
	}
	return expCount
}
