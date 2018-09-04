package autoscaler

import (
	"time"
	"obi/master/model"
	"github.com/sirupsen/logrus"
	"obi/master/utils"
	"fmt"
)

// ScalingAlgorithm is the enum type to specify different scaling algorithms
type ScalingAlgorithm int
const (
	// TimeBased scales the cluster to meet Time Of Completion constraints
	TimeBased ScalingAlgorithm = iota
	// WorkloadBased scales the cluster when the resource utilization is too high
	WorkloadBased
)

// Autoscaler class with properties
type Autoscaler struct {
	Algorithm ScalingAlgorithm
	Timeout int16
	SustainedTimeout int16
	quit chan struct{}
	managedCluster model.Scalable
}

var expCount int32

// New is the constructor of Autoscaler struct
// @param algorithm is the algorithm to follow during scaling policy execution
// @param timeout is the time interval to wait before triggering the scaling-check action again
// @param sustainedTimeoutInterval is the time interval to wait before triggering the scaling action again, when a
// 	`scale-up` or `scale-down` was triggered
// @param cluster is the scalable cluster to be managed
// return the pointer to the instance
func New(algorithm ScalingAlgorithm, timeout int16, sustainedTimeout int16, cluster model.Scalable) *Autoscaler {
	return &Autoscaler{
		algorithm,
		timeout,
		sustainedTimeout,
		make(chan struct{}),
		cluster,
	}
}


// StartMonitoring starts the execution of the autoscaler
func (as *Autoscaler) StartMonitoring() {
	go autoscalerRoutine(as)
}

// StopMonitoring stops the execution of the autoscaler
func (as *Autoscaler) StopMonitoring() {
	close(as.quit)
}

// goroutine which apply the scaling policy at each time interval. It will be stop when an empty object is inserted in
// the `quit` channel
// @param as is the autoscaler
func autoscalerRoutine(as *Autoscaler) {
	var deltaNodes int32
	for {
		select {
		case <-as.quit:
			logrus.WithField("clusterName", as.managedCluster.(model.ClusterBaseInterface).GetName()).Info(
				"Closing autoscaler routine.")
			return
		default:
			deltaNodes = applyPolicy(
					as.managedCluster.(model.ClusterBaseInterface).GetMetricsWindow(),
					as.Algorithm,
			)

			if deltaNodes != 0 {
				as.managedCluster.Scale(deltaNodes)
			}
			//for deltaNodes > 0 && deltaNodes < 64 {
			//	as.managedCluster.Scale(deltaNodes)
			//	time.Sleep(time.Duration(as.SustainedTimeout) * time.Second)
			//	deltaNodes = applyPolicy(
			//		as.managedCluster.(model.ClusterBaseInterface).GetMetricsWindow(),
			//		as.Algorithm,
			//	)
			//}
			//
			//for deltaNodes < 0 {
			//	noSecondaryWorkers := as.managedCluster.Scale(deltaNodes)
			//	if noSecondaryWorkers {
			//		break
			//	}
			//	time.Sleep(time.Duration(as.SustainedTimeout) * time.Second)
			//	deltaNodes = applyPolicy(
			//		as.managedCluster.(model.ClusterBaseInterface).GetMetricsWindow(),
			//		as.Algorithm,
			//	)
			//}
			time.Sleep(time.Duration(as.Timeout) * time.Second)
		}
	}
}

func applyPolicy(metricsWindow *utils.ConcurrentSlice, algorithm ScalingAlgorithm) int32 {
	switch algorithm {
	case WorkloadBased:
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
				fmt.Printf("Allocated containers: %d\n", hb.TotalContainersAllocated)
				fmt.Printf("Released containers: %d\n", hb.TotalContainersReleased)
				fmt.Printf("Released containers before: %d\n", previousMetrics.TotalContainersReleased)
				throughput += float32(hb.TotalContainersReleased - previousMetrics.TotalContainersReleased)

				if hb.PendingContainers > 0 {
					fmt.Printf("Pending containers: %d\n", hb.PendingContainers)
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
			return expCount
		}
		logrus.Info("Applying workload-based policy")
	case TimeBased:
		var count int32
		var memoryUsage int32
		workerMemory := 15000.0

		for obj := range metricsWindow.Iter() {
			if obj.Value == nil {
				continue
			}

			hb := obj.Value.(model.Metrics)
			memoryUsage += hb.PendingMemory - hb.AvailableMemory
			count++
		}

		if count > 0 {
			workers := float64(memoryUsage / count) / workerMemory
			fmt.Printf("Exact workers: %f\n", workers)
			return int32(workers)
		}
		logrus.Info("Applying time-based policy")
	default:
		logrus.WithField("algorithm", algorithm).Error("Unknown algorithm")
	}
	return 0
}
