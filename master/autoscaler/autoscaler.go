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
	var shouldScaleUp, shouldScaleDown bool
	for {
		select {
		case <-as.quit:
			logrus.WithField("clusterName", as.managedCluster.(model.ClusterBaseInterface).GetName()).Info(
				"Closing autoscaler routine.")
			return
		default:
			shouldScaleUp, shouldScaleDown = applyPolicy(
					as.managedCluster.(model.ClusterBaseInterface).GetMetricsWindow(),
					as.Algorithm,
			)

			var nodes int32 = 1
			for shouldScaleUp && nodes < 64 {
				as.managedCluster.Scale(nodes, true)
				time.Sleep(time.Duration(as.SustainedTimeout) * time.Second)
				shouldScaleUp, shouldScaleDown = applyPolicy(
					as.managedCluster.(model.ClusterBaseInterface).GetMetricsWindow(),
					as.Algorithm,
				)
				nodes = nodes << 1
			}

			nodes = 1
			for shouldScaleDown {
				as.managedCluster.Scale(1, false)
				time.Sleep(time.Duration(as.SustainedTimeout) * time.Second)
				_, shouldScaleDown = applyPolicy(
					as.managedCluster.(model.ClusterBaseInterface).GetMetricsWindow(),
					as.Algorithm,
				)
				nodes = nodes << 1
			}
			time.Sleep(time.Duration(as.Timeout) * time.Second)
		}
	}
}

func applyPolicy(metricsWindow *utils.ConcurrentSlice, algorithm ScalingAlgorithm) (bool, bool) {
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
				return true, false
			} else if (pendingGrowthRate == 0) || (throughput > pendingGrowthRate) {
				return false, true
			}
		} else {
			fmt.Println("No metrics available")
		}
		logrus.Info("Applying workload-based policy")
	case TimeBased:
		// TODO
		logrus.Info("Applying time-based policy")
	default:
		logrus.WithField("algorithm", algorithm).Error("Unknown algorithm")
	}
	return false, false
}
