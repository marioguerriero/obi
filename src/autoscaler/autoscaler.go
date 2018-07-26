package autoscaler

import (
	"time"
)

// ScalingAlgorithm is the enum type to specify different scaling algorithms
type ScalingAlgorithm int
const (
	// ThroughputBased scales the cluster to meet Time Of Completion constraints
	ThroughputBased ScalingAlgorithm = iota
	// WorkloadBased scales the cluster when the resource utilization is too high
	WorkloadBased
)

// Autoscaler class with properties
type Autoscaler struct {
	Algorithm ScalingAlgorithm
	Timeout int16
	SustainedTimeout int16
	YarnURL string
	quit chan struct{}
}

// New is the constructor of Autoscaler struct
// @param algorithm is the algorithm to follow during scaling policy execution
// @param timeoutInterval is the time interval to wait before triggering the scaling-check action again
// @param sustainedTimeoutInterval is the time interval to wait before triggering the scaling action again, when a
// 	`scale-up` or `scale-down` was triggered
// @param pool is the pointer to the array of active clusters
// @param yarnURL is the address and port which YARN Resource Manager listen to
// return the pointer to the instance
func New(algorithm ScalingAlgorithm, timeout int16, sustainedTimeout int16, yarnURL string) *Autoscaler {
	return &Autoscaler{
		algorithm,
		timeout,
		sustainedTimeout,
		yarnURL,
		make(chan struct{}),
	}
}


// StartMonitoringScale starts the execution of the autoscaler
func (as *Autoscaler) StartMonitoringScale() {
	go autoscalerRoutine(as)
}

// StopMonitoringScale stops the execution of the autoscaler
func (as *Autoscaler) StopMonitoringScale() {
	as.quit <- struct{}{}
}

// goroutine which apply the scaling policy at each time interval. It will be stop when an empty object is inserted in
// the `quit` channel
// @param as is the autoscaler
func autoscalerRoutine(as *Autoscaler) {
	for {
		select {
		case <-as.quit:
			break
		default:
			if as.Algorithm == WorkloadBased {
				// do something
			} else if as.Algorithm == ThroughputBased {
				// do something
			}
			time.Sleep(time.Duration(as.Timeout) * time.Second)
		}
	}
}
