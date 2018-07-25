package autoscaler

import (
	"time"
	u "obi/src/utils"
)

// enum for scaling algorithms types
type ScalingAlgorithm int
const (
	ThroughputBased ScalingAlgorithm = iota
	WorkloadBased
)

// Autoscaler class with properties
type Autoscaler struct {
	algorithm ScalingAlgorithm
	clusterPool *u.ConcurrentSlice
	timeout int16
	sustainedTimeout int16
}

// singleton instance
var autoscalerInstance *Autoscaler

// channel to interrupt the autoscaler routine
var quit chan struct{}

/**
* Get the singleton instance of autoscaler
* @param algorithm is the algorithm to follow during scaling policy execution
* @param timeoutInterval is the time interval to wait before triggering the scaling-check action again
* @param sustainedTimeoutInterval is the time interval to wait before triggering the scaling action again, when a
* 	`scale-up` or `scale-down` was triggered
* @param pool is the pointer to the array of active clusters
* return the pointer to the instance
 */
func New(algorithm ScalingAlgorithm, timeout int16, sustainedTimeout int16, pool *u.ConcurrentSlice) *Autoscaler {
	if autoscalerInstance == nil {
		autoscalerInstance = &Autoscaler{
			algorithm,
			pool,
			timeout,
			sustainedTimeout,
		}
	}
	return autoscalerInstance
}

/**
* Start the execution of the autoscaler
 */
func (as *Autoscaler) Start() {
	quit = make(chan struct{})
	go autoscalerRoutine(as)

}

/**
* Stop the execution of the autoscaler
 */
func (as *Autoscaler) Stop() {
	quit <- struct{}{}
}

/*
* goroutine which apply the scaling policy at each time interval. It will be stop when an empty object is inserted in
* the `quit` channel
* @param as is the autoscaler
 */
func autoscalerRoutine(as *Autoscaler) {
	for {
		select {
		case <-quit:
			break
		default:
			if as.algorithm == WorkloadBased {
				// do something
			} else if as.algorithm == ThroughputBased {
				// do something
			}
			time.Sleep(time.Duration(as.timeout) * time.Second)
		}
	}
}
