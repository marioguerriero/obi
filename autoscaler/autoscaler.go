package autoscaler

import . "obi/model"


type ScalingAlgorithm int
const (
	ThroughputBased ScalingAlgorithm = iota
	WorkloadBased
)

type Autoscaler struct {
	algorithm ScalingAlgorithm
	clusterPool *[]Cluster
	timeout int16
	sustainedTimeout int16
}
//
var autoscalerInstance *Autoscaler

/**
* Get the singleton instance of autoscaler
* @param algorithm is the algorithm to follow during scaling policy execution
* @param timeoutInterval is the time interval to wait before triggering the scaling-check action again
* @param sustainedTimeoutInterval is the time interval to wait before triggering the scaling action again, when a
* 	`scale-up` or `scale-down` was triggered
* @param pool is the pointer to the array of active clusters
* return the pointer to the instance
 */
func New(algorithm ScalingAlgorithm, timeout int16, sustainedTimeout int16, pool *[]Cluster) *Autoscaler {
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

}

/**
* Stop the execution of the autoscaler
 */
func (as *Autoscaler) Stop() {

}