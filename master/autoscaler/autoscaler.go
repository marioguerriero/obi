package autoscaler

import (
	"time"
	"obi/master/model"
	"github.com/sirupsen/logrus"
	"obi/master/utils"
	)

// Autoscaler class with properties
type Autoscaler struct {
	PolicyHandler func(*utils.ConcurrentSlice) int32
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
func New(
	policy func(*utils.ConcurrentSlice) int32,
	timeout int16,
	sustainedTimeout int16,
	cluster model.Scalable,
	) *Autoscaler {
	return &Autoscaler{
		policy,
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
	var delta int32
	for {
		select {
		case <-as.quit:
			logrus.WithField("clusterName", as.managedCluster.(model.ClusterBaseInterface).GetName()).Info(
				"Closing autoscaler routine.")
			return
		default:
			delta = as.PolicyHandler(as.managedCluster.(model.ClusterBaseInterface).GetMetricsWindow())

			if delta != 0 {
				as.managedCluster.Scale(delta)
			}
			time.Sleep(time.Duration(as.Timeout) * time.Second)
		}
	}
}
