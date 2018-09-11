package autoscaler

import (
	"github.com/sirupsen/logrus"
	"obi/master/model"
	"obi/master/utils"
	"time"
)

// Autoscaler class with properties
type Autoscaler struct {
	Policy Policy
	Timeout int16
	quit chan struct{}
	managedCluster model.Scalable
}

// Policy defines the primitive methods that must be implemented for any type of autoscaling policy
type Policy interface {
	Apply(*utils.ConcurrentSlice) int32
}

// New is the constructor of Autoscaler struct
// @param algorithm is the algorithm to follow during scaling policy execution
// @param timeout is the time interval to wait before triggering the scaling-check action again
// @param sustainedTimeoutInterval is the time interval to wait before triggering the scaling action again, when a
// 	`scale-up` or `scale-down` was triggered
// @param cluster is the scalable cluster to be managed
// return the pointer to the instance
func New(
	policy Policy,
	timeout int16,
	cluster model.Scalable,
	) *Autoscaler {
	return &Autoscaler{
		policy,
		timeout,
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
			delta = as.Policy.Apply(as.managedCluster.(model.ClusterBaseInterface).GetMetricsWindow())

			if delta != 0 {
				as.managedCluster.Scale(delta)
			}
			time.Sleep(time.Duration(as.Timeout) * time.Second)
		}
	}
}
