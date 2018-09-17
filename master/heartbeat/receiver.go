package heartbeat

import (
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"net"
	"obi/master/autoscaler"
	"obi/master/autoscaler/policies"
	"obi/master/model"
	"obi/master/platforms"
	"time"
	"obi/master/pool"
)

// Receiver class with properties
type Receiver struct {
	pool *pool.Pool
}

// channel to interrupt the heartbeat receiver routine
var quit chan struct{}

// UDP connection
var conn *net.UDPConn

// New is the constructor of the heartbeat Receiver struct
// @param pool contains the clusters to update regularly
// @param deleteTimeout is the time interval after which a cluster is assumed down
// @param trackerInterval is the time interval for which the clusters tracker is triggered
// return the pointer to the instance
func New(pool *pool.Pool) *Receiver {
	r := &Receiver{
		pool,
	}

	return r
}

// Start the execution of the heartbeat receiver
func (receiver *Receiver) Start() {
	quit = make(chan struct{})
	logrus.Info("Starting heartbeat receiver routine.")
	go receiverRoutine(receiver.pool)
}

// goroutine which listens to new heartbeats from cluster masters. It will be stop when an empty object is inserted in
// the `quit` channel
// @param pool contains the available clusters to update with new metrics
func receiverRoutine(pool *pool.Pool) {
	var err error

	// listen to incoming udp packets
	addr := net.UDPAddr{
		Port: 8080,
		IP:   net.ParseIP("0.0.0.0"),
	}

	conn, err = net.ListenUDP("udp", &addr)
	if err != nil {
		logrus.WithField("error", err).Error("'ListenUDP' method call for creating new UDP server failed")
		return
	}

	for {
		data := make([]byte, 4096)
		n, err:= conn.Read(data)
		if err != nil {
			select {
			case <-quit:
				logrus.Info("Closing heartbeat receiver routine.")
				// the error was caused by the closing of the listener
				return
			default:
				// temporary error - let's continue
				continue
			}

		}

		m := &HeartbeatMessage{}
		err = proto.Unmarshal(data[0:n], m)

		if err != nil {
			logrus.WithField("error", err).Error("'Unmarshal' method call for new heartbeat message failed")
			continue
		}

		newMetrics := model.Metrics{
			time.Now(),
			m.GetPendingContainers(),
			m.GetPendingMB(),
			m.GetAvailableMB(),
			m.GetAppAttemptFirstContainerAllocationDelayAvgTime(),
			m.GetAggregateContainersAllocated(),
			m.GetAggregateContainersReleased(),
			m.GetAMResourceLimitMB(),
			m.GetAMResourceLimitVCores(),
			m.GetUsedAMResourceMB(),
			m.GetUsedAMResourceVCores(),
			m.GetAppsSubmitted(),
			m.GetAppsRunning(),
			m.GetAppsPending(),
			m.GetAppsCompleted(),
			m.GetAppsKilled(),
			m.GetAppsFailed(),
			m.GetAggregateContainersPreempted(),
			m.GetActiveApplications(),
			m.GetAppAttemptFirstContainerAllocationDelayNumOps(),
			m.GetAppAttemptFirstContainerAllocationDelayAvgTime(),
			m.GetAllocatedMB(),
			m.GetAllocatedVCores(),
			m.GetAllocatedContainers(),
			m.GetAggregateContainersAllocated(),
			m.GetAggregateContainersReleased(),
			m.GetAvailableMB(),
			m.GetAvailableVCores(),
			m.GetPendingMB(),
			m.GetPendingVCores(),
			m.GetNumberOfNodes(),
		}

		if value, ok := pool.GetCluster(m.GetClusterName()); ok {
			cluster := value.(model.ClusterBaseInterface)
			cluster.AddMetricsSnapshot(newMetrics)
			logrus.WithField("clusterName", m.GetClusterName()).Info("Metrics updated")
		} else {
			logrus.Info("Received metrics for a cluster not in the pool.")

			newCluster, err := platforms.NewExistingCluster(m.GetServiceType(), m.GetClusterName())
			if err == nil {
				policy := policies.NewWorkload(0.5)
				a := autoscaler.New(policy, 60, newCluster.(model.Scalable), false)
				pool.AddCluster(newCluster, a)

				logrus.WithField("clusterName", m.GetClusterName()).Info("Added cluster in the pool")
			} else {
				logrus.WithField("Error", err).Error("Existing cluster not inserted in the pool")
			}
		}
	}
}

// Stop the execution of the receiver goroutines
func (receiver *Receiver) Stop() {
	close(quit)
	conn.Close()
}
