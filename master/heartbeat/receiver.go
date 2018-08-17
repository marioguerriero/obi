package heartbeat

import (
	"obi/master/model"
	"net"
	"github.com/sirupsen/logrus"
	"github.com/golang/protobuf/proto"
	"time"
	"obi/master/platforms"
	"obi/master/autoscaler"
	"obi/master/pooling"
)

// Receiver class with properties
type Receiver struct {
	pool *pooling.Pool
	DeleteTimeout int16
	TrackerInterval int16
}

// channel to interrupt the heartbeat receiver routine
var quit chan struct{}

// UDP connection
var conn *net.UDPConn

// New is the constructor of the heartbeat Receiver struct
// @param clustersMap is the pool of the available clusters to update regularly
// @param deleteTimeout is the time interval after which a cluster is assumed down
// @param trackerInterval is the time interval for which the clusters tracker is triggered
// return the pointer to the instance
func New(clustersMap *pooling.Pool, deleteTimeout int16, trackerInterval int16) *Receiver {
	r := &Receiver{
		clustersMap,
		deleteTimeout,
		trackerInterval,
	}

	return r
}

// Start the execution of the heartbeat receiver
func (receiver *Receiver) Start() {
	quit = make(chan struct{})
	logrus.Info("Starting heartbeat receiver routine.")
	go receiverRoutine(receiver.pool)
	logrus.Info("Starting cluster tracker routine.")
	go clustersTrackerRoutine(receiver.pool, receiver.DeleteTimeout, receiver.TrackerInterval)
}

// goroutine which listens to new heartbeats from cluster masters. It will be stop when an empty object is inserted in
// the `quit` channel
// @param pool is the map containing all the available clusters
func receiverRoutine(pool *pooling.Pool) {
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
		}

		if value, ok := pool.GetCluster(m.GetClusterName()); ok {
			cluster := value.(model.ClusterBaseInterface)
			cluster.AddMetricsSnapshot(newMetrics)
			logrus.WithField("clusterName", m.GetClusterName()).Info("Metrics updated")
		} else {
			logrus.Info("Received metrics for a cluster not in the pool.")

			newCluster, err := platforms.NewExistingCluster(m.GetServiceType(), m.GetClusterName())
			if err == nil {
				a := autoscaler.New(autoscaler.WorkloadBased, 30, 15, newCluster.(model.Scalable))
				pool.AddCluster(newCluster, a)

				logrus.WithField("clusterName", m.GetClusterName()).Info("Added cluster in the pool")
			}
		}
	}
}

// goroutine which periodically removes outdated/down clusters. It will be stop when the `quit` channel is closed
// @param pool is the map containing all the available clusters
// @param timeout is the time interval after which a cluster must be removed from the pool
func clustersTrackerRoutine(pool *pooling.Pool, timeout int16, interval int16) {

	for {
		select {
		case <-quit:
			logrus.Info("Closing cluster tracker routine.")
			return
		default:
			for pair := range pool.Clusters() {
				clusterName := pair.Key
				cluster := pair.Value.(model.ClusterBaseInterface)
				var lastHeartbeat model.Metrics
				for hb := range cluster.GetMetricsWindow().Iter() {
					if hb.Value != nil {
						lastHeartbeat = hb.Value.(model.Metrics)
					}
				}
				if lastHeartbeat != (model.Metrics{}) {
					lastHeartbeatInterval := int16(time.Now().Sub(lastHeartbeat.Timestamp).Seconds())
					if lastHeartbeatInterval > timeout {
						logrus.WithField("Name", clusterName).Info("Deleting cluster.")
						pool.RemoveCluster(clusterName)
					}
				}
			}
			time.Sleep(time.Duration(interval) * time.Second)
		}
	}
}

// Stop the execution of the receiver goroutines
func (receiver *Receiver) Stop() {
	close(quit)
	conn.Close()
}
