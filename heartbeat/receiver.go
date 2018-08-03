package heartbeat

import (
	"obi/utils"
	"net"
		"github.com/golang/protobuf/proto"
	"obi/model"
	"time"
	"fmt"
	"obi/platforms"
	"github.com/sirupsen/logrus"
)

// Receiver class with properties
type Receiver struct {
	pool *utils.ConcurrentMap
	DeleteTimeout int16
	TrackerInterval int16
}

// singleton instancevg
var receiverInstance *Receiver

// channel to interrupt the heartbeat receiver routine
var quit chan struct{}

// UDP connection
var conn *net.UDPConn

// GetInstance if for getting the singleton instance of the heartbeat receiver
// @param clustersMap is the pool of the available clusters to update regularly
// @param deleteTimeout is the time interval after which a cluster is assumed down
// @param trackerInterval is the time interval for which the clusters tracker is triggered
// return the pointer to the instance
func GetInstance(clustersMap *utils.ConcurrentMap, deleteTimeout int16, trackerInterval int16) *Receiver {
	if receiverInstance == nil {
		receiverInstance = &Receiver{
			clustersMap,
			deleteTimeout,
			trackerInterval,
		}
	}
	return receiverInstance
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
func receiverRoutine(pool *utils.ConcurrentMap) {
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
		fmt.Println(n)
		m := &HeartbeatMessage{}
		err = proto.Unmarshal(data[0:n], m)
		fmt.Println(m)
		if err != nil {
			logrus.WithField("error", err).Error("'Unmarshal' method call for new heartbeat message failed")
			continue
		}

		newMetrics := model.Metrics{
			Timestamp:           time.Now(),
			PendingContainers:   m.GetPendingContainers(),
			AllocatedContainers: m.GetPendingContainers(),
			PendingMemory:       m.GetPendingMB(),
			AvailableMemory:     m.GetAvailableMB(),
			PendingVCores:       m.GetPendingVCores(),
		}

		if value, ok := pool.Get(m.GetClusterName()); ok {
			cluster := value.(model.ClusterBaseInterface)
			cluster.SetMetricsSnapshot(newMetrics)
			logrus.WithField("clusterName", m.GetClusterName()).Info("Metrics updated")
		} else {
			logrus.Info("Received metrics for a cluster not in the pool.")

			newCluster, err := platforms.NewExistingCluster(m.GetServiceType(), m.GetClusterName())
			if err == nil {
				pool.Set(m.GetClusterName(), newCluster)

				logrus.WithField("clusterName", m.GetClusterName()).Info("Added cluster in the pool")
			}
		}
	}
}

// goroutine which periodically removes outdated/down clusters. It will be stop when the `quit` channel is closed
// @param pool is the map containing all the available clusters
// @param timeout is the time interval after which a cluster must be removed from the pool
func clustersTrackerRoutine(pool *utils.ConcurrentMap, timeout int16, interval int16) {

	for {
		select {
		case <-quit:
			logrus.Info("Closing cluster tracker routine.")
			return
		default:
			for pair := range pool.Iter() {
				key := pair.Key
				cluster := pair.Value.(model.ClusterBaseInterface)
				lastHeartbeatInterval := int16(time.Now().Sub(cluster.GetMetricsSnapshot().Timestamp).Seconds())
				if lastHeartbeatInterval > timeout {
					logrus.WithField("Name", key).Info("Deleting cluster.")
					pool.Delete(key)
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
