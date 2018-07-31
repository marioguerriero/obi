package heartbeat

import (
	"obi/utils"
	"net"
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
		"obi/model"
	"time"
)

// Receiver class with properties
type Receiver struct {
	pool *utils.ConcurrentMap
	DeleteTimeout int16
	TrackerInterval int16
}

// singleton instance
var receiverInstance *Receiver

// channel to interrupt the heartbeat receiver routine
var quit chan struct{}

// listener for UDP packets
var ln net.Listener


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
	glog.Info("Starting heartbeat receiver routine.")
	go receiverRoutine(receiver.pool)
	glog.Info("Starting cluster tracker routine.")
	go clustersTrackerRoutine(receiver.pool, receiver.DeleteTimeout, receiver.TrackerInterval)
}

// goroutine which listens to new heartbeats from cluster masters. It will be stop when an empty object is inserted in
// the `quit` channel
// @param pool is the map containing all the available clusters
func receiverRoutine(pool *utils.ConcurrentMap) {
	var err error

	// listen to incoming udp packets
	ln, err = net.Listen("udp", ":8080")
	if err != nil {
		glog.Errorf("'Listen' method call for creating new UDP server failed: %s")
		return
	}

	for {
		if conn, err := ln.Accept(); err == nil {

			data := make([]byte, 4096)
			n, err:= conn.Read(data)
			if err != nil {
				glog.Errorf("'Read' method call for accepting new connection failed: %s", err)
				continue
			}
			conn.Close()

			m := &HeartbeatMessage{}
			err = proto.Unmarshal(data[0:n], m)
			if err != nil {
				glog.Errorf("'Unmarshal' method call for new heartbeat message failed: %s", err)
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
				glog.Infof("Metrics updated for cluster '%s'.", m.GetClusterName())
			} else {
				glog.Error("Received metrics for a cluster not in the pool.")
			}
		} else {
			glog.Error(err.Error())
			select {
			case <-quit:
				glog.Info("Closing heartbeat receiver routine.")
				// the error was caused by the closing of the listener
				break
			default:
				// temporary error - let's continue
			}
		}
	}
}

// goroutine which periodically removes outdated/down clusters. It will be stop when an empty object is inserted in
// the `quit` channel
// @param pool is the map containing all the available clusters
// @param timeout is the time interval after which a cluster must be removed from the pool
func clustersTrackerRoutine(pool *utils.ConcurrentMap, timeout int16, interval int16) {

	for {
		select {
		case <-quit:
			glog.Info("Closing cluster tracker routine.")
			break
		default:
			for pair := range pool.Iter() {
				key := pair.Key
				cluster := pair.Value.(model.ClusterBaseInterface)
				lastHeartbeatInterval := int16(time.Now().Sub(cluster.GetMetricsSnapshot().Timestamp).Seconds())
				if lastHeartbeatInterval > timeout {
					glog.Infof("Deleting cluster '%s'.", key)
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
	ln.Close()
}
