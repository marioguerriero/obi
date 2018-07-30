package heartbeat

import (
	"obi/utils"
	"net"
	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
		"obi/model"
	)

// HeartbeatReceiver class with properties
type HeartbeatReceiver struct {
	pool *utils.ConcurrentMap
	DeleteTimeout int16
}

// singleton instance
var hbReceiverInstance *HeartbeatReceiver

// channel to interrupt the heartbeat receiver routine
var quit chan struct{}


// GetInstance if for getting the singleton instance of the heartbeat receiver
// @param clustersMap is the pool of the available clusters to update regularly
// @param deleteTimeout is the time interval after which a cluster is assumed down
// return the pointer to the instance
func GetInstance(clustersMap *utils.ConcurrentMap, deleteTimeout int16) *HeartbeatReceiver {
	if hbReceiverInstance == nil {
		hbReceiverInstance = &HeartbeatReceiver{
			clustersMap,
			deleteTimeout,
		}
	}
	return hbReceiverInstance
}

// Start the execution of the heartbeat receiver
func (hbReceiver *HeartbeatReceiver) Start() {
	quit = make(chan struct{})
	go hbReceiverRoutine(hbReceiver)
}

// goroutine which listens to new heartbeats from cluster masters. It will be stop when an empty object is inserted in
// the `quit` channel
// @param hbReceiver is the heartbeat receiver instance
func hbReceiverRoutine(hbReceiver *HeartbeatReceiver) {
	// listen to incoming udp packets
	ln, err := net.Listen("udp", ":8080")
	if err != nil {
		glog.Error("'ListenPacked' method call for creating new UDP server failed")
	}
	defer ln.Close()

	for {
		if conn, err := ln.Accept(); err == nil {

			data := make([]byte, 4096)
			n, err:= conn.Read(data)
			if err != nil {
				glog.Error("'Read' method call for accepting new connection failed")
				continue
			}
			conn.Close()

			hbMessage := &HeartbeatMessage{}
			err = proto.Unmarshal(data[0:n], hbMessage)
			if err != nil {
				glog.Error("'Unmarshal' method call for new heartbeat message failed")
				continue
			}

			newMetrics := model.Metrics{
				PendingContainers:   hbMessage.GetPendingContainers(),
				AllocatedContainers: hbMessage.GetPendingContainers(),
				PendingMemory:       hbMessage.GetPendingMB(),
				AvailableMemory:     hbMessage.GetAvailableMB(),
				PendingVCores:       hbMessage.GetPendingVCores(),
			}

			if value, ok := hbReceiver.pool.Get(hbMessage.GetClusterName()); ok {
				cluster := value.(model.ClusterBaseInterface)
				cluster.SetMetricsSnapshot(newMetrics)
			}
		}
	}

}

// Stop the execution of the heartbeat receiver
func (hbReceiver *HeartbeatReceiver) Stop() {
	// TO DO
}
