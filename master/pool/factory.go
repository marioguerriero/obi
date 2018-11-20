package pool

import (
	"errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"obi/master/autoscaler"
	"obi/master/autoscaler/policies"
	"obi/master/model"
	"obi/master/platforms"
	"os"
	"strconv"
)

func newCluster(name, platform string, highPerformance bool) (model.ClusterBaseInterface, error) {
	var cluster model.ClusterBaseInterface
	var err error

	logrus.WithField("cluster-name", name).Info("Creating new cluster")

	switch platform {
	case "dataproc":
		cluster, err = newDataprocCluster(name, highPerformance)
	default:
		logrus.WithField("platform-type", platform).Error("Invalid platform type")
		return nil, errors.New("invalid platform type")
	}

	// Check if there was an error creating the cluster structure
	if err != nil {
		logrus.WithField("platform-type", platform).Error("Could not create platform")
		return nil, err
	}

	return cluster, err
}

func newDataprocCluster(name string, highPerformance bool) (*platforms.DataprocCluster, error) {
	var minPreemptiveSize int32

	nodePort, _ := strconv.Atoi(os.Getenv("HEARTBEAT_SERVICE_NODEPORT"))

	cb := model.NewClusterBase(name, 2, "dataproc",
		viper.GetString("heartbeatHost"),
		nodePort)

	if highPerformance {
		minPreemptiveSize = 10
	}

	cluster := platforms.NewDataprocCluster(cb, viper.GetString("projectId"),
		viper.GetString("zone"),
		viper.GetString("region"), minPreemptiveSize)

	// Instantiate a new autoscaler for the new cluster and start monitoring
	var lambda float32 = 0.5
	policy := policies.NewWorkload(lambda)
	a := autoscaler.New(policy, 60, cluster, true)

	// Add in the pool
	GetPool().AddCluster(cluster, a)
	logrus.WithFields(logrus.Fields{
		"clusterName": name,
		"scalingFactor": lambda,
		"downscaling": true,
	}).Info("Autoscaler binding.")

	// Allocate cluster resources
	err := cluster.AllocateResources(highPerformance)
	if err != nil {
		return nil, err
	}

	// Start the autoscaler
	a.StartMonitoring()

	return cluster, nil
}
