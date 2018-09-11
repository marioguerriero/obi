package pooling

import (
	"obi/master/model"
	"github.com/sirupsen/logrus"
	"errors"
	"obi/master/platforms"
	"github.com/spf13/viper"
		)

func newCluster(name, platform string) (model.ClusterBaseInterface, error) {
	logrus.WithField("cluster-name", name).Info("Creating new cluster")

	switch platform {
	case "dataproc":
		return newDataprocCluster(name)
	default:
		logrus.WithField("platform-type", platform).Error("Invalid platform type")
		return nil, errors.New("invalid platform type")
	}
}

func newDataprocCluster(name string) (*platforms.DataprocCluster, error) {
	cb := model.NewClusterBase(name, 2, "dataproc",
		viper.GetString("heartbeatHost"),
		viper.GetInt("heartbeatPort"))

	cluster := platforms.NewDataprocCluster(cb, viper.GetString("projectId"),
		viper.GetString("zone"),
		viper.GetString("region"), 0)

	// Allocate cluster resources
	err := cluster.AllocateResources()
	if err != nil {
		return nil, err
	}

	return cluster, nil
}
