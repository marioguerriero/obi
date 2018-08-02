package platforms

import (
	"obi/model"
	"github.com/golang/glog"
)

func NewExistingCluster(platform string, clusterName string) model.ClusterBaseInterface {
	var newCluster model.ClusterBaseInterface

	switch platform {
	case "dataproc":
		// TODO: Define config variables for Google Dataproc.
		newCluster = NewExistingDataprocCluster(
			"dhg-data-intelligence-ops",
			"global",
			"europe-west3-b",
			clusterName,
		)
	default:
		glog.Errorf("Platform '%s' unknown", platform)
	}
	return newCluster
}
