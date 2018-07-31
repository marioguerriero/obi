package main

import (
	"obi/platforms"
	"obi/model"
	"os"
	"github.com/golang/glog"
)

func main() {
	// Read project name
	proj := "projects/" + os.Getenv("GOOGLE_CLOUD_PROJECT")
	if proj == "" {
		glog.Error(" GOOGLE_CLOUD_PROJECT env not set")
	}

	// Create cluster object
	cluster := platforms.NewDataprocCluster(&model.ClusterBase{
		Name: "obi-test-cluster",
		Nodes: 3,
	}, proj, "europe-west3-b","europe-west3", 1, 0.3)

	// Allocate cluster resources
	cluster.AllocateResources()

	// Schedule some jobs
	cluster.SubmitJob("gc://dhg-obi/cluster-script/word_count.py")
}
