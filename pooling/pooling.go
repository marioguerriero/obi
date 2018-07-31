package pooling

import (
	"obi/platforms"
	"obi/model"
)

// Pooling class with properties
type Pooling struct {

}

// New is the constructor of Pooling struct
func New() *Pooling {
	return &Pooling{}
}

// SubmitPySparkJob is for submitting a new Spark job in Python environment
// @param clusterName is the name of the cluster where to run the new job
// @param scriptURI is the script path
// TODO this is only a fake method -> pooling logic should choose the cluster, not the user!
func (p *Pooling) SubmitPySparkJob(clusterName string, scriptURI string) {

	// Create cluster object
	cluster := platforms.NewDataprocCluster(&model.ClusterBase{
		Name: clusterName,
		Nodes: 3,
	}, "dhg-data-intelligence-ops", "europe-west3-b","global", 1, 0.3)

	// Allocate cluster resources
	err := cluster.AllocateResources()
	if err == nil {
		// Schedule some jobs
		cluster.SubmitJob(scriptURI)
	}
}
