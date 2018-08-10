package pooling

import (
	"obi/master/platforms"
	"obi/master/model"
	"obi/master/utils"
	"obi/master/autoscaler"
)

// Pooling class with properties
type Pooling struct {
	pool *utils.ConcurrentMap
}

// New is the constructor of Pooling struct
// @param clustersMap is the pool of the available clusters to update regularly
func New(clustersMap *utils.ConcurrentMap) *Pooling {
	return &Pooling{
		clustersMap,
	}
}

// SubmitPySparkJob is for submitting a new Spark job in Python environment
// @param clusterName is the name of the cluster where to run the new job
// @param scriptURI is the script path
// TODO this is only a fake method -> pooling logic should choose the cluster, not the user!
func (p *Pooling) SubmitPySparkJob(clusterName string, scriptURI string) {

	// Create cluster object
	// TODO: Define config variables for Google Dataproc.
	var cluster model.ClusterBaseInterface
	var err error
	if p.pool.Len() == 0 {
		cb := model.NewClusterBase("obi-test", 3, "dataproc", "35.198.69.116", 8080)
		cluster = platforms.NewDataprocCluster(cb, "dhg-data-intelligence-ops", "europe-west3-b","global", 1, 0.3)

		// Allocate cluster resources
		err = cluster.AllocateResources()
	} else {
		cIface, _ := p.pool.Get("obi-test")
		cluster = cIface.(model.ClusterBaseInterface)

		err = nil
	}

	if err == nil {
		// Add to pool
		p.pool.Set(clusterName, cluster)
		a := autoscaler.New(autoscaler.WorkloadBased, 15, 5, cluster.(model.Scalable))
		a.StartMonitoring()

		// Schedule some jobs
		cluster.SubmitJob(scriptURI)
	}
}
