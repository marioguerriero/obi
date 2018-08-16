package pooling

import (
	"obi/master/platforms"
	"obi/master/model"
	"obi/master/utils"
	"obi/master/autoscaler"
	"github.com/spf13/viper"
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
func (p *Pooling) SubmitPySparkJob(clusterName string, scriptURI string) {

	// Create cluster object
	// TODO: Create a cluster dynamically.
	var cluster model.ClusterBaseInterface
	var err error
	if p.pool.Len() == 0 {
		cb := model.NewClusterBase("obi-test", 2,
			"dataproc",
			viper.GetString("heartbeatHost"),
			viper.GetInt("heartbeatPort"))

		cluster = platforms.NewDataprocCluster(cb, viper.GetString("projectId"),
			viper.GetString("zone"),
			viper.GetString("region"), 1, 0.3)

		// Allocate cluster resources
		err = cluster.AllocateResources()
	} else {
		obj, _ := p.pool.Get("obi-test")
		cluster = obj.(model.ClusterBaseInterface)

		err = nil
	}

	if err == nil {
		// Add to pool
		p.pool.Set(clusterName, cluster)
		a := autoscaler.New(autoscaler.WorkloadBased, 30, 15, cluster.(model.Scalable))
		a.StartMonitoring()

		// Schedule some jobs
		cluster.SubmitJob(scriptURI)
	}
}
