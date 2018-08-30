package pooling

import (
	"obi/master/model"
	"github.com/spf13/viper"
	"obi/master/platforms"
	"obi/master/autoscaler"
	"github.com/sirupsen/logrus"
)

// Pooling class with properties
type Pooling struct {
	pool *Pool
}

// New is the constructor of Pooling struct
// @param pool contains the available clusters to use for job deployments
func New(pool *Pool) *Pooling {
	// TODO: Implement pooling. For the moment only a cluster to use

	logrus.Info("Creating pooling")
	cb := model.NewClusterBase("obi-test", 2,
		"dataproc",
		viper.GetString("heartbeatHost"),
		viper.GetInt("heartbeatPort"))

	cluster := platforms.NewDataprocCluster(cb, viper.GetString("projectId"),
		viper.GetString("zone"),
		viper.GetString("region"), 1, 0.3)

	// Allocate cluster resources
	err := cluster.AllocateResources()

	if err == nil {
		// Instantiate a new autoscaler for the new cluster and start monitoring
		a := autoscaler.New(autoscaler.WorkloadBased, 60, 30, cluster)
		a.StartMonitoring()

		// Add to pool
		pool.AddCluster(cluster, a)
	}

	logrus.Info("Created pool of clusters")
	return &Pooling{
		pool,
	}
}

// SubmitPySparkJob is for submitting a new Spark job in Python environment
// @param clusterName is the name of the cluster where to run the new job
// @param scriptURI is the script path
func (p *Pooling) SubmitPySparkJob(clusterName string, scriptURI string) {

	// Schedule some jobs
	if obj, ok := p.pool.GetCluster("obi-test"); ok {
		cluster := obj.(model.ClusterBaseInterface)
		cluster.SubmitJob(scriptURI)
	}
}
