package pool

import (
	"fmt"
		"github.com/sirupsen/logrus"
	"obi/master/autoscaler"
	"obi/master/autoscaler/policies"
	"obi/master/model"
	"obi/master/utils"
		"obi/master/predictor"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
		)

// Pooling class with properties
type Submitter struct {
	pool           *Pool
	predictorClient *predictor.ObiPredictorClient

}

// New is the constructor of Pooling struct
// @param pool contains the available clusters to use for job deployments
func NewSubmitter(pool *Pool) *Submitter {

	// Create Pooling object
	logrus.Info("Creating cluster scheduling")

	// Open connection to predictor server
	serverAddr := fmt.Sprintf("%s:%s",
		viper.GetString("predictorHost"),
		viper.GetString("predictorPort"))
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure()) // TODO: encrypt communication
	if err != nil {
		logrus.Fatalf("fail to dial: %v", err)
	}
	pClient := predictor.NewObiPredictorClient(conn)

	pooling := &Submitter{
		pool,
		&pClient,
	}

	// TODO: configuration for scheduler (levels, timeouts...)

	return pooling
}

// DeployJobs is for deploying the list of jobs into a single cluster
// @param jobs is the list of jobs to deploy
func (s *Submitter) DeployJobs(jobs []model.Job) {

	// Create new cluster
	clusterName := fmt.Sprintf("obi-%s", utils.RandomString(10))
	cluster, err := newCluster(clusterName, "dataproc")

	if err != nil {
		return
	}

	// Instantiate a new autoscaler for the new cluster and start monitoring
	policy := policies.NewWorkload(0.5)
	a := autoscaler.New(policy, 60, cluster.(model.Scalable))
	a.StartMonitoring()

	// Add in the pool
	s.pool.AddCluster(cluster, a)

	for _, job := range jobs {
		job.AssignedCluster = clusterName
		cluster.SubmitJob(job)
	}
}
