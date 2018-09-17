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

// Submitter is the struct that is used by the scheduler to deploy new jobs.
// It exposes a method that receives as parameter the list of jobs to deploy in the same cluster.
// It creates a new cluster that, after being added in the pool for further actions, will host the new jobs.
type Submitter struct {
	pool           *Pool
	predictorClient *predictor.ObiPredictorClient

}

// NewSubmitter is the constructor of Pooling struct
// @param pool is the list of clusters to update with new ones
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
	a := autoscaler.New(policy, 60, cluster.(model.Scalable), false)
	a.StartMonitoring()

	// Add in the pool
	s.pool.AddCluster(cluster, a)

	for _, job := range jobs {
		job.AssignedCluster = clusterName
		cluster.SubmitJob(job)
	}
}
