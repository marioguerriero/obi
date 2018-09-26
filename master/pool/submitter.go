package pool

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"obi/master/model"
	"obi/master/persistent"
	"obi/master/predictor"
	"obi/master/utils"
)

// Submitter is the struct that is used by the scheduler to deploy new jobs.
// It exposes a method that receives as parameter the list of jobs to deploy in the same cluster.
// It creates a new cluster that, after being added in the pool for further actions, will host the new jobs.
type Submitter struct {
	predictorClient *predictor.ObiPredictorClient

}

// NewSubmitter is the constructor of Pooling struct
// @param pool is the list of clusters to update with new ones
func NewSubmitter() *Submitter {

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
		&pClient,
	}

	return pooling
}

// DeployJobs is for deploying the list of jobs into a single cluster
// @param jobs is the list of jobs to deploy
func (s *Submitter) DeployJobs(jobs []*model.Job) {

	// Create new cluster
	clusterName := fmt.Sprintf("obi-%s", utils.RandomString(10))
	cluster, err := newCluster(clusterName, "dataproc")

	if err != nil {
		return
	}

	for _, job := range jobs {
		// Update job status
		job.Cluster = cluster
		job.Status = model.JobStatusRunning
		// Submit job for execution
		cluster.SubmitJob(job)
		// Update persistent storage
		persistent.Write(job)
	}
}
