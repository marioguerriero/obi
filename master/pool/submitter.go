package pool

import (
	"fmt"
	"github.com/sirupsen/logrus"
			"obi/master/model"
	"obi/master/persistent"
		"obi/master/utils"
)

// Submitter is the struct that is used by the scheduler to deploy new jobs.
// It exposes a method that receives as parameter the list of jobs to deploy in the same cluster.
// It creates a new cluster that, after being added in the pool for further actions, will host the new jobs.
type Submitter struct {

}

// NewSubmitter is the constructor of Pooling struct
// @param pool is the list of clusters to update with new ones
func NewSubmitter() *Submitter {

	// Create Pooling object
	logrus.Info("Creating cluster scheduling")

	pooling := &Submitter{}

	return pooling
}

// DeployJobs is for deploying the list of jobs into a single cluster
// @param jobs is the list of jobs to deploy
func (s *Submitter) DeployJobs(jobs []*model.Job, highPerformance bool) {

	// Create new cluster
	clusterName := fmt.Sprintf("obi-%s", utils.RandomString(10))
	cluster, err := newCluster(clusterName, "dataproc", highPerformance)

	if err != nil {
		for _, job := range jobs {
			// Update job
			job.Status = model.JobStatusFailed
			persistent.Write(job)
		}
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
