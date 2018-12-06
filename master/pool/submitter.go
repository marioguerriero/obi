// Copyright 2018 Delivery Hero Germany
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
//     Unless required by applicable law or agreed to in writing, software
//     distributed under the License is distributed on an "AS IS" BASIS,
//     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//     See the License for the specific language governing permissions and
//     limitations under the License.

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
func (s *Submitter) DeployJobs(jobs []*model.Job, highPerformance bool, autoscalingFactor float32) {

	// Create new cluster
	clusterName := fmt.Sprintf("obi-%s", utils.RandomString(10))
	cluster, err := newCluster(clusterName, "dataproc", highPerformance, autoscalingFactor)

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
