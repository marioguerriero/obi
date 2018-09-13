package pooling

import (
	"fmt"
		"github.com/sirupsen/logrus"
	"obi/master/autoscaler"
	"obi/master/autoscaler/policies"
	"obi/master/model"
	"obi/master/utils"
	"time"
	"obi/master/predictor"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"context"
	"obi/master/scheduler"
)

// Pooling class with properties
type Pooling struct {
	pool           *Pool
	predictorClient *predictor.ObiPredictorClient
	scheduler *scheduler.Scheduler
}

// New is the constructor of Pooling struct
// @param pool contains the available clusters to use for job deployments
func New(pool *Pool) *Pooling {

	// Create Pooling object
	logrus.Info("Creating cluster pooling")

	// Open connection to predictor server
	serverAddr := fmt.Sprintf("%s:%s",
		viper.GetString("predictorHost"),
		viper.GetString("predictorPort"))
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure()) // TODO: encrypt communication
	if err != nil {
		logrus.Fatalf("fail to dial: %v", err)
	}
	pClient := predictor.NewObiPredictorClient(conn)

	pooling := &Pooling{
		pool,
		&pClient,
		scheduler.New(10),
	}

	// TODO: configuration for scheduler (levels, timeouts...)

	return pooling
}

// ScheduleJob submits a new job to the pooling scheduling queues
func (p *Pooling) ScheduleJob(job model.Job) {
	// TODO: configuration file for pooling
	if job.Priority >= 0 && job.Priority <= 7 {
		p.scheduler.AddJob(job)
	} else {
		go p.DeployJobs([]model.Job{job})
	}
}

// DeployJobs is for deploying the list of jobs into a single cluster
// @param jobs is the list of jobs to deploy
func (p *Pooling) DeployJobs(jobs []model.Job) {

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
	p.pool.AddCluster(cluster, a)

	time.Sleep(30 * time.Second)

	var lastHeartbeat model.Metrics
	for hb := range cluster.GetMetricsWindow().Iter() {
		if hb.Value != nil {
			lastHeartbeat = hb.Value.(model.Metrics)
		}
	}

	for _, job := range jobs {

		// Generate predictions before submitting the job
		resp, err := (*p.predictorClient).RequestPrediction(
			context.Background(), &predictor.PredictionRequest{
				JobFilePath: job.ExecutablePath,
				JobArgs: job.Args,
				Metrics: model.MetricsToSnapshot(lastHeartbeat),
			})
		if err != nil {
			logrus.WithField("response", resp).Warning("Could not generate predictions")
		}
		fmt.Println(resp.Duration)
		fmt.Println(resp.FailureProbability)
		fmt.Println(resp.Label)

		job.AssignedCluster = clusterName
		cluster.SubmitJob(job)
	}
}
