package pooling

import (
	"fmt"
	"github.com/Workiva/go-datastructures/queue"
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
)

// Pooling class with properties
type Pooling struct {
	pool           *Pool
	priority *queue.Queue
	bestEffort *queue.Queue
	quit chan struct{}
	schedulerWindow int32
	predictorClient *predictor.ObiPredictorClient
}

// New is the constructor of Pooling struct
// @param pool contains the available clusters to use for job deployments
func New(pool *Pool, timeWindow int32) *Pooling {

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
		queue.New(50),
		queue.New(100),
		make(chan struct{}),
		timeWindow,
		&pClient,
	}

	return pooling
}

// StartScheduling starts the execution of the scheduler routine
func (p *Pooling) StartScheduling() {
	logrus.Info("Starting scheduling routine.")
	go schedulingRoutine(p)
}

// StopScheduling stops the execution of the scheduler routine
func (p *Pooling) StopScheduling() {
	logrus.Info("Stopping scheduling routine.")
	close(p.quit)
}

// This routine periodically scans queues from top to low priority and schedules its contained job
func schedulingRoutine(pooling *Pooling) {
	for {
		select {
		case <-pooling.quit:
			logrus.Info("Closing scheduler routine.")
			return
		default:
			slice, error := pooling.priority.Get(10)
			if error != nil {
				logrus.WithField("error", error).Info("Impossible get the next job in the priority queue")
			} else {
				jobs := make([]*model.Job, len(slice))
				for i, obj := range slice {
					if job, ok := obj.(*model.Job); ok {
						jobs[i] = job
					}
				}
				go pooling.DeployJobs(jobs)
			}
			time.Sleep(time.Duration(pooling.schedulerWindow) * time.Second)
		}
	}
}

// ScheduleJob submits a new job to the pooling scheduling queues
func (p *Pooling) ScheduleJob(job *model.Job) {
	// TODO: configuration file for pooling
	if job.Priority > 0 && job.Priority <= 7 {
		p.priority.Put(job)
	} else if job.Priority == 0 {
		p.bestEffort.Put(job)
	} else {
		go p.DeployJobs([]*model.Job{job})
	}
}

// DeployJobs is for deploying the list of jobs into a single cluster
// @param jobs is the list of jobs to deploy
func (p *Pooling) DeployJobs(jobs []*model.Job) {

	// Create new cluster
	clusterName := fmt.Sprintf("obi-%s", utils.RandomString(10))
	cluster, err := newCluster(clusterName, "dataproc")

	if err != nil {
		return
	}

	// Instantiate a new autoscaler for the new cluster and start monitoring
	policy := policies.NewLinearWorkload()
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
		if job == nil {
			continue
		}

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
