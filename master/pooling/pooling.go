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
)

// Pooling class with properties
type Pooling struct {
	pool           *Pool
	priority *queue.Queue
	bestEffort *queue.Queue
	quit chan struct{}
	schedulerWindow int32
}

// New is the constructor of Pooling struct
// @param pool contains the available clusters to use for job deployments
func New(pool *Pool, timeWindow int32) *Pooling {

	// Create Pooling object
	logrus.Info("Creating cluster pooling")
	pooling := &Pooling{
		pool,
		queue.New(50),
		queue.New(100),
		make(chan struct{}),
		timeWindow,
	}

	// Return created pooling object
	logrus.Info("Created pool of clusters")
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

	// Instantiate a new autoscaler for the new cluster and start monitoring
	policy := policies.NewTimeout()
	a := autoscaler.New(policy, 60, cluster.(model.Scalable))
	a.StartMonitoring()

	// Add in the pool
	p.pool.AddCluster(cluster, a)

	if err != nil {
		return
	}

	for _, job := range jobs {
		if job == nil {
			continue
		}
		job.AssignedCluster = clusterName
		cluster.SubmitJob(job)
	}
}
