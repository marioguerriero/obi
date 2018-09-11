package pooling

import (
	"errors"
	"fmt"
	"github.com/Workiva/go-datastructures/queue"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"obi/master/autoscaler"
	"obi/master/autoscaler/policies"
	"obi/master/model"
	"obi/master/platforms"
	"obi/master/utils"
	"time"
)

// Pooling class with properties
type Pooling struct {
	pool           *Pool
	priority *queue.Queue
	bestEffort *queue.Queue
	quit chan struct{}
	schedulerWindow int16
}

// New is the constructor of Pooling struct
// @param pool contains the available clusters to use for job deployments
func New(pool *Pool, timeWindow int16) *Pooling {

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

func (p *Pooling) StartScheduling() {
	logrus.Info("Starting scheduling routine.")
	go schedulingRoutine(p)
}

func (p *Pooling) StopScheduling() {
	logrus.Info("Stopping scheduling routine.")
	close(p.quit)
}

func (p *Pooling) newCluster(name, platform string) error {
	logrus.WithField("cluster-name", name).Info("Creating new cluster")

	switch platform {
	case "dataproc":
		return p.newDataprocCluster(name)
	default:
		logrus.WithField("platform-type", platform).Error("Invalid platform type")
		return errors.New("invalid platform type")
	}
}

func (p *Pooling) newDataprocCluster(name string) error {
	cb := model.NewClusterBase(name, 2, "dataproc",
		viper.GetString("heartbeatHost"),
		viper.GetInt("heartbeatPort"))

	cluster := platforms.NewDataprocCluster(cb, viper.GetString("projectId"),
		viper.GetString("zone"),
		viper.GetString("region"), 0)

	// Allocate cluster resources
	err := cluster.AllocateResources()
	if err != nil {
		return err
	}

	// Instantiate a new autoscaler for the new cluster and start monitoring
	policy := policies.NewTimeout()
	a := autoscaler.New(policy, 60, 30, cluster)
	a.StartMonitoring()

	// Add to pool
	p.pool.AddCluster(cluster, a)

	return nil
}

// This routine periodically scans queues from top to low priority and schedules its contained job
func schedulingRoutine(pooling *Pooling) {
	for {
		select {
		case <-pooling.quit:
			logrus.Info("Closing scheduler routine.")
			return
		default:
			obj, error := pooling.priority.Get(1)
			if error != nil {
				logrus.WithField("error", error).Info("Impossible get the next job in the priority queue")
			} else {
				// TODO: use another library for heap-based priority queue
				job := obj[0].(*model.Job)
				logrus.WithFields(logrus.Fields{
					"jobID": job.ID,
					"priority": job.Priority,
				}).Info("New job admitted for running")
				go pooling.SubmitJob(job)
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
		go p.SubmitJob(job)
	}
}

// SubmitJob remote procedure call used to submit a job to one of the OBI infrastructures
func (p *Pooling) SubmitJob(job *model.Job) error {

	// TODO: this should not be done here
	job.Platform = "dataproc"

	// Create new cluster
	clusterName := fmt.Sprintf("obi-%s", utils.RandomString(10))
	p.newCluster(clusterName, job.Platform)

	job.AssignedCluster = clusterName

	// Submit job
	switch job.Type {
	case model.JobTypePySpark:
		p.SubmitPySparkJob(clusterName, job)
	default:
		return errors.New("invalid job type")
	}
	return nil
}

// SubmitPySparkJob is for submitting a new Spark job in Python environment
// @param clusterName is the name of the cluster where to run the new job
// @param scriptURI is the script path
func (p *Pooling) SubmitPySparkJob(cluster string, job *model.Job) {
	// Assign job to the given cluster
	logrus.WithField("cluster", cluster).WithField("job", *job).Info("Submitting PySpark job")

	// Schedule some jobs
	if obj, ok := p.pool.GetCluster(cluster); ok {
		cluster := obj.(model.ClusterBaseInterface)
		cluster.SubmitJob(job)
	}
}
