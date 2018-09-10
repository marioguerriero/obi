package pooling

import (
	"container/heap"
	"errors"
	"fmt"
	"github.com/golang-collections/go-datastructures/queue"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"obi/master/autoscaler"
	"obi/master/model"
	"obi/master/platforms"
	"obi/master/utils"
	"obi/master/autoscaler/policies"
)

// Pooling class with properties
type Pooling struct {
	pool           *Pool
	scheduleQueues map[int32]*queue.Queue
}

// New is the constructor of Pooling struct
// @param pool contains the available clusters to use for job deployments
func New(pool *Pool) *Pooling {
	// TODO: Implement pooling

	// Create Pooling object
	logrus.Info("Creating cluster pooling")
	pooling := &Pooling{
		pool,
		make(map[int32]*queue.Queue),
	}

	// Start scheduling routine
	logrus.Info("Initialize scheduling routine")
	go pooling.schedulingRoutine()

	// Return created pooling object
	logrus.Info("Created pool of clusters")
	return pooling
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
	policy := policies.NewWorkload()
	a := autoscaler.New(policy, 120, 30, cluster)
	a.StartMonitoring()

	// Add to pool
	p.pool.AddCluster(cluster, a)

	return nil
}

// This routine periodically scans queues from top to low priority and schedules its contained job
func (p *Pooling) schedulingRoutine() {
	// Endless loop controlling available queues
	for {
		// I need to read keys every time because a new priority value may be added
		h := new(utils.MinHeap)
		heap.Init(h)
		for p := range p.scheduleQueues {
			h.Push(p)
		}

		// Scan queues in priority order
		for h.Len() > 0 {
			priority, err := h.PopInt()
			if err != nil {
				// If the priority value is invalid just skip this queue
				continue
			}
			// Pop and submit job
			q := p.scheduleQueues[priority]
			if q.Len() > 0 {
				items, _ := q.Get(1)
				job := items[0]
				p.SubmitJob(job.(*model.Job))
				break
			}
		}
	}
}

// ScheduleJob submits a new job to the pooling scheduling queues
func (p *Pooling) ScheduleJob(job *model.Job, priority int32) error {
	// Check if queue for the given priority level already exists,
	// if not, create it
	_, ok := p.scheduleQueues[priority]
	if !ok {
		p.scheduleQueues[priority] = queue.New(32)
	}

	// Add job to the request schedule queue
	return p.scheduleQueues[priority].Put(job)
}

// SubmitJob remote procedure call used to submit a job to one of the OBI infrastructures
func (p *Pooling) SubmitJob(job *model.Job) error {
	logrus.WithField("job", job.ID).Info("Submitting job for execution")

	// TODO: this should not be done here
	job.Platform = "dataproc"

	// Create new cluster
	clusterName := fmt.Sprintf("obi-%s", utils.RandomString(10))
	p.newCluster(clusterName, job.Platform)

	job.AssignedCluster = clusterName

	// Submit job
	switch job.Type {
	case model.JobTypePySpark:
		p.SubmitPySparkJob(clusterName, job) // TODO: use real pooling feature
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
