package pooling

import (
	"container/heap"
	"errors"
	"github.com/golang-collections/go-datastructures/queue"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"obi/master/autoscaler"
	"obi/master/model"
	"obi/master/platforms"
	"obi/master/utils"
)

// Pooling class with properties
type Pooling struct {
	pool           *Pool
	scheduleQueues map[int32]*queue.Queue
}

// New is the constructor of Pooling struct
// @param pool contains the available clusters to use for job deployments
func New(pool *Pool) *Pooling {
	// TODO: Implement pooling. For the moment only a cluster to use

	logrus.Info("Creating pooling")
	cb := model.NewClusterBase("obi-test", 2,
		"dataproc",
		viper.GetString("heartbeatHost"),
		viper.GetInt("heartbeatPort"))

	cluster := platforms.NewDataprocCluster(cb, viper.GetString("projectId"),
		viper.GetString("zone"),
		viper.GetString("region"), 1, 0.3)

	// Allocate cluster resources
	err := cluster.AllocateResources()

	if err == nil {
		// Instantiate a new autoscaler for the new cluster and start monitoring
		a := autoscaler.New(autoscaler.WorkloadBased, 60, 30, cluster)
		a.StartMonitoring()

		// Add to pool
		pool.AddCluster(cluster, a)
	}

	// Create Pooling object
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
	logrus.WithField("job", job.Id).Info("Submitting job for execution")

	switch job.Type {
	case model.JobTypePyspark:
		p.SubmitPySparkJob("obi-test", job) // TODO: use real pooling feature
	default:
		return errors.New("invalid job type")
	}
	return nil
}

// SubmitPySparkJob is for submitting a new Spark job in Python environment
// @param clusterName is the name of the cluster where to run the new job
// @param scriptURI is the script path
func (p *Pooling) SubmitPySparkJob(clusterName string, job *model.Job) {
	// Assign job to the given cluster
	job.AssignedCluster = clusterName
	logrus.WithField("cluster", clusterName).WithField("job", *job).Info("Submitting PySpark job")

	// Schedule some jobs
	if obj, ok := p.pool.GetCluster("obi-test"); ok {
		cluster := obj.(model.ClusterBaseInterface)
		cluster.SubmitJob(job.ExecutablePath)
	}
}
