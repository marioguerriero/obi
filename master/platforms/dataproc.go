package platforms

import (
	"cloud.google.com/go/dataproc/apiv1"
	"context"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/api/iterator"
	dataprocpb "google.golang.org/genproto/googleapis/cloud/dataproc/v1"
	"google.golang.org/genproto/protobuf/field_mask"
	"math"
	m "obi/master/model"
	"obi/master/persistent"
	"obi/master/utils"
	"strconv"
	"strings"
	"time"
)

// InitializationAction initialization script for installing necessary requirements
const InitializationAction = "gs://dhg-obi/cluster-script/init_action.sh"

// NormalNodeCostPerSecond unitary cost of a normal node
const NormalNodeCostPerSecond = 0.2448 / 60

// PreemptibleNodeCostPerSecond unitary cost of a preemptible node
const PreemptibleNodeCostPerSecond = 0.04920 / 60

// HeartbeatInterval interval of time at which each heartbeat is sent
const HeartbeatInterval = 10

// DataprocCluster is the extended cluster struct of Google Dataproc
type DataprocCluster struct {
	*m.ClusterBase
	ProjectID string
	Zone string
	Region string
	PreemptibleNodes int32
	isMonitoring bool
}

// NewDataprocCluster is the constructor of DataprocCluster struct
// @param baseInfo is the base object for a cluster
// @param projectId is the project ID in the GCP environment
// @param region is the macro-area where the cluster was deployed (e.g. europe-west3)
// @param zone is a specific area inside region (e.g. europe-west3-b)
// @param preemptibleRatio in the percentage of preemptible VMs that has to be present inside the cluster
// return the pointer to the new DataprocCluster instance
func NewDataprocCluster(
		baseInfo *m.ClusterBase,
		projectID, zone,
		region string,
		preemptibleNodes int32,
    ) *DataprocCluster {
	baseInfo.Platform = "dataproc"
	cluster := &DataprocCluster{
		baseInfo,
		projectID,
		zone,
		region,
		preemptibleNodes,
		false,
	}

	// Recover running jobs (if anny)
	runningJobs, err := persistent.GetRunningJobs(baseInfo.Name)
	if err != nil {
		logrus.WithField("error", err).Error("Could not read previously running jobs")
	}
	for _, j := range runningJobs {
		logrus.WithField("job", *j).Info("Attaching already running job to this cluster")
		// Attach current cluster to the job
		j.Cluster = cluster
		cluster.appendJob(j)
	}

	return cluster
}

// NewExistingDataprocCluster is the constructor of DataprocCluster for already allocated resources in Dataproc
// Even if OBI-master-old fails, it will be capable of rebuilding the pool, simply reading the content of the heartbeats
// @param projectID is the project ID in the GCP environment
// @param region is the the macro-area where the cluster was deployed (e.g. europe-west3)
// @param zone is a specific area inside region (e.g. europe-west3-b)
// @param clusterName is the name of the existing cluster inside Dataproc environment
func NewExistingDataprocCluster(projectID string, region string, zone string, clusterName string) (*DataprocCluster, error) {
	ctx := context.Background()
	c, err := dataproc.NewClusterControllerClient(ctx)
	if err != nil {
		logrus.WithField("error", err).Error("NewClusterControllerClient' method call failed")
		return nil, err
	}

	req := &dataprocpb.ListClustersRequest{
		ProjectId: projectID,
		Region:    region,
		Filter:    "clusterName = " + clusterName,
	}

	it := c.ListClusters(ctx, req)
	var newCluster *DataprocCluster
	for {
		resp, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			logrus.WithField("error", err).Error("'Next' method failed during existing clusters iteration")
			return nil, err
		}

		newBaseCluster := m.NewClusterBase(clusterName,
			resp.Config.WorkerConfig.NumInstances,
			"dataproc",
			viper.GetString("heart	beat.host"),
			8080)

		// Update cluster base creation timestamp
		ts, ok := persistent.GetRunningDatabaseCreationTimestamp(newBaseCluster.Name)
		logrus.WithField("ts", ts).WithField("ok", ok).Info("get running database timestamp")
		if ok {
			newBaseCluster.CreationTimestamp = *ts
			logrus.WithField("timestamp", newBaseCluster.CreationTimestamp).Info("Read timestamp")
		}

		var preemptibleNodes int32
		if resp.Config.SecondaryWorkerConfig != nil {
			preemptibleNodes = resp.Config.SecondaryWorkerConfig.NumInstances
		}

		newCluster = NewDataprocCluster(
			newBaseCluster,
			projectID,
			zone,
			region,
			preemptibleNodes,
		)
	}
	return newCluster, nil
}


// <-- start implementation of `Scalable` interface -->

// Scale is for scaling up the cluster, i.e. add new nodes to increase size
// @param nodes is the number of nodes to add
// @param direction is for specifying if there is the need to add o remove nodes
func (c *DataprocCluster) Scale(delta int32) bool {
	var newSize int32

	if delta < 0 && c.PreemptibleNodes == 0 {
		return true
	}
	newSize = int32(math.Max(0, float64(c.PreemptibleNodes + delta)))

	ctx := context.Background()
	controller, err := dataproc.NewClusterControllerClient(ctx)
	if err != nil {
		logrus.WithField("error", err).Error("'NewClusterControllerClient' method call failed")
		return false
	}

	req := &dataprocpb.UpdateClusterRequest{
		ProjectId:   c.ProjectID,
		Region:      c.Region,
		ClusterName: c.Name,
		Cluster: &dataprocpb.Cluster{
			Config: &dataprocpb.ClusterConfig{
				SecondaryWorkerConfig: &dataprocpb.InstanceGroupConfig{
					NumInstances: newSize,
				},
			},
		},
		UpdateMask:  &field_mask.FieldMask{
			Paths: []string{
				"config.secondary_worker_config.num_instances",
			},
		},
	}

	op, err := controller.UpdateCluster(ctx, req)
	if err != nil {
		logrus.WithField("error", err).Error("'UpdateCluster' method call failed")
		return false
	}

	_, err = op.Wait(ctx)
	if err != nil {
		logrus.WithField("error", err).Error("'Wait' method call for UpdateCluster operation failed")
		return false
	}

	c.PreemptibleNodes= newSize
	logrus.WithFields(logrus.Fields{
		"clusterName": c.Name,
		"additionalWorkers": newSize,
	}).Info("Scaling completed with secondary nodes.")

	if c.PreemptibleNodes == 0 {
		return true
	}
	return false
}

// <-- end implementation of `Scalable` interface -->

// <-- start implementation of `ClusterBaseInterface` interface -->

// GetName is for getting the name of the cluster
func (c *DataprocCluster) GetName() string {
	return c.Name
}

// SubmitJob is for sending a new job to Dataproc
func (c *DataprocCluster) SubmitJob(job *m.Job) error {
	ctx := context.Background()
	controller, err := dataproc.NewJobControllerClient(ctx)
	if err != nil {
		logrus.WithField("error", err).Error("'NewJobControllerClient' method call failed")
		return err
	}

	// TODO generalize this function to deploy any type of job, not only PySpark

	req := &dataprocpb.SubmitJobRequest{
		ProjectId: c.ProjectID,
		Region:    c.Region,
		Job: &dataprocpb.Job{
			Placement: &dataprocpb.JobPlacement{
				ClusterName: c.Name,
			},
			TypeJob: &dataprocpb.Job_PysparkJob{
				PysparkJob: &dataprocpb.PySparkJob{
					MainPythonFileUri: job.ExecutablePath,
					Args: strings.Fields(job.Args),
				},
			},
		},
	}

	dataprocJob, err := controller.SubmitJob(ctx, req)
	job.PlatformDependentID = dataprocJob.Reference.JobId

	logrus.WithField("cluster", c.ClusterBase.Name).Info("Cluster has been assigned with a new job")

	// Add job to the cluster's list
	c.appendJob(job)

	if err != nil {
		logrus.WithField("error", err).Error("'SubmitJob' method call failed")
		return err
	}
	logrus.WithField("clusterName", c.Name).Info("New job deployed")
	return nil
}

// GetMetricsWindow is for getting last metrics of the cluster
func (c *DataprocCluster) GetMetricsWindow() *utils.ConcurrentSlice {
	return c.GetMetrics()
}

// AddMetricsSnapshot is for updating the cluster with new metrics
// @newMetrics is the object filled with new metrics
func (c *DataprocCluster) AddMetricsSnapshot(newMetrics m.HeartbeatMessage) {
	c.SetMetrics(newMetrics)
}

// AllocateResources instantiate physical resources for the given cluster
func (c *DataprocCluster) AllocateResources() error {
	// Create cluster controller
	ctx := context.Background()
	controller, err := dataproc.NewClusterControllerClient(ctx)
	if err != nil {
		logrus.WithField("error", err).Error("NewClusterControllerClient' method call failed")
		return err
	}

	// Send request to allocate cluster resources
	req := &dataprocpb.CreateClusterRequest{
		ProjectId: c.ProjectID,
		Region: c.Region,
		Cluster: &dataprocpb.Cluster{
			ProjectId: c.ProjectID,
			ClusterName: c.Name,
			Config: &dataprocpb.ClusterConfig{
				GceClusterConfig: &dataprocpb.GceClusterConfig{
					ZoneUri: c.Zone,
					Metadata: map[string]string{
						"obi-hb-host": c.HeartbeatHost,
						"obi-hb-port": strconv.Itoa(c.HeartbeatPort),
						"normal-node-cost": strconv.FormatFloat(NormalNodeCostPerSecond, 'f', 16, 64),
						"preemptible-node-cost": strconv.FormatFloat(PreemptibleNodeCostPerSecond, 'f', 16, 64),
						"interval": strconv.Itoa(HeartbeatInterval),
					},
				},
				MasterConfig: &dataprocpb.InstanceGroupConfig{
					ImageUri: "projects/dhg-data-intelligence-ops/global/images/dhg-di-v6",
				},
				WorkerConfig: &dataprocpb.InstanceGroupConfig{
					ImageUri: "projects/dhg-data-intelligence-ops/global/images/dhg-di-v6",
					NumInstances: int32(c.WorkerNodes),
				},
				SecondaryWorkerConfig: &dataprocpb.InstanceGroupConfig{
					ImageUri: "projects/dhg-data-intelligence-ops/global/images/dhg-di-v6",
					NumInstances: int32(c.PreemptibleNodes),
				},
				InitializationActions: []*dataprocpb.NodeInitializationAction{
					{
						ExecutableFile: InitializationAction,
					},
				},
			},
		},
	}
	op, err := controller.CreateCluster(ctx, req)
	if err != nil {
		logrus.WithField("error", err).Error("'CreateCluster' method call failed")
		return err
	}

	// Wait till cluster is successfully created
	_, err = op.Wait(ctx)
	if err != nil {
		logrus.WithField("error", err).Error("'Wait' method call for CreateCluster operation failed")
		return err
	}
	c.Status = m.ClusterStatusRunning
	logrus.WithField("name", c.Name).Info("New cluster on Dataproc platform")

	// Write created cluster interface to persistent database
	persistent.Write(c)

	return nil
}

// FreeResources deletes the given Dataproc cluster
func (c *DataprocCluster) FreeResources() error {
	ctx := context.Background()
	clusterController, _ := dataproc.NewClusterControllerClient(ctx)

	op, err := clusterController.DeleteCluster(ctx, &dataprocpb.DeleteClusterRequest{
		ProjectId:   c.ProjectID,
		Region:      c.Region,
		ClusterName: c.Name,
	})

	if err != nil {
		logrus.WithField("error", err).Error("'DeleteCluster' method call failed")
		return err
	}

	// Wait till cluster is successfully deleted
	err = op.Wait(ctx)
	if err != nil {
		logrus.WithField("error", err).Error("'Wait' method call for DeleteCluster operation failed")
		return err
	}
	logrus.WithField("name", c.Name).Info("Deleted cluster on Dataproc platform")

	// Update persistent storage
	c.Status = m.ClusterStatusClosed
	persistent.Write(c)

	return nil
}

// GetAllocatedJobSlots returns the number of jobs the cluster is currently handling
func (c *DataprocCluster) GetAllocatedJobSlots() int {
	return c.Jobs.Len()
}

// GetPlatform returns cluster's platform type e.g. "dataproc"
func (c *DataprocCluster) GetPlatform() string {
	return c.Platform
}

// GetCreationTimestamp return cluster's creation timestamp
func (c *DataprocCluster) GetCreationTimestamp() time.Time {
	return c.CreationTimestamp
}

// MonitorJobs track job execution status
func (c *DataprocCluster) MonitorJobs() {
	ctx := context.Background()
	controller, err := dataproc.NewJobControllerClient(ctx)
	if err != nil {
		logrus.WithField("error", err).Error("'NewJobControllerClient' method call failed")
	}

	logrus.WithField("cluster-name", c.Name).Info("Starting cluster monitoring routine")

	for {
		time.Sleep(time.Second * 30)
		for elem := range c.Jobs.Iter() {
			logrus.WithField("elem", elem).Info("monitoring")
			job := elem.Value.(*m.Job)
			logrus.WithField("job", job).Info("monitoring job")
			logrus.WithField("job", *job).Info("monitoring job")
			// Query job controller
			j, _ := controller.GetJob(ctx, &dataprocpb.GetJobRequest{
				ProjectId: c.ProjectID,
				Region:    c.Region,
				JobId:     job.PlatformDependentID,
			})
			if j.Status.State == dataprocpb.JobStatus_DONE ||
				j.Status.State == dataprocpb.JobStatus_ERROR ||
				j.Status.State == dataprocpb.JobStatus_CANCELLED {

				// Update persistent storage with new job status
				previousState := job.Status
				if j.Status.State == dataprocpb.JobStatus_DONE {
					job.Status = m.JobStatusCompleted
				} else if j.Status.State == dataprocpb.JobStatus_ERROR ||
					j.Status.State == dataprocpb.JobStatus_CANCELLED {
					job.Status = m.JobStatusFailed
				}

				// Update job in persistent store if its state changed
				if previousState != job.Status {
					persistent.Write(job)
				}

				// Drop job from the cluster's jobs list
				c.Jobs.MarkTombstone(elem.Index)
			}
		}
		// Force synchronization between tombstone markers and concurrent slice
		c.Jobs.Sync()
		// Eventually release resources
		if c.Jobs.Len() == 0 {
			c.FreeResources()
		}
	}
}

// GetCost returns cluster's cost so far in dollars
func (c *DataprocCluster) GetCost() float32 {
	metricsCount := c.GetMetrics().Len()
	m := c.GetMetrics().Get(metricsCount-1).(m.HeartbeatMessage)
	return m.Cost
}

// GetStatus returns cluster's status e.g. "running"
func (c *DataprocCluster) GetStatus() m.ClusterStatus {
	return c.Status
}

// SetStatus set cluster's status e.g. "running"
func (c *DataprocCluster) SetStatus(s m.ClusterStatus) {
	c.Status = s
}
// <-- end implementation of `ClusterBaseInterface` interface -->

func (c *DataprocCluster) appendJob(job *m.Job) {
	// Add job in cluster's execution list
	c.Jobs.Append(job)
	// Start monitoring jobs
	if !c.isMonitoring {
		c.isMonitoring = true
		// Start job monitoring routine
		go c.MonitorJobs()
	}
}