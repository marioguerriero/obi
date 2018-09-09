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
	"obi/master/utils"
	"strconv"
	"strings"
	"time"
)

// InitializationAction initialization script for installing necessary requirements
const InitializationAction = "gs://dhg-obi/cluster-script/init_action.sh"


// DataprocCluster is the extended cluster struct of Google Dataproc
type DataprocCluster struct {
	*m.ClusterBase
	ProjectID string
	Zone string
	Region string
	PreemptibleNodes int32
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
	return &DataprocCluster{
		baseInfo,
		projectID,
		zone,
		region,
		preemptibleNodes,
	}
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

		var preemptibleNodes int32
		if resp.Config.SecondaryWorkerConfig != nil {
			preemptibleNodes = resp.Config.SecondaryWorkerConfig.NumInstances
		}

		newCluster = &DataprocCluster{
			newBaseCluster,
			projectID,
			zone,
			region,
			preemptibleNodes,
		}
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
		"newSize": newSize,
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

	// Start routine to kill the cluster once the job is finished
	go func() {
		for {
			time.Sleep(time.Minute)
			// Query job controller
			j, _ := controller.GetJob(ctx, &dataprocpb.GetJobRequest{
				ProjectId: c.ProjectID,
				Region:    c.Region,
				JobId:     dataprocJob.Reference.JobId,
			})
			if j.Status.State == dataprocpb.JobStatus_DONE ||
				j.Status.State == dataprocpb.JobStatus_ERROR ||
				j.Status.State == dataprocpb.JobStatus_CANCELLED {
				// If the cluster's job is finished, delete the cluster
				c.delete(ctx)
				logrus.WithField("cluster-name", c.Name).Info("Delete Dataproc cluster")
				return
			}
		}
	}()

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
func (c *DataprocCluster) AddMetricsSnapshot(newMetrics m.Metrics) {
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
	logrus.WithField("name", c.Name).Info("New Dataproc cluster")
	return nil
}

// <-- end implementation of `ClusterBaseInterface` interface -->

// delete the given Dataproc cluster
func (c *DataprocCluster) delete(ctx context.Context)  {
	clusterController, _ := dataproc.NewClusterControllerClient(ctx)
	clusterController.DeleteCluster(ctx, &dataprocpb.DeleteClusterRequest{
		ProjectId:   c.ProjectID,
		Region:      c.Region,
		ClusterName: c.Name,
	})
	// Send delete even over cluster's channel
	c.Events <- m.ClusterEvent_DELETE
}
