package platforms

import (
	dataprocpb "google.golang.org/genproto/googleapis/cloud/dataproc/v1"
	"cloud.google.com/go/dataproc/apiv1"
	"google.golang.org/genproto/protobuf/field_mask"
	"context"
		m "obi/master/model"
	"google.golang.org/api/iterator"
	"github.com/sirupsen/logrus"
	"strconv"
	"github.com/spf13/viper"
	"obi/master/utils"
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
	PreemptiveNodesRatio float32
}

// NewDataprocCluster is the constructor of DataprocCluster struct
// @param baseInfo is the base object for a cluster
// @param projectId is the project ID in the GCP environment
// @param region is the macro-area where the cluster was deployed (e.g. europe-west3)
// @param zone is a specific area inside region (e.g. europe-west3-b)
// @param preemptibleRatio in the percentage of preemptible VMs that has to be present inside the cluster
// return the pointer to the new DataprocCluster instance
func NewDataprocCluster(baseInfo *m.ClusterBase, projectID, zone, region string,
		preemptibleNodes int32, preemptibleRatio float32) *DataprocCluster {
	return &DataprocCluster{
		baseInfo,
		projectID,
		zone,
		region,
		preemptibleNodes,
		preemptibleRatio,
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
			0.0,
		}
	}
	return newCluster, nil
}


// <-- start implementation of `Scalable` interface -->

// Scale is for scaling up the cluster, i.e. add new nodes to increase size
// @param nodes is the number of nodes to add
// @param direction is for specifying if there is the need to add o remove nodes
func (c *DataprocCluster) Scale(nodes int32, toAdd bool) {
	var newSize int32

	ctx := context.Background()
	controller, err := dataproc.NewClusterControllerClient(ctx)
	if err != nil {
		logrus.WithField("error", err).Error("'NewClusterControllerClient' method call failed")
		return
	}

	if toAdd {
		newSize = int32(c.PreemptibleNodes + nodes)
	} else {
		newSize = int32(c.PreemptibleNodes - nodes)
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
		return
	}

	_, err = op.Wait(ctx)
	if err != nil {
		logrus.WithField("error", err).Error("'Wait' method call for UpdateCluster operation failed")
		return
	}

	c.PreemptibleNodes = newSize
	logrus.WithFields(logrus.Fields{
		"clusterName": c.Name,
		"newSize": newSize,
	}).Info("Scaling completed.")
}

// <-- end implementation of `Scalable` interface -->

// <-- start implementation of `ClusterBaseInterface` interface -->

// GetName is for getting the name of the cluster
func (c *DataprocCluster) GetName() string {
	return c.Name
}

// SubmitJob is for sending a new job to Dataproc
func (c *DataprocCluster) SubmitJob(scriptURI string) error {
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
					MainPythonFileUri: scriptURI,
				},
			},
		},
	}

	_, err = controller.SubmitJob(ctx, req)

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
				WorkerConfig: &dataprocpb.InstanceGroupConfig{
					NumInstances: int32(c.WorkerNodes),
				},
				SecondaryWorkerConfig: &dataprocpb.InstanceGroupConfig{
					NumInstances: int32(c.PreemptibleNodes),
				},
				InitializationActions: []*dataprocpb.NodeInitializationAction{
					{
						ExecutableFile: InitializationAction,
					},
					{
						// TODO: remove this temporary line
						ExecutableFile: "gs://dhg-obi/tmp.sh",
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