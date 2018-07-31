package platforms

import (
	dataprocpb "google.golang.org/genproto/googleapis/cloud/dataproc/v1"
	"cloud.google.com/go/dataproc/apiv1"
	"google.golang.org/genproto/protobuf/field_mask"
	"context"
	"github.com/golang/glog"
	m "obi/model"
	"google.golang.org/api/option"
	)

// InitializationAction initialization script for installing necessary requirements
const InitializationAction = "gs://dhg-obi/cluster-script/init_action.sh"


// DataprocCluster is the extended cluster struct of Google Dataproc
type DataprocCluster struct {
	*m.ClusterBase
	ProjectID string
	Zone string
	Region string
	PreemptibleNodes int16
	PreemptiveNodesRatio float32
}

// NewDataprocCluster is the constructor of DataprocCluster struct
// @param baseInfo is the base object for a cluster
// @param projectId is the project ID in the GCP environment
// @param region is the geo-region where the cluster was deployed (e.g. europe-west-1)
// @param preemptibleRatio in the percentage of preemptible VMs that has to be present inside the cluster
// return the pointer to the new DataprocCluster instance
func NewDataprocCluster(baseInfo *m.ClusterBase, projectID, zone, region string,
		preemptibleNodes int16, preemptibleRatio float32) *DataprocCluster {
	return &DataprocCluster{
		baseInfo,
		projectID,
		zone,
		region,
		preemptibleNodes,
		preemptibleRatio,
	}
}


// <-- start implementation of `Scalable` interface -->

// Scale is for scaling up the cluster, i.e. add new nodes to increase size
// @param nodes is the number of nodes to add
// @param direction is for specifying if there is the need to add o remove nodes
func (c *DataprocCluster) Scale(nodes int16, toAdd bool) {
	var newSize int32

	ctx := context.Background()
	controller, err := dataproc.NewClusterControllerClient(ctx)
	if err != nil {
		glog.Errorf("'NewClusterControllerClient' method call failed: %s", err)
		return
	}

	if toAdd {
		newSize = int32(c.Nodes + nodes)
	} else {
		newSize = int32(c.Nodes - nodes)
	}

	req := &dataprocpb.UpdateClusterRequest{
		ProjectId:   c.ProjectID,
		Region:      c.Region,
		ClusterName: c.Name,
		Cluster: &dataprocpb.Cluster{
			Config: &dataprocpb.ClusterConfig{
				WorkerConfig: &dataprocpb.InstanceGroupConfig{
					NumInstances: newSize,
				},
				SecondaryWorkerConfig: &dataprocpb.InstanceGroupConfig{
					NumInstances: newSize,
				},
			},
		},
		UpdateMask:  &field_mask.FieldMask{
			Paths: []string{
				"config.worker_config.num_instances",
				"config.secondary_worker_config.num_instances",
			},
		},
	}

	op, err := controller.UpdateCluster(ctx, req)
	if err != nil {
		glog.Errorf("'UpdateCluster' method call failed: %s", err)
		return
	}

	_, err = op.Wait(ctx)
	if err != nil {
		glog.Errorf("'Wait' method call for UpdateCluster operation failed: %s", err)
		return
	}
	glog.Infof("Scaling completed. The new size of cluster '%s' is %d.", c.Name, newSize)
}
// SubmitJob is for sending a new job to Dataproc
func (c *DataprocCluster) SubmitJob(scriptURI string) (*dataprocpb.Job, error){
	ctx := context.Background()
	controller, err := dataproc.NewJobControllerClient(ctx, option.WithCredentialsFile("/Users/l.lombardo/Documents/dataproc-sa.json"))
	if err != nil {
		glog.Errorf("'NewJobControllerClient' method call failed: %s", err)
		return nil, err
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

	job, err := controller.SubmitJob(ctx, req)

	if err != nil {
		glog.Errorf("'SubmitJob' method call failed: %s", err)
		return nil, err
	}
	glog.Infof("New job deployed in cluster '%s'.", c.Name)
	return job, nil

}

// <-- end implementation of `Scalable` interface -->

// <-- start implementation of `ClusterBaseInterface` interface -->

// GetMetricsSnapshot is for getting last metrics of the cluster
func (c *DataprocCluster) GetMetricsSnapshot() m.Metrics {
	return c.GetMetrics()
}

// SetMetricsSnapshot is for updating the cluster with new metrics
// @newMetrics is the object filled with new metrics
func (c *DataprocCluster) SetMetricsSnapshot(newMetrics m.Metrics) {
	c.SetMetrics(newMetrics)
}

// AllocateResources instantiate physical resources for the given cluster
func (c *DataprocCluster) AllocateResources() error {
	// Create cluster controller
	ctx := context.Background()
	controller, err := dataproc.NewClusterControllerClient(ctx, option.WithCredentialsFile("/Users/l.lombardo/Documents/dataproc-sa.json"))
	if err != nil {
		glog.Errorf("Could not create cluster controller for %s: %s", c.Name, err)
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
				},
				WorkerConfig: &dataprocpb.InstanceGroupConfig{
					NumInstances: int32(c.Nodes),
				},
				SecondaryWorkerConfig: &dataprocpb.InstanceGroupConfig{
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
		glog.Errorf("Could not allocate resources for cluster %s: %s", c.Name, err)
		return err
	}

	// Wait till cluster is successfully created
	_, err = op.Wait(ctx)
	if err != nil {
		glog.Errorf("Cluster %s resource allocation failed: %s", c.Name, err)
		return err
	}

	glog.Infof("New Dataproc cluster '%s' created.", c.Name)
	return nil
}

// <-- end implementation of `ClusterBaseInterface` interface -->