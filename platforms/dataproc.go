package platforms

import (
	dataprocpb "google.golang.org/genproto/googleapis/cloud/dataproc/v1"
	"cloud.google.com/go/dataproc/apiv1"
	"google.golang.org/genproto/protobuf/field_mask"
	"context"
	"github.com/golang/glog"
	m "obi/model"
)

// DataprocCluster is the extended cluster struct of Google Dataproc
type DataprocCluster struct {
	*m.ClusterBase
	ProjectID string
	Region string
	PreemptibleNodes int16
	PreemptiveNodesRatio int8
}

// NewDataprocCluster is the constructor of DataprocCluster struct
// @param baseInfo is the base object for a cluster
// @param projectId is the project ID in the GCP environment
// @param region is the geo-region where the cluster was deployed (e.g. europe-west-1)
// @param preemptibleRatio in the percentage of preemptible VMs that has to be present inside the cluster
// return the pointer to the new DataprocCluster instance
func NewDataprocCluster(baseInfo *m.ClusterBase, projectID string, region string, preemptibleNodes int16, preemptibleRatio int8) *DataprocCluster {

	// TO DO: create cluster using by Dataproc rRPC

	glog.Infof("New Dataproc cluster '%s' created.", baseInfo.Name)

	return &DataprocCluster{
		baseInfo,
		projectID,
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
		glog.Error("'NewClusterControllerClient' method call failed.")
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
		glog.Error("'UpdateCluster' method call failed.")
		return
	}

	_, err = op.Wait(ctx)
	if err != nil {
		glog.Error("'Wait' method call for UpdateCluster operation failed.")
		return
	}
	glog.Infof("Scaling completed. The new size of cluster '%s' is %d.", c.Name, newSize)
}

// <-- end implementation of `Scalable` interface -->

// <-- start implementation of `ClusterBaseInterface` interface -->

// SubmitJob is for sending a new job to Dataproc
func (c *DataprocCluster) SubmitJob(scriptURI string) {
	ctx := context.Background()
	controller, err := dataproc.NewJobControllerClient(ctx)
	if err != nil {
		glog.Error("'NewJobControllerClient' method call failed.")
		return
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
		glog.Error("'SubmitJob' method call failed.")
		return
	}
	glog.Infof("New job deployed in cluster '%s'.", c.Name)

}

// GetMetricsSnapshot is for getting last metrics of the cluster
func (c *DataprocCluster) GetMetricsSnapshot() m.Metrics {
	return c.GetMetrics()
}

// SetMetricsSnapshot is for updating the cluster with new metrics
// @newMetrics is the object filled with new metrics
func (c *DataprocCluster) SetMetricsSnapshot(newMetrics m.Metrics) {
	c.SetMetrics(newMetrics)
}

// <-- end implementation of `ClusterBaseInterface` interface -->