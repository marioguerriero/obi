package model

import (
	dataprocpb "google.golang.org/genproto/googleapis/cloud/dataproc/v1"
	"cloud.google.com/go/dataproc/apiv1"
	"google.golang.org/genproto/protobuf/field_mask"
	"context"
	"autoscaler"
)

// DataprocCluster is the extended cluster struct of Google Dataproc
type DataprocCluster struct {
	*ClusterBase
	*autoscaler.Autoscaler
	projectID string
	region string
	preemptiveNodesRatio int8
}

// NewDataprocCluster is the constructor of DataprocCluster struct
// @param baseInfo is the base object for a cluster
// @param projectId is the project ID in the GCP environment
// @param region is the geo-region where the cluster was deployed (e.g. europe-west-1)
// @param preemptibleRatio in the percentage of preemptible VMs that has to be present inside the cluster
// @param autoscaler is autoscaler struct which will take care about the size of the cluster
// return the pointer to the new DataprocCluster instance
func NewDataprocCluster(baseInfo *ClusterBase, projectID string, region string, preemptibleRatio int8, autoscaler *autoscaler.Autoscaler) *DataprocCluster {
	return &DataprocCluster{
		baseInfo,
		autoscaler,
		projectID,
		region,
		preemptibleRatio,
	}
}

// <-- start implementation of `Scalable` interface -->

// ScaleUp is for scaling up the cluster, i.e. add new nodes to increase size
// @param nodes is the number of nodes to add
func (c *DataprocCluster) ScaleUp(nodes int) {
	ctx := context.Background()
	controller, err := dataproc.NewClusterControllerClient(ctx)
	if err != nil {
		// TODO: Handle error.
	}

	req := &dataprocpb.UpdateClusterRequest{
		ProjectId:   c.projectID,
		Region:      c.region,
		ClusterName: c.name,
		Cluster: &dataprocpb.Cluster{
			Config: &dataprocpb.ClusterConfig{
				SecondaryWorkerConfig: &dataprocpb.InstanceGroupConfig{
					NumInstances: 10,
				},
			},
		},
		UpdateMask:  &field_mask.FieldMask{
			Paths: []string{"config.secondary_worker_config.num_instances"},
		},
	}

	op, err := controller.UpdateCluster(ctx, req)
	if err != nil {
		// TODO: Handle error.
	}

	resp, err := op.Wait(ctx)
	if err != nil {
		// TODO: Handle error.
	}
	// TODO: Use resp.
	_ = resp
}


// ScaleDown is for scaling down the cluster, i.e. remove nodes to decrease size
// @param nodes is the number of nodes to remove
func (c *DataprocCluster) ScaleDown(nodes int) {

}

// <-- end implementation of `Scalable` interface -->
