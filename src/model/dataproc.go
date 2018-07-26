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
	ProjectID string
	Region string
	PreemptiveNodesRatio int8
}

// NewDataprocCluster is the constructor of DataprocCluster struct
// @param baseInfo is the base object for a cluster
// @param projectId is the project ID in the GCP environment
// @param region is the geo-region where the cluster was deployed (e.g. europe-west-1)
// @param preemptibleRatio in the percentage of preemptible VMs that has to be present inside the cluster
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

// Scale is for scaling up the cluster, i.e. add new nodes to increase size
// @param nodes is the number of nodes to add
// @param direction is for specifying if there is the need to add o remove nodes
func (c *DataprocCluster) Scale(nodes int16, direction autoscaler.ScalingDirection) {
	var newSize int32

	ctx := context.Background()
	controller, err := dataproc.NewClusterControllerClient(ctx)

	if direction == autoscaler.Up {
		newSize = int32(c.Nodes + nodes)
	} else {
		newSize = int32(c.Nodes - nodes)
	}

	if err != nil {
		// TODO: log error.
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
			Paths: []string{"config.secondary_worker_config.num_instances"},
		},
	}

	op, err := controller.UpdateCluster(ctx, req)
	if err != nil {
		// TODO: log error.
	}

	_, err = op.Wait(ctx)
	if err != nil {
		// TODO: log error.
	} else {
		// TODO: log success.
	}
}

// <-- end implementation of `Scalable` interface -->
