package model

type DataprocCluster struct {
	*ClusterBase
	projectId string
	region string
	preemptiveNodesRatio int8
}

func New(baseInfo *ClusterBase, projectId string, region string, preemptibleRatio int8) *DataprocCluster {
	return &DataprocCluster{
		baseInfo,
		projectId,
		region,
		preemptibleRatio,
	}
}

// <-- start interface `Cluster` implementation -->

/**
* Scale up the cluster, i.e. add new nodes to increase size
* @param nodes is the number of nodes to add
 */
func (c *DataprocCluster) ScaleUp(nodes int) {

}

/**
* Scale down the cluster, i.e. remove nodes to decrease size
* @param nodes is the number of nodes to remove
 */
func (c *DataprocCluster) ScaleDown(nodes int) {

}

// <-- end interface `Cluster` implementation -->
