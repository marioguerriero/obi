package model

type DataprocCluster struct {
	*ClusterBase
	projectId string
	region string
	preemptiveNodesRatio int8
}

/**
* Constructor of DataprocCluster
* @param baseInfo is the base object for a cluster
* @param projectId is the project ID in the GCP environment
* @param region is the geo-region where the cluster was deployed (e.g. europe-west-1)
* @param preemptibleRatio in the percentage of preemptible VMs that has to be present inside the cluster
* return the pointer to the new DataprocCluster instance
 */
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
