package model

type DataprocCluster struct {
	*ClusterBase
}

/**
* <-- interface `Cluster` implementation -->
* Scale up the cluster, i.e. add new nodes to increase size
* @param nodes is the number of nodes to add
 */
func (c *DataprocCluster) ScaleUp(nodes int) {

}

/**
* <-- interface `Cluster` implementation -->
* Scale down the cluster, i.e. remove nodes to decrease size
* @param nodes is the number of nodes to remove
 */
func (c *DataprocCluster) ScaleDown(nodes int) {

}