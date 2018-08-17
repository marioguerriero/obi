package pooling

import (
	"obi/master/utils"
	"obi/master/autoscaler"
	"obi/master/model"
)

// Pool struct with properties
type Pool struct {
	clusters *utils.ConcurrentMap
	autoscalers *utils.ConcurrentMap
}

// singleton instance
var poolInstance *Pool

// GetPool is for retrieving the singleton Pool struct
// return the pointer to the instance
func GetPool() *Pool {
	if poolInstance == nil {
		poolInstance = &Pool{
			utils.NewConcurrentMap(),
			utils.NewConcurrentMap(),
		}
	}
	return poolInstance
}

// AddCluster is for adding a new cluster inside the pool
// @param cluster is a generic cluster struct
// @param autoscaler is the autoscaler object that will monitor the cluster
func (p *Pool) AddCluster(cluster model.ClusterBaseInterface, autoscaler *autoscaler.Autoscaler) {
	clusterName := cluster.GetName()
	p.clusters.Set(clusterName, cluster)
	p.autoscalers.Set(clusterName, autoscaler)
}

// RemoveCluster is for deleting a cluster from the pool, turning off its autoscaler
// @param clusterName is the name of the cluster
func (p *Pool) RemoveCluster(clusterName string) {
	p.clusters.Delete(clusterName)
	obj, ok := p.autoscalers.Get(clusterName)
	if ok {
		obj.(*autoscaler.Autoscaler).StopMonitoring()
	}
	p.autoscalers.Delete(clusterName)
}

// Clusters is for getting the list of all clusters inside the pool
// return a channel containing the cluster objects
func (p *Pool) Clusters() <-chan utils.ConcurrentMapItem {
	return p.clusters.Iter()
}

// GetCluster is for getting a specific cluster inside the pool
// @param clusterName is the name of the cluster
// return the optional object and a bool to check if it is present
func (p *Pool) GetCluster(clusterName string) (interface{}, bool) {
	return p.clusters.Get(clusterName)
}


