package pooling

import (
	"obi/master/utils"
	"obi/master/autoscaler"
	"obi/master/model"
)

// ClusterBase is the base class for any type of cluster
type Pool struct {
	clusters *utils.ConcurrentMap
	autoscalers *utils.ConcurrentMap
}

// singleton instance
var poolInstance *Pool

func GetPool() *Pool {
	if poolInstance == nil {
		poolInstance = &Pool{
			utils.NewConcurrentMap(),
			utils.NewConcurrentMap(),
		}
	}
	return poolInstance
}

func (p *Pool) AddCluster(cluster model.ClusterBaseInterface, autoscaler *autoscaler.Autoscaler) {
	clusterName := cluster.GetName()
	p.clusters.Set(clusterName, cluster)
	p.autoscalers.Set(clusterName, autoscaler)
}

func (p *Pool) RemoveCluster(clusterName string) {
	p.clusters.Delete(clusterName)
	obj, ok := p.autoscalers.Get(clusterName)
	if ok {
		obj.(*autoscaler.Autoscaler).StopMonitoring()
	}
	p.autoscalers.Delete(clusterName)
}

func (p *Pool) Clusters() <-chan utils.ConcurrentMapItem {
	return p.clusters.Iter()
}

func (p *Pool) GetCluster(clusterName string) (interface{}, bool) {
	return p.clusters.Get(clusterName)
}


