package policies

import (
	"obi/master/model"
	"obi/master/predictor"
)

// MetricsToSnapshot converts a master's metric snapshot to a predictor metric snapshot
func MetricsToSnapshot(metrics *model.Metrics) *predictor.MetricsSnasphot {
	return &predictor.MetricsSnasphot{
		AMResourceLimitMB:                              metrics.AMResourceLimitMB,
		AMResourceLimitVCores:                          metrics.AMResourceLimitVCores,
		UsedAMResourceMB:                               metrics.UsedAMResourceMB,
		UsedAMResourceVCores:                           metrics.UsedAMResourceVCores,
		AppsSubmitted:                                  metrics.AppsSubmitted,
		AppsRunning:                                    metrics.AppsRunning,
		AppsPending:                                    metrics.AppsPending,
		AppsCompleted:                                  metrics.AppsCompleted,
		AppsKilled:                                     metrics.AppsKilled,
		AppsFailed:                                     metrics.AppsFailed,
		AggregateContainersPreempted:                   metrics.AggregateContainersPreempted,
		ActiveApplications:                             metrics.ActiveApplications,
		AppAttemptFirstContainerAllocationDelayNumOps:  metrics.AppAttemptFirstContainerAllocationDelayNumOps,
		AppAttemptFirstContainerAllocationDelayAvgTime: metrics.AppAttemptFirstContainerAllocationDelayAvgTime,
		AllocatedMB:                  					metrics.AllocatedMB,
		AllocatedVCores:              					metrics.AllocatedVCores,
		AllocatedContainers:          					metrics.AllocatedContainers,
		AggregateContainersAllocated: 					metrics.AggregateContainersAllocated,
		AggregateContainersReleased:  					metrics.AggregateContainersReleased,
		AvailableMB:                  					metrics.AvailableMB,
		AvailableVCores:              					metrics.AvailableVCores,
		PendingMB:                    					metrics.PendingMB,
		PendingVCores:                					metrics.PendingVCores,
		PendingContainers:            					metrics.PendingContainers,
		NumberOfNodes:                					metrics.NumberOfNodes,
	}
}
