package model

import (
		"obi/master/predictor"
)

// MetricsToSnapshot converts an heartbeat to a snapshot of metrics to be sent to the predictor module
func MetricsToSnapshot(message Metrics) *predictor.MetricsSnasphot {
	return &predictor.MetricsSnasphot{
		AMResourceLimitMB: message.AMResourceLimitMB,
		AMResourceLimitVCores: message.AMResourceLimitVCores,
		UsedAMResourceMB: message.UsedAMResourceMB,
		UsedAMResourceVCores: message.UsedAMResourceVCores,
		AppsSubmitted: message.AppsSubmitted,
		AppsRunning: message.AppsRunning,
		AppsPending: message.AppsPending,
		AppsCompleted: message.AppsCompleted,
		AppsKilled: message.AppsKilled,
		AppsFailed: message.AppsFailed,
		AggregateContainersPreempted: message.AggregateContainersPreempted,
		ActiveApplications: message.ActiveApplications,
		AppAttemptFirstContainerAllocationDelayNumOps: message.AppAttemptFirstContainerAllocationDelayNumOps,
		AppAttemptFirstContainerAllocationDelayAvgTime: message.AppAttemptFirstContainerAllocationDelayAvgTime,
		AllocatedMB: message.AllocatedMB,
		AllocatedVCores: message.AllocatedVCores,
		AllocatedContainers: message.AllocatedContainers,
		AggregateContainersAllocated: message.AggregateContainersAllocated,
		AggregateContainersReleased: message.AggregateContainersReleased,
		AvailableMB: message.AvailableMB,
		AvailableVCores: message.AvailableVCores,
		PendingMB: message.PendingMB,
		PendingVCores: message.PendingVCores,
		PendingContainers: message.PendingContainers,
		NumberOfNodes: message.NumberOfNodes,
	}
}

// TODO: remove this dirty code
var MetricsDidBorn *predictor.MetricsSnasphot = &predictor.MetricsSnasphot{
	AMResourceLimitMB:                              0,
	AMResourceLimitVCores:                          0,
	UsedAMResourceMB:                               0,
	UsedAMResourceVCores:                           0,
	AppsSubmitted:                                  0,
	AppsRunning:                                    0,
	AppsPending:                                    0,
	AppsCompleted:                                  0,
	AppsKilled:                                     0,
	AppsFailed:                                     0,
	AggregateContainersPreempted:                   0,
	ActiveApplications:                             0,
	AppAttemptFirstContainerAllocationDelayNumOps:  0,
	AppAttemptFirstContainerAllocationDelayAvgTime: 0,
	AllocatedMB:                  0,
	AllocatedVCores:              0,
	AllocatedContainers:          0,
	AggregateContainersAllocated: 0,
	AggregateContainersReleased:  0,
	AvailableMB:                  24576,
	AvailableVCores:              8,
	PendingMB:                    0,
	PendingVCores:                0,
	PendingContainers:            0,
	NumberOfNodes:                2,
}
