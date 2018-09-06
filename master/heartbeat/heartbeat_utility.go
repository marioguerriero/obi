package heartbeat

import (
	"obi/master/model"
	"obi/master/predictor"
)

// HeartbeatToSnapshot converts an heartbeat to a snapshot of metrics to be sent to the predictor module
func HeartbeatToSnapshot(message *HeartbeatMessage) *predictor.MetricsSnasphot {
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

// MergeHeartbeatMetric converts an heartbeat to a snapshot of metrics to be sent to the predictor module
func MergeHeartbeatMetric(message *HeartbeatMessage, metrics *model.Metrics) {
	metrics.AMResourceLimitMB = message.AMResourceLimitMB
	metrics.AMResourceLimitVCores = message.AMResourceLimitVCores
	metrics.UsedAMResourceMB = message.UsedAMResourceMB
	metrics.UsedAMResourceVCores = message.UsedAMResourceVCores
	metrics.AppsSubmitted = message.AppsSubmitted
	metrics.AppsRunning = message.AppsRunning
	metrics.AppsPending = message.AppsPending
	metrics.AppsCompleted = message.AppsCompleted
	metrics.AppsKilled = message.AppsKilled
	metrics.AppsFailed = message.AppsFailed
	metrics.AggregateContainersPreempted = message.AggregateContainersPreempted
	metrics.ActiveApplications = message.ActiveApplications
	metrics.AppAttemptFirstContainerAllocationDelayNumOps = message.AppAttemptFirstContainerAllocationDelayNumOps
	metrics.AppAttemptFirstContainerAllocationDelayAvgTime = message.AppAttemptFirstContainerAllocationDelayAvgTime
	metrics.AllocatedMB = message.AllocatedMB
	metrics.AllocatedVCores = message.AllocatedVCores
	metrics.AllocatedContainers = message.AllocatedContainers
	metrics.AggregateContainersAllocated = message.AggregateContainersAllocated
	metrics.AggregateContainersReleased = message.AggregateContainersReleased
	metrics.AvailableMB = message.AvailableMB
	metrics.AvailableVCores = message.AvailableVCores
	metrics.PendingMB = message.PendingMB
	metrics.PendingVCores = message.PendingVCores
	metrics.PendingContainers = message.PendingContainers
	metrics.NumberOfNodes = message.NumberOfNodes
}
