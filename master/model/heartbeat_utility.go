package model

// MetricsDidBorn is a temporary var
// TODO: remove this dirty code
var MetricsDidBorn = &HeartbeatMessage{
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
