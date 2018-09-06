package model

import (
	"time"
	)

// Metrics is the struct composing an any type cluster to save last snapshot about metrics
type Metrics struct {
	Timestamp time.Time
	PendingContainers int32
	PendingMemory int32
	AvailableMemory int32
	FirstContainerDelayTime float32
	TotalContainersAllocated int32
	TotalContainersReleased int32
	// Heartbeat related fields
	AMResourceLimitMB                              int32
	AMResourceLimitVCores                          int32
	UsedAMResourceMB                               int32
	UsedAMResourceVCores                           int32
	AppsSubmitted                                  int32
	AppsRunning                                    int32
	AppsPending                                    int32
	AppsCompleted                                  int32
	AppsKilled                                     int32
	AppsFailed                                     int32
	AggregateContainersPreempted                   int32
	ActiveApplications                             int32
	AppAttemptFirstContainerAllocationDelayNumOps  int32
	AppAttemptFirstContainerAllocationDelayAvgTime float32
	AllocatedMB                                    int32
	AllocatedVCores                                int32
	AllocatedContainers                            int32
	AggregateContainersAllocated                   int32
	AggregateContainersReleased                    int32
	AvailableMB                                    int32
	AvailableVCores                                int32
	PendingMB                                      int32
	PendingVCores                                  int32
	NumberOfNodes        						   int32
}
