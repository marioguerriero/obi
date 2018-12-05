// Copyright 2018 Delivery Hero Germany
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
//     Unless required by applicable law or agreed to in writing, software
//     distributed under the License is distributed on an "AS IS" BASIS,
//     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//     See the License for the specific language governing permissions and
//     limitations under the License.

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
