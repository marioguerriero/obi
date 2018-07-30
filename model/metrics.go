package model

import "time"

// Metrics is the struct composing an any type cluster to save last snapshot about metrics
type Metrics struct {
	Timestamp time.Time
	PendingContainers int32
	AllocatedContainers int32
	PendingMemory int32
	AvailableMemory int32
	PendingVCores int32
}
