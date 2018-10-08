package model

import (
	"time"
)

// JobStatus defines the status of a job
type JobStatus int
const (
	// JobStatusRunning attached to a job when it is running
	JobStatusRunning = iota
	// JobStatusPending attached to a job when it is waiting to be executed
	JobStatusPending = iota
	// JobStatusCompleted attached to a job when it has completed its execution
	JobStatusCompleted = iota
	// JobStatusFailed attached to a job when it failed
	JobStatusFailed  = iota
)

// JobType defines the type of a job, e.g. PySpark, MapReduce, etc.
type JobType int
const (
	// JobTypePySpark Python Spark job type
	JobTypePySpark   = iota
	// JobTypeUndefined unsupported/unrecognized job type
	JobTypeUndefined = iota
)

// JobStatusNames descriptive names for different job statuses
var JobStatusNames = map[JobStatus]string {
	JobStatusRunning: "running",
	JobStatusPending: "pending",
	JobStatusCompleted: "completed",
	JobStatusFailed: "failed",
}

// JobTypeNames descriptive names for different job types
var JobTypeNames = map[JobType]string {
	JobTypePySpark: "pyspark",
	JobTypeUndefined: "undefined",
}

// Job models the job abstraction of OBI
type Job struct {
	ID                 int
	Cluster    ClusterBaseInterface
	Author int
	CreationTimestamp time.Time
	ExecutablePath     string
	Type               JobType
	Priority           int32
	Status             JobStatus
	PredictedDuration  int32
	FailureProbability float32
	Args 			   string
	PlatformDependentID string
	DriverOutputPath string
}
