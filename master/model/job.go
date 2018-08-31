package model

// JobStatus defines the status of a job
type JobStatus int
const (
	// JobStatusRunning
	JobStatusRunning = iota
	// JobStatusPending
	JobStatusPending = iota
	// JobStatusFailed
	JobStatusFailed  = iota
)

// JobType defines the type of a job, e.g. PySpark, MapReduce, etc.
type JobType int
const (
	// JobTypePySpark
	JobTypePySpark   = iota
	// JobTypeUndefined
	JobTypeUndefined = iota
)

// Job models the job abstraction of OBI
type Job struct {
	Id int
	ExecutablePath string
	Type JobType
	Status JobStatus
	AssignedCluster string
	PredictedDuration int64
	FailureProbability float32
}
