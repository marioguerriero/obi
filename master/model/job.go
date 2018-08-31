package model

// JobStatus defines the status of a job
type JobStatus int
const (
	JobStatusRunning = iota
	JobStatusPending = iota
	JobStatusFailed  = iota
)

// JobType defines the type of a job, e.g. PySpark, MapReduce, etc.
type JobType int
const (
	JobTypePyspark   = iota
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
