package model

// JobStatus defines the status of a job
type JobStatus int
const (
	// JobStatusRunning attached to a job when it is running
	JobStatusRunning = iota
	// JobStatusPending attached to a job when it is waiting to be executed
	JobStatusPending = iota
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

// Job models the job abstraction of OBI
type Job struct {
	ID                 int
	ExecutablePath     string
	Type               JobType
	Priority           int32
	Status             JobStatus
	Platform 		   string
	AssignedCluster    string
	PredictedDuration  int64
	FailureProbability float32
	Args 			   string
}
