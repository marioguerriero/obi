package model

// JobStatus defines the status of a job
type JobStatus int
const (
	JOB_STATUS_RUNNING = iota
	JOB_STATUS_PENDING = iota
	JOB_STATUS_FAILED = iota
)

// JobType defines the type of a job, e.g. PySpark, MapReduce, etc.
type JobType int
const (
	JOB_TYPE_PYSPARK = iota
	JOB_TYPE_UNDEFINED = iota
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
