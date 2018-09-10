package model

import "github.com/golang-collections/go-datastructures/queue"

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
	Priority           int8
	Status             JobStatus
	Platform 		   string
	AssignedCluster    string
	PredictedDuration  int64
	FailureProbability float32
	Args 			   string
}

func (j *Job) Compare(other queue.Item) int {
	otherJob := other.(*Job)
	if j.Priority > otherJob.Priority {
		return 1
	} else if j.Priority == otherJob.Priority {
		if j.PredictedDuration < otherJob.PredictedDuration {
			return 1
		} else if j.PredictedDuration == otherJob.PredictedDuration {
			return 0
		} else {
			return -1
		}
	} else {
		return -1
	}
}
