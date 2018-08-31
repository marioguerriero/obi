package model

// JobStatus defines the status of a job
type JobStatus int
const (
	RUNNING = iota
	PENDING = iota
	FAILED = iota
)

// Job models the job abstraction of OBI
type Job struct {
	Id int64
	Cluster string
	Status JobStatus
}
