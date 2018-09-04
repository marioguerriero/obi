package main

import (
	"obi/master/model"
	"time"
)

// WALRecordType identify the type of a WAL record
type WALRecordType int

const (
	// WALPending identify a record appended for tracking a pending job
	WALPending = iota
	// WALFailed identify a record for a failed job
	WALFailed = iota
	// WALCompleted identify a record for a completed job
	WALCompleted = iota
)

// WAL Write Ahead Log handler
type WAL struct {
	Path string
}

// WalRecord defines the necessary information to be stored in a WAL record
type WALRecord struct {
	Job model.Job
	Type WALRecordType
	Timestamp time.Time
}

// New create a new WAL data structure
func New(path string) *WAL {
	return &WAL{
		Path: path,
	}
}

// Append used to add job records to WAL
func (wal *WAL) Append(job *model.Job) {

}

// Restore used to recover from failure
func (wal *WAL) Restore() {

}
