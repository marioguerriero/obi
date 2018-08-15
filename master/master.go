package main

import (
	"context"
	"obi/master/pooling"
	"obi/master/utils"
	"obi/master/heartbeat"
	"github.com/sirupsen/logrus"
)

// ObiMaster structure representing one master instance for OBI
type ObiMaster struct {
	Pooling *pooling.Pooling
	HeartbeatReceiver *heartbeat.Receiver
}

// ListInfrastructures RPC for listing the available infrastructure services
// @param ctx
// @param msg
func (m *ObiMaster) ListInfrastructures(ctx context.Context,
		msg *EmptyRequest) (*ListInfrastructuresResponse, error) {
	return nil, nil
}

// SubmitJob remote procedure call used to submit a job to one of the OBI infrastructures
func (m *ObiMaster) SubmitJob(ctx context.Context,
		jobRequest *SubmitJobRequest) (*EmptyResponse, error) {
	logrus.WithField("request", *jobRequest).Info("Received job request")

	switch jobRequest.Job.Type {
	case Job_PYSPARK:
		m.Pooling.SubmitPySparkJob("obi-test", jobRequest.Job.ExecutablePath)
	default:
		logrus.WithField("job-type", jobRequest.Job.Type).Error("Unsupported job type")
	}

	return new(EmptyResponse), nil
}

// CreateMaster generates a new OBI master instance
func CreateMaster() (*ObiMaster) {
	// Create new cluster pooling object
	pool := utils.NewConcurrentMap()
	p := pooling.New(pool)
	hb := heartbeat.GetInstance(pool, 60, 30)
	hb.Start()

	// Create and return OBI master object
	master := ObiMaster {
		Pooling: p,
		HeartbeatReceiver: hb,
	}
	return &master
}