package main

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"log"
	"math/rand"
	"obi/master/heartbeat"
	"obi/master/model"
	"obi/master/pooling"
	"obi/master/predictor"
)

// ObiMaster structure representing one master instance for OBI
type ObiMaster struct {
	Pooling *pooling.Pooling
	HeartbeatReceiver *heartbeat.Receiver
	PredictorClient *predictor.ObiPredictorClient
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
		jobRequest *JobSubmissionRequest) (*EmptyResponse, error) {
	logrus.WithField("path", jobRequest.ExecutablePath).Info("Received job request")

	// Generate predictions before submitting the job
	resp, err := (*m.PredictorClient).RequestPrediction(
		context.Background(), &predictor.PredictionRequest{
			JobFilePath: jobRequest.ExecutablePath,
		}) // TODO: read metrics for executor cluster
	if err != nil {
		logrus.WithField("response", resp).Warning("Could not generate predictions")
	}
	duration, failure := 0, 0.0 // TODO: use predicted values

	// Create job object to be submitted to the pooling component
	var jobType model.JobType
	switch jobRequest.Type {
	case JobSubmissionRequest_PYSPARK:
		jobType = model.JobTypePySpark
	default:
		jobType = model.JobTypeUndefined
	}

	job := &model.Job{
		ID:                 rand.Int(),
		ExecutablePath:     jobRequest.ExecutablePath,
		Type:               jobType,
		PredictedDuration:  int64(duration),
		FailureProbability: float32(failure),
		Args: 				jobRequest.JobArgs,
	}

	// Send job execution request
	logrus.WithField("priority-level", jobRequest.Priority).Info("Schedule job for execution")
	m.Pooling.ScheduleJob(job, jobRequest.Priority)

	return new(EmptyResponse), nil
}

// CreateMaster generates a new OBI master instance
func CreateMaster() (*ObiMaster) {
	// Create new cluster pooling object
	pool := pooling.GetPool()
	p := pooling.New(pool)
	hb := heartbeat.New(pool)

	hb.Start()
	pool.StartLivelinessMonitoring()

	// Open connection to predictor server
	serverAddr := fmt.Sprintf("%s:%s",
		viper.GetString("predictorHost"),
		viper.GetString("predictorPort"))
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure()) // TODO: encrypt communication
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	pClient := predictor.NewObiPredictorClient(conn)
	// Create and return OBI master object
	master := ObiMaster {
		Pooling: p,
		HeartbeatReceiver: hb,
		PredictorClient: &pClient,
	}
	return &master
}