package main

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"io"
	"obi/master/heartbeat"
	"obi/master/model"
	"obi/master/persistent"
	"obi/master/pool"
	"obi/master/predictor"
	"obi/master/scheduling"
	"obi/master/utils"
	"os"
	"path/filepath"
	"time"
)

// ObiMaster structure representing one master instance for OBI
type ObiMaster struct {
	scheduler *scheduling.Scheduler
	heartbeatReceiver *heartbeat.Receiver
	predictorClient *predictor.ObiPredictorClient
	priorities map[string]int
}

// ListInfrastructures RPC for listing the available infrastructure services
// @param ctx
// @param msg
func (m *ObiMaster) ListInfrastructures(ctx context.Context,
		msg *Empty) (*ListInfrastructuresResponse, error) {
	return nil, nil
}

// SubmitJob remote procedure call used to submit a job to one of the OBI infrastructures
func (m *ObiMaster) SubmitJob(ctx context.Context,
		jobRequest *JobSubmissionRequest) (*Empty, error) {

	// Create job object to be submitted to the scheduling component
	var jobType model.JobType
	switch jobRequest.Type {
	case JobSubmissionRequest_PYSPARK:
		jobType = model.JobTypePySpark
	default:
		jobType = model.JobTypeUndefined
	}

	// Create job structure
	job := model.Job{
		CreationTimestamp:  time.Now(),
		ExecutablePath:     jobRequest.ExecutablePath,
		Type:               jobType,
		Priority:           jobRequest.Priority,
		Status: 			model.JobStatusPending,
		Args:               jobRequest.JobArgs,
	}

	// Generate predictions before submitting the job
	logrus.WithField("path", jobRequest.ExecutablePath).Info("Analyzing new job request")
	resp, err := (*m.predictorClient).RequestPrediction(
		context.Background(), &predictor.PredictionRequest{
			JobFilePath: jobRequest.ExecutablePath,
			JobArgs: jobRequest.JobArgs,
			Metrics: model.MetricsDidBorn,
		})
	if err != nil {
		logrus.WithField("response", resp).Warning("Could not generate predictions")
		job.PredictedDuration = 0

		if job.Priority < 0 {
			job.Priority = 0
		}
	} else {
		logrus.WithFields(logrus.Fields{
			"type": resp.Label,
			"duration": resp.Duration,
		}).Info("New job")

		job.PredictedDuration = resp.Duration

		if val, ok := m.priorities[resp.Label]; ok && job.Priority < 0 {
			job.Priority = int32(val)
		}
	}

	// Write submitted job into persistent storage
	persistent.Write(&job)

	// Send job execution request
	logrus.WithField("priority-level", job.Priority).Info("Schedule job for execution")
	m.scheduler.ScheduleJob(&job)

	return new(Empty), nil
}

// SubmitExecutable accepts and store an executable file
func (m *ObiMaster) SubmitExecutable(stream ObiMaster_SubmitExecutableServer) error {
	var filename string
	var f *os.File

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			absPath, _ := filepath.Abs(filename)
			return stream.SendAndClose(&ExecutableSubmissionResponse{
				Filename:fmt.Sprintf("file://%s", absPath),
			})
		}
		if f == nil {
			// Create file
			filename = fmt.Sprintf("%s-%s", utils.RandomString(5), filepath.Base(req.Filename))
			logrus.WithField("filename", filename).Info("Storing local executable")
			f, err = os.Create(filename)
			if err != nil {
				return err
			}
		}
		f.WriteString(req.Chunk)
	}
}

// CreateMaster generates a new OBI master instance
func CreateMaster() (*ObiMaster) {

	// Load priority map
	priorityMap := map[string]int{}
	tmp := viper.GetStringMap("priorityMap")
	for k, v := range tmp {
		if vInt, ok := v.(int); ok {
			priorityMap[k] = vInt
		} else {
			logrus.Panicln("Not integer value in the priority map.")
		}

	}

	// Start up the pool
	pool.GetPool().StartLivelinessMonitoring()

	// Setup scheduler
	submitter := pool.NewSubmitter()
	scheduler := scheduling.New(submitter)
	scheduler.SetupConfig()

	// Setup heartbeat
	hb := heartbeat.New()

	// Start everything
	hb.Start()
	scheduler.Start()

	// Open connection to predictor server
	serverAddr := fmt.Sprintf("%s:%s",
		viper.GetString("predictorHost"),
		viper.GetString("predictorPort"))
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure()) // TODO: encrypt communication
	if err != nil {
		logrus.Fatalf("fail to dial: %v", err)
	}
	pClient := predictor.NewObiPredictorClient(conn)

	// Open connection to persistent storage
	err = persistent.CreatePersistentConnection()
	if err != nil {
		logrus.Fatal("Could not connect to persistent database")
	}
	logrus.Info("Connected to persistent database")

	// Create and return OBI master object
	master := ObiMaster {
		scheduler: scheduler,
		heartbeatReceiver: hb,
		predictorClient: &pClient,
		priorities: priorityMap,
	}

	return &master
}