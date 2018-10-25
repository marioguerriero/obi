package main

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
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
	"strconv"
	"time"
)

// ObiMaster structure representing one master instance for OBI
type ObiMaster struct {
	scheduler *scheduling.Scheduler
	heartbeatReceiver *heartbeat.Receiver
	predictorClient *predictor.ObiPredictorClient
	priorities map[string]int
}

// SubmitJob remote procedure call used to submit a job to one of the OBI infrastructures
func (m *ObiMaster) SubmitJob(ctx context.Context,
		jobRequest *JobSubmissionRequest) (*SubmitJobResponse, error) {

	// Create job object to be submitted to the scheduling component
	var jobType model.JobType
	switch jobRequest.Type {
	case JobSubmissionRequest_PYSPARK:
		jobType = model.JobTypePySpark
	default:
		jobType = model.JobTypeUndefined
	}

	md, _ := metadata.FromIncomingContext(ctx)
	userID, _ := strconv.Atoi(md["userid"][0])

	// Create job structure
	job := model.Job{
		CreationTimestamp:  time.Now(),
		ExecutablePath:     jobRequest.ExecutablePath,
		Type:               jobType,
		Priority:           jobRequest.Priority,
		Status: 			model.JobStatusPending,
		Args:               jobRequest.JobArgs,
		Author:             userID,
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
		logrus.WithField("error", err).Warning("Could not generate predictions")
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

	return &SubmitJobResponse{Succeded: true, JobID: int32(job.ID)}, nil
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
	serverAddr := fmt.Sprintf("%s:%d",
		os.Getenv("PREDICTOR_SERVICE_DNS_NAME"),
		8080)
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
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

	// Recover from failure by rescheduling any jobs which are still in the pending state
	pendingJobs, err := persistent.GetPendingJobs()
	if err != nil {
		logrus.WithField("error", err).Error("Unable to load pending jobs from database")
	}
	for _, job := range pendingJobs {
		master.scheduler.ScheduleJob(job)
	}

	return &master
}
