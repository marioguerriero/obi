package main

import (
	flag "github.com/spf13/pflag"
	"context"
	"google.golang.org/grpc"
	"log"
	"google.golang.org/grpc/credentials"
	"crypto/tls"
			"strings"
	"os"
	"cloud.google.com/go/storage"
	"io"
	"path"
	uuid "github.com/satori/go.uuid"
	"fmt"
)

type obiCreds struct {
	username string
	password string
}

func (c obiCreds) GetRequestMetadata(ctx context.Context, in ...string) (map[string]string, error) {
	return map[string]string{
		"username": c.username,
		"password": c.password,
	}, nil
}

func (c obiCreds) RequireTransportSecurity() bool {
	return true
}


func main() {
	var jobRequestType JobSubmissionRequest_JobType
	execPath := flag.StringP("path", "f", "", "a string")
	infrastracture := flag.StringP("infrastructure", "i", "", "a string")
	jobType := flag.StringP("type", "t", "", "a string")
	priority := flag.Int32P("priority", "p", 0, "an int")

	flag.Parse()

	jobArgs :=strings.Join(flag.Args(), " ")

	if *jobType == "PySpark" {
		jobRequestType = JobSubmissionRequest_PYSPARK
	} else {
		log.Fatal("Job type unknown")
	}

	file, err := os.Open(*execPath)
	if err == nil {
		// local file, let's update on GCS
		ctx := context.Background()
		client, err := storage.NewClient(ctx)
		if err != nil {
			log.Fatal(err)
		}
		bkt := client.Bucket("dhg-obi")
		filename := uuid.Must(uuid.NewV4()).String() + path.Ext(*execPath)
		obj := bkt.Object("tmp/" + filename)
		w := obj.NewWriter(ctx)
		if _, err := io.Copy(w, file); err != nil {
			log.Fatal(err)
		}
		if err := w.Close(); err != nil {
			log.Fatal(err)
		}
		*execPath = "gs://dhg-obi/tmp/" + filename
	}

	jobRequest := JobSubmissionRequest{
		ExecutablePath:       *execPath,
		Infrastructure:       *infrastracture,
		Type:                 jobRequestType,
		JobArgs:              jobArgs,
		Priority:             *priority,
	}
	submitJob(jobRequest)
}

func submitJob(request JobSubmissionRequest) {
	creds := obiCreds {
		"luca",
		"ciao",
	}
	credentials := credentials.NewTLS( &tls.Config{ InsecureSkipVerify: true } )
	conn, err := grpc.Dial(
		"35.242.194.12:8081",
		grpc.WithTransportCredentials(credentials),
		grpc.WithPerRPCCredentials(creds),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := NewObiMasterClient(conn)
	resp, err := client.SubmitJob(context.Background(), &request)

	if err != nil {
		log.Fatal(err)
	}
	if resp.Succeded == false {
		fmt.Println("An error occurred during job submission. Please, contact the administrator.")
	}
	fmt.Println("The job has been submitted correctly.")
	fmt.Printf("The JobID is %d\n", resp.JobID)
}
