// Copyright 2018 
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
//     Unless required by applicable law or agreed to in writing, software
//     distributed under the License is distributed on an "AS IS" BASIS,
//     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//     See the License for the specific language governing permissions and
//     limitations under the License.

package main

import (
	"cloud.google.com/go/storage"
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	flag "github.com/spf13/pflag"
	"golang.org/x/crypto/ssh/terminal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"io"
	"io/ioutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/tools/clientcmd"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

// JobInfoResponse defines the response type for job information requests
type JobInfoResponse struct {
	Status string
	User string
	CreationTimeStamp string
	ScriptPath string
	Args string
	DriverOutputURI string
}

type obiCreds struct {
	Username string
	Password string
}

func md5FileContent(path string) string {
	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// Copy file into hash structure
	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		log.Fatal(err)
	}

	hashInBytes := h.Sum(nil)
	md5String := hex.EncodeToString(hashInBytes)
	return md5String
}

// GetRequestMetadata indicates whether the credentials requires transport security.
func (c obiCreds) GetRequestMetadata(ctx context.Context, in ...string) (map[string]string, error) {
	return map[string]string{
		"username": c.Username,
		"password": c.Password,
	}, nil
}

// RequireTransportSecurity unimplemented fo
func (c obiCreds) RequireTransportSecurity() bool {
	return true
}

func submitJob(request JobSubmissionRequest, creds obiCreds, address string) int32 {
	credentials := credentials.NewTLS( &tls.Config{ InsecureSkipVerify: true } )
	conn, err := grpc.Dial(
		address + ":8081",
		grpc.WithTransportCredentials(credentials),
		grpc.WithPerRPCCredentials(creds),
	)
	if err != nil {
		fmt.Println("Err")
		log.Fatal(err)
	}
	defer conn.Close()

	client := NewObiMasterClient(conn)
	resp, err := client.SubmitJob(context.Background(), &request)

	if err != nil {
		fmt.Println("error")
		log.Fatal(err)
	}
	if resp.Succeded == false {
		log.Fatal("An error occurred during job submission. Please, contact the administrator.")
	}
	fmt.Println("The job has been submitted correctly.")
	fmt.Printf("The JobID is %d\n", resp.JobID)
	return resp.JobID
}

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}

func getEndpoints(infrastructure string) string {
	var kubeconfig *string

	// load kubeconfig
	if home := homeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	// get service to contact obi master endpoint
	masterService, err := clientset.CoreV1().Services("obi").Get(infrastructure + "-submit-jobs", metav1.GetOptions{})
	if err != nil {
		panic(err.Error())
	}
	if ingresses := masterService.Status.LoadBalancer.Ingress; len(ingresses) == 0 {
		log.Fatal("Master service not reachable.")
	}

	return masterService.Status.LoadBalancer.Ingress[0].IP
}

func prepareJobRequest(jobType string, execPath string, infrastructure string, priority int32) JobSubmissionRequest {
	var jobRequestType JobSubmissionRequest_JobType

	// fill job request struct
	jobArgs :=strings.Join(flag.Args(), " ")

	if jobType == "PySpark" {
		jobRequestType = JobSubmissionRequest_PYSPARK
	} else {
		log.Fatal("Job type unknown")
	}

	file, err := os.Open(execPath)
	if err == nil {
		// local file, let's update on GCS
		ctx := context.Background()
		client, err := storage.NewClient(ctx)
		if err != nil {
			log.Fatal(err)
		}
		bkt := client.Bucket("dhg-obi")
		filename := md5FileContent(execPath) + "/" + path.Base(execPath)
		obj := bkt.Object("tmp/" + filename)
		w := obj.NewWriter(ctx)
		if _, err := io.Copy(w, file); err != nil {
			log.Fatal(err)
		}
		if err := w.Close(); err != nil {
			log.Fatal(err)
		}
		execPath = "gs://dhg-obi/tmp/" + filename
	}

	jobRequest := JobSubmissionRequest{
		ExecutablePath:       execPath,
		Infrastructure:       infrastructure,
		Type:                 jobRequestType,
		JobArgs:              jobArgs,
		Priority:             priority,
	}

	return jobRequest
}

func main() {
	var credentials obiCreds

	// parsing arguments
	execPath := flag.StringP("path", "f", "", "a string")
	infrastructure := flag.StringP("infrastructure", "i", "", "a string")
	jobType := flag.StringP("type", "t", "", "a string")
	priority := flag.Int32P("priority", "p", 0, "an int")
	wait := flag.BoolP("wait", "w", false, "wait for job completion")
	useLocalCreds := flag.Bool("localcreds", false, "get local credentials")

	flag.Parse()

	jobRequest := prepareJobRequest(*jobType, *execPath, *infrastructure, *priority)

	if *useLocalCreds == true {
		credsFile, err := ioutil.ReadFile("/usr/local/airflow/dags/obi-exec/credentials")
		if err != nil {
			log.Fatal("Impossible to get local credentials.")
		}
		creds := string(credsFile)
		credsArray := strings.Split(creds, ",")

		if len(credsArray) != 2 {
			log.Fatal("Credentials file wrong format.")
		}

		credentials.Username = credsArray[0]
		credentials.Password = credsArray[1]
	} else {
		var username string
		// ask for credentials
		fmt.Println("Username: ")
		fmt.Scanf("%s\n", &username)
		fmt.Println("Password: ")
		password, err := terminal.ReadPassword(0)
		if err != nil {
			log.Fatal("Something went wrong. Sorry.")
		}

		credentials.Username = username
		credentials.Password = string(password)
	}


	masterServiceAddress := getEndpoints(*infrastructure)
	jobID := submitJob(jobRequest, credentials, masterServiceAddress)
	if *wait {
		fmt.Println("Waiting for job completion...")
		client := &http.Client{Timeout: 30 * time.Second}
		apiJobs := "http://obi.dataops.deliveryhero.de/" + *infrastructure +"/api/jobs"
		req, _ := http.NewRequest("GET", apiJobs, nil)
		q := req.URL.Query()
		q.Add("jobid", fmt.Sprint(jobID))
		req.URL.RawQuery = q.Encode()
		for {
			resp, err := client.Do(req)
			if err != nil {
				log.Fatal("An error occurring during status request.")
				break
			}
			defer resp.Body.Close()
			jobInfo := JobInfoResponse{}
			err = json.NewDecoder(resp.Body).Decode(&jobInfo)
			if err != nil {
				log.Fatal("An error occurring during status request.")
				break
			}
			if jobInfo.Status == "completed" {
				break
			} else if jobInfo.Status == "failed" {
				url := fmt.Sprintf("%s/%s",
					"https://console.cloud.google.com/storage/browser",
					strings.Replace(filepath.Dir(jobInfo.DriverOutputURI), "gs:/", "", 1))
				log.Fatal("The job execution failed. For more information see the driver output of the job: " +
					url)
			}
			time.Sleep(10 * time.Second)
		}
	}
}
