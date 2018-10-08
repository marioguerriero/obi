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
	"github.com/satori/go.uuid"
	"fmt"
	"golang.org/x/crypto/ssh/terminal"
			"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"path/filepath"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"net/http"
	"time"
	"encoding/json"
)

type JobInfoResponse struct {
	Status string
	User string
	CreationTimeStamp string
	ScriptPath string
	Args string
}

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
	var username string

	// parsing arguments
	execPath := flag.StringP("path", "f", "", "a string")
	infrastructure := flag.StringP("infrastructure", "i", "", "a string")
	jobType := flag.StringP("type", "t", "", "a string")
	priority := flag.Int32P("priority", "p", 0, "an int")
	wait := flag.BoolP("wait", "w", true, "wait for job completion")


	flag.Parse()

	// fill job request struct
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
		Infrastructure:       *infrastructure,
		Type:                 jobRequestType,
		JobArgs:              jobArgs,
		Priority:             *priority,
	}

	// ask for credentials
	fmt.Println("Username: ")
	fmt.Scanf("%s\n", &username)
	fmt.Println("Password: ")
	password, err := terminal.ReadPassword(0)
	if err != nil {
		log.Fatal("Something went wrong. Sorry.")
	}

	creds := obiCreds {
		username,
		string(password),
	}

	masterServiceAddress, apiServiceAddress := getEndpoints(*infrastructure)
	jobID := submitJob(jobRequest, creds, masterServiceAddress)
	if *wait {
		fmt.Println("Waiting for job completion...")
		client := &http.Client{Timeout: 30 * time.Second}
		apiJobs := "http://" + apiServiceAddress + ":8083/api/jobs"
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
				log.Fatal("The job execution failed.")
			}
			time.Sleep(10 * time.Second)
		}
	}
}

func submitJob(request JobSubmissionRequest, creds obiCreds, address string) int32 {
	credentials := credentials.NewTLS( &tls.Config{ InsecureSkipVerify: true } )
	conn, err := grpc.Dial(
		address + ":8081",
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

func getEndpoints(infrastructure string) (string, string) {
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
	deployment, err := clientset.AppsV1().Deployments("obi").Get(infrastructure, metav1.GetOptions{})
	if err != nil {
		panic(err.Error())
	}
	masterServiceName := deployment.Annotations["master-service-name"]
	masterService, err := clientset.CoreV1().Services("obi").Get(masterServiceName, metav1.GetOptions{})
	if err != nil {
		panic(err.Error())
	}
	if ingresses := masterService.Status.LoadBalancer.Ingress;  len(ingresses) == 0 {
		log.Fatal("Master service not reachable.")
	}

	apiServiceName := deployment.Annotations["api-service-name"]
	apiService, err := clientset.CoreV1().Services("obi").Get(apiServiceName, metav1.GetOptions{})
	if err != nil {
		panic(err.Error())
	}
	if ingresses := apiService.Status.LoadBalancer.Ingress;  len(ingresses) == 0 {
		log.Fatal("API service not reachable.")
	}
	return masterService.Status.LoadBalancer.Ingress[0].IP, apiService.Status.LoadBalancer.Ingress[0].IP
}
