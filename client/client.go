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
	var username string
	var kubeconfig *string

	// parsing arguments
	execPath := flag.StringP("path", "f", "", "a string")
	infrastructure := flag.StringP("infrastructure", "i", "", "a string")
	jobType := flag.StringP("type", "t", "", "a string")
	priority := flag.Int32P("priority", "p", 0, "an int")

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
	deployment, err := clientset.AppsV1().Deployments("obi").Get(*infrastructure, metav1.GetOptions{})
	if err != nil {
		panic(err.Error())
	}
	serviceName := deployment.Annotations["master-service-name"]
	service, err := clientset.CoreV1().Services("obi").Get(serviceName, metav1.GetOptions{})
	if ingresses := service.Status.LoadBalancer.Ingress;  len(ingresses) == 0 {
		log.Fatal("Service not reachable.")
	}
	serviceAddress := service.Status.LoadBalancer.Ingress[0].IP
	submitJob(jobRequest, creds, serviceAddress)
}

func submitJob(request JobSubmissionRequest, creds obiCreds, address string) {
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
		fmt.Println("An error occurred during job submission. Please, contact the administrator.")
	}
	fmt.Println("The job has been submitted correctly.")
	fmt.Printf("The JobID is %d\n", resp.JobID)
}

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}
