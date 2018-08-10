package main

import (
	"os"
	"github.com/sirupsen/logrus"
	"net"
	"fmt"
	"google.golang.org/grpc"
)

const ConfigPath = ""
var Port int

func parseConfig() {
	logrus.WithField("config-path", ConfigPath).Info("Reading configuration")

	// TODO
	Port = 8080
}

func main() {
	// Show logs on stdout
	logrus.SetOutput(os.Stdout)

	// Read configuration file
	parseConfig()

	// Create ObiMaster instance
	master := CreateMaster()

	// Open connection
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", Port))
	if err != nil {
		logrus.WithField("error", err).Error("Unable to open server listener")
	}
	logrus.Info("Successfully opened connection listener")

	// Create gRPC server
	grpcServer := grpc.NewServer()
	RegisterObiMasterServer(grpcServer, master)
	logrus.WithField("obi-master-old", *master).Info("Successfully registered OBI Master server")

	// TODO: Use encrypted TLS connection

	// Start serving
	logrus.Info("Start serving requests on port ", Port)
	grpcServer.Serve(listener)
}
