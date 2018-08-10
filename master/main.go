package main

import (
	"net"
	"fmt"
	"google.golang.org/grpc"
	"github.com/sirupsen/logrus"
)

const ConfigPath = ""
var Port int

func parseConfig() {
	logrus.WithField("config-path", ConfigPath).Info("Reading configuration")

	// TODO
	Port = 8080
}

func main() {
	// Read configuration file
	parseConfig()

	// Create ObiMaster instance
	var master = ObiMaster{

	}

	// Open connection
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", Port))
	if err != nil {
		logrus.WithField("error", err).Error("Unable to open server listener")
	}
	logrus.Info("Successfully opened connection listener")

	// Create gRPC server
	grpcServer := grpc.NewServer()
	RegisterObiMasterServer(grpcServer, &master)
	logrus.WithField("obi-master", master).Info("Successfully registered OBI Master server")

	// TODO: Use encrypted TLS connection

	// Start serving
	logrus.Info("Start serving requests on port ", Port)
	grpcServer.Serve(listener)
}