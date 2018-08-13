package main

import (
	"os"
	"github.com/sirupsen/logrus"
	"net"
	"fmt"
	"google.golang.org/grpc"
	"github.com/spf13/viper"
	"path/filepath"
)


func parseConfig() {
	configPath := os.Getenv("CONFIG_PATH")
	dir, file := filepath.Split(configPath)

	logrus.WithField("config-path", dir).Info("Reading configuration")


	viper.SetConfigName(file)
	viper.AddConfigPath(dir)
	err := viper.ReadInConfig()
	if err != nil {
		logrus.Fatal("Unable to read configuration", err)
	}
}

func main() {
	// Show logs on stdout
	logrus.SetOutput(os.Stdout)

	// Read configuration file
	parseConfig()

	// Create ObiMaster instance
	master := CreateMaster()

	// Open connection
	port := viper.GetString("masterPort")
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		logrus.WithField("error", err).Fatal("Unable to open server listener")
	}
	logrus.Info("Successfully opened connection listener")

	// Create gRPC server
	grpcServer := grpc.NewServer()
	RegisterObiMasterServer(grpcServer, master)
	logrus.WithField("obi-master-old", *master).Info("Successfully registered OBI Master server")

	// TODO: Use encrypted TLS connection

	// Start serving
	logrus.Info("Start serving requests on port ", port)
	grpcServer.Serve(listener)
}
