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
	dir, filename := filepath.Split(configPath)
	ext := filepath.Ext(filename)
	name := filename[0:len(filename)-len(ext)]

	logrus.Info("Reading configuration")

	viper.AddConfigPath(dir)
	viper.SetConfigName(name)
	err := viper.ReadInConfig()
	if err != nil {
		logrus.Info("Unable to read configuration", err)
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
