package main

import (
	"os"
	"github.com/sirupsen/logrus"
	"net"
	"fmt"
	"google.golang.org/grpc"
	"github.com/spf13/viper"
	"path/filepath"
	"context"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/codes"
	"obi/master/persistent"
	"strconv"
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
		logrus.WithField("err", err).Fatalln("Unable to read configuration")
	}
}

func streamInterceptor(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := authorize(stream.Context()); err != nil {
		return err
	}

	return handler(srv, stream)
}

func unaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if err := authorize(ctx); err != nil {
		return nil, err
	}

	return handler(ctx, req)
}

func authorize(ctx context.Context) error {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		username := md.Get("username")[0]
		password := md.Get("password")[0]
		if id, err := persistent.GetUserID(username, password); err == nil {
			md.Set("UserID", strconv.Itoa(id))
			return nil
		}

		return status.Errorf(codes.PermissionDenied, "Invalid credentials")
	}

	return status.Errorf(codes.PermissionDenied, "Missing credentials")
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
		logrus.WithField("error", err).Fatalln("Unable to open server listener")
	}
	logrus.Info("Successfully opened connection listener")

	// Create gRPC server
	grpcServer := grpc.NewServer(
		grpc.StreamInterceptor(streamInterceptor),
		grpc.UnaryInterceptor(unaryInterceptor),
	)
	RegisterObiMasterServer(grpcServer, master)
	logrus.Info("Successfully registered OBI Master server")

	// Start serving
	logrus.Info("Start serving requests on port ", port)
	grpcServer.Serve(listener)
}
