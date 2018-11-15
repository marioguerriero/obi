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
	"google.golang.org/grpc/credentials"
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
		if len(md.Get("username")) > 0 && len(md.Get("password")) > 0 {
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

	creds, err := credentials.NewServerTLSFromFile("/go/src/obi/master/server.crt", "/go/src/obi/master/server.key")
	if err != nil {
		logrus.WithField("error", err).Fatalln("Unable to load server certificates")
	}

	// Create gRPC server
	grpcServer := grpc.NewServer(
		grpc.StreamInterceptor(streamInterceptor),
		grpc.UnaryInterceptor(unaryInterceptor),
		grpc.Creds(creds),
	)
	RegisterObiMasterServer(grpcServer, master)
	logrus.Info("Successfully registered OBI Master server")

	// Start serving
	logrus.Info("Start serving requests on port ", port)
	grpcServer.Serve(listener)
}
