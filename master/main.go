package main

import (
	"net"
	"fmt"
	"log"
	"google.golang.org/grpc"
)

const Port = 8080

func main() {
	// Create ObiMaster instance
	var master = ObiMaster{

	}

	// Open connection
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", Port))
	if err != nil {
		log.Fatal("Failed to listen:", err)
	}

	// Create gRPC server
	grpcServer := grpc.NewServer()
	RegisterObiMasterServer(grpcServer, &master)

	// TODO: Use encrypted TLS connection

	// Start serving
	grpcServer.Serve(listener)
}