#!/usr/bin/env bash

cd proto

# Generate master RPC services both for server and client side
python3 -m grpc_tools.protoc -I . --python_out=./../client --grpc_python_out=./../client  master-rpc-service.proto
protoc -I . master-rpc-service.proto --go_out=plugins=grpc:./../master
protoc -I . master-rpc-service.proto --go_out=plugins=grpc:./../client

# Generate predictor RPC services
protoc -I . message.proto  --python_out=./../predictor
python3 -m grpc_tools.protoc -I . --python_out=./../predictor --grpc_python_out=./../predictor predictor-service.proto

# Generate heartbeat model
protoc -I . message.proto  --go_out=./../master/model