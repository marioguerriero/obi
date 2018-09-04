#!/bin/bash
# Execute this file from the master folder to generate all the necessary protobuf files for master-client
python3 -m grpc_tools.protoc -I . --python_out=../client/ --grpc_python_out=../client/ --go_out=.  master-rpc-service.proto
protoc -I . master-rpc-service.proto --go_out=plugins=grpc:.