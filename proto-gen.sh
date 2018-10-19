#!/usr/bin/env bash

cd proto

# Generate master RPC services both for server and client side
python3 -m grpc_tools.protoc -I . --python_out=./../client --grpc_python_out=./../client --go_out=./../master master-rpc-service.proto
protoc -I . master-rpc-service.proto --go_out=plugins=grpc:./../master
protoc -I . master-rpc-service.proto --go_out=plugins=grpc:./../client

# Generate predictor RPC services
protoc -I . message.proto  --python_out=./../predictor

cpp -DPYTHON predictor-service.proto.template | sed -e 's-^#.*--g' > predictor-service.proto
python3 -m grpc_tools.protoc -I . --python_out=./../predictor --grpc_python_out=./../predictor predictor-service.proto

cp message.proto ./../master/model
cpp -DGOLANG predictor-service.proto.template | sed -e 's-^#.*--g' > predictor-service.proto
protoc -I . -I ../../ predictor-service.proto  --go_out=plugins=grpc:./../master/predictor

rm predictor-service.proto
rm ./../master/model/message.proto

# Generate heartbeat model
protoc -I . message.proto  --go_out=./../master/model