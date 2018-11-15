#!/usr/bin/env bash
# Copyright 2018
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

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