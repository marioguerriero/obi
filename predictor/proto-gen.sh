#!/bin/bash
python3 -m grpc_tools.protoc -I . -I ../../ --python_out=. --grpc_python_out=. predictor-service.proto
protoc -I . -I ../../ predictor-service.proto  --go_out=plugins=grpc:../master/predictor
cd