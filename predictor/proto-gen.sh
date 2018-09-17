#!/bin/bash
python3 -m grpc_tools.protoc -I . -I ../master/model/ --python_out=. --grpc_python_out=. predictor-service.proto
protoc -I . -I ../master/model/ predictor-service.proto  --go_out=plugins=grpc:../master/predictor
