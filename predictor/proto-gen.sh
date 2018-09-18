#!/bin/bash
protoc -I . message.proto  --python_out=.
python3 -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. predictor-service.proto