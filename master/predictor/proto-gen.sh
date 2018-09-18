#!/bin/bash
protoc -I . -I ../../../ predictor-service.proto  --go_out=plugins=grpc:.