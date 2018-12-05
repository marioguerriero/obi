# This Dockerfile is just the dependency for the Go master image
FROM golang:1.11-alpine

LABEL maintainer="mario.guerriero@deliveryhero.com, luca.lombardo@deliveryhero.com"

ENV REQUIREMENTS context fmt github.com/golang/protobuf/proto github.com/sirupsen/logrus github.com/spf13/viper golang.org/x/net/context google.golang.org/grpc log math net os path/filepath cloud.google.com/go/dataproc/apiv1 google.golang.org/api/iterator github.com/golang-collections/go-datastructures/queue github.com/Workiva/go-datastructures/queue github.com/lib/pq github.com/gin-gonic/gin

RUN apk add --no-cache git mercurial \
    && go get $REQUIREMENTS \
    && apk del git mercurial
