FROM eu.gcr.io/dhg-data-intelligence-ops/obi-master-env

# Set some configuration environment variables
ENV SRC_DIR /go/src/obi/master

# Change workdir
WORKDIR /app

# Put source code into the images
ADD . $SRC_DIR

# Compile executable
RUN cd $SRC_DIR; go build -o master; cp master /app/

# Expose external port for the heartbeat services
EXPOSE 8080/udp

# Expose external port for offering the main services
EXPOSE 8081/tcp

ENTRYPOINT [ "./master" ]