FROM golang

# Set some configuration environment variables
ENV SRC_DIR /go/src/obi/master

# Change workdir
WORKDIR /app

# Put source code into the images
ADD . $SRC_DIR

# Compile executable
RUN cd $SRC_DIR; go get .; go build -o obimaster; cp obimaster /app/

# Copy configuration file
RUN cp /go/src/obi/master/obi_master_config.yaml /app/

# Expose external port for the heartbeat services
EXPOSE 8080/udp

# Expose external port for offering the main services
EXPOSE 8081/tcp

ENTRYPOINT [ "./obimaster" ]