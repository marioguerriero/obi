FROM golang

# Set some configuration environment variables
ENV SRC_DIR /go/src/obi/master

# Change workdir
WORKDIR /app

# Put source code into the images
ADD . $SRC_DIR

# Compile executable
RUN cd $SRC_DIR; go get .; go build -o obimaster; cp obimaster /app/

# Expose external port for the heartbeat services
EXPOSE 8080/udp

ENTRYPOINT [ "./obimaster" ]