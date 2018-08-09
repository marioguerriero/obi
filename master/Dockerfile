FROM golang

# Set some configuration environment variables
ENV SRC_DIR /go/src/obi/obimaster

# Change workdir
WORKDIR /app

# Put source code into the images
ADD . $SRC_DIR

# Compile executable
RUN cd $SRC_DIR; go get .; go build -o obimaster; cp obilet /app/

# Expose external port for the communication with the client
EXPOSE 8080

ENTRYPOINT [ "./obimaster" ]
