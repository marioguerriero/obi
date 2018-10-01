FROM eu.gcr.io/dhg-data-intelligence-ops/obi-master-env

# Set some configuration environment variables
ENV SRC_DIR /go/src/obi/api

# Change workdir
WORKDIR /app

# Put source code into the images
ADD . $SRC_DIR

# Compile executable
RUN cd $SRC_DIR; go build -o api_server; cp api_server /app/

# Expose external port for offering the main services
EXPOSE 8083/tcp

ENTRYPOINT [ "./api_server" ]