#!/usr/bin/env bash

INTERVAL=10

SERVICE_EXEC=/usr/local/bin/heartbeat_service

# Install dependencies
sudo apt-get install python3-pip -y
pip3 install protobuf

# Copy heartbeat service script to local file system
gsutil cp gs://dhg-obi/cluster-script/heartbeat.py $SERVICE_EXEC
chmod +x $SERVICE_EXEC

# Schedule cron job
(while True; sleep ${INTERVAL}; do ${SERVICE_EXEC}; done) &