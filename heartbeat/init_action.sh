#!/usr/bin/env bash

INTERVAL=10

SERVICE_EXEC=/urs/local/bin/heartbeat_service

# Install dependencies
sudo apt-get install python3-pip -y
pip3 install protobuf

# Copy heartbeat service script to local file system
gsutil cp gs://dhg-obi/cluster-script/heartbeat.py $SERVICE_EXEC
chmod +x $SERVICE_EXEC

# Schedule cron job
echo "* * * * * ( sleep $INTERVAL ; SERVICE_EXEC)" | crontab
