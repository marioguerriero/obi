#!/usr/bin/env bash
# Copyright 2018 Delivery Hero Germany
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

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
