# Copyright 2018
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