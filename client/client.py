#!/usr/bin/env python
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

import os

import yaml
from client_args import args, CMD_DESCRIBE, CMD_GET, CMD_CREATE, CMD_DELETE
from logger import log

# Define some constant values
CONFIG_PATH = 'obi_client_config.yaml'


def parse_config(config_path=CONFIG_PATH):
    """
    Parse the configuration file for this client
    :return: a dictionary containing all the configuration fields
    """
    with open(config_path, 'r') as cf:
        log.info('Reading configuration file "{}"'.format(config_path))
        return yaml.load(cf)


def run(config):
    """
    This function represents the main entry point for OBI client
    """
    # Create Kubernetes client
    client = ClientClass(config)

    # Decide which client's function to call
    # Create client functions lookup table with command as key
    client_cmd_map = {
        CMD_GET: client.get_objects,
        CMD_CREATE: client.create_object,
        CMD_DESCRIBE: client.describe_object,
        CMD_DELETE: client.delete_object,
    }

    # Prepare arguments for the client function
    params = vars(args)
    # Params is something of type {'cmd': 'create', 'create':
    # 'infrastructure', 'infrastructure_path': 'PATH_HERE'}
    req_cmd = params['cmd']
    params['type'] = params[req_cmd]
    del params[req_cmd]
    del params['cmd']

    # Call client's function
    client_cmd_map[req_cmd](**params)


if __name__ == '__main__':
    # Load configuration file
    cfg = parse_config(CONFIG_PATH)
    # Import client depending on the desired deployment type
    if cfg['deploymentPlatform'] == 'k8s' \
            or cfg['deploymentPlatform'] == 'kubernetes':
        from k8s import KubernetesClient as ClientClass
    elif cfg['deploymentPlatform'] == 'local':
        from local import LocalClient as ClientClass
    # Execute command
    run(cfg)
    os._exit(0)
