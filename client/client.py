#!/usr/bin/env python3
import os
import threading
import traceback

import yaml
import sys

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
