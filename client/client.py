#!/usr/bin/python3

import logging
import sys
import yaml

from client_args import args
import master_rpc_service_pb2

# Define some constant values
CONFIG_PATH = 'obi_client_config.yaml'

# Prepare for logging
root = logging.getLogger()
root.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
fmt = '%(asctime)s %(levelname)s %(message)s'
formatter = logging.Formatter(fmt, '%y/%m/%d %H:%M:%S')
ch.setFormatter(formatter)
root.addHandler(ch)


def map_job_type(job_type):
    """
    Performs type mapping between the given job type (as a string)
    to the protobuf enum type
    :param job_type:
    :return:
    """
    if job_type == 'PySpark':
        return master_rpc_service_pb2.Job.PYSPARK


def parse_config(config_path=CONFIG_PATH):
    """
    Parse the configuration file for this client
    :return: a dictionary containing all the configuration fields
    """
    with open(config_path, 'r') as cf:
        logging.info('Reading configuration file "{}"'.format(config_path))
        return yaml.load(cf)


def submit_job(config, client):
    """
    Send submit job request to OBI Master
    :param config:
    :param client:
    :return:
    """
    # Check if the job type is valid or not
    sup_types = [type['name'] for type in config['obiSupportedJobTypes']]
    if args.job_type not in sup_types:
        logging.error('{} job type is invalid. '
                      'Supported job types: {}'.format(args.job_type,
                                                       sup_types))
        sys.exit(1)

    # Build submit job request object
    job = master_rpc_service_pb2.Job()
    job.executablePath = args.job_path
    job.type = map_job_type(args.job_type)

    infrastructure = master_rpc_service_pb2.Infrastructure()

    req = master_rpc_service_pb2.SubmitJobRequest(
        job=job,
        infrastructure=infrastructure)

    # Submit job to the given client
    client.submit_job(req)


def run(config):
    """
    This function represents the main entry point for OBI client
    """
    # Create Kubernetes client
    client = ClientClass(config)

    # Check which command to execute
    if args.job_path is not None and args.job_type is not None:
        # Create job request object
        submit_job(config, client)


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
