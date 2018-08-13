#!/usr/bin/python3

import argparse
import logging
import sys

import master_rpc_service_pb2
import yaml

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

# Specify command line arguments
parser = argparse.ArgumentParser(description='OBI client tool')
subparsers = parser.add_subparsers(help='Available commands', dest='cmd')
subparsers.required = True

# Get arguments
get_args = subparsers.add_parser('get', help='Those commands are used to '
                                             'obtain information from the '
                                             'remote OBI interface')
get_args_subparsers = get_args.add_subparsers(dest='get')
get_args_subparsers.required = True
get_args_subparsers.add_parser('infrastructures', help='Obtain available '
                                                       'infrastructure '
                                                       'services')
get_args_subparsers.add_parser('jobs', help='Obtain all running jobs on OBI '
                                            'infrastructures')

# Create arguments
create_args = subparsers.add_parser('create', help='Create jobs and '
                                                   'infrastructure services '
                                                   'for OBI')
create_args_subparsers = create_args.add_subparsers(dest='create')
create_args_subparsers.required = True

# Create job arguments
create_job_args = create_args_subparsers.add_parser('job')
create_job_args.add_argument('-f', help='Job file path', type=str,
                             required=True, dest='job_path')
create_job_args.add_argument('-t', help='Job type', type=str,
                             required=True, dest='job_type')
create_job_args.add_argument('-i', help='Infrastructure on which execute the '
                                        'given job. If omitted the default '
                                        'infrastructure from the '
                                        'configuration file will be used',
                             type=str, required=True, dest='job_platform')

# Create infrastructure arguments
create_infrastructure_args = create_args_subparsers.add_parser('infrastructure')
create_infrastructure_args.add_argument('-f', help='Infrastructure config '
                                                   'file path', type=str,
                                        required=True,
                                        dest='infrastructure_path')

# Describe arguments
describe_args = subparsers.add_parser('describe', help='Describe job and '
                                                       'infrastructure '
                                                       'services '
                                                       'from OBI')
describe_args_subparsers = describe_args.add_subparsers(dest='describe')
describe_args_subparsers.required = True
describe_infrastructure_args_subparsers = describe_args_subparsers.add_parser(
    'infrastructure', help='Describe an infrastructure given its name')
describe_infrastructure_args_subparsers.add_argument('infrastructure_name',
                                                     type=str)
describe_job_args_subparsers = \
    describe_args_subparsers.add_parser('job', help='Describe a job given its '
                                                    'name')
describe_job_args_subparsers.add_argument('job_name', type=str)

# Delete arguments
delete_args = subparsers.add_parser('delete', help='Delete job and '
                                                   'infrastructure services '
                                                   'from OBI')
delete_args_subparsers = delete_args.add_subparsers(dest='delete')
delete_args_subparsers.required = True
delete_infrastructure_args_subparsers = delete_args_subparsers.add_parser(
    'infrastructure', help='Delete an infrastructure given its name')
delete_infrastructure_args_subparsers.add_argument('infrastructure_name',
                                                   type=str)
delete_job_args_subparsers = delete_args_subparsers.add_parser('job',
                                                               help='Delete a '
                                                                    'job '
                                                                    'given '
                                                                    'its name')
delete_job_args_subparsers.add_argument('job_name', type=str)

# Finalize command line argument parsing
args = parser.parse_args()
print(args)


def map_job_type(type):
    """
    Performs type mapping between the given job type (as a string)
    to the protobuf enum type
    :param type:
    :return:
    """
    if type == 'PySpark':
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
