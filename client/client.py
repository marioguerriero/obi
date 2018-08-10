import argparse
import configparser
import logging
import sys

import grpc

import master_rpc_service_pb2
import master_rpc_service_pb2_grpc

# Define some constant values
CONFIG_PATH = 'obi_client_config.ini'

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
parser = argparse.ArgumentParser(description='OBI CLI client tool')
parser.add_argument('--job-path ', type=str, dest='job_path',
                    required=True,
                    help='Path to the job executable')
parser.add_argument('--job-type', type=str, dest='job_type',
                    required=True,
                    help='Type of the submitted job (e.g. PySpark)')

args = parser.parse_args()


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
    logging.info('Reading configuration file "{}"'.format(config_path))
    config = configparser.ConfigParser(allow_no_value=True)
    config.read(config_path)
    return config


def run(config):
    """
    This function represents the main entry point for OBI client
    """
    host = config['master']['MASTER_HOST']
    port = config['master']['MASTER_PORT']
    logging.info('Starting communication channel on {}:{}'.format(host, port))
    with grpc.insecure_channel('{}:{}'.format(host, port)) as channel:
        # Create request stub
        stub = master_rpc_service_pb2_grpc.ObiMasterStub(channel)
        # Check fi we need to submit a job
        if args.job_path is not None and args.job_type is not None:
            # Check if the job type is valid or not
            sup_types = config['obi']['OBI_SUPPORTED_JOB_TYPES']
            if args.job_type not in sup_types.split(','):
                logging.error('{} job type is invalid. '
                              'Supported job types: {}'.format(args.job_type,
                                                              sup_types))
                sys.exit(1)

            # Send submit job request
            job = master_rpc_service_pb2.Job()
            job.executablePath = args.job_path
            job.type = map_job_type(args.job_type)

            infrastructure = master_rpc_service_pb2.Infrastructure()

            req = master_rpc_service_pb2.SubmitJobRequest(
                job=job,
                infrastructure=infrastructure)
            res = stub.SubmitJob(req)

            logging.info(res)


if __name__ == '__main__':
    cfg = parse_config(CONFIG_PATH)
    run(cfg)
