"""
This file contains all the function calls responsible for command line
argument parsing in the OBI command line client tool.
"""

import argparse
import yaml

# Read config file
CONFIG_PATH = 'obi_client_config.yaml'
with open(CONFIG_PATH, 'r') as cf:
    config = yaml.load(cf)

CMD_CREATE = 'create'
CMD_GET = 'get'
CMD_DELETE = 'delete'
CMD_DESCRIBE = 'describe'

# Specify command line arguments
parser = argparse.ArgumentParser(description='OBI client tool')
subparsers = parser.add_subparsers(help='Available commands', dest='cmd')
subparsers.required = True

# Get arguments
get_args = subparsers.add_parser(CMD_GET, help='Those commands are used to '
                                               'obtain information from the '
                                               'remote OBI interface')
get_args_subparsers = get_args.add_subparsers(dest=CMD_GET)
get_args_subparsers.required = True
get_args_subparsers.add_parser('infrastructures', help='Obtain available '
                                                       'infrastructure '
                                                       'services')
get_args_subparsers.add_parser('jobs', help='Obtain all running jobs on OBI '
                                            'infrastructures')

# Create arguments
create_args = subparsers.add_parser(CMD_CREATE, help='Create jobs and '
                                                     'infrastructure services '
                                                     'for OBI')
create_args_subparsers = create_args.add_subparsers(dest=CMD_CREATE)
create_args_subparsers.required = True

# Create job arguments
create_job_args = create_args_subparsers.add_parser('job')
create_job_args.add_argument('-f', help='Job file path', type=str,
                             required=True, dest='job_path')
create_job_args.add_argument('-t', help='Job type', type=str,
                             required=True, dest='job_type',
                             choices=[
                                 t['name']
                                 for t in config['supportedJobTypes']
                             ])
create_job_args.add_argument('-i', help='Infrastructure on which execute the '
                                        'given job. If omitted the default '
                                        'infrastructure from the '
                                        'configuration file will be used',
                             type=str, required=True,
                             dest='job_infrastructure')
create_job_args.add_argument('job_args', nargs=argparse.REMAINDER)

# Create infrastructure arguments
create_infrastructure_args = \
    create_args_subparsers.add_parser('infrastructure')
create_infrastructure_args.add_argument('-f', help='Infrastructure config '
                                                   'file path', type=str,
                                        required=True,
                                        dest='infrastructure_path')

# Describe arguments
describe_args = subparsers.add_parser(CMD_DESCRIBE, help='Describe job and '
                                                         'infrastructure '
                                                         'services '
                                                         'from OBI')
describe_args_subparsers = describe_args.add_subparsers(dest=CMD_DESCRIBE)
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
delete_args = subparsers.add_parser(CMD_DELETE, help='Delete job and '
                                                     'infrastructure services '
                                                     'from OBI')
delete_args_subparsers = delete_args.add_subparsers(dest=CMD_DELETE)
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
