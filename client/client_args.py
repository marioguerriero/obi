"""
This file contains all the function calls responsible for command line
argument parsing in the OBI command line client tool.
"""

import argparse

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
