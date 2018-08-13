import kubernetes as k8s

import grpc

import master_rpc_service_pb2
import master_rpc_service_pb2_grpc

from logger import log

import utils

from generic_client import GenericClient


class KubernetesClient(GenericClient):
    #############
    #  START: Abstract methods from GenericClient
    #############
    def __init__(self, user_config):
        """
        Create k8s client object and other basic objects
        """
        # Create lookup maps for functions
        self._create_f = {
            'job': self._submit_job,
            'infrastructure': self._submit_infrastructure,
        }
        self._get_f = {
            'job': self._get_jobs,
            'infrastructure': self._get_infrastructures,
        }
        self._describe_f = {
            'job': self._describe_job,
            'infrastructure': self._describe_infrastructure(),
        }
        self._delete_f = {
            'job': self._delete_job,
            'infrastructure': self._delete_infrastructure,
        }

        # Load user configuration
        self._user_config = user_config

        # Load cluster configuration
        k8s.config.load_kube_config()

        # Prepare client object
        self._client = k8s.client.CoreV1Api()

    def get_objects(self, **kwargs):
        """
        Discover all the OBI available platform services
        :return: list of available services
        """
        log.info('Get request: {}'.format(kwargs))
        self._get_f[kwargs['type']](**kwargs)

    def create_object(self, **kwargs):
        """
        Generates a new platform service for the given configuration
        :return:
        """
        log.info('Create request: {}'.format(kwargs))
        self._create_f[kwargs['type']](**kwargs)

    def delete_object(self, **kwargs):
        """
        Deletes all k8s objects for the given platform
        :return:
        """
        log.info('Delete request: {}'.format(kwargs))
        self._delete_f[kwargs['type']](**kwargs)

    def describe_object(self, **kwargs):
        """
        Submit a job to OBI according to the given request
        :return:
        """
        log.info('Describe request: {}'.format(kwargs))
        self._describe_f[kwargs['type']](**kwargs)
    #############
    #  END: Abstract methods from GenericClient
    #############

    def _submit_job(self, **kwargs):
        """
        Submit a job to OBI according to the given request
        :param submit_job_request:
        :return:
        """
        # Obtain connection information
        host, port = self._get_connection_info(
            kwargs['job_infrastructure'])

        # Check if the job type is valid or not
        sup_types = [t['name'] for t in self._user_config['supportedJobTypes']]
        if kwargs['job_type'] not in sup_types:
            log.error('{} job type is invalid. '
                      'Supported job types: {}'.format(kwargs['job_type'],
                                                       sup_types))

        # Build submit job request object
        job = master_rpc_service_pb2.Job()
        job.executablePath = kwargs['job_path']
        job.type = utils.map_job_type(kwargs['job_type'])

        infrastructure = master_rpc_service_pb2.Infrastructure()

        req = master_rpc_service_pb2.SubmitJobRequest(
            job=job,
            infrastructure=infrastructure)

        # Create connection object
        with grpc.insecure_channel('{}:{}'.format(host, port)) as channel:
            stub = master_rpc_service_pb2_grpc.ObiMasterStub(channel)
            # Submit job creation request
            stub.SubmitJob(req)

    def _submit_infrastructure(self, **kwargs):
        """
        Submit an infrastructure to OBI according to the given request
        :param submit_job_request:
        :return:
        """

    def _delete_job(self, **kwargs):
        """
        Deletes all k8s objects for the given job
        :return:
        """

    def _delete_infrastructure(self, **kwargs):
        """
        Deletes all k8s objects for the given infrastructure
        :return:
        """

    def _get_jobs(self, **kwargs):
        """
        Return information for all the running jobs
        :return:
        """

    def _get_infrastructures(self, **kwargs):
        """
        Return information for all the infrastructures
        :return:
        """

    def _describe_job(self, **kwargs):
        """
        Return detailed information for the given job
        :return:
        """

    def _describe_infrastructure(self, **kwargs):
        """
        Return detailed information for the given infrastructure
        :return:
        """

    #############
    #  START: Generic utility functions
    #############
    def _get_connection_info(self, infrastructure):
        """
        Given an infrastructure object, this function returns a tuple
        with IP address and port for connecting to the given infrastructure
        :param infrastructure:
        :return:
        """
        # TODO
        return self._user_config['masterHost'], self._user_config['masterPort']

    def _discover_services(self):
        """
        Discover all the OBI available platform services
        :return: list of available services
        """
    #############
    #  END: Generic utility functions
    #############
