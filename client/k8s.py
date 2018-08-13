import kubernetes as k8s

import grpc

import master_rpc_service_pb2
import master_rpc_service_pb2_grpc

from generic_client import GenericClient


class KubernetesClient(GenericClient):
    def __init__(self, config):
        """
        Create k8s client object and other basic objects
        """
        super(self).__init__

        # Load user configuration
        self.user_config = config

        # Load cluster configuration
        k8s.config.load_kube_config()

        # Prepare client object
        self.client = k8s.client.CoreV1Api()

    def discover_services(self):
        """
        Discover all the OBI available platform services
        :return: list of available services
        """

    def submit_platform(self, platform_config):
        """
        Generates a new platform service for the given configuration
        :param platform_config:
        :return:
        """

    def delete_platform(self, platform_name):
        """
        Deletes all k8s objects for the given platform
        :param platform_name:
        :return:
        """

    def submit_job(self, submit_job_request):
        """
        Submit a job to OBI according to the given request
        :param submit_job_request:
        :return:
        """
        # Obtain connection information
        host, port = self._get_connection_info(
            submit_job_request.infrastructure)

        # Create connection object
        with grpc.insecure_channel('{}:{}'.format(host, port)) as channel:
            stub = master_rpc_service_pb2_grpc.ObiMasterStub(channel)
            # Submit job creation request
            stub.SubmitJob(submit_job_request)

    def _get_connection_info(self, infrastructure):
        """
        Given an infrastructure object, this function returns a tuple
        with IP address and port for connecting to the given infrastructure
        :param infrastructure:
        :return:
        """
        # TODO
        return self.user_config['masterHost'], self.user_config['masterPort']
