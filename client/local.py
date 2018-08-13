from generic_client import GenericClient


class LocalClient(GenericClient):
    def __init__(self):
        """
        Create k8s client object and other basic objects
        """
        raise NotImplementedError()

    def discover_services(self):
        """
        Discover all the OBI available platform services
        :return: list of available services
        """
        raise NotImplementedError()

    def submit_platform(self, platform_config):
        """
        Generates a new platform service for the given configuration
        :param platform_config:
        :return:
        """
        raise NotImplementedError()

    def delete_platform(self, platform_name):
        """
        Deletes all k8s objects for the given platform
        :param platform_name:
        :return:
        """
        raise NotImplementedError()

    def submit_job(self, submit_job_request):
        """
        Submit a job to OBI according to the given request
        :param submit_job_request:
        :return:
        """
        raise NotImplementedError()
