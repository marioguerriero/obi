from abc import ABC, abstractmethod


class GenericClient(ABC):
    """
    General abstract class which is implemented by any of the clients.
    By using this class we ensure that we are able to implement clients
    for several different configurations, e.g. Kubernetes, local, etc.
    """

    _instance = None

    # I want this class to be a singleton
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(GenericClient, cls).__new__(
                cls, *args, **kwargs)
        return cls._instance

    @abstractmethod
    def discover_services(self):
        """
        Discover all the OBI available platform services
        :return: list of available services
        """

    @abstractmethod
    def submit_platform(self, platform_config):
        """
        Generates a new platform service for the given configuration
        :param platform_config:
        :return:
        """

    @abstractmethod
    def delete_platform(self, platform_name):
        """
        Deletes all k8s objects for the given platform
        :param platform_name:
        :return:
        """

    @abstractmethod
    def submit_job(self, submit_job_request):
        """
        Submit a job to OBI according to the given request
        :param submit_job_request:
        :return:
        """
