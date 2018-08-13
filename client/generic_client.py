from abc import ABC, abstractmethod


class GenericClient(ABC):
    """
    General abstract class which is implemented by any of the clients.
    By using this class we ensure that we are able to implement clients
    for several different configurations, e.g. Kubernetes, local, etc.
    """

    @abstractmethod
    def __init__(self, user_config):
        """
        Constructor method accepting the user configuration
        for the given client
        :param user_config:
        """

    @abstractmethod
    def get_objects(self, **kwargs):
        """
        Discover all the OBI available platform services
        :return: list of available services
        """

    @abstractmethod
    def create_object(self, **kwargs):
        """
        Generates a new platform service for the given configuration
        :return:
        """

    @abstractmethod
    def delete_object(self, **kwargs):
        """
        Deletes all k8s objects for the given platform
        :return:
        """

    @abstractmethod
    def describe_object(self, **kwargs):
        """
        Submit a job to OBI according to the given request
        :return:
        """
