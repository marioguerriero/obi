# Copyright 2018
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

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
