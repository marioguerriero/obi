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

from generic_client import GenericClient


class LocalClient(GenericClient):
    def __init__(self, user_config):
        """
        Create k8s client object and other basic objects
        """
        raise NotImplementedError()

    def get_objects(self, **kwargs):
        """
        Discover all the OBI available platform services
        :return: list of available services
        """
        raise NotImplementedError()

    def create_object(self, **kwargs):
        """
        Generates a new platform service for the given configuration
        :return:
        """
        raise NotImplementedError()

    def delete_object(self, **kwargs):
        """
        Deletes all k8s objects for the given platform
        :return:
        """
        raise NotImplementedError()

    def describe_object(self, **kwargs):
        """
        Submit a job to OBI according to the given request
        :return:
        """
        raise NotImplementedError()

    @property
    def user_configs(self):
        """
        This abstract method refers to a property field each client should
        have. This property keeps the value of the user specified
        configuration for the given client
        :return:
        """
        raise NotImplementedError()
