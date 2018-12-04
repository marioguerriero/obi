# Copyright 2018 Delivery Hero Germany
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

from .generic_predictor import GenericPredictor


class FailurePredictor(GenericPredictor):
    """
    This class defines the methods to generate failure probability given a job
    and its respective profile.
    """

    def __init__(self):
        self._load_model()

    def predict(self, metrics, **kwargs):
        """
        This function generate and returns prediction for the failure
        probability of the given job. The user should only pass to this
        function a snapshot of the metrics for the cluster on which he is
        trying to execute the job and the input size information.
        :param metrics:
        :return int: Duration prediction in seconds
        """

    def _load_model(self):
        """
        Loads a pre-trained model
        :return:
        """
