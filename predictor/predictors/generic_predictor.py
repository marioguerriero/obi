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


class PredictionException(Exception):
    """
    Predictor exception class
    """
    pass


class GenericPredictor(ABC):
    """
    General abstract class which is implemented by any of the prediction
    predictors made available for OBI
    """

    @abstractmethod
    def predict(self, metrics, **kwargs):
        """
        This function generate and returns prediction whose value depends on
        the class which is implementing it. The user should only pass to this
        function a snapshot of the metrics for the cluster on which he is
        trying to execute the job and the input size information.
        :param metrics:
        :return:
        """
