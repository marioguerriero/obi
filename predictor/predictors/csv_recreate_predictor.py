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

import os

import numpy as np
import xgboost
import yaml
from scipy.special import inv_boxcox
from sklearn.externals import joblib

from .generic_predictor import GenericPredictor


class CsvRecreatePredictor(GenericPredictor):

    def __init__(self):
        base_dir = os.path.join(os.environ['BUCKET_DIRECTORY'],
                                'models',
                                'csv_recreate')

        # Load pre-trained model
        model_path = os.path.join(base_dir, 'model.pkl')
        self._model = joblib.load(model_path)

        # Load configuration file
        config_path = os.path.join(base_dir, 'config.yaml')
        config_f = open(config_path, 'r')
        self._config = yaml.load(config_f)

    def predict(self, metrics, **kwargs):
        """
        This function generate and returns prediction of the duration of a
        CSV recreate job. The user should only pass to this
        function a snapshot of the metrics for the cluster on which he is
        trying to execute the job and the input size information.
        :param metrics:
        :return int: Duration prediction in seconds
        """
        # Feature selection
        features = np.array([
            float(metrics.AvailableMB),  # YARN_AVAILABLE_MEMORY
            float(metrics.AvailableVCores),  # YARN_AVAILABLE_VIRTUAL_CORES
        ])
        data = xgboost.DMatrix(features.reshape(1, -1), feature_names=[
            'YARN_AVAILABLE_MEMORY', 'YARN_AVAILABLE_VIRTUAL_CORES'
        ])

        # Generate predictions
        prediction = self._model.predict(data)

        # Apply inverse Boxcox function on generate prediction
        return inv_boxcox(prediction, self._config['boxcoxMaxLog'])
