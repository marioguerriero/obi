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
from datetime import datetime

import yaml

import input_files_utils
from logger import log
from .generic_predictor import GenericPredictor

import numpy as np

from scipy.special import inv_boxcox
from sklearn.externals import joblib

import xgboost


class CsvFindPredictor(GenericPredictor):

    def __init__(self):
        base_dir = os.path.join(os.environ['BUCKET_DIRECTORY'],
                                'models',
                                'csv_find')

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
        CSV find job. The user should only pass to this
        function a snapshot of the metrics for the cluster on which he is
        trying to execute the job and the input size information.
        :param metrics:
        :return int: Duration prediction in seconds
        """
        # Get input information
        try:
            input_info = input_files_utils.get_input_size(
                'csv', 'find', kwargs['backend'],
                datetime.now(), kwargs['day_diff'])
        except ValueError:
            log.error('Could not generate predictions')
            return None

        # Feature selection
        features_names = [
            'INPUT_SIZE',
            'pde_archive_de', 'lh_payment_de', '9c', 'lh_click_to_claim_de',
            'pde_payment_de', 'pde_joker_de', 'midas', 'bgk_de', 'fd_de',
            'lh_de', 'pde_de'
        ]

        features = np.array([
            float(input_info[1]),  # INPUT_SIZE
            int(kwargs['backend'] == 'pde_archive_de'),  # bgk_de
            int(kwargs['backend'] == 'lh_payment_de'),  # bgk_de
            int(kwargs['backend'] == '9c'),  # bgk_de
            int(kwargs['backend'] == 'lh_click_to_claim_de'),  # bgk_de
            int(kwargs['backend'] == 'pde_payment_de'),  # bgk_de
            int(kwargs['backend'] == 'pde_joker_de'),  # bgk_de
            int(kwargs['backend'] == 'midas'),  # bgk_de
            int(kwargs['backend'] == 'bgk_de'),  # bgk_de
            int(kwargs['backend'] == 'fd_de'),  # fd_de
            int(kwargs['backend'] == 'lh_de'),  # lh_de
            int(kwargs['backend'] == 'pde_de'),  # pde_de
        ])

        data = xgboost.DMatrix(
            features.reshape(1, -1), feature_names=features_names)

        # Generate predictions
        prediction = self._model.predict(data)

        # Apply inverse Boxcox function on generate prediction
        return inv_boxcox(prediction, self._config['boxcoxMaxLog'])
