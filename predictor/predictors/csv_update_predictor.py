import os
from datetime import date

import numpy as np

import yaml
from scipy.special import inv_boxcox
from sklearn.externals import joblib

import input_files_utils
from logger import log
from .generic_predictor import GenericPredictor

import xgboost


class CsvUpdatePredictor(GenericPredictor):

    def __init__(self):
        base_dir = os.path.join(os.environ['BUCKET_DIRECTORY'],
                                'models',
                                'csv_update')

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
        CSV update job. The user should only pass to this
        function a snapshot of the metrics for the cluster on which he is
        trying to execute the job and the input size information.
        :return int: Duration prediction in seconds
        """
        # Get input information
        try:
            input_info = input_files_utils.get_input_size(
                'csv', 'update', kwargs['backend'],
                date.today(), kwargs['day_diff'])
        except ValueError:
            log.error('Could not generate predictions')
            return None

        # Feature selection
        features = np.array([
            float(input_info[0]),  # INPUT_FILES_COUNT
            float(input_info[1]),  # INPUT_SIZE
            float(metrics.AvailableMB),  # YARN_AVAILABLE_MEMORY
            float(metrics.AvailableVCores),  # YARN_AVAILABLE_VIRTUAL_CORES
        ])
        data = xgboost.DMatrix(features.reshape(1, -1), feature_names=[
            'INPUT_FILES_COUNT', 'INPUT_SIZE',
            'YARN_AVAILABLE_MEMORY', 'YARN_AVAILABLE_VIRTUAL_CORES'
        ])

        # Generate predictions
        prediction = self._model.predict(data)

        # Apply inverse Boxcox function on generate prediction
        return inv_boxcox(prediction, self._config['boxcoxMaxLog'])
