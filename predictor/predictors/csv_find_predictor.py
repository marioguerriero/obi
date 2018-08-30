import os
import yaml

from .generic_predictor import GenericPredictor

import numpy as np

from scipy.special import inv_boxcox
from sklearn.externals import joblib


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

    def predict(self, metrics, input_info):
        """
        This function generate and returns prediction of the duration of a
        CSV find job. The user should only pass to this
        function a snapshot of the metrics for the cluster on which he is
        trying to execute the job and the input size information.
        :param input_info:
        :param metrics:
        :return int: Duration prediction in seconds
        """
        # Feature selection
        features = np.array([
            input_info[0],  # INPUT_FILES_COUNT
            input_info[1],  # INPUT_SIZE
            metrics.AvailableMB,  # YARN_AVAILABLE_MEMORY
            metrics.AvailableVCores,  # YARN_AVAILABLE_VIRTUAL_CORES
            metrics.PendingVCores,  # YARN_PENDING_VIRTUAL_CORES
        ])

        # Generate predictions
        prediction = self._model.predict(features)

        # Apply inverse Boxcox function on generate prediction
        return inv_boxcox(prediction, self._config['boxcoxMaxLog'])
