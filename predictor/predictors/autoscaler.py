import os

import yaml

from .generic_predictor import GenericPredictor

import numpy as np

import keras

from sklearn.externals import joblib


class AutoscalerPredictor(GenericPredictor):
    """
    This class is used to provide scaling factor predictions to
    the OBI's Machine Learning based autoscaler feature
    """

    def __init__(self):
        base_dir = os.path.join(os.environ['BUCKET_DIRECTORY'],
                                'models',
                                'autoscaler')

        # Load configuration file
        config_path = os.path.join(base_dir, 'config.yaml')
        config_f = open(config_path, 'r')
        self._config = yaml.load(config_f)

        # Load metrics models
        self._metric_models = dict()
        metrics = self._config['usedMetrics']
        for m in metrics:
            self._metric_models[m] = joblib.load(
                os.path.join(base_dir, 'model_{}.pkl'.format(m)))

        # Load input transformers
        self._standard_scaler = joblib.load(
            os.path.join(base_dir, 'scaling_factor_standard_scaler.pkl'))
        self._minmax_scaler = joblib.load(
            os.path.join(base_dir, 'scaling_factor_minmax_scaler.pkl'))

        # Load scaling factor vector
        self._scaling_factors = joblib.load(
            os.path.join(base_dir, 'scaling_factor.vect'))

        # Load scaling factor predictor model
        self._scaling_factor_model = keras.models.load_model(os.path.join(
            base_dir, 'scaling_factor_model.h5'
        ))

    def predict(self, metrics, **kwargs):
        """
        This function generate and returns prediction of the scaling factor
        by which the number of nodes in a cluster should be increased (or
        decreased).
        :param metrics:
        :return int: scaling factor in number of nodes. It may be negative
        """
        # Get performance index
        performance_before = kwargs['performance']

        # Generate metrics prediction
        metrics_before = list()
        metrics_after = list()
        for m in self._config['userMetrics']:
            # Store metrics before scaling
            metrics_before.append(metrics[m])

            # Generate after scaling prediction
            pred = self._predict_after_scaling_averaged_metric(m, metrics[m], metrics.NumberOfNodes)
            metrics_after.append(pred)

        # Fix desired performance after scaling
        performance_after = 0.0

        # Generate predictions for the scaling factor
        data = np.array([
            metrics.NumberOfNodes, performance_before, performance_after,
            *metrics_before, *metrics_after
        ])
        data = self._standard_scaler.transform(data)

        prediction = self._scaling_factor_model.predict(data)
        return self._minmax_scaler.inverse_transform(prediction)

    def _predict_after_scaling_averaged_metric(self, metric_name, metric, n, n_samples=10):
        """
        Predict the impact a scaling may potentially have on a certain
        metric on a cluster having n nodes
        :param metric:
        :param n: number of nodes in the cluster before scaling
        :param n_samples: number of samples to average on
        :return:
        """
        return np.mean([
            self._predict_after_scaling_metric(metric_name, metric, n, k)
            for k in np.random.choice(self._scaling_factors, size=(n_samples,))
        ])

    def _predict_after_scaling_metric(self, metric_name, metric, k, n):
        """
        Predicts the impact a scaling will have on a certain metric
        :param metric: metric value before scaling
        :param metric_name:
        :param k: scaling factor
        :param n: number of nodes before scaling
        :return:
        """
        data = np.array([
            metric * (n / (n + k)),
            metric * (k / (n + k)),
        ])
        return self._metric_models[metric_name].predict(data)
