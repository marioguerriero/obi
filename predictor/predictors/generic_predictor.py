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
