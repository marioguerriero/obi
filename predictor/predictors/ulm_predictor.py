from .generic_predictor import GenericPredictor


class UlmPredictor(GenericPredictor):

    def __init__(self):
        pass

    def predict(self, metrics, **kwargs):
        """
        This function generate and returns prediction of the duration of a
        ULM job. The user should only pass to this
        function a snapshot of the metrics for the cluster on which he is
        trying to execute the job and the input size information.
        :param metrics:
        :return int: Duration prediction in seconds
        """
