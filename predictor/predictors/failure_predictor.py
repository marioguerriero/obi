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
