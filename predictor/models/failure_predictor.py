class PredictionException(Exception):
    pass


class FailurePredictor(object):
    """
    This class defines the methods to generate failure probability given a job
    and its respective profile.
    """

    def __init__(self):
        self._load_model()

    def predict_failure(self, **kwargs):
        """
        Produces a failure probability value between 0 and 1 where 1 means
        high probability of failure
        :param kwargs:
        :return:
        """

    def _load_model(self):
        """
        Loads a pre-trained model
        :return:
        """
