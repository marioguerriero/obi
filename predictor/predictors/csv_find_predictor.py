from generic_predictor import GenericDurationPredictor


class CsvFindPredictor(GenericDurationPredictor):

    def __init__(self):
        pass

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
