from .failure_predictor import FailurePredictor

from .csv_find_predictor import CsvFindPredictor
from .csv_update_predictor import CsvUpdatePredictor
from .csv_recreate_predictor import CsvRecreatePredictor

from .ulm_predictor import UlmPredictor

_DURATION_PREDICTORS = {
    'csv_find': CsvFindPredictor(),
    'csv_update': CsvUpdatePredictor(),
    'csv_recreate': CsvRecreatePredictor(),
    'ulm': UlmPredictor(),
}

FAILURE_PREDICTOR = FailurePredictor()


def get_predictor_instance(name):
    """
    Returns a predictor instance given its name
    :param name:
    :return:
    """
    return _DURATION_PREDICTORS[name]


def get_duration_predictor(predictor_name):
    """
    Return a profile instance given its name
    :param predictor_name:
    :return:
    """
    if predictor_name not in _DURATION_PREDICTORS:
        return None
    return _DURATION_PREDICTORS[predictor_name]


def predict_failure(data):
    """
    Generate failure prediction for the given job information
    :param data:
    :return:
    """
    return FAILURE_PREDICTOR.predict(data)
