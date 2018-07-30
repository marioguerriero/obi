import os

APP_NAME = 'OBI_Predictor_Server'
MODELS_PATH = os.path.join(os.getcwd(), 'models')

REQUIRED_FIELDS = {
    'ulm': None,
    'csv': ['input_size', 'input_count']
}
