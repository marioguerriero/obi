from flask import Flask
from flask import request

import os
import json

from config import APP_NAME, ACCEPTED_JOBS, HOST

from models.failure_predictor import FailurePredictor, PredictionException

from profile_manager import get_profile

app = Flask(APP_NAME)

PROFILE_ARG = 'profile'


@app.route("/predict-duration")
def duration():
    # Parse request arguments
    profile = request.args.get(PROFILE_ARG)

    # Handle missing profile errors
    if profile is None or profile not in ACCEPTED_JOBS:
        resp = app.response_class(
            response=json.dumps({
                'error': 'Malformed request. '
                         'Profile query parameter missing or invalid.'
            }),
            status=400,
            mimetype='application/json'
        )
        return resp

    # Obtain other (eventual) arguments from query
    args = request.args
    del args[PROFILE_ARG]

    # Obtain profile and generate predictions
    profile = get_profile[profile]
    return profile.predict_duration(args)


@app.route("/predict-failure")
def failure():
    predictor = FailurePredictor()
    try:
        pred = predictor.predict_failure(**request.args)
        return pred
    except PredictionException:
        resp = app.response_class(
            response=json.dumps({
                'error': 'Exception generated while attempting '
                         'to generate prediction'
            }),
            status=400,
            mimetype='application/json'
        )
        return resp


if __name__ == '__main__':
    app.run(host=HOST, port=int(os.environ['SERVICE_PORT']))
