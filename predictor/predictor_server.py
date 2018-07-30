from flask import Flask
from flask import request

import json

from .config import APP_NAME, REQUIRED_FIELDS

app = Flask(APP_NAME)


@app.route("/predict-duration")
def hello():
    # Parse request arguments
    profile = request.args.get('profile')

    # Handle missing profile errors
    if profile is None or profile not in REQUIRED_FIELDS:
        resp = app.response_class(
            response=json.dumps({
                'error': 'Malformed request. '
                         'Profile query parameter missing or invalid.'
            }),
            status=400,
            mimetype='application/json'
        )
        return resp

    # Handle missing query fields errors
    required_fields = REQUIRED_FIELDS[profile]
    for f in required_fields:
        val = request.args.get(f)
        if val is None:
            resp = app.response_class(
                response=json.dumps({
                    'error': 'Malformed request. '
                             'Profile {}  requires "{}" field.'.format(profile,
                                                                       f)
                }),
                status=400,
                mimetype='application/json'
            )
            return resp

    return "Hello World!"
