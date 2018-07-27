from flask import Flask

from .config import *

app = Flask(APP_NAME)


@app.route("/")
def hello():
    return "Hello World!"
