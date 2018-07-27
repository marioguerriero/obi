from flask import Flask

from .config import APP_NAME

app = Flask(APP_NAME)


@app.route("/")
def hello():
    return "Hello World!"
