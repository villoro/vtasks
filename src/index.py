import gevent

from flask import Flask
from flask import request

from gevent import monkey
from global_utilities.secrets import get_secret
from master import run_etl

monkey.patch_all()


app = Flask(__name__)


@app.route("/")
def index():
    if request.headers.get("token") == get_secret("FLASK_PASSWORD"):

        # Schedule job
        gevent.spawn(run_etl)

        return "ETL scheduled"

    return "Invalid token"


if __name__ == "__main__":
    app.run()
