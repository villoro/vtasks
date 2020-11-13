from flask import Flask
from flask import request

from global_utilities.secrets import get_secret
from master import run_etl


app = Flask(__name__)

WORKING = False


@app.route("/")
def index():
    if request.headers.get("token") == get_secret("FLASK_PASSWORD"):

        if WORKING:
            return "already working"

        WORKING = True
        run_etl()
        return "vtasks done"

    return "Invalid token"


if __name__ == "__main__":
    app.run()
