from flask import Flask
from flask import request


from global_utilities.secrets import get_secret

app = Flask(__name__)


@app.route("/")
def index():

    if request.headers.get("token") == get_secret("FLASK_PASSWORD"):
        return "Hello!"

    return "Invalid token"


if __name__ == "__main__":
    app.run()
