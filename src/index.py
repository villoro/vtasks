from flask import Flask
from flask_httpauth import HTTPDigestAuth

from global_utilities.secrets import get_secret

app = Flask(__name__)

# Needed for digesting
app.config["SECRET_KEY"] = get_secret("FLASK_SECRET")
auth = HTTPDigestAuth()


@auth.get_password
def get_password(user):
    return get_secret("FLASK_PASSWORD")


@app.route("/")
@auth.login_required
def index():
    return "Hello, {}!".format(auth.username())


if __name__ == "__main__":
    app.run()
