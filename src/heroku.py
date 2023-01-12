import subprocess

from main import detect_env
from utils import get_secret

PREFECT_LOGIN = "prefect cloud login --key {} --workspace villoro/vtasks"
VTASKS_RUN = f"python src/main.py --env {detect_env()}"


def prefect_login():
    """ugly way to login in heroku machine"""
    login = PREFECT_LOGIN.format(get_secret("PREFECT_TOKEN"))
    subprocess.run(login, shell=True)


def run_prefect_externally():
    """Calling it with subprocess so that prefect recoginzes the login"""
    subprocess.run(VTASKS_RUN, shell=True)


if __name__ == "__main__":
    prefect_login()
    run_prefect_externally()
