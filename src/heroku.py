import subprocess

import utils as u


PREFECT_LOGIN = "prefect cloud login --key {} --workspace villoro/vtasks"
VTASKS_RUN = f"python src/main.py --env {u.detect_env()}"


def prefect_login():
    """ugly way to login in heroku machine"""
    login = PREFECT_LOGIN.format(u.get_secret("PREFECT_TOKEN"))
    subprocess.run(login, shell=True)


def run_prefect_externally():
    """Calling it with subprocess so that prefect recoginzes the login"""
    result = subprocess.run(VTASKS_RUN, shell=True)

    # Check that the process succeed and if not, raise and exception
    if result.returncode != 0:
        raise RuntimeError(f"Vtasks process failed ({result.returncode=})")


if __name__ == "__main__":
    prefect_login()
    run_prefect_externally()
