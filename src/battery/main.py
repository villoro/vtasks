import asyncio

from datetime import datetime
from datetime import timedelta

import pandas as pd

from prefect import flow
from prefect import get_run_logger
from prefect import task
from mailjet import Email

import utils as u

from vprefect.query import query_task_runs

PATH_PHONE = "/Aplicaciones/pixel"
PATH_CSV = f"{PATH_PHONE}/bmw_history.txt"
PATH_BATTERY = f"{PATH_PHONE}/battery.parquet"

SEND_ALERT_TASK_NAME = "vtasks.battery.alert"

COL_MAP = {
    9: "time",
    1: "battery",
    4: "temperature",
    2: "mA",
    5: "mV",
    6: "plugged",
    7: "screen",
    3: "change",
    8: "restart",
}

MAX_DAYS = 20


def read_bmw_history(vdp):

    log = get_run_logger()
    log.info(f"Reading {PATH_CSV=}")
    df = vdp.read_csv(PATH_CSV, header=None)

    df = df.rename(columns=COL_MAP)

    log.info(f"Formating dataframe")
    df["time"] = df["time"].apply(lambda x: datetime.fromtimestamp(x / 1000))
    df["battery"] = df["battery"].str.split(" ").str[-1].str[:-1].apply(int)
    df["temperature"] = df["temperature"].str[:-2].apply(float)
    df["mA"] = df["mA"].str[:-2].apply(int)
    df["mV"] = df["mV"].str[:-2].apply(int)
    df["screen"] = df["screen"].map({"on": True, "off": False})
    df["plugged"] = df["plugged"].map({"ac": True, "unplugged": False})
    return df[COL_MAP.values()]


def update_parquet(vdp, df_new, parquet_path=PATH_BATTERY):

    if vdp.file_exists(parquet_path):
        df_history = vdp.read_parquet(parquet_path)

        df = pd.concat([df_new, df_history])
        df = df.drop_duplicates("time").reset_index(drop=True)
    else:
        df = df_new.copy()

    vdp.write_parquet(df, parquet_path)


@task(name="vtasks.battery.extract")
def extract_battery_data():
    vdp = u.get_vdropbox()

    if vdp.file_exists(PATH_CSV):
        df = read_bmw_history(vdp)
        update_parquet(vdp, df)
        vdp.delete(PATH_CSV)


def check_last_day_battery():
    vdp = u.get_vdropbox()
    df = vdp.read_parquet(PATH_BATTERY)
    return (datetime.now() - df["time"].max()) / timedelta(days=1)


@task(name="vtasks.battery.needs_alert")
def needs_alert():

    log = get_run_logger()

    task_name = SEND_ALERT_TASK_NAME

    env = u.detect_env()
    if env != "prod":
        log.info(f"Skipping summary since {env=}")
        return False

    log.info(f"Checking last days with battery info")
    days = check_last_day_battery()
    if days < MAX_DAYS:
        log.info(f"Battery data is {int(days)=} old (not older than {MAX_DAYS=})")
        return False

    log.info(f"Checking if '{task_name}' has already run today")
    task_runs = asyncio.run(query_task_runs(name_like=task_name, env=env))

    if task_runs:
        log.warning("Alert already send")
        return False

    log.info(f"Sending alert to extract battery ({int(days)=})")
    return True


@task(name=SEND_ALERT_TASK_NAME)
def send_alert():

    days = check_last_day_battery()

    html = f"""<h3>Missing battery data</h3>
    The last battery extraction is from {days=}

    Please upload the battery data from the phone.
    You can do it by going to:
    <ol>
      <li>Burger menu</li>
      <li>Settings</li>
      <li>Battery</li>
      <li>Export battery history</li>
    </ol>
    """

    Email(subject=f"Missing battery data", html=html).send()


@flow(**u.get_prefect_args("vtasks.battery"))
def battery():
    extract_battery_data()
    if needs_alert():
        send_alert()
