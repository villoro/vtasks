from datetime import datetime

import pandas as pd

from prefect import get_run_logger
from prefect import flow
from prefect import task

import utils as u

PATH_PHONE = "/Aplicaciones/pixel"
PATH_CSV = f"{PATH_PHONE}/bmw_history.txt"
PATH_BATTERY = f"{PATH_PHONE}/battery.parquet"

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


def read_bmw_history(vdp):

    # log = get_run_logger()
    # log.info(f"Reading {PATH_CSV=}")
    df = vdp.read_csv(PATH_CSV, header=None)

    df = df.rename(columns=COL_MAP)

    # log.info(f"Formating dataframe")
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
