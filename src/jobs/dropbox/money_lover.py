from prefect import flow
from prefect import task

from src.common import dropbox
from src.common.duck import write_df
from src.common.logs import get_logger

PATH_ML = "/Aplicaciones/expensor/Money Lover"
REGEX_MONEY_LOVER = (
    r"^(MoneyLover-)?(?P<date>\d{4}-\d{2}-\d{2})( \((?P<num>\d+)\))?(.xls|.csv)$"
)

SCHEMA_OUT = "raw__dropbox"
TABLE_OUT = "money_lover"

FLOW_NAME = "dropbox.money_lover"


@task(name=f"{FLOW_NAME}.get_files")
def get_files(vdp):
    matches = dropbox.scan_folder_by_regex(PATH_ML, REGEX_MONEY_LOVER, vdp=vdp)
    return [x[1] for x in matches]


@task(name=f"{FLOW_NAME}.export_last_file")
def export_last_file(vdp, files):
    logger = get_logger()

    last_file = f"{PATH_ML}/{files[-1]}"
    sep = dropbox.infer_separator(last_file, vdp=vdp)

    logger.info(f"Reading {last_file=}")
    df = vdp.read_csv(last_file, index_col=0, sep=sep)

    df["_source"] = f"dropbox:/{last_file}"
    write_df(df, SCHEMA_OUT, TABLE_OUT, mode="append")


@task(name=f"{FLOW_NAME}.remove_files")
def remove_files(vdp, files):
    logger = get_logger()

    logger.info(f"Removing {len(files)} money lover files")
    for file in files:
        vdp.delete(f"{PATH_ML}/{file}")


@flow(name=f"{FLOW_NAME}")
def export_money_lover():
    vdp = dropbox.get_vdropbox()

    files = get_files(vdp)
    export_last_file(vdp, files)
    # TODO: Enable this once migrated
    # remove_files(vdp, files)


if __name__ == "__main__":
    export_money_lover()
