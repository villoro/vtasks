import re

from prefect import flow
from prefect import task

from vtasks.common import dropbox
from vtasks.common.duck import write_df
from vtasks.common.logs import get_logger

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

    if not files:
        logger.info("There are no files to process")
        return False

    filename = files[-1]
    file_path = f"{PATH_ML}/{filename}"

    sep = dropbox.infer_separator(file_path, vdp=vdp)

    logger.info(f"Reading {file_path=}")
    df = vdp.read_csv(file_path, index_col=0, sep=sep)

    if match := re.match(REGEX_MONEY_LOVER, filename):
        file_date = match.groupdict().get("date")
        path_parquet = f"{PATH_ML}/{file_date[:4]}/{file_date}.parquet"
        logger.info(f"Exporting {path_parquet=}")
        vdp.write_parquet(df, path_parquet)

    df["_source"] = f"dropbox:/{file_path}"
    write_df(df, SCHEMA_OUT, TABLE_OUT)
    return True


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
    remove_files(vdp, files)


if __name__ == "__main__":
    export_money_lover()
