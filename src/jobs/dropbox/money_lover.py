from common.logs import get_logger
from common.dropbox import get_vdropbox, scan_folder_by_regex, infer_separator
from common.duck import write_df

PATH_ML = "/Aplicaciones/expensor/Money Lover"
REGEX_MONEY_LOVER = r"^(MoneyLover-)?(?P<date>\d{4}-\d{2}-\d{2})( \((?P<num>\d+)\))?(.xls|.csv)$"

SCHEMA_OUT = "raw__dropbox"
TABLE_OUT = "money_lover"


def main():
    logger = get_logger()
    vdp = get_vdropbox()

    matches = scan_folder_by_regex(PATH_ML, REGEX_MONEY_LOVER, vdp=vdp)
    files = [x[1] for x in matches]

    last_file = f"{PATH_ML}/{files[-1]}"
    sep = infer_separator(last_file, vdp=vdp)

    logger.info(f"Reading {last_file=}")
    df = vdp.read_csv(last_file, index_col=0, sep=sep)

    df["_source"] = f"dropbox:/{last_file}"
    write_df(df, SCHEMA_OUT, TABLE_OUT, mode="append")

    # TODO: Enable this once migrated
    # logger.info(f"Removing {len(files)} money lover files")
    # for file in files:
    #     vdp.delete(f"{PATH_ML}/{file}")
