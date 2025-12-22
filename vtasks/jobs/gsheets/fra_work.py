import pandas as pd
from prefect import flow

from vtasks.common.duck import read_query
from vtasks.common.gsheets import df_to_gspread

SPREADSHEET = "registro_lavoro"
SHEET = "data"

QUERY = "SELECT * FROM villoro._marts__gcal.marts_gcal__fra"


def get_empty_df(df_in, margin=20):
    df_empty = pd.DataFrame(columns=df_in.columns, index=range(df_in.shape[0] + margin))
    df_empty[:] = ""
    return df_empty


@flow(name="gsheets.update_fra_work")
def update_fra_work():
    df = read_query(QUERY, use_md=True)

    for col in ["start_day", "week"]:
        df[col] = df[col].dt.strftime("%Y-%m-%d")

    df_empty = get_empty_df(df)
    df_to_gspread(SPREADSHEET, SHEET, df_empty)
    df_to_gspread(SPREADSHEET, SHEET, df)


if __name__ == "__main__":
    update_fra_work()
