import pandas as pd
from prefect import flow

from vtasks.common.duck import read_query
from vtasks.common.gsheets import df_to_gspread

SPREADSHEET = "Reforma HZ22"
SHEET = "data"

QUERY = """
SELECT
    category AS categoria,
    amount AS cost,
    transaction_date AS data,
    notes AS comentaris
FROM villoro._marts__gcal.stg_dropbox__money_lover
WHERE lower(account) = 'home'
"""


def get_empty_df(df_in, margin=20):
    df_empty = pd.DataFrame(columns=df_in.columns, index=range(df_in.shape[0] + margin))
    df_empty[:] = ""
    return df_empty


@flow(name="gsheets.update_home_costs")
def update_home_costs():
    df = read_query(QUERY, use_md=True)
    df["data"] = df["data"].dt.strftime("%Y-%m-%d")

    df_empty = get_empty_df(df)
    df_to_gspread(SPREADSHEET, SHEET, df_empty, start_column=1)
    df_to_gspread(SPREADSHEET, SHEET, df, start_column=1)


if __name__ == "__main__":
    update_home_costs()
