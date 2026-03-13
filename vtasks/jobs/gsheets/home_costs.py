from prefect import flow

from vtasks.common.duck import read_query
from vtasks.common.gsheets import df_to_gspread

SPREADSHEET = "Reforma HZ22"
SHEET = "Costos"

QUERY = """
SELECT
    transaction_date AS data,
    category AS categoria,
    amount AS cost,
    notes AS comentaris
FROM villoro._stg__dropbox.stg_dropbox__money_lover
WHERE lower(account) = 'home'
ORDER BY ALL
"""


@flow(name="gsheets.update_home_costs")
def update_home_costs():
    df = read_query(QUERY, use_md=True)
    df["data"] = df["data"].dt.strftime("%Y-%m-%d")
    df_to_gspread(SPREADSHEET, SHEET, df, start_column=0)


if __name__ == "__main__":
    update_home_costs()
