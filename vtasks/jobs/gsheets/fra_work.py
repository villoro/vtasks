from prefect import flow

from vtasks.common.duck import read_query
from vtasks.common.gsheets import df_to_gspread

SPREADSHEET = "registro_lavoro"
SHEET = "data"

QUERY = """
SELECT
    start_day AS day,
    title AS concept,
    duration_hours AS total_hours,
    week,
    month
FROM villoro._marts__gcal.marts_gcal__fra
"""


@flow(name="gsheets.update_fra_work")
def update_fra_work():
    df = read_query(QUERY, use_md=True)

    for col in ["day", "week"]:
        df[col] = df[col].dt.strftime("%Y-%m-%d")

    df_to_gspread(SPREADSHEET, SHEET, df, start_column=1)


if __name__ == "__main__":
    update_fra_work()
