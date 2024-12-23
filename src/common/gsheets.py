from common.paths import get_path


PATH_GSPREADSHEET_KEY = get_path("auth/gspreadsheets.json")
SECRET_NAME = "GSPREADSHEET_JSON"

GDRIVE = None


def init_gdrive(force=False):
    """Export gdrive json auth"""

    # Init GDRIVE if it has not been init
    global GDRIVE
    if GDRIVE is None or force:
        export_secret(PATH_GSPREADSHEET_KEY, SECRET_NAME)

        GDRIVE = gspread.service_account(filename=PATH_GSPREADSHEET_KEY)
