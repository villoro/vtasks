"""
    Constants and config vars
"""

from .config import PATH_ROOT

FILE_SECRETS = f"{PATH_ROOT}secrets.yaml"
FILE_MASTER_PASSWORD = f"{PATH_ROOT}master.password"

FILE_TRANSACTIONS = "/transactions.xlsx"
FILE_DATA = "/data.xlsx"
FILE_CONFIG = "/config.yml"

VAR_DROPBOX_TOKEN = "EXPENSOR_DROPBOX_TOKEN"

PATH_MONEY_LOVER = "/Money Lover/"
FILE_TRANSACTIONS = "/transactions.xlsx"

COL_DATE = "Date"
COL_MONTH_DATE = "Month_date"
COL_MONTH = "Month"
COL_YEAR = "Year"
COL_NOTES = "Note"
COL_AMOUNT = "Amount"
COL_CATEGORY = "Category"
COL_TYPE = "Type"

EXPENSES = "Expenses"
INCOMES = "Incomes"


DF_TRANS = "trans_m"
DF_LIQUID = "liquid_m"
DF_CATEG = "trans_categ"
DF_INVEST = "invest_m"
DF_WORTH = "worth_m"

DFS_ALL_FROM_DATA = [DF_CATEG, DF_LIQUID, DF_INVEST, DF_WORTH]
DFS_ALL = [DF_TRANS] + DFS_ALL_FROM_DATA
