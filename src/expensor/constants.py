"""
    Constants and config vars
"""

from .config import PATH_ROOT

FILE_SECRETS = f"{PATH_ROOT}secrets.yaml"
FILE_MASTER_PASSWORD = f"{PATH_ROOT}master.password"

VAR_DROPBOX_TOKEN = "EXPENSOR_DROPBOX_TOKEN"
VAR_USER = "EXPENSOR_USER"
VAR_PASSWORD = "EXPENSOR_PASSWORD"

PATH_MONEY_LOVER = "/Money Lover/"
FILE_TRANSACTIONS = "/transactions.xlsx"

FORBIDDEN_CATEGORIES = ["Debt", "Loan"]

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

REPLACES_DF_TRANS = {
    "DATE": COL_DATE,
    "CATEGORY": COL_CATEGORY,
    "AMOUNT": COL_AMOUNT,
    "NOTE": COL_NOTES,
}

COLS_DF_TRANS = [
    COL_CATEGORY,
    COL_AMOUNT,
    COL_TYPE,
    COL_DATE,
    COL_MONTH_DATE,
    COL_MONTH,
    COL_YEAR,
    COL_NOTES,
]
