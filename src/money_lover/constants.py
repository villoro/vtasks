"""
    Constants and config vars
"""

VAR_DROPBOX_TOKEN = "DROPBOX_TOKEN_EXPENSOR"

PATH_MONEY_LOVER = "/Money Lover"
FILE_TRANSACTIONS = "/transactions.xlsx"

# fmt: off
FORBIDDEN_CATEGORIES = [
    "Debt", "Deuda",
    "Préstamo", "Loan",
    "Pago", "Repayment",
    "Colección de pago", "Debt Collection",
    "Otros",
]
# fmt: on

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
