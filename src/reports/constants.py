"""
    Constants and config vars
"""


FILE_TRANSACTIONS = "/transactions.xlsx"
FILE_DATA = "/data.xlsx"
FILE_CONFIG = "/config.yml"

VAR_DROPBOX_TOKEN = "DROPBOX_TOKEN_EXPENSOR"

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
COL_NAME = "Name"

EXPENSES = "Expenses"
INCOMES = "Incomes"
EBIT = "EBIT"
LIQUID = "Liquid"
INVEST = "Investment"
ACCOUNTS = "accounts"
COLOR_NAME = "color_name"
COLOR_INDEX = "color_index"

DF_TRANS = "trans_m"
DF_LIQUID = "liquid_m"
DF_CATEG = "trans_categ"
DF_INVEST = "invest_m"
DF_WORTH = "worth_m"

DFS_ALL_FROM_DATA = [DF_CATEG, DF_LIQUID, DF_INVEST, DF_WORTH]
DFS_ALL = [DF_TRANS] + DFS_ALL_FROM_DATA

DEFAULT_COLORS = {
    EXPENSES: ("red", 500),
    f"{EXPENSES}_smooth": ("red", 300),
    INCOMES: ("green", 500),
    f"{INCOMES}_smooth": ("green", 300),
    "Incomes_passive": ("green", 700),
    EBIT: ("amber", 500),
    f"{EBIT}_smooth": ("amber", 300),
    LIQUID: ("blue", 500),
    f"{LIQUID}_smooth": ("blue", 300),
    "liquid_min_rec": ("grey", 700),
    "liquid_rec": ("grey", 400),
    "worth": ("lime", 400),
    "invest": ("orange", 400),
    "savings": ("purple", 500),
}
