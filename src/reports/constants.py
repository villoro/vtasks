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
SAVINGS = "Savings"

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

# fmt: off
DEFAULT_COLORS = {
    EXPENSES: ("red", 500), f"{EXPENSES}_dim": ("red", 100),
    INCOMES: ("green", 500), f"{INCOMES}_dim": ("green", 100),
    "Incomes_passive": ("green", 700),
    EBIT: ("amber", 500), f"{EBIT}_dim": ("amber", 100),
    LIQUID: ("blue", 500), f"{LIQUID}_dim": ("blue", 100),
    "Liquid_min_rec": ("grey", 700), "Liquid_rec": ("grey", 400),
    "Worth": ("lime", 400),
    "Invest": ("orange", 400),
    SAVINGS: ("purple", 500), f"{SAVINGS}_dim": ("purple", 100),
    "Survival": ("brown", 500), f"Survival_dim": ("brown", 100),
}
# fmt: on
