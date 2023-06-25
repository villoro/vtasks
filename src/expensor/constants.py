FILE_DATA = "expensor_data"

PATH_EXPENSOR = "/Aplicaciones/expensor"
PATH_MONEY_LOVER = f"{PATH_EXPENSOR}/Money Lover"

FILE_TRANSACTIONS = f"{PATH_EXPENSOR}/transactions.xlsx"

COL_DATE = "Date"
COL_NOTES = "Note"
COL_AMOUNT = "Amount"
COL_CATEGORY = "Category"
COL_TYPE = "Type"
COL_SUBTYPE = "Subtype"
COL_COLOR_NAME = "Color Name"
COL_COLOR_INDEX = "Color Index"

EXPENSES = "Expenses"
INCOMES = "Incomes"
RESULT = "Result"
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
DF_SALARY = "salary"
DF_ACCOUNTS = "accounts"
DF_TOTAL_WORTH = "total_worth"  # autocalculated

DFS_ALL_FROM_DATA = {
    DF_LIQUID: "all",
    DF_CATEG: ["Color Index"],
    DF_INVEST: "all",
    DF_WORTH: "all",
    DF_SALARY: ["Fixed", "Bonus", "EAGI", "Total"],
    DF_ACCOUNTS: ["Color Index"],
}
DFS_ALL = [DF_TRANS] + [*DFS_ALL_FROM_DATA.keys()]

# fmt: off
DEFAULT_COLORS = {
    EXPENSES: ("red", 500), f"{EXPENSES}_dim": ("red", 100),
    INCOMES: ("green", 500), f"{INCOMES}_dim": ("green", 100),
    "Passive_income_5": ("lime", 100),
    "Passive_income_4": ("lime", 300),
    "Passive_income_3": ("lime", 500),
    RESULT: ("amber", 500), f"{RESULT}_dim": ("amber", 100),
    LIQUID: ("blue", 500), f"{LIQUID}_dim": ("blue", 100),
    "Min_value": ("grey", 700), "Recomended_value": ("grey", 400),
    "Max_value": ("orange", 500),
    "Worth": ("green", 700),
    "Invest": ("pink", 300), "Invest_dim": ("pink", 100),
    SAVINGS: ("purple", 500), f"{SAVINGS}_dim": ("purple", 100),
    "Survival": ("brown", 500), f"Survival_dim": ("brown", 100),
    "Total_Worth": ("brown", 500), "Total_Worth_dim": ("brown", 100)
}
# fmt: on
