PATH_EXPENSOR = "/Aplicaciones/expensor"

PATH_MONEY_LOVER = f"{PATH_EXPENSOR}/Money Lover"
FILE_TRANSACTIONS = f"{PATH_EXPENSOR}/transactions.xlsx"

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
COL_NOTES = "Note"
COL_AMOUNT = "Amount"
COL_TOTAL_AMOUNT = "Total_amount"
COL_CATEGORY = "Category"
COL_TYPE = "Type"
COL_ACCOUNT = "Account"
COL_EVENT = "Event"

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
    COL_ACCOUNT,
    COL_TOTAL_AMOUNT,
    COL_EVENT,
    COL_NOTES,
]

ACCOUNT_FRAVI = "FraVi"
