"""
    Constants and config vars
"""

VAR_DROPBOX_TOKEN = "DROPBOX_TOKEN_FLIGHTS"

FILE_AIRPORTS = "/airports.xlsx"
FILE_FLIGHTS = "/data.xlsx"
FILE_FLIGHTS_DAY = "/history/{:%Y_%m_%d}.xlsx"

COL_PRICE = "Price"
COL_DIRECT = "Direct"
COL_QUOTE_DATE = "Quote_date"
COL_DATE = "Date"
COL_CARRIER = "Carrier"
COL_ORIGIN = "Origin"
COL_DESTINATION = "Destination"
COL_INSTERTED = "Inserted"

COL_RENAMES = {"MinPrice": COL_PRICE, "QuoteDateTime": COL_QUOTE_DATE, "DepartureDate": COL_DATE}

COLS_INDEX = [
    COL_DATE,
    COL_QUOTE_DATE,
    COL_ORIGIN,
    COL_DESTINATION,
    COL_DIRECT,
    COL_CARRIER,
    COL_PRICE,
]
