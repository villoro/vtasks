SPREADSHEET = "books_data"
SHEET_BOOKS = "Books"
SHEET_TODO = "TODO"

PATH_VBOOKS = "/Aplicaciones/vbooks"

COL_DATE = "Date"
COL_PAGES = "Pages"
COL_LANGUAGE = "Language"
COL_AUTHOR = "Author"
COL_SOURCE = "Source"
COL_OWNED = "Owned?"
COL_TYPE = "Type"

STATUS_OWNED = "Owned"
STATUS_IN_LIBRARY = "In Library"
STATUS_NOT_OWNED = "Not Owned"


# fmt: off
COLORS = {
    "Total": ("black", 500), "Total_dim": ("grey", 600),
    # Languages
    "Català": ("blue", 500), "Català_dim": ("blue", 300),
    "Español": ("red", 500), "Español_dim": ("red", 300),
    "English": ("green", 500), "English_dim": ("green", 300),
    "Italiano": ("purple", 500), "Italiano_dim": ("purple", 300),
    # Types:
    "Fiction": ("green", 400),
    "CP": ("purple", 400),
    "Finances": ("blue", 400),
    "IT": ("orange", 400),
    "Other non fiction": ("red", 400),
    # Ownership
    STATUS_OWNED: ("green", 500), STATUS_IN_LIBRARY: ("amber", 500), STATUS_NOT_OWNED: ("red", 500),
    # Others
    "Years": ("grey", 500),
    "Authors": ("blue", 800),
    "Sources": ("deep orange", 600),
}
# fmt: on
