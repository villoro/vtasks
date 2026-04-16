import csv
import os

from calibre.library import db


LIBRARY_PATH = r"C:\calibre"
OUTPUT_CSV = r"C:\GIT\vtasks\scripts\calibre\calibre_export.csv"

COLUMNS = [
    "ondevice",
    "title",
    "authors",
    "#kobo_percent_read",
    "#kobo_last_read",
    "timestamp",
    "languages",
    "series",
    "size",
    "rating",
    "tags",
    "publisher",
    "pubdate",
    "last_modified",
    "id",
    "formats",
    "path",
    "#kobo_reading_location",
    "#kobo_time_spent_reading",
    "#kobo_rest_of_book_estimate",
]


def stringify(value):
    if value is None:
        return ""

    if isinstance(value, bool):
        return "true" if value else "false"

    if isinstance(value, dict):
        return " | ".join(f"{k}:{v}" for k, v in sorted(value.items()))

    if isinstance(value, (list, tuple, set)):
        return " | ".join("" if x is None else str(x) for x in value)

    return str(value)


def main():
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

    calibre_db = db(LIBRARY_PATH).new_api
    book_ids = sorted(calibre_db.all_book_ids())

    print(f"{LIBRARY_PATH=}")
    print(f"books_found={len(book_ids)}")

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(COLUMNS)

        for book_id in book_ids:
            row = []

            for lookup_name in COLUMNS:
                if lookup_name == "id":
                    value = book_id
                else:
                    value = calibre_db.field_for(lookup_name, book_id)

                row.append(stringify(value))

            writer.writerow(row)

    print(f"Exported {len(book_ids)} books to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
