import csv

from calibre.library import db


LIBRARY_PATH = r"C:\calibre"
CSV_PATH = r"C:\GIT\vtasks\scripts\calibre\calibre_export.csv"

ID_COLUMN = "id"
PERCENT_READ_COLUMN = "#kobo_percent_read"
LAST_READ_COLUMN = "#kobo_last_read"
RATING_COLUMN = "rating"


def is_blank(value):
    return value is None or str(value).strip() == ""


def parse_percent(value):
    if is_blank(value):
        return None
    return int(float(str(value).strip().replace("%", "")))


def parse_rating(value):
    if is_blank(value):
        return None

    # calibre_export.csv already stores Calibre's native 0-10 rating scale.
    rating_0_to_10 = int(round(float(str(value).strip().replace(",", "."))))
    return max(0, min(10, rating_0_to_10))


def parse_date(value):
    if is_blank(value):
        return None
    return str(value).strip()


def main():
    calibre_db = db(LIBRARY_PATH).new_api
    existing_book_ids = set(calibre_db.all_book_ids())

    update_percent = {}
    update_last_read = {}
    update_rating = {}

    skipped_missing_id = 0
    skipped_unknown_book = 0

    with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for row in reader:
            raw_book_id = row.get(ID_COLUMN, "").strip()

            if not raw_book_id:
                skipped_missing_id += 1
                continue

            try:
                book_id = int(raw_book_id)
            except ValueError:
                skipped_missing_id += 1
                continue

            if book_id not in existing_book_ids:
                skipped_unknown_book += 1
                continue

            percent_value = parse_percent(row.get(PERCENT_READ_COLUMN))
            last_read_value = parse_date(row.get(LAST_READ_COLUMN))
            rating_value = parse_rating(row.get(RATING_COLUMN))

            has_progress_update = (
                percent_value is not None or last_read_value is not None
            )

            if percent_value is not None:
                update_percent[book_id] = percent_value

            if last_read_value is not None:
                update_last_read[book_id] = last_read_value

            # Only sync rating for rows that are part of the read-progress update.
            if has_progress_update and rating_value is not None:
                update_rating[book_id] = rating_value

    if update_percent:
        calibre_db.set_field(PERCENT_READ_COLUMN, update_percent)

    if update_last_read:
        calibre_db.set_field(LAST_READ_COLUMN, update_last_read)

    if update_rating:
        calibre_db.set_field(RATING_COLUMN, update_rating)

    print(f"books_in_library={len(existing_book_ids)}")
    print(f"updated_{PERCENT_READ_COLUMN}={len(update_percent)}")
    print(f"updated_{LAST_READ_COLUMN}={len(update_last_read)}")
    print(f"updated_{RATING_COLUMN}={len(update_rating)}")
    print(f"skipped_missing_id={skipped_missing_id}")
    print(f"skipped_unknown_book={skipped_unknown_book}")


if __name__ == "__main__":
    main()
