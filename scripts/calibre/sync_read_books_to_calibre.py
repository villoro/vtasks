import csv
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
CALIBRE_CSV = BASE_DIR / "calibre_export.csv"
UPDATED_CALIBRE_CSV = BASE_DIR / "calibre_export.updated.csv"
READ_BOOKS_CSV = BASE_DIR / "books_data.csv"
NOT_FOUND_CSV = BASE_DIR / "books_not_found_in_calibre.csv"

STOPWORDS = {
    "a",
    "al",
    "als",
    "an",
    "and",
    "de",
    "del",
    "dels",
    "des",
    "e",
    "el",
    "els",
    "en",
    "gli",
    "i",
    "il",
    "l",
    "la",
    "las",
    "le",
    "les",
    "lo",
    "los",
    "of",
    "the",
    "un",
    "una",
    "y",
}

ROMAN_TO_ARABIC = {
    "i": "1",
    "ii": "2",
    "iii": "3",
    "iv": "4",
    "v": "5",
    "vi": "6",
    "vii": "7",
    "viii": "8",
    "ix": "9",
    "x": "10",
}

# High-confidence cross-language aliases for books that are clearly present in Calibre.
TITLE_ALIASES = {
    ("carlos ruiz zafon", "el palau de mitjanit"): {
        "el palacio de la medianoche",
    },
    ("carlos ruiz zafon", "le luci di setembre"): {
        "las luces de septiembre",
    },
    ("e l james", "50 sombras de grey"): {
        "cincuenta sombras de grey",
    },
    ("j k rowling", "harry potter i el misteri del princep"): {
        "harry potter e il principe mezzosangue",
    },
    ("j k rowling", "harry potter i les reliquies de la mort"): {
        "harry potter e i doni della morte",
    },
    ("orson scott card", "el juego de ender"): {
        "enders game",
        "ender s game",
    },
    ("orson scott card", "la voz de los muertos"): {
        "speaker for the dead",
    },
    ("orson scott card", "ender el xenocida"): {
        "xenocide",
    },
    ("orson scott card", "hijos de la mente"): {
        "children of the mind",
    },
    ("orson scott card", "la sombra de ender"): {
        "enders shadow",
        "ender s shadow",
    },
    ("orson scott card", "marionetas de la sombra"): {
        "shadow puppets",
    },
    ("orson scott card", "la sombra del gigante"): {
        "shadow of the giant",
    },
    ("orson scott card", "guerra de regalos"): {
        "a war of gifts",
    },
    ("orson scott card", "ender en el exilio"): {
        "ender in exile",
    },
    ("orson scott card", "la sombra del hegemon"): {
        "shadow of the hegemon",
    },
    ("patrick rothfuss", "el nombre del viento"): {
        "il nome del vento",
        "name of the wind",
        "the name of the wind",
    },
    ("samantha shannon", "el priorato del naranjo"): {
        "the priory of the orange tree",
    },
    ("suzanne collins", "el mati de la sega"): {
        "sunrise on the reaping",
    },
    ("anne frank", "diari d anne frank"): {
        "diari d ana frank",
    },
}


@dataclass
class ReadEntry:
    row: dict
    parsed_date: datetime


@dataclass
class MatchCandidate:
    row: dict
    author_score: float
    title_score: float
    overall_score: float
    exact_level: int
    alias_hit: bool


def normalize_text(value: str) -> str:
    value = value or ""
    value = value.replace("’", "'").replace("‘", "'")
    value = unicodedata.normalize("NFKD", value)
    value = "".join(char for char in value if not unicodedata.combining(char))
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return " ".join(value.split())


def normalize_num_token(token: str) -> str:
    return ROMAN_TO_ARABIC.get(token, token)


def remove_leading_articles(value: str) -> str:
    tokens = value.split()
    while tokens and tokens[0] in {
        "a",
        "an",
        "el",
        "els",
        "gli",
        "i",
        "il",
        "l",
        "la",
        "las",
        "les",
        "lo",
        "los",
        "the",
        "un",
        "una",
    }:
        tokens = tokens[1:]
    return " ".join(tokens)


def canonicalize_volume_tokens(value: str) -> str:
    tokens = value.split()
    output = []
    index = 0
    markers = {
        "book",
        "episode",
        "episodio",
        "part",
        "parte",
        "tomo",
        "vol",
        "volume",
        "volumen",
    }

    while index < len(tokens):
        token = tokens[index]
        if token in markers and index + 1 < len(tokens):
            output.append(normalize_num_token(tokens[index + 1]))
            index += 2
            continue

        output.append(normalize_num_token(token))
        index += 1

    return " ".join(output)


def strip_number_tokens(value: str) -> str:
    return " ".join(token for token in value.split() if not token.isdigit())


def title_base_pieces(raw_title: str) -> list[str]:
    base = re.sub(r"\([^)]*\)", " ", raw_title or "")
    pieces = [base]

    for separator in (":", "."):
        if separator in base:
            pieces.append(base.split(separator, 1)[0])

    return pieces


def title_level_variants(raw_title: str) -> dict[int, set[str]]:
    levels: dict[int, set[str]] = defaultdict(set)

    for index, piece in enumerate(title_base_pieces(raw_title)):
        normalized = canonicalize_volume_tokens(normalize_text(piece))
        if not normalized:
            continue

        reduced = remove_leading_articles(normalized)
        numberless = strip_number_tokens(normalized)
        numberless_reduced = strip_number_tokens(reduced)

        if index == 0:
            levels[4].update({normalized, reduced})
            levels[2].update({numberless, numberless_reduced})
        else:
            levels[3].update({normalized, reduced})
            levels[1].update({numberless, numberless_reduced})

    return {
        level: {variant for variant in variants if variant}
        for level, variants in levels.items()
    }


def all_title_variants(raw_title: str) -> set[str]:
    variants: set[str] = set()
    for level_variants in title_level_variants(raw_title).values():
        variants.update(level_variants)
    return variants


def significant_tokens(value: str) -> set[str]:
    return {token for token in value.split() if token not in STOPWORDS}


def title_similarity(left: str, right: str) -> tuple[float, int]:
    left_levels = title_level_variants(left)
    right_levels = title_level_variants(right)

    exact_level = 0
    for level in (4, 3, 2, 1):
        if left_levels.get(level, set()) & right_levels.get(level, set()):
            exact_level = level
            break

    best = 1.0 if exact_level else 0.0
    left_variants = all_title_variants(left)
    right_variants = all_title_variants(right)

    for left_variant in left_variants:
        for right_variant in right_variants:
            seq = SequenceMatcher(None, left_variant, right_variant).ratio()
            left_tokens = significant_tokens(left_variant)
            right_tokens = significant_tokens(right_variant)
            if left_tokens and right_tokens:
                common = left_tokens & right_tokens
                jaccard = len(common) / len(left_tokens | right_tokens)
                overlap = max(
                    len(common) / len(left_tokens),
                    len(common) / len(right_tokens),
                )
            else:
                jaccard = 0.0
                overlap = 0.0

            contains = 0.0
            if min(len(left_tokens), len(right_tokens)) >= 2 and (
                left_variant in right_variant or right_variant in left_variant
            ):
                contains = 1.0

            best = max(
                best,
                seq,
                jaccard * 0.48 + overlap * 0.42 + contains * 0.10,
                contains * 0.90 + seq * 0.10,
            )

    return best, exact_level


def split_author_segments(raw_author: str) -> list[str]:
    segments = [raw_author or ""]
    for separator in ("|", " & ", " and ", " y ", " i "):
        next_segments = []
        for segment in segments:
            if separator in segment:
                next_segments.extend(segment.split(separator))
            else:
                next_segments.append(segment)
        segments = next_segments
    return [segment.strip() for segment in segments if segment.strip()]


def author_variants(raw_author: str) -> set[str]:
    variants: set[str] = set()
    for segment in split_author_segments(raw_author):
        normalized = normalize_text(segment)
        if normalized:
            variants.add(normalized)

        if "," in segment:
            parts = [part.strip() for part in segment.split(",")]
            if len(parts) == 2:
                variants.add(normalize_text(f"{parts[1]} {parts[0]}"))

    return {variant for variant in variants if variant}


def author_similarity(left: str, right: str) -> float:
    best = 0.0
    for left_variant in author_variants(left):
        for right_variant in author_variants(right):
            if left_variant == right_variant:
                return 1.0

            left_tokens = set(left_variant.split())
            right_tokens = set(right_variant.split())
            if left_tokens and right_tokens:
                jaccard = len(left_tokens & right_tokens) / len(
                    left_tokens | right_tokens
                )
            else:
                jaccard = 0.0

            seq = SequenceMatcher(None, left_variant, right_variant).ratio()

            subset = 0.0
            if (
                left_variant
                and (left_variant in right_variant or right_variant in left_variant)
                and min(len(left_tokens), len(right_tokens)) >= 2
            ):
                subset = 0.95

            best = max(best, jaccard, seq, subset)

    return best


def alias_hit(read_row: dict, calibre_row: dict) -> bool:
    key = (
        normalize_text(read_row["Author"]),
        normalize_text(read_row["Title"]),
    )
    aliases = TITLE_ALIASES.get(key, set())
    if not aliases:
        return False

    calibre_variants = all_title_variants(calibre_row["title"])
    for alias in aliases:
        if all_title_variants(alias) & calibre_variants:
            return True

    return False


def parse_read_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y/%m/%d")


def format_calibre_date(value: datetime) -> str:
    return value.strftime("%Y-%m-%d 00:00:00+00:00")


def parse_read_rating(value: str) -> str | None:
    value = (value or "").strip()
    if not value:
        return None

    rating_1_to_5 = float(value.replace(",", "."))
    rating_0_to_10 = int(round(rating_1_to_5 * 2))
    rating_0_to_10 = max(0, min(10, rating_0_to_10))
    return str(rating_0_to_10)


def read_key(row: dict) -> tuple[str, str]:
    return normalize_text(row["Title"]), normalize_text(row["Author"])


def candidate_sort_key(
    candidate: MatchCandidate,
) -> tuple[float, float, float, float, str]:
    return (
        1.0 if candidate.alias_hit else 0.0,
        float(candidate.exact_level),
        candidate.overall_score,
        candidate.author_score,
        candidate.row["id"],
    )


def build_candidate(read_row: dict, calibre_row: dict) -> MatchCandidate:
    title_score, exact_level = title_similarity(read_row["Title"], calibre_row["title"])
    author_score = author_similarity(read_row["Author"], calibre_row["authors"])
    overall_score = title_score * 0.74 + author_score * 0.26

    return MatchCandidate(
        row=calibre_row,
        author_score=author_score,
        title_score=title_score,
        overall_score=overall_score,
        exact_level=exact_level,
        alias_hit=alias_hit(read_row, calibre_row),
    )


def is_confident_match(best: MatchCandidate, second: MatchCandidate | None) -> bool:
    margin = best.overall_score - (second.overall_score if second else 0.0)

    if best.alias_hit and best.author_score >= 0.72:
        return True

    if best.exact_level >= 4 and best.author_score >= 0.72:
        return True

    if best.exact_level >= 4 and best.author_score >= 0.40 and best.title_score == 1.0:
        return True

    if best.exact_level >= 3 and best.author_score >= 0.72 and margin >= 0.05:
        return True

    if (
        best.exact_level >= 2
        and best.author_score >= 0.72
        and best.overall_score >= 0.90
        and margin >= 0.10
    ):
        return True

    if best.title_score >= 0.96 and best.author_score >= 0.80 and margin >= 0.03:
        return True

    if (
        best.overall_score >= 0.92
        and best.title_score >= 0.88
        and best.author_score >= 0.72
        and margin >= 0.03
    ):
        return True

    if (
        best.overall_score >= 0.88
        and best.title_score >= 0.84
        and best.author_score >= 0.82
        and margin >= 0.05
    ):
        return True

    return False


def write_calibre_csv(fieldnames: list[str], rows: list[dict]) -> Path:
    try:
        with CALIBRE_CSV.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return CALIBRE_CSV
    except PermissionError:
        with UPDATED_CALIBRE_CSV.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        return UPDATED_CALIBRE_CSV


def main() -> None:
    with CALIBRE_CSV.open("r", newline="", encoding="utf-8-sig") as f:
        calibre_rows = list(csv.DictReader(f))
        calibre_fieldnames = list(calibre_rows[0].keys()) if calibre_rows else []

    with READ_BOOKS_CSV.open("r", newline="", encoding="utf-8-sig") as f:
        read_rows = list(csv.DictReader(f))
        read_fieldnames = list(read_rows[0].keys()) if read_rows else []

    reads_by_key: dict[tuple[str, str], list[ReadEntry]] = defaultdict(list)
    for row in read_rows:
        reads_by_key[read_key(row)].append(
            ReadEntry(row=row, parsed_date=parse_read_date(row["Date"]))
        )

    calibre_by_id = {row["id"]: row for row in calibre_rows}
    update_by_calibre_id: dict[str, datetime] = {}
    rating_by_calibre_id: dict[str, str] = {}
    not_found_rows: list[dict] = []
    matched_read_books = 0

    for entries in reads_by_key.values():
        latest_entry = max(entries, key=lambda item: item.parsed_date)
        candidates = [
            build_candidate(latest_entry.row, calibre_row)
            for calibre_row in calibre_rows
        ]
        candidates.sort(key=candidate_sort_key, reverse=True)

        best = candidates[0] if candidates else None
        second = candidates[1] if len(candidates) > 1 else None

        if not best or not is_confident_match(best, second):
            not_found_rows.append(latest_entry.row)
            continue

        matched_read_books += 1
        current_value = update_by_calibre_id.get(best.row["id"])
        if current_value is None or latest_entry.parsed_date > current_value:
            update_by_calibre_id[best.row["id"]] = latest_entry.parsed_date

        rating_value = parse_read_rating(latest_entry.row.get("Rating", ""))
        if rating_value is not None:
            rating_by_calibre_id[best.row["id"]] = rating_value

    for calibre_id, read_date in update_by_calibre_id.items():
        calibre_row = calibre_by_id[calibre_id]
        calibre_row["#kobo_percent_read"] = "100"
        calibre_row["#kobo_last_read"] = format_calibre_date(read_date)

        rating_value = rating_by_calibre_id.get(calibre_id)
        if rating_value is not None:
            calibre_row["rating"] = rating_value

    output_calibre_csv = write_calibre_csv(calibre_fieldnames, calibre_rows)

    with NOT_FOUND_CSV.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=read_fieldnames)
        writer.writeheader()
        writer.writerows(not_found_rows)

    print(f"read_books={len(reads_by_key)}")
    print(f"matched_read_books={matched_read_books}")
    print(f"updated_calibre_rows={len(update_by_calibre_id)}")
    print(f"updated_rating_rows={len(rating_by_calibre_id)}")
    print(f"not_found_books={len(not_found_rows)}")
    print(f"updated_calibre_csv={output_calibre_csv}")
    print(f"not_found_csv={NOT_FOUND_CSV}")


if __name__ == "__main__":
    main()
