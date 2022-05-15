import os
import re

import pandas as pd

from exif import Image
from loguru import logger as log
from tqdm.notebook import tqdm

REGEX = re.compile(r"(?P<year>\d{4})(_(?P<month>\d{2}))?(_(?P<day>\d{2}))?(.*)")
IMAGE_FORMATS = ["jpg", "png"]


def is_image(name):
    return name.split(".")[-1].lower() in IMAGE_FORMATS


def show_file_info(path):

    log.debug(f"Showing info for {path=}")

    with open(path, "rb") as stream:
        image = Image(stream)

    for name in image.list_all():
        print(name, "-", image.get(name))


def get_folder_date(path):

    log.info(f"Extracting date from {path=}")

    # Find the first match going from the deepest folder
    for folder in path.split("/")[::-1]:
        data = REGEX.match(folder)

        if data:
            data = data.groupdict()
            break

    else:
        log.error(f"Unable to extract data for {folder=}")
        return False

    log.debug(f"Result: {data=}")

    out = str(data["year"])

    for key in ["month", "day"]:
        value = data[key]

        if (value is not None) and (int(value) > 0):
            out += f":{value}"

    return out


def get_approx_date(value):

    # Add month
    if len(value) < 5:
        value += ":01"

    # Add day
    if len(value) < 8:
        value += ":01"

    return value


def update_one_meta(path, folder_date, dry_run=True):

    log.debug(f"Updating metadata for {path=} with {folder_date=}")

    with open(path, "rb") as stream:
        image = Image(stream)

    updated = 0

    for field in ["datetime", "datetime_original"]:

        # Use `datetime` as the default
        try:
            value = image.get(field, image.get("datetime"))
        except KeyError:
            approx_date = get_approx_date(folder_date)

            log.warning(f"{field=} not found in {path=}. Setting default '{approx_date}'")
            image[field] = approx_date
            updated += 1
            continue

        if value:
            if not value.startswith(folder_date):
                updated += 1

            new_value = folder_date + value[len(folder_date) :]
            image[field] = new_value

    if (not dry_run) and updated:
        with open(path, "wb") as stream:
            stream.write(image.get_file())

        log.debug(f"{updated} updates to {path=}")

    return bool(updated)


def get_all_files_in_path(base_path, verbose=False, dry_run=True):

    log.info(f"Starting process for {base_path=} and {dry_run=}")

    # Get total number of steps to do
    count = len([0 for _ in os.walk(base_path)])

    out = []

    for root, _, files in tqdm(os.walk(base_path), total=count):

        log.debug(f"Scanning {root=} with {len(files)} files")

        # For reporting strip base path
        folder = root[len(base_path) + 1 :]

        if not files:
            log.info(f"Skipping {root=}")
            continue

        root = root.replace("\\", "/")

        folder_date = get_folder_date(root)

        if not folder_date:
            out += [(base_path, folder, folder_date, None, None)]
            continue

        files_updated = []

        for file in files:
            if not is_image(file):
                log.info(f"Skipping {file=}")
                continue

            path = f"{root}/{file}"

            result = update_one_meta(path, folder_date, dry_run)

            if result and verbose:
                files_updated.append(file)

            out += [(base_path, folder, folder_date, file, result)]

    log.info(f"Exporting results")

    df = pd.DataFrame(out, columns=["base_path", "folder", "folder_date", "file", "to_update"])
    df.to_excel("result.xlsx")

    return df
