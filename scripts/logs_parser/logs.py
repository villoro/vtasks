import re
import os

PATH_LOGS = "C:/GIT/vtasks/logs"
REGEX_CLEAN_TIME = re.compile(
    r"(?P<task>[\w_]*)\s(ended|done)\s(in)\s(?P<time>\d\.\d\d)\s(?P<unit>(min|s))"
)


def get_log_paths():
    paths = []
    for root, _, files in os.walk(PATH_LOGS):
        for file in files:
            path = f"{root}/{file}".replace("\\", "/")
            paths.append(path)
    return paths


def read_lines(path):
    with open() as stream:
        data = stream.read()
    return data.split("\n")


def extract_times(lines):
    data = []
    for x in lines:
        out = REGEX_CLEAN_TIME.search(x)
        if not out:
            continue

        mdict = out.groupdict()
        mdict["timestamp"] = x[:23]

        data.append(mdict)

    return data
