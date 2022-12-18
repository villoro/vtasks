import os

import pandas as pd
from tqdm.notebook import tqdm

from models import Movie, Sub

PATH = f"V:/Movies"


def process_one(folder):
    path = f"{PATH}/{folder}"
    movie = None
    subs = []

    for file in os.listdir(path):

        if file.split(".")[-1] in ["mp4", "mkv"]:
            movie = file

        if file.split(".")[-1] in ["srt"]:
            subs.append(file)

    subs = [Sub(movie=movie, name=x) for x in subs]
    return Movie(folder=folder, movie=movie, subs=subs)


def process_all():
    movies = []
    for folder in tqdm(os.listdir(PATH), desc="movies"):
        movies.append(process_one(folder))

    df = pd.DataFrame([x.dict() for x in movies])
    del df["subs"]
    return df
