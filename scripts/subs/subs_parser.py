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
    return Movie(path=path, movie=movie, subs=subs)


def process_all():
    movies = []
    for folder in tqdm(os.listdir(PATH), desc="movies"):
        movies.append(process_one(folder))
    return movies


def movies_to_df(movies):
    df = pd.DataFrame([x.dict() for x in movies])
    del df["subs"]
    return df


def default_subs_to_english(movies):
    count = 0

    for movie in tqdm(movies, desc="default_subs_to_english"):
        subs = movie.subs
        if (len(subs) == 1) and ((sub := subs[0]).language is None):
            old_path = f"{movie.path}/{sub.name}"
            new_path = f"{movie.path}/{sub.name[:-4]}.en.srt"

            os.rename(old_path, new_path)
            count += 1
    return count


def fix_using_smart_language(movies):
    count = 0

    for movie in tqdm(movies, desc="fix_using_smart_language"):
        for sub in movie.subs:
            if (sub.language != sub.smart_language) and (sub.smart_language is not None):
                old_path = f"{movie.path}/{sub.name}"
                new_path = f"{movie.path}/{sub.movie}.{sub.smart_language}.srt"

                os.rename(old_path, new_path)
                count += 1
    return count
