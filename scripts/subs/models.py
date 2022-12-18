import re

from pydantic import BaseModel, validator

REGEX_YEAR = re.compile(r"\((?P<year>\d{4})\)")
REGEX_QUALITY = re.compile(r"[\s\.](?P<quality>\d{3,4})p[\s\.]")


class Sub(BaseModel):
    movie: str
    name: str
    good_format: bool = False
    language: str = None

    @staticmethod
    def strip_extension(text):
        extension_length = len(text.split(".")[-1])
        return text[: -(extension_length + 1)]

    @validator("good_format", pre=True, always=True)
    def sub_should_contain_film(cls, v, values):
        movie_name = cls.strip_extension(values["movie"])
        sub_name = cls.strip_extension(values["name"])

        if sub_name.startswith(movie_name):
            return True

        return False

    @validator("language", pre=True, always=True)
    def check_language(cls, v, values):
        name = cls.strip_extension(values["name"])

        language = name.split(".")[-1]

        valid_langs = ("es", "esp", "spa", "en", "eng", "it", "ita", "ca", "cat")

        if language in valid_langs:
            return language
        return None


class Movie(BaseModel):
    folder: str
    movie: str
    subs: list[Sub]
    year: int = 0
    has_good_subs: bool = None
    quality: int = 0
    # Specific subs
    sub_ca: bool = None
    sub_en: bool = None
    sub_es: bool = None
    sub_it: bool = None

    @validator("year", pre=True, always=True)
    def check_language(cls, v, values):
        out = REGEX_YEAR.search(values["folder"])
        if out:
            return out.groupdict().get("year")
        return 0

    @validator("has_good_subs", pre=True, always=True)
    def check_good_subs(cls, v, values):
        for x in values["subs"]:
            if x.good_format and (x.language is not None):
                return True
        return False

    @validator("quality", pre=True, always=True)
    def check_quality(cls, v, values):
        out = REGEX_QUALITY.search(values["folder"])
        if out:
            return int(out.groupdict().get("quality"))
        return 0

    @validator("sub_ca", pre=True, always=True)
    def check_sub_es(cls, v, values):
        for x in values["subs"]:
            if x.language in ("ca", "cat"):
                return True
        return False

    @validator("sub_en", pre=True, always=True)
    def check_sub_es(cls, v, values):
        for x in values["subs"]:
            if x.language in ("en", "eng"):
                return True
        return False

    @validator("sub_es", pre=True, always=True)
    def check_sub_es(cls, v, values):
        for x in values["subs"]:
            if x.language in ("es", "esp", "spa"):
                return True
        return False

    @validator("sub_it", pre=True, always=True)
    def check_sub_es(cls, v, values):
        for x in values["subs"]:
            if x.language in ("it", "ita"):
                return True
        return False
