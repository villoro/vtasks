from datetime import datetime as dt  # To avoid collisions with 'datetime' attr

from pydantic import BaseModel, validator
from exif import Image

IMAGE_FORMATS = ["jpg", "jpeg"]  # , "png"]


class Doc(BaseModel):
    folder: str
    name: str
    extension: str = None
    is_image: bool = False
    level: int = -1
    missing_meta: bool = False
    # Dates
    datetime: str = ""
    datetime_original: str = ""
    datetime_taken: str = ""
    folder_date: str = None
    # Validations
    error_dt: bool = None
    error_dt_original: bool = None
    updated_at: dt = None

    @validator("updated_at", always=True)
    def set_updated_at(cls, v, values):
        return dt.now()

    @validator("extension", always=True)
    def get_extension(cls, v, values):
        """Used for extracting the extension from the file name"""
        return values["name"].split(".")[-1].lower()

    @validator("is_image", always=True)
    def get_is_image(cls, v, values):
        return values["extension"] in IMAGE_FORMATS

    def load(self):
        """Retrive metadata from document"""
        path = f"{self.folder}/{self.name}"

        if self.is_image:
            with open(path, "rb") as stream:
                image = Image(stream)

            self.missing_meta = len(image.list_all()) == 0

            for field in ["datetime", "datetime_original", "datetime_taken"]:
                try:
                    value = image.get(field)
                except KeyError:
                    continue

                if value:
                    setattr(self, field, value)

            self.error_dt = not self.datetime.startswith(self.folder_date)
            self.error_dt_original = not self.datetime_original.startswith(self.folder_date)

        # Simplify processing by returning the data as dict here
        return self.dict()
