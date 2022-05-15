from pydantic import BaseModel, validator
from exif import Image

IMAGE_FORMATS = ["jpg", "jpeg", "png"]


class Doc(BaseModel):
    folder: str
    name: str
    extension: str = None
    is_image: bool = False
    level: int = -1
    # Dates
    datetime: str = None
    datetime_original: str = None
    datetime_taken: str = None
    folder_date: str = None
    # Validations
    check_dt: bool = None
    check_dt_original: bool = None

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

            for field in ["datetime", "datetime_original", "datetime_taken"]:
                try:
                    value = image.get(field)
                    setattr(self, field, value)
                except KeyError:
                    pass

            self.check_dt = self.datetime.startswith(self.folder_date)
            self.check_dt_original = self.datetime_original.startswith(self.folder_date)

        # Simplify processing by returning the data as dict here
        return self.dict()
