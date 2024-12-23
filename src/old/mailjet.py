import base64

from mailjet_rest import Client
from pydantic import BaseModel
from pydantic import validator

import utils as u


class Attachment(BaseModel):
    filename: str
    content: str
    content_type: str = None

    @staticmethod
    def to_b64(x):
        return base64.b64encode(x).decode("ascii")

    @validator("content_type", pre=True, always=True)
    def check_content_type(cls, v, values):
        filename = values["filename"]
        extension = filename.split(".")[-1]

        if extension in ["jpg", "jpeg"]:
            return "image/jpg"

        raise ValueError(f"'content_type' can't be infered from {filename=}")

    def export(self):
        return {
            "Filename": self.filename,
            "Base64Content": self.content,
            "ContentType": self.content_type,
        }


class InlineAttachment(Attachment):
    cid: str

    def export(self):
        return {
            "Filename": self.filename,
            "Base64Content": self.content,
            "ContentType": self.content_type,
            "ContentID": self.cid,
        }


class Person(BaseModel):
    name: str

    # Needs to be registed in mailjet before changing this
    email: str = "villoro7@gmail.com"

    def export(self):
        return {"Email": self.email, "Name": self.name}


class Email(BaseModel):
    from_email: Person = Person(name="Vtasks")
    to_email: list[Person] = [Person(name="Villoro")]
    subject: str
    html: str

    attachments: list[Attachment] = None
    inline_attachments: list[InlineAttachment] = None

    def export(self):
        message = {
            "From": self.from_email.export(),
            "To": [x.export() for x in self.to_email],
            "Subject": self.subject,
            "HTMLPart": self.html,
        }

        if self.attachments:
            message["Attachments"] = [x.export() for x in self.attachments]

        if self.inline_attachments:
            message["InlinedAttachments"] = [x.export() for x in self.inline_attachments]

        return {"Messages": [message]}

    @staticmethod
    def get_mailjet_client():
        auth = (u.get_secret("MAILJET_API_KEY"), u.get_secret("MAILJET_API_TOKEN"))
        return Client(auth=auth, version="v3.1")

    def send(self):
        client = self.get_mailjet_client()
        data = self.export()

        result = client.send.create(data=data)

        assert result.status_code == 200
        return result.json()
