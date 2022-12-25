import base64

from mailjet_rest import Client

import utils as u


def get_mailjet_client():
    auth = (u.get_secret("MAILJET_API_KEY"), u.get_secret("MAILJET_API_TOKEN"))
    return Client(auth=auth, version="v3.1")


def create_attachment(etype, filename, content):

    assert etype in ["image/jpg"]

    return {
        "ContentType": etype,
        "Filename": filename,
        "Base64Content": content,
    }


def plotly_to_attachment(fig, filename):
    data = fig.to_image(format="jpg", width=1920, height=1080, scale=2)
    content = base64.b64encode(data).decode("ascii")
    return create_attachment("image/jpg", filename, content)


def create_email(subject, html, attachments=None):

    message = {
        "From": {"Email": "villoro7@gmail.com", "Name": "Vtasks"},
        "To": [{"Email": "villoro7@gmail.com", "Name": "Villoro"}],
        "Subject": subject,
        "HTMLPart": html,
    }

    if attachments:
        message["Attachments"] = attachments

    return {"Messages": [message]}
