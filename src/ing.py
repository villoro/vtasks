import datetime
import hashlib
import json
import requests
import uuid

from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from Crypto.Signature import PKCS1_v1_5
from base64 import b64decode
from base64 import b64encode


class INGAPI:
    def __init__(
        self,
        host="https://api.sandbox.ing.com",
        client_id="e77d776b-90af-4684-bebc-521e5b2614dd",
        file_key="certs/example_client_signing.key",
        tls_cert="certs/example_client_tls.cer",
        tls_key="certs/example_client_tls.key",
    ):

        self.host = host
        self.client_id = client_id
        self.file_key = file_key
        self.tls_cert = tls_cert
        self.tls_key = tls_key

    def calculate_digest(self, payload):
        payload_digest = hashlib.sha256()
        payload_digest.update(payload.encode())
        return b64encode(payload_digest.digest()).decode()

    def sign(self, stringToSign):

        with open(self.file_key, "r") as mykey:
            private_key = RSA.importKey(mykey.read())
            signer = PKCS1_v1_5.new(private_key)
            digest = SHA256.new()
            digest.update(stringToSign.encode())
            signingString = signer.sign(digest)
            signingString = b64encode(signingString).decode()
            return signingString

    def calculate_signature(self, httpMethod, endpoint, reqDate, digest, reqId):
        stringToSign = (
            f"(request-target): {httpMethod} {endpoint}\n"
            + f"date: {reqDate}\n"
            + f"digest: SHA-256={digest}\n"
            + f"x-ing-reqid: {reqId}"
        )
        return self.sign(stringToSign)

    def query(self, httpMethod, endpoint, body="", token=None):

        reqId = str(uuid.uuid4())
        reqDate = datetime.datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
        cert = (self.tls_cert, self.tls_key)
        digest = self.calculate_digest(body)

        signature = self.calculate_signature(httpMethod.lower(), endpoint, reqDate, digest, reqId)

        headers = {
            "Date": reqDate,
            "Digest": f"SHA-256={digest}",
            "X-ING-ReqID": reqId,
            "Authorization": (
                f'Signature keyId="{self.client_id}"'
                + ',algorithm="rsa-sha256"'
                + ',headers="(request-target) date digest x-ing-reqid"'
                + f',signature="{signature}"'
            ),
        }
        if token:
            headers.update(
                {
                    "Authorization": f"Bearer {token}",
                    "Signature": (
                        f'keyId="{self.client_id}"'
                        + ',algorithm="rsa-sha256"'
                        + ',headers="(request-target) date digest x-ing-reqid"'
                        + f',signature="{signature}"'
                    ),
                    "Accept": "application/json",
                }
            )
        else:
            headers.update({"Content-Type": "application/x-www-form-urlencoded"})

        return requests.request(
            httpMethod.upper(), self.host + endpoint, headers=headers, data=body, cert=cert
        )

    def get_token(self):
        res = self.query(
            httpMethod="POST",
            endpoint="/oauth2/token",
            body="grant_type=client_credentials&scope=greetings%3Aview",
        )
        res.raise_for_status()
        return res.json()["access_token"]
