import hashlib
import json
import requests
import uuid

from base64 import b64decode
from base64 import b64encode
from datetime import datetime

from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from Crypto.Signature import PKCS1_v1_5


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

        # Set an empty token until it's requested
        self.token = None

    def digest_body(self, text):
        """ digest a text with SHA256 """

        digest = hashlib.sha256()
        digest.update(text.encode())

        return b64encode(digest.digest()).decode()

    def sign(self, stringToSign):

        with open(self.file_key, "r") as file:
            data = file.read()

        private_key = RSA.importKey(data)
        signer = PKCS1_v1_5.new(private_key)

        digest = SHA256.new()
        digest.update(stringToSign.encode())

        signingString = signer.sign(digest)
        signingString = b64encode(signingString).decode()

        return signingString

    def calculate_signature(self, method, endpoint, mdate, digest, reqId):
        stringToSign = (
            f"(request-target): {method} {endpoint}\n"
            + f"date: {mdate}\n"
            + f"digest: SHA-256={digest}\n"
            + f"x-ing-reqid: {reqId}"
        )
        return self.sign(stringToSign)

    def query(self, method, endpoint, body="", token=None):

        reqId = str(uuid.uuid4())
        mdate = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
        cert = (self.tls_cert, self.tls_key)
        digest = self.digest_body(body)

        signature = self.calculate_signature(method.lower(), endpoint, mdate, digest, reqId)

        headers = {
            "Date": mdate,
            "Digest": f"SHA-256={digest}",
            "X-ING-ReqID": reqId,
        }

        signature_details = (
            f'keyId="{self.client_id}"',
            'algorithm="rsa-sha256"',
            'headers="(request-target) date digest x-ing-reqid"',
            f'signature="{signature}"',
        )

        if token:
            headers.update(
                {
                    "Authorization": f"Bearer {token}",
                    "Signature": ",".join(signature_details),
                    "Accept": "application/json",
                }
            )
        else:
            headers.update(
                {
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Authorization": "Signature " + ",".join(signature_details),
                }
            )

        result = requests.request(
            method.upper(), self.host + endpoint, headers=headers, data=body, cert=cert
        )

        result.raise_for_status()

        return result

    def get_token(self):
        res = self.query(
            method="POST",
            endpoint="/oauth2/token",
            body="grant_type=client_credentials&scope=greetings%3Aview",
        )
        self.token = res.json()["access_token"]
        return token
