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
        sign_key="certificates/example_client_signing.key",
        tls_cert="certificates/example_client_tls.cer",
        tls_key="certificates/example_client_tls.key",
    ):

        self.host = host
        self.client_id = client_id
        self.sign_key = sign_key
        self.tls_cert = tls_cert
        self.tls_key = tls_key

        # Set an empty token until it's requested
        self.token = None

    def encode_sha256(self, text, sign=False):
        """ digest a text with SHA256 """

        digest = SHA256.new()
        digest.update(text.encode())

        if sign:
            with open(self.sign_key, "r") as file:
                data = file.read()

            private_key = RSA.importKey(data)
            signer = PKCS1_v1_5.new(private_key)
            out = signer.sign(digest)

        else:
            out = digest.digest()

        return b64encode(out).decode()

    def sign(self, method, endpoint, mdate, digest, req_id):

        text = (
            f"(request-target): {method} {endpoint}\n"
            + f"date: {mdate}\n"
            + f"digest: SHA-256={digest}\n"
            + f"x-ing-reqid: {req_id}"
        )

        return self.encode_sha256(text, True)

    def query(self, endpoint, method="GET", body="", token=None):
        """ Query an endpoint """

        mdate = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
        digest = self.encode_sha256(body)
        req_id = str(uuid.uuid4())

        signature = self.sign(method.lower(), endpoint, mdate, digest, req_id)
        signature_details = (
            f'keyId="{self.client_id}"',
            'algorithm="rsa-sha256"',
            'headers="(request-target) date digest x-ing-reqid"',
            f'signature="{signature}"',
        )

        headers = {
            "Date": mdate,
            "Digest": f"SHA-256={digest}",
            "X-ING-ReqID": req_id,
        }

        # Add extra headers
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

        # Do the real query
        result = requests.request(
            method.upper(),
            self.host + endpoint,
            headers=headers,
            data=body,
            cert=(self.tls_cert, self.tls_key),
        )

        result.raise_for_status()
        return result

    def get_token(self):
        """ Update self.token """

        res = self.query(
            endpoint="/oauth2/token", method="POST", body="grant_type=client_credentials",
        )
        self.token = res.json()["access_token"]
