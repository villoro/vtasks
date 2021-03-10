import json
import requests
import uuid

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

    def get_signing_key(self):
        """ Gets the signing key """

        with open(self.sign_key, "r") as file:
            return file.read()

    def encode_sha256(self, text, sign=False):
        """ digest a text with SHA256 """

        digest = SHA256.new()
        digest.update(text.encode())

        if sign:
            private_key = RSA.importKey(self.get_signing_key())
            signer = PKCS1_v1_5.new(private_key)
            out = signer.sign(digest)

        else:
            out = digest.digest()

        return b64encode(out).decode()

    def get_signature(self, method, endpoint, mdate, digest, req_id):

        text = [
            f"(request-target): {method.lower()} {endpoint}",
            f"date: {mdate}",
            f"digest: SHA-256={digest}",
            f"x-ing-reqid: {req_id}",
        ]

        signature_digest = self.encode_sha256("\n".join(text), True)

        signature = [
            f'keyId="{self.client_id}"',
            'algorithm="rsa-sha256"',
            'headers="(request-target) date digest x-ing-reqid"',
            f'signature="{signature_digest}"',
        ]

        return ",".join(signature)

    def query(self, endpoint, method="GET", body="", token=None):
        """ Query an endpoint """

        mdate = datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
        digest = self.encode_sha256(body)
        req_id = str(uuid.uuid4())

        # Basic headers
        headers = {
            "Date": mdate,
            "Digest": f"SHA-256={digest}",
            "X-ING-ReqID": req_id,
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }

        # Add Authorization header
        signature = self.get_signature(method, endpoint, mdate, digest, req_id)
        if token:
            headers.update({"Authorization": f"Bearer {token}", "Signature": signature})
        else:
            headers.update({"Authorization": f"Signature {signature}"})

        # Do the real query
        result = requests.request(
            method.upper(),
            self.host + endpoint,
            headers=headers,
            data=body,
            cert=(self.tls_cert, self.tls_key),
        )

        # Check that the result is valid
        result.raise_for_status()
        return result

    def get_token(self):
        """ Update self.token """

        res = self.query(
            endpoint="/oauth2/token", method="POST", body="grant_type=client_credentials",
        )
        self.token = res.json()["access_token"]
