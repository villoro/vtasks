from vcrypto import export_secret
from vcrypto import init_vcrypto
from vcrypto import read_secret

from src.common.paths import get_path

init_vcrypto(secrets_file=get_path("secrets.yaml"), environ_var_name="VTASKS_TOKEN")

# Explicitly define public API
__all__ = ["read_secret", "export_secret"]
