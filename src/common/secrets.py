from vcrypto import init_vcrypto

from src.common.paths import get_path

VCRYPTO_KWARGS = {
    "secrets_file": get_path("secrets.yaml"),
    "environ_var_name": "VTASKS_TOKEN",
}

init_vcrypto(**VCRYPTO_KWARGS)
