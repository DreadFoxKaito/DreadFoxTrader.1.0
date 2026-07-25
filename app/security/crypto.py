from __future__ import annotations

import base64
import json
import os
import hashlib
from typing import Any, Optional

# We intentionally rely on a well-vetted implementation.
# If cryptography isn't installed, we fail loudly and clearly.
try:
    from cryptography.fernet import Fernet, InvalidToken  # type: ignore
except Exception as e:  # pragma: no cover
    Fernet = None  # type: ignore
    InvalidToken = Exception  # type: ignore
    _IMPORT_ERR = e
else:
    _IMPORT_ERR = None


_ENC_PREFIX = "enc:v1:"


class CryptoError(Exception):
    pass


def _require_crypto() -> None:
    if Fernet is None:
        raise CryptoError(
            "Encryption requires the 'cryptography' package. "
            "Install it with: pip install cryptography. "
            f"Import error: {_IMPORT_ERR}"
        )


def _get_master_secret() -> str:
    """
    Master secret source.

    Set ONE of:
      - APP_SECRET_KEY   (preferred)
      - CRYPTID_SECRET_KEY (fallback)

    This should be a long random string. Example:
      python -c "import secrets; print(secrets.token_urlsafe(48))"
    """
    secret = os.getenv("APP_SECRET_KEY") or os.getenv("CRYPTID_SECRET_KEY")
    if not secret:
        raise CryptoError(
            "Missing APP_SECRET_KEY (or CRYPTID_SECRET_KEY). "
            "Set it in your environment/.env to enable encrypted secret storage."
        )
    return secret


def _fernet_from_secret(secret: str) -> "Fernet":
    """
    Derive a Fernet key from an arbitrary secret string.

    Fernet key must be 32 urlsafe-base64-encoded bytes.
    We derive 32 bytes via SHA-256(secret) and then base64-url encode.
    """
    _require_crypto()
    digest = hashlib.sha256(secret.encode("utf-8")).digest()
    key = base64.urlsafe_b64encode(digest)
    return Fernet(key)  # type: ignore[misc]


def encrypt_str(plaintext: str) -> str:
    """
    Encrypt a string -> returns a tagged ciphertext string: "enc:v1:<token>"
    """
    f = _fernet_from_secret(_get_master_secret())
    token = f.encrypt(plaintext.encode("utf-8")).decode("utf-8")
    return _ENC_PREFIX + token


def decrypt_str(ciphertext: str) -> str:
    """
    Decrypt a tagged ciphertext string.

    If ciphertext is not tagged, we treat it as plaintext and return it unchanged.
    This allows gradual migrations.
    """
    if not isinstance(ciphertext, str):
        raise CryptoError("decrypt_str expected a string.")

    if not ciphertext.startswith(_ENC_PREFIX):
        # Backwards-compatible: treat as plaintext
        return ciphertext

    token = ciphertext[len(_ENC_PREFIX) :]
    f = _fernet_from_secret(_get_master_secret())
    try:
        pt = f.decrypt(token.encode("utf-8")).decode("utf-8")
        return pt
    except InvalidToken as e:  # type: ignore[misc]
        raise CryptoError("Invalid encrypted token or wrong APP_SECRET_KEY.") from e


def encrypt_json(obj: Any) -> str:
    """
    JSON-serialize + encrypt -> returns tagged ciphertext string
    """
    try:
        plaintext = json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
    except Exception as e:
        raise CryptoError(f"encrypt_json: object is not JSON-serializable: {e}") from e
    return encrypt_str(plaintext)


def decrypt_json(ciphertext: str, default: Optional[Any] = None) -> Any:
    """
    Decrypt + parse JSON.

    If ciphertext is plaintext JSON (not tagged), it will be parsed directly.
    If parsing fails, returns `default` (or raises if default is None).
    """
    try:
        plaintext = decrypt_str(ciphertext)
        return json.loads(plaintext) if plaintext else (default if default is not None else {})
    except Exception as e:
        if default is not None:
            return default
        raise CryptoError(f"decrypt_json failed: {e}") from e


def is_encrypted(value: Any) -> bool:
    return isinstance(value, str) and value.startswith(_ENC_PREFIX)