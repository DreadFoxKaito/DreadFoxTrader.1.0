from __future__ import annotations

import json
import sqlite3
import time
from typing import Any, Optional

# Encryption helpers (Step 10)
try:
    from .security.crypto import CryptoError, decrypt_json, encrypt_json, is_encrypted
except Exception:  # pragma: no cover
    CryptoError = Exception  # type: ignore

    def encrypt_json(obj: Any) -> str:  # type: ignore
        raise RuntimeError("Encryption helpers not available; did you create app/security/crypto.py?")

    def decrypt_json(ciphertext: str, default: Optional[Any] = None) -> Any:  # type: ignore
        # Backwards-compat: attempt plaintext JSON parse
        try:
            return json.loads(ciphertext) if ciphertext else (default if default is not None else {})
        except Exception:
            return default if default is not None else {}

    def is_encrypted(value: Any) -> bool:  # type: ignore
        return False


# -------------------------
# Time / JSON helpers
# -------------------------
def utc_ts() -> int:
    return int(time.time())


def safe_json_loads(s: str, default: Any) -> Any:
    try:
        return json.loads(s) if s else default
    except Exception:
        return default


# -------------------------
# DB connection
# -------------------------
def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


# -------------------------
# Broker connection helpers
# -------------------------
def list_broker_connections(db_path: str) -> list[sqlite3.Row]:
    conn = connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT * FROM broker_connections ORDER BY id DESC")
    rows = list(cur.fetchall())
    conn.close()
    return rows


def get_broker_connection(db_path: str, connection_id: int) -> Optional[sqlite3.Row]:
    conn = connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT * FROM broker_connections WHERE id=?", (int(connection_id),))
    row = cur.fetchone()
    conn.close()
    return row


def _encode_secrets(secrets: dict[str, Any], *, allow_plaintext: bool) -> str:
    """
    Encrypt secrets when possible. If encryption isn't available and allow_plaintext
    is True, store plaintext JSON instead.
    """
    try:
        return encrypt_json(secrets)
    except Exception:
        if allow_plaintext:
            return json.dumps(secrets)
        raise


def create_broker_connection(
    *,
    db_path: str,
    broker: str,
    label: str,
    status: str = "new",
    metadata: dict[str, Any] | None = None,
    secrets: dict[str, Any] | None = None,
    allow_plaintext: bool = False,
) -> int:
    """
    Creates a broker connection row.

    secrets are encrypted-at-rest (requires APP_SECRET_KEY and cryptography).
    If encryption isn't available, this will raise to avoid storing plaintext.
    """
    meta_txt = json.dumps(metadata or {})
    secrets_obj = secrets or {}

    secrets_txt = _encode_secrets(secrets_obj, allow_plaintext=allow_plaintext)

    conn = connect(db_path)
    cur = conn.cursor()
    now = utc_ts()
    cur.execute(
        """
        INSERT INTO broker_connections
        (broker, label, status, metadata_json, secrets_json, created_ts, updated_ts)
        VALUES (?,?,?,?,?,?,?)
        """,
        (broker, label, status, meta_txt, secrets_txt, now, now),
    )
    conn.commit()
    cid = int(cur.lastrowid)
    conn.close()
    return cid


def update_broker_connection(
    *,
    db_path: str,
    connection_id: int,
    broker: Optional[str] = None,
    label: Optional[str] = None,
    status: Optional[str] = None,
    metadata: Optional[dict[str, Any]] = None,
    secrets: Optional[dict[str, Any]] = None,
    allow_plaintext: bool = False,
) -> None:
    """
    Updates broker connection row fields. If secrets are provided, they are encrypted-at-rest.
    """
    row = get_broker_connection(db_path, connection_id)
    if not row:
        return

    new_broker = broker if broker is not None else str(row["broker"])
    new_label = label if label is not None else str(row["label"])
    new_status = status if status is not None else str(row["status"])

    if metadata is None:
        meta_txt = str(row["metadata_json"] or "{}")
    else:
        meta_txt = json.dumps(metadata)

    if secrets is None:
        secrets_txt = str(row["secrets_json"] or "{}")
    else:
        secrets_txt = _encode_secrets(secrets, allow_plaintext=allow_plaintext)

    conn = connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE broker_connections
        SET broker=?, label=?, status=?, metadata_json=?, secrets_json=?, updated_ts=?
        WHERE id=?
        """,
        (new_broker, new_label, new_status, meta_txt, secrets_txt, utc_ts(), int(connection_id)),
    )
    conn.commit()
    conn.close()


def set_broker_status(
    *, db_path: str, connection_id: int, status: str, metadata: Optional[dict[str, Any]] = None
) -> None:
    """
    Update just status (and optionally metadata).
    """
    row = get_broker_connection(db_path, connection_id)
    if not row:
        return

    meta_txt = str(row["metadata_json"] or "{}") if metadata is None else json.dumps(metadata)

    conn = connect(db_path)
    cur = conn.cursor()
    cur.execute(
        "UPDATE broker_connections SET status=?, metadata_json=?, updated_ts=? WHERE id=?",
        (status, meta_txt, utc_ts(), int(connection_id)),
    )
    conn.commit()
    conn.close()


def delete_broker_connection(db_path: str, connection_id: int) -> None:
    conn = connect(db_path)
    cur = conn.cursor()
    cur.execute("DELETE FROM broker_connections WHERE id=?", (int(connection_id),))
    conn.commit()
    conn.close()


def read_connection_metadata(row: sqlite3.Row) -> dict[str, Any]:
    return safe_json_loads(str(row["metadata_json"] or "{}"), default={})


def read_connection_secrets(row: sqlite3.Row, *, default: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    """
    Decrypt secrets_json (supports plaintext JSON for backwards compatibility).
    """
    raw = str(row["secrets_json"] or "")
    if default is None:
        default = {}
    try:
        return decrypt_json(raw, default=default) or default
    except Exception:
        return default


# -------------------------
# Migration: encrypt existing plaintext secrets_json
# -------------------------
def migrate_encrypt_broker_secrets(db_path: str) -> dict[str, Any]:
    """
    One-shot migration:
    - For each broker_connections row:
      - If secrets_json is plaintext JSON and not encrypted, encrypt it in-place.

    Returns stats dict.

    Notes:
    - This requires APP_SECRET_KEY and cryptography installed.
    - If encryption isn't available, migration will stop with a clear error.
    """
    conn = connect(db_path)
    cur = conn.cursor()
    cur.execute("SELECT id, secrets_json FROM broker_connections")
    rows = cur.fetchall()

    updated = 0
    skipped = 0
    failed = 0

    for r in rows:
        cid = int(r["id"])
        s = str(r["secrets_json"] or "")

        if not s:
            skipped += 1
            continue

        if is_encrypted(s):
            skipped += 1
            continue

        # Try parse plaintext json
        try:
            obj = json.loads(s)
        except Exception:
            # Not JSON -> skip; better not to corrupt unknown format
            skipped += 1
            continue

        try:
            enc = encrypt_json(obj)
        except CryptoError as e:
            conn.close()
            raise  # fail fast; we don't want partial migrations without encryption
        except Exception:
            failed += 1
            continue

        cur.execute(
            "UPDATE broker_connections SET secrets_json=?, updated_ts=? WHERE id=?",
            (enc, utc_ts(), cid),
        )
        updated += 1

    conn.commit()
    conn.close()

    return {"updated": updated, "skipped": skipped, "failed": failed}
