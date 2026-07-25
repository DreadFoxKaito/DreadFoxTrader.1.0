from __future__ import annotations

import base64
import json
import os
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import httpx

from ..db import read_connection_secrets

from . import (
    BrokerAuthError,
    BrokerConnector,
    BrokerConnectorError,
    NormalizedAccount,
    NormalizedPortfolioSnapshot,
    NormalizedPosition,
    safe_float,
)


# -------------------------
# SQLite helpers
# -------------------------
def _db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _utc_ts() -> int:
    return int(time.time())


def _safe_json(s: str, default: Any) -> Any:
    try:
        return json.loads(s) if s else default
    except Exception:
        return default


# -------------------------
# Schwab REST (Official)
# -------------------------
class SchwabAuthError(RuntimeError):
    def __init__(self, msg: str, status_code: int = 401):
        super().__init__(msg)
        self.status_code = status_code


def _basic_auth_header(client_id: str, client_secret: str) -> str:
    raw = f"{client_id}:{client_secret}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def _token_is_fresh(token: dict[str, Any], early_refresh_s: int = 60) -> bool:
    """
    Token file convention (as used in your app):
      token["obtained_at"] = epoch seconds OR ISO-8601 when access token was obtained
      token["expires_in"]  = lifetime seconds (Trader API: 1800)
    """
    try:
        access = str(token.get("access_token") or "")
        expires_in = int(token.get("expires_in") or 0)
        obtained_raw = token.get("obtained_at")
        obtained_ts: int | None = None
        if isinstance(obtained_raw, (int, float)):
            obtained_ts = int(obtained_raw)
        elif isinstance(obtained_raw, str) and obtained_raw:
            try:
                dt = datetime.fromisoformat(obtained_raw.replace("Z", "+00:00"))
                obtained_ts = int(dt.timestamp())
            except Exception:
                obtained_ts = None

        if not access or expires_in <= 0 or not obtained_ts:
            return False
        return _utc_ts() < (obtained_ts + expires_in - early_refresh_s)
    except Exception:
        return False


def _refresh_access_token(token: dict[str, Any]) -> dict[str, Any]:
    """
    Official refresh flow from your pasted docs:
      POST https://api.schwabapi.com/v1/oauth/token
      Authorization: Basic base64(client_id:client_secret)
      grant_type=refresh_token&refresh_token=...
    """
    client_id = os.getenv("SCHWAB_CLIENT_ID", "").strip()
    client_secret = os.getenv("SCHWAB_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        raise SchwabAuthError(
            "Missing SCHWAB_CLIENT_ID / SCHWAB_CLIENT_SECRET env vars needed for token refresh.",
            status_code=401,
        )

    refresh_token = str(token.get("refresh_token") or "").strip()
    if not refresh_token:
        raise SchwabAuthError("Token missing refresh_token. Reconnect Schwab.", status_code=401)

    url = "https://api.schwabapi.com/v1/oauth/token"
    headers = {
        "Authorization": _basic_auth_header(client_id, client_secret),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }

    with httpx.Client(timeout=30.0) as c:
        r = c.post(url, headers=headers, data=data)

    if r.status_code >= 400:
        raise SchwabAuthError(f"Token refresh failed ({r.status_code}): {r.text}", status_code=r.status_code)

    new_tok = r.json()
    new_tok["obtained_at"] = _utc_ts()

    # NOTE: Per your pasted docs, refresh_token may rotate. Prefer returned refresh_token if present.
    if not new_tok.get("refresh_token"):
        new_tok["refresh_token"] = refresh_token

    return new_tok


def _ensure_access_token(token: dict[str, Any], token_write_func: Optional[callable] = None) -> str:
    if _token_is_fresh(token):
        return str(token.get("access_token") or "")

    new_tok = _refresh_access_token(token)
    if token_write_func:
        try:
            token_write_func(new_tok)
        except Exception:
            pass
    access = str(new_tok.get("access_token") or "")
    if not access:
        raise SchwabAuthError("Refresh succeeded but access_token missing in response.", status_code=401)
    return access


class SchwabTraderREST:
    """
    Minimal REST client for official Schwab Trader API endpoints used by the UI portfolio views.
    We do NOT guess the trader base URL. You must set:

      SCHWAB_TRADER_API_BASE="https://<your-trader-host>/<prefix>"

    Your pasted schemas show relative endpoints like:
      /accounts/accountNumbers
      /accounts?fields=positions
    """

    def __init__(self, *, base_url: str):
        self.base_url = base_url.rstrip("/")

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return self.base_url + path

    def _headers(self, access_token: str) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
        }

    def get_account_numbers(self, access_token: str) -> list[dict[str, Any]]:
        url = self._url("/accounts/accountNumbers")
        with httpx.Client(timeout=30.0) as c:
            r = c.get(url, headers=self._headers(access_token))
        if r.status_code >= 400:
            raise SchwabAuthError(f"GET /accounts/accountNumbers failed ({r.status_code}): {r.text}", status_code=r.status_code)
        data = r.json()
        if not isinstance(data, list):
            raise SchwabAuthError("Unexpected response for /accounts/accountNumbers (expected list).", status_code=500)
        return data

    def get_accounts_positions(self, access_token: str) -> list[dict[str, Any]]:
        # GET /accounts?fields=positions
        url = self._url("/accounts")
        params = {"fields": "positions"}
        with httpx.Client(timeout=30.0) as c:
            r = c.get(url, headers=self._headers(access_token), params=params)
        if r.status_code >= 400:
            raise SchwabAuthError(f"GET /accounts?fields=positions failed ({r.status_code}): {r.text}", status_code=r.status_code)
        data = r.json()
        if not isinstance(data, list):
            raise SchwabAuthError("Unexpected response for /accounts (expected list).", status_code=500)
        return data

    def get_account_positions(self, access_token: str, account_hash: str) -> dict[str, Any]:
        # GET /accounts/{accountHash}?fields=positions
        hv = str(account_hash or "").strip()
        if not hv:
            raise SchwabAuthError("Missing account hash for per-account positions fetch.", status_code=400)
        url = self._url(f"/accounts/{hv}")
        params = {"fields": "positions"}
        with httpx.Client(timeout=30.0) as c:
            r = c.get(url, headers=self._headers(access_token), params=params)
        if r.status_code >= 400:
            raise SchwabAuthError(
                f"GET /accounts/{hv}?fields=positions failed ({r.status_code}): {r.text}",
                status_code=r.status_code,
            )
        data = r.json()
        if isinstance(data, dict):
            return data
        if isinstance(data, list) and len(data) == 1 and isinstance(data[0], dict):
            return data[0]
        raise SchwabAuthError(
            f"Unexpected response for /accounts/{hv} (expected object).",
            status_code=500,
        )

    def get_quotes(self, access_token: str, symbols: list[str]) -> dict[str, Any]:
        clean: list[str] = []
        seen: set[str] = set()
        for raw in symbols:
            sym = str(raw).strip().upper()
            if not sym or sym in seen:
                continue
            seen.add(sym)
            clean.append(sym)
        if not clean:
            return {}
        market_base = _normalize_market_base(self.base_url, str(os.getenv("SCHWAB_MARKET_DATA_BASE", "")).strip())
        url = market_base.rstrip("/") + "/quotes"

        def _fetch_batch(batch: list[str]) -> tuple[int, dict[str, Any]]:
            params = {
                "symbols": ",".join(batch),
                "fields": "quote,regular,extended,reference",
                "indicative": "false",
            }
            try:
                with httpx.Client(timeout=30.0) as c:
                    r = c.get(url, headers=self._headers(access_token), params=params)
            except Exception:
                return 599, {}
            if r.status_code >= 400:
                return int(r.status_code), {}
            try:
                data = r.json()
            except Exception:
                return int(r.status_code), {}
            return int(r.status_code), (data if isinstance(data, dict) else {})

        batch_size_raw = os.getenv("SCHWAB_QUOTES_BATCH_SIZE", "75").strip()
        try:
            batch_size = max(1, int(batch_size_raw))
        except Exception:
            batch_size = 75

        out: dict[str, Any] = {}
        fallback_syms: list[str] = []
        for i in range(0, len(clean), batch_size):
            batch = clean[i : i + batch_size]
            status, data = _fetch_batch(batch)
            if status in (400, 404):
                fallback_syms.extend(batch)
                continue
            if status >= 400:
                continue
            out.update(data)

        if fallback_syms:
            seen_fb: set[str] = set()
            for sym in fallback_syms:
                if sym in seen_fb:
                    continue
                seen_fb.add(sym)
                status, data = _fetch_batch([sym])
                if status >= 400:
                    continue
                out.update(data)
        return out


def fetch_portfolio_snapshot(*, token: dict[str, Any], token_write_func: Optional[callable] = None) -> dict[str, Any]:
    """
    Returns a raw snapshot dict used by the connector normalizer:
      {
        "accounts": [...],
        "accountNumbers": [...]
      }

    - Ensures/refreshes access token as needed.
    - Pulls accountNumbers (hash mapping) and accounts with positions.
    """
    trader_base = os.getenv("SCHWAB_TRADER_API_BASE", "").strip()
    if not trader_base:
        raise SchwabAuthError(
            "Missing SCHWAB_TRADER_API_BASE env var. "
            "Your portal docs show only relative endpoints; set the full Trader API base URL explicitly.",
            status_code=500,
        )

    access = _ensure_access_token(token, token_write_func=token_write_func)
    api = SchwabTraderREST(base_url=trader_base)

    account_numbers = api.get_account_numbers(access)
    try:
        accounts = api.get_accounts_positions(access)
    except SchwabAuthError as e:
        # Schwab Trader occasionally returns transient 5xx on the bulk endpoint.
        # Fallback: fetch positions per account hash so dashboard data still loads.
        status_code = int(getattr(e, "status_code", 0) or 0)
        if status_code < 500:
            raise

        account_hashes: list[str] = []
        seen_hashes: set[str] = set()
        for row in account_numbers:
            if not isinstance(row, dict):
                continue
            hv = ""
            for key in ("hashValue", "accountHash", "encryptedAccountNumber"):
                v = str(row.get(key) or "").strip()
                if v:
                    hv = v
                    break
            if not hv or hv in seen_hashes:
                continue
            seen_hashes.add(hv)
            account_hashes.append(hv)

        accounts = []
        fallback_errors: list[str] = []
        for hv in account_hashes:
            try:
                acct = api.get_account_positions(access, hv)
                if isinstance(acct, dict):
                    accounts.append(acct)
            except SchwabAuthError as per_acc_err:
                err_code = getattr(per_acc_err, "status_code", "?")
                fallback_errors.append(f"{hv}:{err_code}")
            except Exception:
                fallback_errors.append(f"{hv}:unexpected")

        if not accounts:
            detail = ""
            if fallback_errors:
                preview = ", ".join(fallback_errors[:3])
                if len(fallback_errors) > 3:
                    preview += f", ... ({len(fallback_errors)} total)"
                detail = f" Fallback per-account calls failed: {preview}"
            raise SchwabAuthError(f"{e}.{detail}".strip(), status_code=(status_code or 500))

    symbols: list[str] = []
    seen_symbols: set[str] = set()
    for account_obj in accounts:
        if not isinstance(account_obj, dict):
            continue
        sec = account_obj.get("securitiesAccount") if isinstance(account_obj.get("securitiesAccount"), dict) else account_obj
        pos_in = sec.get("positions")
        if not isinstance(pos_in, list):
            continue
        for p in pos_in:
            if not isinstance(p, dict):
                continue
            inst = p.get("instrument")
            inst = inst if isinstance(inst, dict) else {}
            sym = str(inst.get("symbol") or inst.get("cusip") or "").strip().upper()
            if not sym or sym in seen_symbols:
                continue
            seen_symbols.add(sym)
            symbols.append(sym)

    quotes = api.get_quotes(access, symbols)
    return {"accounts": accounts, "accountNumbers": account_numbers, "quotes": quotes}


def _schwab_position_quantity(position: dict[str, Any]) -> Optional[float]:
    long_qty = safe_float(position.get("longQuantity"))
    short_qty = safe_float(position.get("shortQuantity"))
    if long_qty is not None and abs(long_qty) > 0:
        return float(long_qty)
    if short_qty is not None and abs(short_qty) > 0:
        return -float(short_qty)
    qty = safe_float(position.get("quantity"))
    if qty is not None:
        return float(qty)
    return None


def _schwab_position_unrealized_pl(
    *,
    position: dict[str, Any],
    quantity: Optional[float],
    average_price: Optional[float],
    market_price: Optional[float],
    market_value: Optional[float],
) -> Optional[float]:
    # Prefer Schwab's open/unrealized P/L fields when available.
    for key in (
        "longOpenProfitLoss",
        "shortOpenProfitLoss",
        "openProfitLoss",
        "open_profit_loss",
        "unrealizedProfitLoss",
        "unrealized_profit_loss",
    ):
        val = safe_float(position.get(key))
        if val is not None:
            return float(val)

    # Fallback math: (qty * current) - (qty * average), with market_value preference.
    if market_value is not None and average_price is not None and quantity is not None:
        return float(market_value) - (float(average_price) * float(quantity))
    if market_price is not None and average_price is not None and quantity is not None:
        return (float(market_price) - float(average_price)) * float(quantity)
    return None


def _schwab_position_market_price(position: dict[str, Any]) -> Optional[float]:
    if not isinstance(position, dict):
        return None
    for key in (
        "marketPrice",
        "currentDayPrice",
        "currentPrice",
        "price",
        "markPrice",
        "mark",
        "lastPrice",
    ):
        val = safe_float(position.get(key))
        if val is not None and val > 0:
            return float(val)
    return None


def _normalize_market_base(trader_base: str, market_base: str) -> str:
    base = str(market_base or trader_base or "").strip()
    if not base:
        return ""
    if "/trader/" in base:
        base = base.replace("/trader/", "/marketdata/")
    if base.rstrip("/").endswith("/trader/v1"):
        base = base.rsplit("/trader/v1", 1)[0] + "/marketdata/v1"
    return base.rstrip("/")


def _normalize_symbol_key(symbol: str) -> str:
    return "".join(str(symbol or "").upper().split())


def _quote_for_symbol(quotes_map: dict[str, Any], symbol: str) -> dict[str, Any]:
    if not isinstance(quotes_map, dict):
        return {}
    raw = str(symbol or "").strip()
    if not raw:
        return {}
    for key in (raw, raw.upper(), _normalize_symbol_key(raw)):
        val = quotes_map.get(key)
        if isinstance(val, dict):
            return val
    wanted = _normalize_symbol_key(raw)
    for key, val in quotes_map.items():
        if not isinstance(key, str) or not isinstance(val, dict):
            continue
        if _normalize_symbol_key(key) == wanted:
            return val
    return {}


def _schwab_price_from_quote(quote_obj: dict[str, Any], *, prefer_extended: bool) -> Optional[float]:
    if not isinstance(quote_obj, dict):
        return None
    quote = quote_obj.get("quote") if isinstance(quote_obj.get("quote"), dict) else quote_obj
    regular = quote_obj.get("regular") if isinstance(quote_obj.get("regular"), dict) else {}
    last_trade = safe_float(quote.get("lastPrice")) or 0.0
    ext_last = safe_float(
        quote.get("lastExtendedHoursTradePrice")
        or quote.get("extendedHoursLastPrice")
        or quote.get("extendedHoursPrice")
    ) or 0.0
    mark_price = safe_float(
        quote.get("mark")
        or quote.get("markPrice")
        or quote.get("mark_price")
    ) or 0.0
    regular_last = safe_float(regular.get("regularMarketLastPrice")) if isinstance(regular, dict) else None
    if regular_last is None:
        regular_last = 0.0
    close_price = safe_float(quote.get("closePrice")) or 0.0
    ask_price = safe_float(quote.get("askPrice")) or 0.0
    bid_price = safe_float(quote.get("bidPrice")) or 0.0
    midpoint = 0.0
    if ask_price > 0 and bid_price > 0:
        midpoint = (ask_price + bid_price) / 2.0
    if prefer_extended:
        # Prefer off-hours-aware quote values before regular-session reference fields.
        candidates = (
            ext_last,
            mark_price,
            midpoint,
            last_trade,
            ask_price,
            bid_price,
            regular_last,
            close_price,
        )
    else:
        candidates = (regular_last, close_price, last_trade, mark_price, midpoint, ask_price, bid_price)
    for value in candidates:
        if value is not None and float(value) > 0:
            return float(value)
    return None


# -------------------------
# Connector
# -------------------------
class SchwabConnector(BrokerConnector):
    broker_id = "schwab"
    broker_name = "Schwab"

    # NOTE: Schwab linking is OAuth-based and is handled by /broker/connect + /callback in main.py.
    # This connector focuses on reading the linked token + fetching portfolio data.

    def link(self, *, db_path: str, label: str, **kwargs: Any) -> int:
        raise BrokerConnectorError(
            "Schwab linking is handled via OAuth. Use /broker/connect in the UI.",
            status_code=400,
        )

    def unlink(self, *, db_path: str, connection_id: int) -> None:
        """
        Remove the connection row and (optionally) delete the token file if it is file-backed.
        """
        conn = _db(db_path)
        cur = conn.cursor()
        cur.execute("SELECT id, metadata_json FROM broker_connections WHERE id=?", (int(connection_id),))
        row = cur.fetchone()

        token_path: Optional[Path] = None
        if row:
            meta = _safe_json(row["metadata_json"], default={})
            token_path = self._resolve_token_path(db_path=db_path, metadata=meta)

        cur.execute("DELETE FROM broker_connections WHERE id=?", (int(connection_id),))
        conn.commit()
        conn.close()

        try:
            if token_path and token_path.exists():
                token_path.unlink()
        except Exception:
            pass

    def portfolio_snapshot(self, *, db_path: str, connection_id: int) -> NormalizedPortfolioSnapshot:
        conn = _db(db_path)
        cur = conn.cursor()
        cur.execute("SELECT * FROM broker_connections WHERE id=?", (int(connection_id),))
        row = cur.fetchone()
        conn.close()

        if not row:
            raise BrokerConnectorError(f"Schwab connection not found (id {connection_id}).", status_code=404)

        label = str(row["label"] or "Schwab")
        status = str(row["status"] or "")
        metadata = _safe_json(row["metadata_json"], default={})
        secrets = read_connection_secrets(row, default={})

        # Allow recovery attempts from needs_auth/error states.
        if status not in ("connected", "needs_auth", "needs_attention", "error", "ok", ""):
            raise BrokerAuthError(f"Schwab connection status is '{status}'. Reconnect via /broker.", status_code=401)

        token_path = self._resolve_token_path(db_path=db_path, metadata=metadata)
        if not token_path.exists():
            raise BrokerAuthError("Schwab token not found. Connect Schwab in the Broker page.", status_code=401)

        token = self._read_token(token_path)
        if not token:
            raise BrokerAuthError("Schwab token file unreadable. Reconnect Schwab.", status_code=401)

        def _write_token(tok: dict[str, Any]) -> None:
            try:
                token_path.write_text(json.dumps(tok, indent=2), encoding="utf-8")
            except Exception:
                pass

        env_overlay = {
            "SCHWAB_CLIENT_ID": str(metadata.get("client_id") or "").strip(),
            "SCHWAB_CLIENT_SECRET": str(secrets.get("client_secret") or "").strip(),
            "SCHWAB_TRADER_API_BASE": str(metadata.get("trader_api_base") or "").strip(),
            "SCHWAB_MARKET_DATA_BASE": str(metadata.get("market_data_base") or "").strip(),
            "SCHWAB_ACCOUNT_HASH": str(metadata.get("account_hash") or "").strip(),
        }
        previous_env: dict[str, Optional[str]] = {}
        try:
            for key, value in env_overlay.items():
                previous_env[key] = os.environ.get(key)
                if value and not os.environ.get(key):
                    os.environ[key] = value
            raw_snap = fetch_portfolio_snapshot(token=token, token_write_func=_write_token)
        except SchwabAuthError as e:
            raise BrokerAuthError(str(e), status_code=getattr(e, "status_code", 401))
        except Exception as e:
            raise BrokerConnectorError(f"Schwab portfolio fetch failed: {e}")
        finally:
            for key, old_value in previous_env.items():
                if old_value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = old_value

        accounts: list[NormalizedAccount] = []
        quotes_map = raw_snap.get("quotes") if isinstance(raw_snap.get("quotes"), dict) else {}
        for a in (raw_snap.get("accounts") or []):
            if not isinstance(a, dict):
                continue

            # Schwab account responses typically have securitiesAccount wrapper
            sec = a.get("securitiesAccount") if isinstance(a.get("securitiesAccount"), dict) else a

            bal: dict[str, Any] = {}
            initial_bal = sec.get("initialBalances")
            current_bal = sec.get("currentBalances")
            projected_bal = sec.get("projectedBalances")
            generic_bal = sec.get("balances")
            if isinstance(initial_bal, dict):
                bal.update(initial_bal)
            if isinstance(current_bal, dict):
                bal.update(current_bal)
            if isinstance(projected_bal, dict):
                for k, v in projected_bal.items():
                    bal.setdefault(f"projected_{k}", v)
            if isinstance(generic_bal, dict):
                for k, v in generic_bal.items():
                    bal.setdefault(k, v)

            pos_in = sec.get("positions")
            positions: list[NormalizedPosition] = []
            if isinstance(pos_in, list):
                for p in pos_in:
                    if not isinstance(p, dict):
                        continue
                    inst = p.get("instrument")
                    inst = inst if isinstance(inst, dict) else {}
                    symbol = inst.get("symbol") or inst.get("cusip") or "—"
                    quote_obj = _quote_for_symbol(quotes_map, str(symbol))
                    qty = _schwab_position_quantity(p)
                    avg_price = safe_float(p.get("averagePrice") or p.get("avgPrice"))
                    quote_price = _schwab_price_from_quote(quote_obj, prefer_extended=True) if isinstance(quote_obj, dict) else None
                    market_price = quote_price if quote_price is not None else _schwab_position_market_price(p)
                    market_value = safe_float(p.get("marketValue"))
                    if market_price is None and market_value is not None and qty is not None and abs(float(qty)) > 1e-9:
                        market_price = float(market_value) / float(qty)
                    if qty is not None and market_price is not None and (quote_price is not None or market_value is None):
                        market_value = float(qty) * float(market_price)
                    if quote_price is not None and qty is not None and avg_price is not None:
                        unrealized_pl = (float(market_price) - float(avg_price)) * float(qty)
                    else:
                        unrealized_pl = _schwab_position_unrealized_pl(
                            position=p,
                            quantity=qty,
                            average_price=avg_price,
                            market_price=market_price,
                            market_value=market_value,
                        )

                    positions.append(
                        NormalizedPosition(
                            symbol=str(symbol),
                            quantity=qty,
                            average_price=avg_price,
                            market_price=market_price,
                            market_value=market_value,
                            unrealized_pl=unrealized_pl,
                            asset_type=str(inst.get("assetType") or ""),
                            raw={"position": p, "quote": quote_obj},
                        )
                    )

            accounts.append(
                NormalizedAccount(
                    account_id=str(sec.get("accountNumber", "—")),
                    account_type=str(sec.get("type", "")),
                    balances=bal,
                    positions=positions,
                    raw=a,
                )
            )

        return NormalizedPortfolioSnapshot(
            broker="schwab",
            label=label,
            connection_id=int(connection_id),
            accounts=accounts,
            raw=raw_snap,
        )

    # -------------------------
    # Internal helpers
    # -------------------------
    def _resolve_token_path(self, *, db_path: str, metadata: dict[str, Any]) -> Path:
        """
        Determine where the Schwab token lives.

        Priority:
        1) broker_connections.metadata_json.token_path
        2) <data_dir>/schwab_token.json where data_dir = parent directory of sqlite db_path
        """
        if isinstance(metadata, dict):
            tp = metadata.get("token_path")
            if tp:
                try:
                    return Path(str(tp)).expanduser().resolve()
                except Exception:
                    pass

        data_dir = Path(db_path).resolve().parent
        return (data_dir / "schwab_token.json").resolve()

    def _read_token(self, token_path: Path) -> Optional[dict[str, Any]]:
        try:
            return json.loads(token_path.read_text(encoding="utf-8"))
        except Exception:
            return None
