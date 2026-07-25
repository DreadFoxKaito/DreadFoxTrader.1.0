#!/usr/bin/env python3
"""
IndicatorForge.Schwab.py (Web-App Compatible)

Schwab-backed IndicatorForge stock engine that mirrors IndicatorForge.Robinhood
rule semantics while using Schwab Trader + Market Data APIs for execution.

Key behavior:
- IndicatorForge rule stack (SMA/EMA, optional derivative + unless override, RSI, dRSI, MACD)
- Rule-level RSI/MACD signal overrides by targeted rule IDs/names
- Strict signal resolution (all SELL checks pass -> SELL; else all BUY checks pass -> BUY; else HOLD)
- Schwab-native execution in regular session only:
  - selectable BUY/SELL order type: market, trailing-stop, or midpoint limit
- Optional stop-loss arming/trigger logic
- Hard no-loss rule on SELL paths

Required CLI args (expected by the web app launcher):
  --run-dir <path>
  --params-json <path>
  --db-path <path_to_sqlite>
  --connection-id <int>   (Schwab broker_connections.id)
"""

from __future__ import annotations

import argparse
import base64
import json
import math
import os
import sys
import time
import traceback
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import httpx
import numpy as np

# -------------------------
# Ensure project imports work when executed as a script file
# -------------------------
THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.db import (  # noqa: E402
    get_broker_connection,
    list_broker_connections,
    read_connection_metadata,
    set_broker_status,
)
from app.schwab_history import fetch_price_history_with_min_candles  # noqa: E402

# -----------------------------
# Globals
# -----------------------------
stoploss_state: Dict[str, Dict[str, Any]] = {}
_TRADER: Optional["SchwabTraderClient"] = None
_MARKET: Optional["SchwabMarketDataClient"] = None
_ACCOUNT_HASH: Optional[str] = None
_ACCOUNT_NUMBER: Optional[str] = None

CHART_POINTS = 90
ATR_PERIOD = 14
MIN_TRAIL_AMOUNT_USD = 0.01
NON_PDT_DAY_TRADE_LIMIT = 3

MARKET_STATE_TTL = 60
_MARKET_STATE_CACHE: Dict[str, Any] = {"ts": 0, "state": "closed", "session_tag": "NORMAL"}
_SEAMLESS_UNSUPPORTED_SYMBOLS: set[str] = set()
ORDER_TYPE_CHOICES = {"market", "trailing_stop", "limit_midpoint"}
_ET_TZ = ZoneInfo("America/New_York")

# -----------------------------
# Schwab schema enums (subset needed for orders)
# -----------------------------
SESSION_ENUM = {"NORMAL", "AM", "PM", "SEAMLESS"}
DURATION_ENUM = {
    "DAY",
    "GOOD_TILL_CANCEL",
    "FILL_OR_KILL",
    "IMMEDIATE_OR_CANCEL",
    "END_OF_WEEK",
    "END_OF_MONTH",
    "NEXT_END_OF_MONTH",
}
ORDER_TYPE_ENUM = {
    "MARKET",
    "LIMIT",
    "STOP",
    "STOP_LIMIT",
    "TRAILING_STOP",
    "CABINET",
    "NON_MARKETABLE",
    "MARKET_ON_CLOSE",
    "EXERCISE",
    "TRAILING_STOP_LIMIT",
    "NET_DEBIT",
    "NET_CREDIT",
    "NET_ZERO",
    "LIMIT_ON_CLOSE",
}
ORDER_STRATEGY_TYPE_ENUM = {"SINGLE", "CANCEL", "RECALL", "PAIR", "FLATTEN", "TWO_DAY_SWAP", "BLAST_ALL", "OCO", "TRIGGER"}
STOP_PRICE_LINK_BASIS_ENUM = {"MANUAL", "BASE", "TRIGGER", "LAST", "BID", "ASK", "ASK_BID", "MARK", "AVERAGE"}
STOP_PRICE_LINK_TYPE_ENUM = {"VALUE", "PERCENT", "TICK"}
STOP_TYPE_ENUM = {"STANDARD", "BID", "ASK", "LAST", "MARK"}
INSTRUCTION_ENUM = {
    "BUY",
    "SELL",
    "BUY_TO_COVER",
    "SELL_SHORT",
    "BUY_TO_OPEN",
    "BUY_TO_CLOSE",
    "SELL_TO_OPEN",
    "SELL_TO_CLOSE",
    "EXCHANGE",
    "SELL_SHORT_EXEMPT",
}

# -----------------------------
# Timeframe config (Schwab PriceHistory)
# -----------------------------
TIMEFRAMES: Dict[str, Dict[str, Any]] = {
    # Mirror Robinhood's stock historical windows as closely as Schwab permits.
    # Robinhood: 5m/10m -> week, 30m -> month, 1h -> 3month, 1d -> year.
    # Schwab fetches regular-hours candles first, then optional same-day extended merge.
    "1m": {
        "periodType": "day",
        "period": 10,
        "frequencyType": "minute",
        "frequency": 1,
        "target_bars": 390,
        "fetch_scale": 1,
        "aggregate_hourly": False,
    },
    "5m": {
        "periodType": "day",
        "period": 10,
        "frequencyType": "minute",
        "frequency": 5,
        "target_bars": 390,
        "fetch_scale": 1,
        "aggregate_hourly": False,
    },
    "10m": {
        "periodType": "day",
        "period": 10,
        "frequencyType": "minute",
        "frequency": 10,
        "target_bars": 195,
        "fetch_scale": 1,
        "aggregate_hourly": False,
    },
    "15m": {
        "periodType": "day",
        "period": 10,
        "frequencyType": "minute",
        "frequency": 15,
        "target_bars": 130,
        "fetch_scale": 1,
        "aggregate_hourly": False,
    },
    "30m": {
        "periodType": "day",
        "period": 10,
        "frequencyType": "minute",
        "frequency": 30,
        "target_bars": 273,
        "fetch_scale": 1,
        "aggregate_hourly": False,
    },
    "1h": {
        "periodType": "day",
        "period": 10,
        "frequencyType": "minute",
        "frequency": 15,
        "target_bars": 409,
        "fetch_scale": 4,
        "aggregate_hourly": True,
    },
    "1d": {
        "periodType": "year",
        "period": 1,
        "frequencyType": "daily",
        "frequency": 1,
        "target_bars": 252,
        "fetch_scale": 1,
        "aggregate_hourly": False,
    },
}


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_enum(value: Any, allowed: set[str], default: str) -> str:
    text = str(value or "").strip().upper()
    if text in allowed:
        return text
    return default


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        if val in (None, "None", ""):
            return default
        return float(val)
    except Exception:
        return default


def _to_float_opt(val: Any) -> Optional[float]:
    try:
        if val in (None, "None", ""):
            return None
        return float(val)
    except Exception:
        return None


def _to_int_opt(val: Any) -> Optional[int]:
    try:
        if val in (None, "None", ""):
            return None
        return int(float(val))
    except Exception:
        return None


_MA_RIBBON_LEVEL_ORDER = ("short", "medium", "long")
_MA_RIBBON_DEFAULT_LENGTHS = {"short": 30, "medium": 78, "long": 190}


def _normalize_ma_mode(value: Any, default: str = "single") -> str:
    txt = str(value or default or "single").strip().lower()
    if txt in ("ribbon", "ma_ribbon"):
        return "ribbon"
    if txt in ("mapped", "level", "action_map", "ribbon_level"):
        return "mapped"
    return "single"


def _normalize_ma_type(value: Any, default: str = "sma") -> str:
    txt = str(value or default or "sma").strip().lower()
    return "ema" if txt == "ema" else "sma"


def _normalize_ma_action(value: Any, default: str = "hold") -> str:
    txt = str(value or default or "hold").strip().lower()
    if txt in ("buy", "sell"):
        return txt
    return "hold"


def _ma_ribbon_levels(params: Dict[str, Any]) -> List[Dict[str, Any]]:
    cfg = params if isinstance(params, dict) else {}
    levels_raw = cfg.get("levels")
    levels_by_slot: Dict[str, Dict[str, Any]] = {}
    if isinstance(levels_raw, list):
        for raw in levels_raw:
            if not isinstance(raw, dict):
                continue
            slot = str(raw.get("slot") or raw.get("name") or "").strip().lower()
            if slot in _MA_RIBBON_LEVEL_ORDER and slot not in levels_by_slot:
                levels_by_slot[slot] = raw

    def _pick(raw_value: Any, *fallbacks: Any) -> Any:
        if raw_value not in (None, "", "None"):
            return raw_value
        for item in fallbacks:
            if item not in (None, "", "None"):
                return item
        return None

    out: List[Dict[str, Any]] = []
    for slot in _MA_RIBBON_LEVEL_ORDER:
        raw = levels_by_slot.get(slot, {})
        raw_type = raw.get("ma_type") if isinstance(raw, dict) else None
        raw_length = raw.get("length") if isinstance(raw, dict) else None
        raw_above = raw.get("above_action") if isinstance(raw, dict) else None
        raw_below = raw.get("below_action") if isinstance(raw, dict) else None
        out.append(
            {
                "slot": slot,
                "label": slot.title(),
                "ma_type": _normalize_ma_type(
                    _pick(raw_type, cfg.get(f"{slot}_type"), cfg.get(f"ribbon_{slot}_type")),
                    "sma",
                ),
                "length": max(
                    2,
                    int(
                        _to_int_opt(
                            _pick(raw_length, cfg.get(f"{slot}_length"), cfg.get(f"ribbon_{slot}_length"))
                        )
                        or _MA_RIBBON_DEFAULT_LENGTHS[slot]
                    ),
                ),
                "above_action": _normalize_ma_action(
                    _pick(raw_above, cfg.get(f"{slot}_above_action"), cfg.get(f"ribbon_{slot}_above_action")),
                    "hold",
                ),
                "below_action": _normalize_ma_action(
                    _pick(raw_below, cfg.get(f"{slot}_below_action"), cfg.get(f"ribbon_{slot}_below_action")),
                    "hold",
                ),
            }
        )
    return out


def _ma_ribbon_level_rule_id(parent_rule_id: Any, slot: Any) -> str:
    rid = str(parent_rule_id or "").strip()
    slot_key = str(slot or "").strip().lower()
    if not rid or slot_key not in _MA_RIBBON_LEVEL_ORDER:
        return ""
    return f"{rid}::{slot_key}"


def _ma_ribbon_level_name(parent_name: Any, level: Dict[str, Any]) -> str:
    base_name = str(parent_name or "MA Ribbon").strip() or "MA Ribbon"
    slot = str(level.get("slot") or "").strip().lower()
    label = str(level.get("label") or slot.title()).strip() or "Level"
    ma_type = str(level.get("ma_type") or "sma").strip().lower()
    line_tag = "EMA" if ma_type == "ema" else "SMA"
    length = max(2, int(_to_int_opt(level.get("length")) or _MA_RIBBON_DEFAULT_LENGTHS.get(slot, 30)))
    return f"{base_name} - {label} {line_tag}{length}"


def _to_bool(val: Any, default: bool = False) -> bool:
    if isinstance(val, bool):
        return val
    if val is None:
        return default
    txt = str(val).strip().lower()
    if txt in ("1", "true", "yes", "y", "on"):
        return True
    if txt in ("0", "false", "no", "n", "off", ""):
        return False
    return default


def _parse_symbol_cap_map(raw: Any) -> Dict[str, float]:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return {}
    if not isinstance(raw, dict):
        return {}
    out: Dict[str, float] = {}
    for key, val in raw.items():
        sym = str(key or "").strip().upper()
        if not sym:
            continue
        try:
            pct = float(val)
        except Exception:
            continue
        if pct > 0:
            out[sym] = pct
    return out


def _normalize_order_type(val: Any, default: str = "market") -> str:
    txt = str(val or "").strip().lower()
    if txt in ORDER_TYPE_CHOICES:
        return txt
    if txt in ("trail", "trailing", "trailingstop"):
        return "trailing_stop"
    if txt in ("limit", "mid", "midpoint", "limit_mid"):
        return "limit_midpoint"
    d = str(default or "market").strip().lower()
    return d if d in ORDER_TYPE_CHOICES else "market"


def _fmt(val: Any, digits: int = 4) -> str:
    try:
        if val is None:
            return "—"
        return f"{float(val):.{digits}f}"
    except Exception:
        return "—"


def _mid_price(bid: float, ask: float, fallback: float) -> float:
    if bid > 0 and ask > 0:
        return (bid + ask) / 2.0
    if bid > 0:
        return bid
    if ask > 0:
        return ask
    return fallback


# -----------------------------
# Schwab OAuth helpers
# -----------------------------
class SchwabAuthError(RuntimeError):
    pass


def _basic_auth_header(client_id: str, client_secret: str) -> str:
    raw = f"{client_id}:{client_secret}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def _parse_obtained_at(value: Any) -> Optional[int]:
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str) and value:
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return int(dt.timestamp())
        except Exception:
            return None
    return None


def _token_is_fresh(token: Dict[str, Any], early_refresh_s: int = 60) -> bool:
    try:
        access = str(token.get("access_token") or "")
        expires_in = int(token.get("expires_in") or 0)
        obtained_ts = _parse_obtained_at(token.get("obtained_at"))
        if not access or expires_in <= 0 or not obtained_ts:
            return False
        return int(time.time()) < (obtained_ts + expires_in - early_refresh_s)
    except Exception:
        return False


def _refresh_access_token(token: Dict[str, Any]) -> Dict[str, Any]:
    client_id = os.getenv("SCHWAB_CLIENT_ID", "").strip()
    client_secret = os.getenv("SCHWAB_CLIENT_SECRET", "").strip()
    if not client_id or not client_secret:
        raise SchwabAuthError("Missing SCHWAB_CLIENT_ID / SCHWAB_CLIENT_SECRET env vars.")

    refresh_token = str(token.get("refresh_token") or "").strip()
    if not refresh_token:
        raise SchwabAuthError("Token missing refresh_token. Reconnect Schwab.")

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
        raise SchwabAuthError(f"Token refresh failed ({r.status_code}): {r.text}")

    new_tok = r.json()
    new_tok["obtained_at"] = int(time.time())
    if not new_tok.get("refresh_token"):
        new_tok["refresh_token"] = refresh_token
    return new_tok


class SchwabOAuth:
    def __init__(self, *, token_path: Path):
        self.token_path = token_path

    def load_token(self) -> Optional[Dict[str, Any]]:
        try:
            return json.loads(self.token_path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def save_token(self, tok: Dict[str, Any]) -> None:
        self.token_path.parent.mkdir(parents=True, exist_ok=True)
        self.token_path.write_text(json.dumps(tok, indent=2), encoding="utf-8")

    def ensure_access_token(self) -> str:
        tok = self.load_token()
        if not tok:
            raise SchwabAuthError(f"No token file found at {self.token_path}")

        if _token_is_fresh(tok):
            return str(tok.get("access_token") or "")

        new_tok = _refresh_access_token(tok)
        self.save_token(new_tok)
        access = str(new_tok.get("access_token") or "")
        if not access:
            raise SchwabAuthError("Refresh succeeded but access_token missing in response.")
        return access


# -----------------------------
# REST clients
# -----------------------------
class SchwabTraderError(RuntimeError):
    pass


class SchwabTraderClient:
    def __init__(self, *, base_url: str, oauth: SchwabOAuth):
        self.base_url = base_url.rstrip("/")
        self.oauth = oauth

    def _headers(self) -> Dict[str, str]:
        access = self.oauth.ensure_access_token()
        return {
            "Authorization": f"Bearer {access}",
            "Accept": "application/json",
        }

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return self.base_url + path

    def get_account_numbers(self) -> List[Dict[str, Any]]:
        url = self._url("/accounts/accountNumbers")

        def _call():
            with httpx.Client(timeout=30.0) as c:
                r = c.get(url, headers=self._headers())
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, list):
                raise SchwabTraderError("Unexpected response for /accounts/accountNumbers")
            return data

        return safe_call(_call, name="accountNumbers")

    def get_accounts(self) -> List[Dict[str, Any]]:
        url = self._url("/accounts")
        params = {"fields": "positions"}

        def _call():
            with httpx.Client(timeout=30.0) as c:
                r = c.get(url, headers=self._headers(), params=params)
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, list):
                raise SchwabTraderError("Unexpected response for /accounts")
            return data

        return safe_call(_call, name="accounts")

    def place_order(self, *, encrypted_account_number: str, order_obj: Dict[str, Any]) -> httpx.Response:
        url = self._url(f"/accounts/{encrypted_account_number}/orders")
        headers = self._headers()
        headers["Content-Type"] = "application/json"

        def _call():
            with httpx.Client(timeout=30.0) as c:
                r = c.post(url, headers=headers, json=order_obj)
            r.raise_for_status()
            return r

        return safe_call(_call, name="place_order")


class SchwabMarketDataClient:
    def __init__(self, *, base_url: str, oauth: SchwabOAuth):
        self.base_url = base_url.rstrip("/")
        self.oauth = oauth

    def _headers(self) -> Dict[str, str]:
        access = self.oauth.ensure_access_token()
        return {
            "Authorization": f"Bearer {access}",
            "Accept": "application/json",
        }

    def _url(self, path: str) -> str:
        if not path.startswith("/"):
            path = "/" + path
        return self.base_url + path

    def get_quotes(self, symbols: List[str]) -> Dict[str, Any]:
        url = self._url("/quotes")
        params = {
            "symbols": ",".join(symbols),
            "fields": "quote,regular,extended,reference",
            "indicative": "false",
        }

        def _call():
            with httpx.Client(timeout=30.0) as c:
                r = c.get(url, headers=self._headers(), params=params)
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, dict):
                raise SchwabTraderError("Unexpected response for /quotes")
            return data

        return safe_call(_call, name="quotes")

    def get_price_history(
        self,
        *,
        symbol: str,
        period_type: str,
        period: int,
        frequency_type: str,
        frequency: int,
        need_extended: bool,
        start_date_ms: Optional[int] = None,
        end_date_ms: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        url = self._url("/pricehistory")
        params = {
            "symbol": symbol,
            "periodType": period_type,
            "period": int(period),
            "frequencyType": frequency_type,
            "frequency": int(frequency),
            "needExtendedHoursData": str(bool(need_extended)).lower(),
        }
        if start_date_ms is not None:
            params["startDate"] = int(start_date_ms)
        if end_date_ms is not None:
            params["endDate"] = int(end_date_ms)

        def _call():
            with httpx.Client(timeout=30.0) as c:
                r = c.get(url, headers=self._headers(), params=params)
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, dict):
                raise SchwabTraderError("Unexpected response for /pricehistory")
            candles = data.get("candles") or []
            if not isinstance(candles, list):
                return []
            return candles

        return safe_call(_call, name=f"pricehistory({symbol})")

    def get_market_hours(self, *, market_id: str, date_str: str) -> Dict[str, Any]:
        url = self._url(f"/markets/{market_id}")
        params = {"date": date_str}

        def _call():
            with httpx.Client(timeout=30.0) as c:
                r = c.get(url, headers=self._headers(), params=params)
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, dict):
                raise SchwabTraderError("Unexpected response for /markets")
            return data

        return safe_call(_call, name="market_hours")


# -----------------------------
# Retry helpers
# -----------------------------
def _status_code(exc: Exception) -> Optional[int]:
    return getattr(getattr(exc, "response", None), "status_code", None)


def _error_text(exc: Exception) -> str:
    resp = getattr(exc, "response", None)
    try:
        if resp is not None:
            txt = str(getattr(resp, "text", "") or "")
            if txt:
                return txt
    except Exception:
        pass
    return str(exc)


def _is_session_rejection(exc: Exception) -> bool:
    status = _status_code(exc)
    if status not in (400, 403, 422):
        return False
    txt = _error_text(exc).lower()
    hints = (
        "session",
        "seamless",
        "after hours",
        "extended",
        "overnight",
        "outside",
        "not allowed",
        "not available",
        "invalid session",
        "market is closed",
    )
    return any(h in txt for h in hints)


def safe_sleep(seconds: float) -> None:
    time.sleep(max(0.0, float(seconds)))


def safe_call(fn, retries: int = 3, backoff: float = 0.8, name: str = "call"):
    for attempt in range(retries):
        try:
            return fn()
        except httpx.HTTPStatusError as e:
            status = _status_code(e)
            if status in (429, 500, 502, 503) and attempt < retries - 1:
                safe_sleep(backoff * (2**attempt))
                continue
            raise
        except httpx.RequestError:
            if attempt < retries - 1:
                safe_sleep(backoff * (2**attempt))
                continue
            raise
        except Exception:
            if attempt < retries - 1:
                safe_sleep(backoff * (2**attempt))
                continue
            raise


# -----------------------------
# Schwab session + account helpers
# -----------------------------
def _resolve_db_path(db_path: Optional[str]) -> Path:
    if db_path:
        return Path(db_path).expanduser().resolve()
    return (PROJECT_ROOT / "app" / "data" / "cryptid_exchange.sqlite3").resolve()


def _resolve_token_path(*, db_path: Path, metadata: Dict[str, Any]) -> Path:
    tp = metadata.get("token_path") if isinstance(metadata, dict) else None
    if tp:
        try:
            return Path(str(tp)).expanduser().resolve()
        except Exception:
            pass
    return (db_path.parent / "schwab_token.json").resolve()


def _resolve_connection_id(db_path: str, connection_id: Optional[int]) -> int:
    if connection_id is not None:
        return int(connection_id)
    rows = list_broker_connections(db_path)
    for r in rows:
        if str(r["broker"]) != "schwab":
            continue
        status = str(r["status"] or "")
        if status in ("connected", "ok", ""):
            return int(r["id"])
    raise RuntimeError("No linked Schwab connections found. Link via the Broker page.")


def _set_connection_error(
    *, db_path: str, connection_id: int, status: str, metadata: Dict[str, Any], error: str
) -> None:
    meta = dict(metadata or {})
    meta["error"] = error
    set_broker_status(db_path=db_path, connection_id=connection_id, status=status, metadata=meta)


def _resolve_account_hash(
    trader: SchwabTraderClient,
    *,
    explicit_hash: Optional[str],
    account_index: int,
) -> Tuple[str, Optional[str]]:
    accounts = trader.get_account_numbers()
    if not accounts:
        raise SchwabTraderError("No accounts returned from /accounts/accountNumbers.")

    if explicit_hash:
        for a in accounts:
            if str(a.get("hashValue") or "") == explicit_hash:
                return explicit_hash, str(a.get("accountNumber") or "") or None
        return explicit_hash, None

    if account_index < 0 or account_index >= len(accounts):
        raise SchwabTraderError(f"account_index={account_index} out of range; got {len(accounts)} accounts.")

    hv = accounts[account_index].get("hashValue")
    if not hv:
        raise SchwabTraderError("hashValue missing in /accounts/accountNumbers response.")
    return str(hv), str(accounts[account_index].get("accountNumber") or "") or None


def _normalize_market_base(trader_base: str, market_base: str) -> str:
    base = market_base or trader_base
    if "/trader/" in base:
        base = base.replace("/trader/", "/marketdata/")
    if base.rstrip("/").endswith("/trader/v1"):
        base = base.rsplit("/trader/v1", 1)[0] + "/marketdata/v1"
    return base


def _require_trader() -> SchwabTraderClient:
    if _TRADER is None:
        raise RuntimeError("Schwab trader client not initialized.")
    return _TRADER


def _require_market() -> SchwabMarketDataClient:
    if _MARKET is None:
        raise RuntimeError("Schwab market data client not initialized.")
    return _MARKET


def _require_account_hash() -> str:
    if not _ACCOUNT_HASH:
        raise RuntimeError("Schwab account hash not initialized.")
    return _ACCOUNT_HASH


def _pick_balance(securities_account: Dict[str, Any], *keys: str) -> float:
    cb = securities_account.get("currentBalances") or {}
    ib = securities_account.get("initialBalances") or {}
    for key in keys:
        if key in cb and cb[key] is not None:
            return _safe_float(cb[key], default=0.0)
    for key in keys:
        if key in ib and ib[key] is not None:
            return _safe_float(ib[key], default=0.0)
    return 0.0


def _pick_equity(securities_account: Dict[str, Any]) -> float:
    eq = _pick_balance(securities_account, "liquidationValue", "equity")
    if eq > 0:
        return eq
    cash = _pick_balance(securities_account, "cashBalance", "cashAvailableForTrading")
    long_value = _pick_balance(securities_account, "longMarketValue", "longStockValue")
    return cash + long_value


def _pick_buying_power(securities_account: Dict[str, Any]) -> float:
    return _pick_balance(
        securities_account,
        "buyingPower",
        "availableFunds",
        "cashAvailableForTrading",
        "cashBalance",
    )


def _pick_available_cash(securities_account: Dict[str, Any]) -> float:
    return _pick_balance(
        securities_account,
        "availableFunds",
        "cashAvailableForTrading",
        "cashBalance",
    )


def _day_trade_metrics(securities_account: Dict[str, Any]) -> Dict[str, Any]:
    used_opt = _to_int_opt(securities_account.get("roundTrips"))
    used = max(0, int(used_opt)) if used_opt is not None else None
    is_day_trader = _to_bool(securities_account.get("isDayTrader"), False)
    remaining: Optional[int] = None
    if used is not None and not is_day_trader:
        remaining = max(0, int(NON_PDT_DAY_TRADE_LIMIT) - int(used))
    return {
        "round_trips_used": used,
        "remaining": remaining,
        "is_day_trader": bool(is_day_trader),
    }


def _select_account(accounts: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not accounts:
        return None
    if _ACCOUNT_NUMBER:
        for acct in accounts:
            sec = acct.get("securitiesAccount") or acct
            if not isinstance(sec, dict):
                continue
            if str(sec.get("accountNumber") or "") == _ACCOUNT_NUMBER:
                return sec
    sec = accounts[0].get("securitiesAccount") or accounts[0]
    return sec if isinstance(sec, dict) else None


def get_account_snapshot() -> Dict[str, Any]:
    trader = _require_trader()
    accounts = trader.get_accounts()
    sec = _select_account(accounts)
    if not sec:
        raise RuntimeError("No Schwab account data available.")
    return sec


def _instrument_type(inst: Dict[str, Any]) -> str:
    return str(inst.get("assetType") or inst.get("type") or "").upper()


def _is_option(inst: Dict[str, Any]) -> bool:
    return "OPTION" in _instrument_type(inst)


def get_open_stock_positions(account: Dict[str, Any]) -> List[Dict[str, Any]]:
    positions = account.get("positions") or []
    if not isinstance(positions, list):
        return []
    out: List[Dict[str, Any]] = []
    for p in positions:
        inst = p.get("instrument") or {}
        if _is_option(inst):
            continue
        out.append(p)
    return out


def build_positions_map(positions: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    pos_map: Dict[str, Dict[str, float]] = {}
    for pos in positions:
        inst = pos.get("instrument") or {}
        symbol = str(inst.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        qty = _safe_float(pos.get("longQuantity"), default=0.0)
        if qty <= 0:
            qty = _safe_float(pos.get("quantity"), default=0.0)
        avg_buy = _safe_float(
            pos.get("averagePrice")
            or pos.get("averageLongPrice")
            or pos.get("taxLotAverageLongPrice"),
            default=0.0,
        )
        pos_map[symbol] = {"quantity": qty, "average_buy_price": avg_buy}
    return pos_map


# -----------------------------
# Market hours + quotes helpers
# -----------------------------
def _parse_iso_ts(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        text = str(value).strip()
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _market_session_state() -> str:
    now = time.time()
    if now - float(_MARKET_STATE_CACHE.get("ts", 0)) < MARKET_STATE_TTL:
        return str(_MARKET_STATE_CACHE.get("state", "closed"))

    state = "closed"
    session_tag = "NORMAL"
    try:
        market = _require_market()
        today = datetime.now(timezone.utc).date().isoformat()
        data = market.get_market_hours(market_id="equity", date_str=today)
        hours = None
        if isinstance(data, dict):
            for _k, v in data.items():
                if isinstance(v, dict):
                    for _k2, v2 in v.items():
                        if isinstance(v2, dict) and "sessionHours" in v2:
                            hours = v2
                            break
                if hours:
                    break
        now_dt = datetime.now(timezone.utc)
        if hours:
            sessions = hours.get("sessionHours") or {}
            pre = sessions.get("preMarket") or []
            reg = sessions.get("regularMarket") or []
            post = sessions.get("postMarket") or []

            def _in_session(ranges: List[Dict[str, Any]]) -> bool:
                for r in ranges:
                    start = _parse_iso_ts(r.get("start"))
                    end = _parse_iso_ts(r.get("end"))
                    if start and end and start <= now_dt <= end:
                        return True
                return False

            if _in_session(reg):
                state = "regular"
                session_tag = "NORMAL"
            elif _in_session(pre):
                state = "extended"
                session_tag = "AM"
            elif _in_session(post):
                state = "extended"
                session_tag = "PM"
            else:
                state = "closed"
                session_tag = "NORMAL"
    except Exception:
        state = "closed"
        session_tag = "NORMAL"

    _MARKET_STATE_CACHE["ts"] = now
    _MARKET_STATE_CACHE["state"] = state
    _MARKET_STATE_CACHE["session_tag"] = session_tag
    return state


def _order_session_for_state(state: str) -> str:
    if state == "regular":
        session = "NORMAL"
    elif state == "extended":
        session = str(_MARKET_STATE_CACHE.get("session_tag") or "SEAMLESS")
    elif state == "overnight":
        session = "SEAMLESS"
    else:
        session = "NORMAL"
    return _normalize_enum(session, SESSION_ENUM, "NORMAL")


def _execution_state_from_market(
    market_state: str,
    *,
    allow_extended_hours_orders: bool,
    allow_seamless_overnight_orders: bool,
) -> str:
    def _is_overnight_et_window() -> bool:
        try:
            now_et = datetime.now(timezone.utc).astimezone(_ET_TZ)
            minute_of_day = int(now_et.hour) * 60 + int(now_et.minute)
            return minute_of_day >= (20 * 60) or minute_of_day < (4 * 60)
        except Exception:
            return False

    state = str(market_state or "closed").strip().lower()
    if state == "regular":
        return "regular"
    if state == "extended":
        # Schwab market-hours responses can occasionally remain in extended state
        # while the execution window should be overnight routing.
        if allow_seamless_overnight_orders and _is_overnight_et_window():
            return "overnight"
        return "extended"
    if state == "closed" and allow_seamless_overnight_orders:
        # Treat closed windows as overnight-tradable when SEAMLESS is enabled.
        return "overnight"
    return state


def _price_from_quote(quote_obj: Dict[str, Any], *, prefer_extended: bool) -> Optional[float]:
    if not isinstance(quote_obj, dict):
        return None
    quote = quote_obj.get("quote") or quote_obj
    regular = quote_obj.get("regular") or {}
    extended = quote_obj.get("extended") if isinstance(quote_obj.get("extended"), dict) else {}
    last_trade = _safe_float(quote.get("lastPrice"), default=0.0)
    ext_last = _safe_float(
        quote.get("lastExtendedHoursTradePrice")
        or quote.get("extendedHoursLastPrice")
        or quote.get("extendedHoursPrice")
        or extended.get("lastExtendedHoursTradePrice")
        or extended.get("extendedHoursLastPrice")
        or extended.get("extendedHoursPrice"),
        default=0.0,
    )
    mark_price = _safe_float(
        quote.get("mark")
        or quote.get("markPrice")
        or quote.get("mark_price")
        or extended.get("mark"),
        default=0.0,
    )
    regular_last = _safe_float(regular.get("regularMarketLastPrice"), default=0.0) if isinstance(regular, dict) else 0.0
    close_price = _safe_float(quote.get("closePrice"), default=0.0)
    ask_price = _safe_float(quote.get("askPrice"), default=0.0)
    bid_price = _safe_float(quote.get("bidPrice"), default=0.0)
    midpoint = (ask_price + bid_price) / 2.0 if ask_price > 0 and bid_price > 0 else 0.0

    # Align price preference with market-hours intent:
    # - Regular-hours logic should favor regular session last.
    # - Extended-hours logic can favor last trade first.
    if prefer_extended:
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

    for val in candidates:
        if val > 0:
            return val
    return None


def get_quotes_map(symbols: List[str]) -> Dict[str, Any]:
    if not symbols:
        return {}
    market = _require_market()
    return market.get_quotes(symbols)


def _get_quote_for_symbol(quotes: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    if symbol in quotes:
        return quotes[symbol]
    if symbol.upper() in quotes:
        return quotes[symbol.upper()]
    return {}


def get_price_history(
    symbol: str,
    *,
    period_type: str,
    period: int,
    frequency_type: str,
    frequency: int,
    need_extended: bool,
    start_date_ms: Optional[int] = None,
    end_date_ms: Optional[int] = None,
) -> List[Dict[str, Any]]:
    market = _require_market()
    return market.get_price_history(
        symbol=symbol,
        period_type=period_type,
        period=period,
        frequency_type=frequency_type,
        frequency=frequency,
        need_extended=need_extended,
        start_date_ms=start_date_ms,
        end_date_ms=end_date_ms,
    )


# -----------------------------
# Indicator helpers
# -----------------------------
def _ma_value(prices: List[float], window: int) -> Optional[float]:
    if len(prices) < window:
        return None
    vals = prices[-window:]
    return float(sum(vals) / float(window))


def _ema_series(prices: List[float], window: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(prices)
    if window < 2 or len(prices) < window:
        return out
    seed = sum(float(x) for x in prices[:window]) / float(window)
    out[window - 1] = seed
    alpha = 2.0 / (float(window) + 1.0)
    prev = float(seed)
    for i in range(window, len(prices)):
        prev = (float(prices[i]) * alpha) + (prev * (1.0 - alpha))
        out[i] = prev
    return out


def _ema_value(prices: List[float], window: int) -> Optional[float]:
    s = _ema_series(prices, window)
    return s[-1] if s else None


def _line_value(prices: List[float], *, ma_type: str, length: int) -> Optional[float]:
    return _ema_value(prices, length) if str(ma_type).lower() == "ema" else _ma_value(prices, length)


def _line_derivative(prices: List[float], *, ma_type: str, length: int) -> Optional[float]:
    if len(prices) < length + 1:
        return None
    prev = _line_value(prices[:-1], ma_type=ma_type, length=length)
    now = _line_value(prices, ma_type=ma_type, length=length)
    if prev is None or now is None:
        return None
    return float(now - prev)


def _rsi(prices: List[float], period: int = 14) -> Optional[float]:
    if len(prices) < period + 1:
        return None
    gains = 0.0
    losses = 0.0
    for i in range(1, period + 1):
        d = float(prices[i] - prices[i - 1])
        if d >= 0:
            gains += d
        else:
            losses += -d
    avg_gain = gains / float(period)
    avg_loss = losses / float(period)
    for i in range(period + 1, len(prices)):
        d = float(prices[i] - prices[i - 1])
        gain = d if d > 0 else 0.0
        loss = -d if d < 0 else 0.0
        avg_gain = ((avg_gain * (period - 1.0)) + gain) / float(period)
        avg_loss = ((avg_loss * (period - 1.0)) + loss) / float(period)
    if avg_loss == 0.0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100.0 - (100.0 / (1.0 + rs)))


def _rsi_derivative(prices: List[float], period: int = 14) -> Optional[float]:
    if len(prices) < period + 2:
        return None
    r0 = _rsi(prices, period)
    r1 = _rsi(prices[:-1], period)
    if r0 is None or r1 is None:
        return None
    return float(r0 - r1)


def _macd_series(
    prices: List[float], *, fast_len: int = 12, slow_len: int = 26, signal_len: int = 9
) -> Tuple[List[Optional[float]], List[Optional[float]], List[Optional[float]]]:
    fast = _ema_series(prices, max(2, int(fast_len)))
    slow = _ema_series(prices, max(2, int(slow_len)))
    n = len(prices)
    macd_line: List[Optional[float]] = [None] * n
    macd_vals: List[float] = []
    macd_idx: List[int] = []
    for i in range(n):
        f = fast[i]
        s = slow[i]
        if f is None or s is None:
            continue
        v = float(f - s)
        macd_line[i] = v
        macd_vals.append(v)
        macd_idx.append(i)
    sig_line: List[Optional[float]] = [None] * n
    hist: List[Optional[float]] = [None] * n
    sig_vals = _ema_series(macd_vals, max(2, int(signal_len)))
    for j, idx in enumerate(macd_idx):
        sig = sig_vals[j] if j < len(sig_vals) else None
        sig_line[idx] = sig
        m = macd_line[idx]
        if sig is not None and m is not None:
            hist[idx] = float(m - sig)
    return macd_line, sig_line, hist


def _heikin_ashi_series(
    opens: List[float], highs: List[float], lows: List[float], closes: List[float]
) -> Tuple[List[float], List[float], List[float], List[float]]:
    n = min(len(opens), len(highs), len(lows), len(closes))
    if n <= 0:
        return [], [], [], []
    ha_open: List[float] = []
    ha_high: List[float] = []
    ha_low: List[float] = []
    ha_close: List[float] = []
    for i in range(n):
        o = float(opens[i])
        h = float(highs[i])
        l = float(lows[i])
        c = float(closes[i])
        hc = (o + h + l + c) / 4.0
        if i == 0:
            ho = (o + c) / 2.0
        else:
            ho = (ha_open[-1] + ha_close[-1]) / 2.0
        hh = max(h, ho, hc)
        hl = min(l, ho, hc)
        ha_open.append(float(ho))
        ha_high.append(float(hh))
        ha_low.append(float(hl))
        ha_close.append(float(hc))
    return ha_open, ha_high, ha_low, ha_close


def _ha_candle_state(ha_open: float, ha_close: float, *, doji_tolerance_pct: float = 0.0) -> str:
    body = float(ha_close) - float(ha_open)
    tol_pct = max(0.0, float(doji_tolerance_pct))
    if tol_pct > 0.0:
        ref = max(abs(float(ha_open)), abs(float(ha_close)), 1.0e-9)
        if abs(body) <= (ref * (tol_pct / 100.0)):
            return "doji"
    if body > 0.0:
        return "bullish"
    if body < 0.0:
        return "bearish"
    return "doji"


def _normalize_bb_condition(value: Any, *, default: str = "hold") -> str:
    raw = str(value or "").strip().lower()
    aliases = {
        "touch_upper_band": "touch_upper",
        "touch_lower_band": "touch_lower",
        "close_outside_upper_band": "close_outside_upper",
        "close_outside_lower_band": "close_outside_lower",
        "reenter_band_from_above": "reenter_from_above",
        "reenter_band_from_below": "reenter_from_below",
        "price_above_middle": "above_middle",
        "price_below_middle": "below_middle",
    }
    s = aliases.get(raw, raw)
    allowed = {
        "hold",
        "touch_upper",
        "touch_lower",
        "close_outside_upper",
        "close_outside_lower",
        "reenter_from_above",
        "reenter_from_below",
        "width_increasing",
        "width_decreasing",
        "width_below_threshold",
        "width_expanding_from_squeeze",
        "above_middle",
        "below_middle",
        "percent_b_above",
        "percent_b_below",
    }
    if not s:
        s = str(default or "hold").strip().lower()
    if s not in allowed:
        s = str(default or "hold").strip().lower()
    if s not in allowed:
        s = "hold"
    return s


def _bb_snapshot(closes: List[float], *, length: int = 20, std_mult: float = 2.0) -> Optional[Dict[str, float]]:
    ln = max(2, int(length))
    if len(closes) < ln:
        return None
    try:
        window = [float(v) for v in closes[-ln:]]
    except Exception:
        return None
    if not window:
        return None
    middle = sum(window) / float(ln)
    variance = sum((float(v) - float(middle)) ** 2 for v in window) / float(ln)
    std_dev = max(0.0, float(variance)) ** 0.5
    mult = max(0.1, float(std_mult))
    upper = float(middle) + (mult * float(std_dev))
    lower = float(middle) - (mult * float(std_dev))
    band_range = float(upper) - float(lower)
    close_now = float(closes[-1])
    width_pct = ((float(upper) - float(lower)) / float(middle) * 100.0) if float(middle) != 0.0 else 0.0
    percent_b = ((float(close_now) - float(lower)) / float(band_range)) if band_range > 0.0 else 0.5
    return {
        "middle": float(middle),
        "upper": float(upper),
        "lower": float(lower),
        "width_pct": float(width_pct),
        "percent_b": float(percent_b),
    }


def _bb_condition_hit(
    cond: str,
    *,
    close_now: float,
    high_now: Optional[float],
    low_now: Optional[float],
    upper: float,
    lower: float,
    middle: float,
    prev_close: Optional[float],
    prev_upper: Optional[float],
    prev_lower: Optional[float],
    width_pct: float,
    prev_width_pct: Optional[float],
    squeeze_threshold_pct: float,
    percent_b: float,
    percent_b_threshold: float,
) -> bool:
    c = _normalize_bb_condition(cond, default="hold")
    if c == "hold":
        return False
    if c == "touch_upper":
        touch_price = float(high_now) if high_now is not None else float(close_now)
        return touch_price >= float(upper)
    if c == "touch_lower":
        touch_price = float(low_now) if low_now is not None else float(close_now)
        return touch_price <= float(lower)
    if c == "close_outside_upper":
        return float(close_now) > float(upper)
    if c == "close_outside_lower":
        return float(close_now) < float(lower)
    if c == "reenter_from_above":
        if prev_close is None or prev_upper is None:
            return False
        return float(prev_close) > float(prev_upper) and float(close_now) <= float(upper)
    if c == "reenter_from_below":
        if prev_close is None or prev_lower is None:
            return False
        return float(prev_close) < float(prev_lower) and float(close_now) >= float(lower)
    if c == "width_increasing":
        return prev_width_pct is not None and float(width_pct) > float(prev_width_pct)
    if c == "width_decreasing":
        return prev_width_pct is not None and float(width_pct) < float(prev_width_pct)
    if c == "width_below_threshold":
        return float(width_pct) <= float(squeeze_threshold_pct)
    if c == "width_expanding_from_squeeze":
        return (
            prev_width_pct is not None
            and float(prev_width_pct) <= float(squeeze_threshold_pct)
            and float(width_pct) > float(prev_width_pct)
        )
    if c == "above_middle":
        return float(close_now) > float(middle)
    if c == "below_middle":
        return float(close_now) < float(middle)
    if c == "percent_b_above":
        return float(percent_b) >= float(percent_b_threshold)
    if c == "percent_b_below":
        return float(percent_b) <= float(percent_b_threshold)
    return False


def _normalize_ichi_condition(value: Any, *, default: str = "hold") -> str:
    raw = str(value or "").strip().lower()
    aliases = {
        "above_cloud": "price_above_cloud",
        "below_cloud": "price_below_cloud",
        "inside_cloud": "price_inside_cloud",
        "span_a_above_span_b": "cloud_bullish",
        "span_a_below_span_b": "cloud_bearish",
        "leading_span_a_above_leading_span_b": "cloud_bullish",
        "leading_span_a_below_leading_span_b": "cloud_bearish",
        "leading_a_above_leading_b": "cloud_bullish",
        "leading_a_below_leading_b": "cloud_bearish",
        "senkou_a_above_senkou_b": "cloud_bullish",
        "senkou_a_below_senkou_b": "cloud_bearish",
        "tenkan_crosses_above_kijun": "tenkan_cross_above",
        "tenkan_crosses_below_kijun": "tenkan_cross_below",
        "conversion_line_crosses_above_base_line": "tenkan_cross_above",
        "conversion_line_crosses_below_base_line": "tenkan_cross_below",
        "conversion_line_cross_above_base_line": "tenkan_cross_above",
        "conversion_line_cross_below_base_line": "tenkan_cross_below",
        "conversion_line_above_base_line": "tenkan_above_kijun",
        "conversion_line_below_base_line": "tenkan_below_kijun",
        "price_above_conversion_base_below_cloud": "price_above_tenkan_kijun_below_cloud",
        "price_above_conversion_base_inside_cloud": "price_above_tenkan_kijun_inside_cloud",
        "price_below_conversion_base_above_cloud": "price_below_tenkan_kijun_above_cloud",
        "price_below_conversion_base_inside_cloud": "price_below_tenkan_kijun_inside_cloud",
        "price_stretched_above_base_line": "price_stretched_above_kijun",
        "price_stretched_below_base_line": "price_stretched_below_kijun",
        "base_line_flat": "kijun_flat",
        "base_line_rising": "kijun_rising",
        "base_line_falling": "kijun_falling",
        "conversion_line_accelerating_up": "tenkan_accelerating_up",
        "conversion_line_accelerating_down": "tenkan_accelerating_down",
        "base_line_bounce_bullish": "kijun_bounce_bullish",
        "base_line_reject_bearish": "kijun_reject_bearish",
        "conversion_base_cross_inside_cloud": "weak_cross_inside_cloud",
        "chikou_above": "chikou_above_price",
        "chikou_below": "chikou_below_price",
        "lagging_line_above": "chikou_above_price",
        "lagging_line_below": "chikou_below_price",
        "lagging_line_above_price": "chikou_above_price",
        "lagging_line_below_price": "chikou_below_price",
        "lagging_line_clears_past_cloud_bullish": "chikou_clears_past_cloud_bullish",
        "lagging_line_clears_past_cloud_bearish": "chikou_clears_past_cloud_bearish",
        "lagging_line_blocked_by_past_price": "chikou_blocked_by_past_price",
        "lagging_line_in_congestion_zone": "chikou_in_congestion_zone",
        "full_trend_confirmation": "strong_long_confirm",
        "full_trend_confirmation_long": "strong_long_confirm",
        "full_trend_confirmation_short": "strong_short_confirm",
        "cloud_breakout": "cloud_breakout_bullish",
        "cloud_breakdown": "cloud_breakout_bearish",
        "future_twist_up": "future_twist_bullish",
        "future_twist_down": "future_twist_bearish",
        "strong_bull_cross": "bullish_cross_strong_above_cloud",
        "medium_bull_cross": "bullish_cross_medium_at_cloud",
        "weak_bull_cross": "bullish_cross_weak_below_cloud",
        "strong_bear_cross": "bearish_cross_strong_below_cloud",
        "medium_bear_cross": "bearish_cross_medium_at_cloud",
        "weak_bear_cross": "bearish_cross_weak_above_cloud",
        "above_tk_kj_below_cloud": "price_above_tenkan_kijun_below_cloud",
        "above_tk_kj_inside_cloud": "price_above_tenkan_kijun_inside_cloud",
        "below_tk_kj_above_cloud": "price_below_tenkan_kijun_above_cloud",
        "below_tk_kj_inside_cloud": "price_below_tenkan_kijun_inside_cloud",
        "kijun_bounce": "kijun_bounce_bullish",
        "kijun_rejection": "kijun_reject_bearish",
        "weak_signal_filter": "weak_cross_inside_cloud",
        "cloud_rejection": "cloud_rejection_bearish",
    }
    s = aliases.get(raw, raw)
    allowed = {
        "hold",
        "price_above_cloud",
        "price_below_cloud",
        "price_inside_cloud",
        "cloud_bullish",
        "cloud_bearish",
        "cloud_thickness_above",
        "cloud_thickness_below",
        "tenkan_above_kijun",
        "tenkan_below_kijun",
        "tenkan_cross_above",
        "tenkan_cross_below",
        "chikou_above_price",
        "chikou_below_price",
        "strong_long_confirm",
        "strong_short_confirm",
        "future_twist_bullish",
        "future_twist_bearish",
        "approaching_future_twist_bullish",
        "approaching_future_twist_bearish",
        "delayed_bullish_cross_valid",
        "delayed_bearish_cross_valid",
        "bullish_cross_strong_above_cloud",
        "bullish_cross_medium_at_cloud",
        "bullish_cross_weak_below_cloud",
        "bearish_cross_strong_below_cloud",
        "bearish_cross_medium_at_cloud",
        "bearish_cross_weak_above_cloud",
        "cloud_breakout_bullish",
        "cloud_breakout_bearish",
        "price_above_tenkan_kijun_below_cloud",
        "price_above_tenkan_kijun_inside_cloud",
        "price_below_tenkan_kijun_above_cloud",
        "price_below_tenkan_kijun_inside_cloud",
        "price_extended_above_cloud",
        "price_extended_below_cloud",
        "price_stretched_above_kijun",
        "price_stretched_below_kijun",
        "kijun_flat",
        "kijun_rising",
        "kijun_falling",
        "tenkan_accelerating_up",
        "tenkan_accelerating_down",
        "shallow_cloud_entry_from_above",
        "shallow_cloud_entry_from_below",
        "deep_inside_cloud",
        "cloud_exit_up_with_momentum",
        "cloud_exit_down_with_momentum",
        "bullish_breakout_retest_hold",
        "bearish_breakdown_retest_fail",
        "chikou_clears_past_cloud_bullish",
        "chikou_clears_past_cloud_bearish",
        "chikou_blocked_by_past_price",
        "chikou_in_congestion_zone",
        "cloud_expanding",
        "cloud_contracting",
        "cloud_thin_to_thick",
        "bullish_to_neutral_transition",
        "bearish_to_neutral_transition",
        "partial_bullish_stack",
        "partial_bearish_stack",
        "full_bullish_stack",
        "full_bearish_stack",
        "bullish_stack_weakening",
        "bearish_stack_weakening",
        "kijun_bounce_bullish",
        "kijun_reject_bearish",
        "weak_cross_inside_cloud",
        "cloud_rejection_bearish",
        "cloud_rejection_bullish",
    }
    if not s:
        s = str(default or "hold").strip().lower()
    if s not in allowed:
        s = str(default or "hold").strip().lower()
    if s not in allowed:
        s = "hold"
    return s


def _normalize_ichi_conditions(value: Any, *, default: str = "hold") -> List[str]:
    raw_items: List[Any] = []
    if isinstance(value, list):
        raw_items = list(value)
    elif isinstance(value, str):
        raw = str(value or "").strip()
        if "," in raw:
            raw_items = [part.strip() for part in raw.split(",")]
        elif raw:
            raw_items = [raw]
    elif value is not None:
        raw_items = [value]

    out: List[str] = []
    seen: set[str] = set()
    for item in raw_items:
        cond = _normalize_ichi_condition(item, default=default)
        if cond in seen:
            continue
        seen.add(cond)
        out.append(cond)

    if not out:
        out = [_normalize_ichi_condition(default, default="hold")]
    if len(out) > 1:
        out = [c for c in out if c != "hold"]
    if not out:
        out = ["hold"]
    return out


def _normalize_ichi_match_mode(value: Any, *, default: str = "all") -> str:
    mode = str(value or default or "all").strip().lower()
    return "any" if mode == "any" else "all"


def _normalize_ttm_condition(value: Any, *, default: str = "hold") -> str:
    raw = str(value or "").strip().lower()
    aliases = {
        "ttm_squeeze_on": "squeeze_on",
        "ttm_squeeze_off": "squeeze_off",
        "squeeze_release": "squeeze_fired",
        "squeeze_fire": "squeeze_fired",
        "momentum_positive": "momentum_above_zero",
        "momentum_negative": "momentum_below_zero",
        "momentum_rising": "momentum_increasing",
        "momentum_falling": "momentum_decreasing",
        "mom_cross_up": "momentum_cross_up",
        "mom_cross_down": "momentum_cross_down",
        "trend_long": "long_trend",
        "trend_short": "short_trend",
        "release_long": "long_release",
        "release_short": "short_release",
    }
    s = aliases.get(raw, raw)
    allowed = {
        "hold",
        "squeeze_on",
        "squeeze_off",
        "squeeze_fired",
        "momentum_above_zero",
        "momentum_below_zero",
        "momentum_increasing",
        "momentum_decreasing",
        "momentum_cross_up",
        "momentum_cross_down",
        "long_trend",
        "short_trend",
        "long_release",
        "short_release",
    }
    if not s:
        s = str(default or "hold").strip().lower()
    if s not in allowed:
        s = str(default or "hold").strip().lower()
    if s not in allowed:
        s = "hold"
    return s


def _normalize_roc_condition(value: Any, *, default: str = "hold") -> str:
    raw = str(value or "").strip().lower()
    aliases = {
        "above_threshold": "roc_above_threshold",
        "below_threshold": "roc_below_threshold",
        "cross_up_zero": "roc_cross_up_zero",
        "cross_down_zero": "roc_cross_down_zero",
        "cross_up_threshold": "roc_cross_up_threshold",
        "cross_down_threshold": "roc_cross_down_threshold",
        "increasing": "roc_increasing",
        "decreasing": "roc_decreasing",
        "positive": "roc_positive",
        "negative": "roc_negative",
        "long_trend": "momentum_long",
        "short_trend": "momentum_short",
    }
    s = aliases.get(raw, raw)
    allowed = {
        "hold",
        "roc_above_threshold",
        "roc_below_threshold",
        "roc_cross_up_zero",
        "roc_cross_down_zero",
        "roc_cross_up_threshold",
        "roc_cross_down_threshold",
        "roc_increasing",
        "roc_decreasing",
        "roc_positive",
        "roc_negative",
        "momentum_long",
        "momentum_short",
    }
    if not s:
        s = str(default or "hold").strip().lower()
    if s not in allowed:
        s = str(default or "hold").strip().lower()
    if s not in allowed:
        s = "hold"
    return s


def _normalize_sar_condition(value: Any, *, default: str = "hold") -> str:
    raw = str(value or "").strip().lower()
    aliases = {
        "above_sar": "price_above_sar",
        "below_sar": "price_below_sar",
        "cross_up": "sar_cross_up",
        "cross_down": "sar_cross_down",
        "price_cross_up": "sar_cross_up",
        "price_cross_down": "sar_cross_down",
        "sar_up": "sar_rising",
        "sar_down": "sar_falling",
        "long": "trend_long",
        "short": "trend_short",
    }
    s = aliases.get(raw, raw)
    allowed = {
        "hold",
        "price_above_sar",
        "price_below_sar",
        "sar_cross_up",
        "sar_cross_down",
        "sar_rising",
        "sar_falling",
        "trend_long",
        "trend_short",
    }
    if not s:
        s = str(default or "hold").strip().lower()
    if s not in allowed:
        s = str(default or "hold").strip().lower()
    if s not in allowed:
        s = "hold"
    return s


def _ichimoku_state(
    closes: List[float],
    *,
    highs: Optional[List[float]] = None,
    lows: Optional[List[float]] = None,
    tenkan_length: int = 9,
    kijun_length: int = 26,
    senkou_b_length: int = 52,
    displacement: int = 26,
) -> Optional[Dict[str, Any]]:
    n = len(closes)
    if n <= 0:
        return None
    close_vals: List[float] = []
    for raw in closes:
        fv = _to_float_opt(raw)
        if fv is None:
            return None
        close_vals.append(float(fv))
    if not close_vals:
        return None
    high_vals: List[float] = []
    low_vals: List[float] = []
    for idx, close_now in enumerate(close_vals):
        h = _to_float_opt(highs[idx]) if isinstance(highs, list) and idx < len(highs) else None
        l = _to_float_opt(lows[idx]) if isinstance(lows, list) and idx < len(lows) else None
        high_vals.append(float(h) if h is not None else float(close_now))
        low_vals.append(float(l) if l is not None else float(close_now))

    tenkan_len = max(1, int(tenkan_length))
    kijun_len = max(1, int(kijun_length))
    senkou_b_len = max(2, int(senkou_b_length))
    disp = max(1, int(displacement))

    def _midpoint(length: int, idx: int) -> Optional[float]:
        if idx < 0 or idx >= n:
            return None
        start = idx - int(length) + 1
        if start < 0:
            return None
        hi_window = high_vals[start : idx + 1]
        lo_window = low_vals[start : idx + 1]
        if not hi_window or not lo_window:
            return None
        return (max(hi_window) + min(lo_window)) / 2.0

    def _span_a_raw(idx: int) -> Optional[float]:
        tenkan = _midpoint(tenkan_len, idx)
        kijun = _midpoint(kijun_len, idx)
        if tenkan is None or kijun is None:
            return None
        return (float(tenkan) + float(kijun)) / 2.0

    def _span_b_raw(idx: int) -> Optional[float]:
        return _midpoint(senkou_b_len, idx)

    curr_idx = n - 1
    prev_idx = curr_idx - 1
    cloud_idx = curr_idx - disp
    prev_cloud_idx = prev_idx - disp if prev_idx >= 0 else None
    chikou_ref_idx = curr_idx - disp
    chikou_prev_ref_idx = prev_idx - disp if prev_idx >= 0 else None

    tenkan_now = _midpoint(tenkan_len, curr_idx)
    kijun_now = _midpoint(kijun_len, curr_idx)
    if tenkan_now is None or kijun_now is None:
        return None
    tenkan_prev = _midpoint(tenkan_len, prev_idx) if prev_idx >= 0 else None
    kijun_prev = _midpoint(kijun_len, prev_idx) if prev_idx >= 0 else None
    tenkan_prev2 = _midpoint(tenkan_len, prev_idx - 1) if prev_idx >= 1 else None
    kijun_prev2 = _midpoint(kijun_len, prev_idx - 1) if prev_idx >= 1 else None

    span_a = _span_a_raw(cloud_idx) if cloud_idx >= 0 else None
    span_b = _span_b_raw(cloud_idx) if cloud_idx >= 0 else None
    future_span_a = _span_a_raw(curr_idx)
    future_span_b = _span_b_raw(curr_idx)
    if span_a is None or span_b is None or future_span_a is None or future_span_b is None:
        return None
    prev_span_a = _span_a_raw(prev_cloud_idx) if (prev_cloud_idx is not None and prev_cloud_idx >= 0) else None
    prev_span_b = _span_b_raw(prev_cloud_idx) if (prev_cloud_idx is not None and prev_cloud_idx >= 0) else None
    future_span_a_prev = _span_a_raw(prev_idx) if prev_idx >= 0 else None
    future_span_b_prev = _span_b_raw(prev_idx) if prev_idx >= 0 else None

    close_now = float(close_vals[curr_idx])
    close_prev = float(close_vals[prev_idx]) if prev_idx >= 0 else None
    cloud_top = max(float(span_a), float(span_b))
    cloud_bottom = min(float(span_a), float(span_b))
    cloud_top_prev = max(float(prev_span_a), float(prev_span_b)) if (prev_span_a is not None and prev_span_b is not None) else None
    cloud_bottom_prev = min(float(prev_span_a), float(prev_span_b)) if (prev_span_a is not None and prev_span_b is not None) else None
    cloud_mid = (float(span_a) + float(span_b)) / 2.0
    cloud_thickness_pct = (abs(float(span_a) - float(span_b)) / max(abs(cloud_mid), 1.0e-9)) * 100.0
    cloud_thickness_prev_pct: Optional[float] = None
    if prev_span_a is not None and prev_span_b is not None:
        prev_mid = (float(prev_span_a) + float(prev_span_b)) / 2.0
        cloud_thickness_prev_pct = (abs(float(prev_span_a) - float(prev_span_b)) / max(abs(prev_mid), 1.0e-9)) * 100.0

    chikou_ref_price = float(close_vals[chikou_ref_idx]) if chikou_ref_idx >= 0 else None
    chikou_prev_ref_price = float(close_vals[chikou_prev_ref_idx]) if (chikou_prev_ref_idx is not None and chikou_prev_ref_idx >= 0) else None
    chikou_cloud_idx = chikou_ref_idx - disp
    chikou_span_a = _span_a_raw(chikou_cloud_idx) if chikou_cloud_idx >= 0 else None
    chikou_span_b = _span_b_raw(chikou_cloud_idx) if chikou_cloud_idx >= 0 else None
    chikou_cloud_top = max(float(chikou_span_a), float(chikou_span_b)) if (chikou_span_a is not None and chikou_span_b is not None) else None
    chikou_cloud_bottom = min(float(chikou_span_a), float(chikou_span_b)) if (chikou_span_a is not None and chikou_span_b is not None) else None

    last_cross_up_idx: Optional[int] = None
    last_cross_down_idx: Optional[int] = None
    prev_tk: Optional[float] = None
    prev_kj: Optional[float] = None
    for j in range(n):
        tk_j = _midpoint(tenkan_len, j)
        kj_j = _midpoint(kijun_len, j)
        if tk_j is None or kj_j is None:
            prev_tk = tk_j
            prev_kj = kj_j
            continue
        if prev_tk is not None and prev_kj is not None:
            if float(prev_tk) <= float(prev_kj) and float(tk_j) > float(kj_j):
                last_cross_up_idx = j
            if float(prev_tk) >= float(prev_kj) and float(tk_j) < float(kj_j):
                last_cross_down_idx = j
        prev_tk = tk_j
        prev_kj = kj_j
    bars_since_cross_up = (curr_idx - last_cross_up_idx) if last_cross_up_idx is not None else None
    bars_since_cross_down = (curr_idx - last_cross_down_idx) if last_cross_down_idx is not None else None

    return {
        "close_now": float(close_now),
        "close_prev": float(close_prev) if close_prev is not None else None,
        "tenkan": float(tenkan_now),
        "tenkan_prev": float(tenkan_prev) if tenkan_prev is not None else None,
        "tenkan_prev2": float(tenkan_prev2) if tenkan_prev2 is not None else None,
        "kijun": float(kijun_now),
        "kijun_prev": float(kijun_prev) if kijun_prev is not None else None,
        "kijun_prev2": float(kijun_prev2) if kijun_prev2 is not None else None,
        "span_a": float(span_a),
        "span_b": float(span_b),
        "span_a_prev": float(prev_span_a) if prev_span_a is not None else None,
        "span_b_prev": float(prev_span_b) if prev_span_b is not None else None,
        "future_span_a": float(future_span_a),
        "future_span_b": float(future_span_b),
        "future_span_a_prev": float(future_span_a_prev) if future_span_a_prev is not None else None,
        "future_span_b_prev": float(future_span_b_prev) if future_span_b_prev is not None else None,
        "cloud_top": float(cloud_top),
        "cloud_bottom": float(cloud_bottom),
        "cloud_top_prev": float(cloud_top_prev) if cloud_top_prev is not None else None,
        "cloud_bottom_prev": float(cloud_bottom_prev) if cloud_bottom_prev is not None else None,
        "cloud_thickness_pct": float(cloud_thickness_pct),
        "cloud_thickness_prev_pct": float(cloud_thickness_prev_pct) if cloud_thickness_prev_pct is not None else None,
        "chikou_ref_price": float(chikou_ref_price) if chikou_ref_price is not None else None,
        "chikou_prev_ref_price": float(chikou_prev_ref_price) if chikou_prev_ref_price is not None else None,
        "chikou_cloud_top": float(chikou_cloud_top) if chikou_cloud_top is not None else None,
        "chikou_cloud_bottom": float(chikou_cloud_bottom) if chikou_cloud_bottom is not None else None,
        "bars_since_cross_up": int(bars_since_cross_up) if bars_since_cross_up is not None else None,
        "bars_since_cross_down": int(bars_since_cross_down) if bars_since_cross_down is not None else None,
    }


def _ichimoku_condition_hit(
    cond: str,
    *,
    state: Dict[str, Any],
    cloud_thickness_threshold_pct: float,
    kijun_bounce_tolerance_pct: float,
    delayed_cross_lookback: int,
) -> bool:
    c = _normalize_ichi_condition(cond, default="hold")
    if c == "hold":
        return False

    close_now = float(state.get("close_now") or 0.0)
    close_prev = _to_float_opt(state.get("close_prev"))
    tenkan = float(state.get("tenkan") or 0.0)
    tenkan_prev = _to_float_opt(state.get("tenkan_prev"))
    tenkan_prev2 = _to_float_opt(state.get("tenkan_prev2"))
    kijun = float(state.get("kijun") or 0.0)
    kijun_prev = _to_float_opt(state.get("kijun_prev"))
    kijun_prev2 = _to_float_opt(state.get("kijun_prev2"))
    cloud_top = float(state.get("cloud_top") or 0.0)
    cloud_bottom = float(state.get("cloud_bottom") or 0.0)
    cloud_top_prev = _to_float_opt(state.get("cloud_top_prev"))
    cloud_bottom_prev = _to_float_opt(state.get("cloud_bottom_prev"))
    span_a = float(state.get("span_a") or 0.0)
    span_b = float(state.get("span_b") or 0.0)
    future_span_a = float(state.get("future_span_a") or 0.0)
    future_span_b = float(state.get("future_span_b") or 0.0)
    future_span_a_prev = _to_float_opt(state.get("future_span_a_prev"))
    future_span_b_prev = _to_float_opt(state.get("future_span_b_prev"))
    cloud_thickness_pct = float(state.get("cloud_thickness_pct") or 0.0)
    cloud_thickness_prev_pct = _to_float_opt(state.get("cloud_thickness_prev_pct"))
    chikou_ref_price = _to_float_opt(state.get("chikou_ref_price"))
    chikou_prev_ref_price = _to_float_opt(state.get("chikou_prev_ref_price"))
    chikou_cloud_top = _to_float_opt(state.get("chikou_cloud_top"))
    chikou_cloud_bottom = _to_float_opt(state.get("chikou_cloud_bottom"))
    bars_since_cross_up = _to_int_opt(state.get("bars_since_cross_up"))
    bars_since_cross_down = _to_int_opt(state.get("bars_since_cross_down"))

    inside_cloud = float(cloud_bottom) <= float(close_now) <= float(cloud_top)
    price_above_cloud = float(close_now) > float(cloud_top)
    price_below_cloud = float(close_now) < float(cloud_bottom)
    cloud_bullish = float(span_a) > float(span_b)
    cloud_bearish = float(span_a) < float(span_b)
    tenkan_above = float(tenkan) > float(kijun)
    tenkan_below = float(tenkan) < float(kijun)
    price_above_tenkan = float(close_now) > float(tenkan)
    price_below_tenkan = float(close_now) < float(tenkan)
    price_above_kijun = float(close_now) > float(kijun)
    price_below_kijun = float(close_now) < float(kijun)
    chikou_above = chikou_ref_price is not None and float(close_now) > float(chikou_ref_price)
    chikou_below = chikou_ref_price is not None and float(close_now) < float(chikou_ref_price)
    cross_up = (
        tenkan_prev is not None
        and kijun_prev is not None
        and float(tenkan_prev) <= float(kijun_prev)
        and tenkan_above
    )
    cross_down = (
        tenkan_prev is not None
        and kijun_prev is not None
        and float(tenkan_prev) >= float(kijun_prev)
        and tenkan_below
    )
    distance_to_kijun_pct = (abs(float(close_now) - float(kijun)) / max(abs(float(kijun)), 1.0e-9)) * 100.0
    bounce_tol = max(0.0, float(kijun_bounce_tolerance_pct))
    boundary_tol = max(0.05, bounce_tol)
    cloud_ext_thr = max(0.5, float(cloud_thickness_threshold_pct))
    kijun_ext_thr = max(0.5, bounce_tol * 2.0)
    delayed_bars = max(1, int(delayed_cross_lookback))
    close_ref = max(abs(float(close_now)), 1.0e-9)
    dist_to_top_pct = (abs(float(close_now) - float(cloud_top)) / close_ref) * 100.0
    dist_to_bottom_pct = (abs(float(close_now) - float(cloud_bottom)) / close_ref) * 100.0
    near_cloud_boundary = min(dist_to_top_pct, dist_to_bottom_pct) <= boundary_tol
    cloud_range = max(float(cloud_top) - float(cloud_bottom), 1.0e-9)
    depth_ratio = (float(close_now) - float(cloud_bottom)) / cloud_range
    shallow_band = 0.25
    deep_low = 0.4
    deep_high = 0.6
    inside_prev = (
        close_prev is not None
        and cloud_top_prev is not None
        and cloud_bottom_prev is not None
        and float(cloud_bottom_prev) <= float(close_prev) <= float(cloud_top_prev)
    )
    price_momentum_up = close_prev is not None and float(close_now) > float(close_prev)
    price_momentum_down = close_prev is not None and float(close_now) < float(close_prev)
    above_cloud_prev = close_prev is not None and cloud_top_prev is not None and float(close_prev) > float(cloud_top_prev)
    below_cloud_prev = close_prev is not None and cloud_bottom_prev is not None and float(close_prev) < float(cloud_bottom_prev)
    future_diff = float(future_span_a) - float(future_span_b)
    future_prev_diff: Optional[float] = None
    if future_span_a_prev is not None and future_span_b_prev is not None:
        future_prev_diff = float(future_span_a_prev) - float(future_span_b_prev)
    twist_prox_pct = (abs(float(future_diff)) / close_ref) * 100.0
    approaching_bull_twist = (
        future_prev_diff is not None
        and float(future_prev_diff) < 0.0
        and float(future_diff) < 0.0
        and abs(float(future_diff)) < abs(float(future_prev_diff))
        and twist_prox_pct <= boundary_tol
    )
    approaching_bear_twist = (
        future_prev_diff is not None
        and float(future_prev_diff) > 0.0
        and float(future_diff) > 0.0
        and abs(float(future_diff)) < abs(float(future_prev_diff))
        and twist_prox_pct <= boundary_tol
    )
    future_twist_bullish = future_prev_diff is not None and float(future_prev_diff) <= 0.0 and float(future_diff) > 0.0
    future_twist_bearish = future_prev_diff is not None and float(future_prev_diff) >= 0.0 and float(future_diff) < 0.0
    delayed_bull_valid = (
        bars_since_cross_up is not None
        and int(bars_since_cross_up) <= delayed_bars
        and tenkan_above
        and price_above_tenkan
        and price_above_kijun
    )
    delayed_bear_valid = (
        bars_since_cross_down is not None
        and int(bars_since_cross_down) <= delayed_bars
        and tenkan_below
        and price_below_tenkan
        and price_below_kijun
    )
    above_cloud_ext_pct = (
        ((float(close_now) - float(cloud_top)) / max(abs(float(cloud_top)), 1.0e-9)) * 100.0
        if price_above_cloud
        else 0.0
    )
    below_cloud_ext_pct = (
        ((float(cloud_bottom) - float(close_now)) / max(abs(float(cloud_bottom)), 1.0e-9)) * 100.0
        if price_below_cloud
        else 0.0
    )
    kijun_flat = (
        kijun_prev is not None
        and (abs(float(kijun) - float(kijun_prev)) / max(abs(float(kijun_prev)), 1.0e-9)) * 100.0 <= max(0.05, bounce_tol / 2.0)
    )
    kijun_rising = kijun_prev is not None and float(kijun) > float(kijun_prev)
    kijun_falling = kijun_prev is not None and float(kijun) < float(kijun_prev)
    tenkan_slope_now = (float(tenkan) - float(tenkan_prev)) if tenkan_prev is not None else None
    tenkan_slope_prev = (float(tenkan_prev) - float(tenkan_prev2)) if tenkan_prev is not None and tenkan_prev2 is not None else None
    tenkan_accel_up = (
        tenkan_slope_now is not None
        and tenkan_slope_prev is not None
        and float(tenkan_slope_now) > 0.0
        and float(tenkan_slope_now) > float(tenkan_slope_prev)
    )
    tenkan_accel_down = (
        tenkan_slope_now is not None
        and tenkan_slope_prev is not None
        and float(tenkan_slope_now) < 0.0
        and float(tenkan_slope_now) < float(tenkan_slope_prev)
    )
    shallow_entry_from_above = inside_cloud and depth_ratio >= (1.0 - shallow_band) and above_cloud_prev
    shallow_entry_from_below = inside_cloud and depth_ratio <= shallow_band and below_cloud_prev
    deep_inside_cloud = inside_cloud and deep_low <= depth_ratio <= deep_high
    cloud_exit_up_momentum = inside_prev and price_above_cloud and price_momentum_up and tenkan_above
    cloud_exit_down_momentum = inside_prev and price_below_cloud and price_momentum_down and tenkan_below
    bullish_retest_hold = (
        above_cloud_prev
        and float(close_now) >= float(cloud_top)
        and dist_to_top_pct <= boundary_tol
        and tenkan_above
    )
    bearish_retest_fail = (
        below_cloud_prev
        and float(close_now) <= float(cloud_bottom)
        and dist_to_bottom_pct <= boundary_tol
        and tenkan_below
    )
    chikou_clears_past_cloud_bull = chikou_cloud_top is not None and float(close_now) > float(chikou_cloud_top)
    chikou_clears_past_cloud_bear = chikou_cloud_bottom is not None and float(close_now) < float(chikou_cloud_bottom)
    chikou_blocked = (
        chikou_ref_price is not None
        and (abs(float(close_now) - float(chikou_ref_price)) / max(abs(float(chikou_ref_price)), 1.0e-9)) * 100.0 <= boundary_tol
    )
    chikou_congestion = (
        chikou_blocked
        and chikou_cloud_top is not None
        and chikou_cloud_bottom is not None
        and float(chikou_cloud_bottom) <= float(close_now) <= float(chikou_cloud_top)
    )
    cloud_expanding = cloud_thickness_prev_pct is not None and cloud_thickness_pct > float(cloud_thickness_prev_pct)
    cloud_contracting = cloud_thickness_prev_pct is not None and cloud_thickness_pct < float(cloud_thickness_prev_pct)
    cloud_thin_to_thick = (
        cloud_thickness_prev_pct is not None
        and float(cloud_thickness_prev_pct) <= float(cloud_thickness_threshold_pct)
        and cloud_thickness_pct > float(cloud_thickness_threshold_pct)
    )
    bullish_to_neutral = above_cloud_prev and inside_cloud
    bearish_to_neutral = below_cloud_prev and inside_cloud
    bullish_components = [
        price_above_cloud,
        tenkan_above,
        bool(chikou_above),
    ]
    bearish_components = [
        price_below_cloud,
        tenkan_below,
        bool(chikou_below),
    ]
    bull_count = sum(1 for ok in bullish_components if bool(ok))
    bear_count = sum(1 for ok in bearish_components if bool(ok))
    prev_price_above_cloud = above_cloud_prev
    prev_price_below_cloud = below_cloud_prev
    prev_tenkan_above = tenkan_prev is not None and kijun_prev is not None and float(tenkan_prev) > float(kijun_prev)
    prev_tenkan_below = tenkan_prev is not None and kijun_prev is not None and float(tenkan_prev) < float(kijun_prev)
    prev_chikou_above = close_prev is not None and chikou_prev_ref_price is not None and float(close_prev) > float(chikou_prev_ref_price)
    prev_chikou_below = close_prev is not None and chikou_prev_ref_price is not None and float(close_prev) < float(chikou_prev_ref_price)
    bull_prev_count = sum(1 for ok in (prev_price_above_cloud, prev_tenkan_above, prev_chikou_above) if bool(ok))
    bear_prev_count = sum(1 for ok in (prev_price_below_cloud, prev_tenkan_below, prev_chikou_below) if bool(ok))

    if c == "price_above_cloud":
        return price_above_cloud
    if c == "price_below_cloud":
        return price_below_cloud
    if c == "price_inside_cloud":
        return inside_cloud
    if c == "cloud_bullish":
        return cloud_bullish
    if c == "cloud_bearish":
        return cloud_bearish
    if c == "cloud_thickness_above":
        return cloud_thickness_pct >= float(cloud_thickness_threshold_pct)
    if c == "cloud_thickness_below":
        return cloud_thickness_pct <= float(cloud_thickness_threshold_pct)
    if c == "tenkan_above_kijun":
        return tenkan_above
    if c == "tenkan_below_kijun":
        return tenkan_below
    if c == "tenkan_cross_above":
        return bool(cross_up)
    if c == "tenkan_cross_below":
        return bool(cross_down)
    if c == "chikou_above_price":
        return bool(chikou_above)
    if c == "chikou_below_price":
        return bool(chikou_below)
    if c == "strong_long_confirm":
        return price_above_cloud and tenkan_above and bool(chikou_above)
    if c == "strong_short_confirm":
        return price_below_cloud and tenkan_below and bool(chikou_below)
    if c == "future_twist_bullish":
        return bool(future_twist_bullish)
    if c == "future_twist_bearish":
        return bool(future_twist_bearish)
    if c == "approaching_future_twist_bullish":
        return bool(approaching_bull_twist)
    if c == "approaching_future_twist_bearish":
        return bool(approaching_bear_twist)
    if c == "delayed_bullish_cross_valid":
        return bool(delayed_bull_valid)
    if c == "delayed_bearish_cross_valid":
        return bool(delayed_bear_valid)
    if c == "bullish_cross_strong_above_cloud":
        return bool(cross_up) and price_above_cloud
    if c == "bullish_cross_medium_at_cloud":
        return bool(cross_up) and (inside_cloud or near_cloud_boundary)
    if c == "bullish_cross_weak_below_cloud":
        return bool(cross_up) and price_below_cloud
    if c == "bearish_cross_strong_below_cloud":
        return bool(cross_down) and price_below_cloud
    if c == "bearish_cross_medium_at_cloud":
        return bool(cross_down) and (inside_cloud or near_cloud_boundary)
    if c == "bearish_cross_weak_above_cloud":
        return bool(cross_down) and price_above_cloud
    if c == "cloud_breakout_bullish":
        return (
            close_prev is not None
            and cloud_top_prev is not None
            and float(close_prev) <= float(cloud_top_prev)
            and price_above_cloud
            and float(future_span_a) > float(future_span_b)
        )
    if c == "cloud_breakout_bearish":
        return (
            close_prev is not None
            and cloud_bottom_prev is not None
            and float(close_prev) >= float(cloud_bottom_prev)
            and price_below_cloud
            and float(future_span_a) < float(future_span_b)
        )
    if c == "price_above_tenkan_kijun_below_cloud":
        return price_above_tenkan and price_above_kijun and price_below_cloud
    if c == "price_above_tenkan_kijun_inside_cloud":
        return price_above_tenkan and price_above_kijun and inside_cloud
    if c == "price_below_tenkan_kijun_above_cloud":
        return price_below_tenkan and price_below_kijun and price_above_cloud
    if c == "price_below_tenkan_kijun_inside_cloud":
        return price_below_tenkan and price_below_kijun and inside_cloud
    if c == "price_extended_above_cloud":
        return above_cloud_ext_pct >= cloud_ext_thr
    if c == "price_extended_below_cloud":
        return below_cloud_ext_pct >= cloud_ext_thr
    if c == "price_stretched_above_kijun":
        return float(close_now) > float(kijun) and distance_to_kijun_pct >= kijun_ext_thr
    if c == "price_stretched_below_kijun":
        return float(close_now) < float(kijun) and distance_to_kijun_pct >= kijun_ext_thr
    if c == "kijun_flat":
        return bool(kijun_flat)
    if c == "kijun_rising":
        return bool(kijun_rising)
    if c == "kijun_falling":
        return bool(kijun_falling)
    if c == "tenkan_accelerating_up":
        return bool(tenkan_accel_up)
    if c == "tenkan_accelerating_down":
        return bool(tenkan_accel_down)
    if c == "shallow_cloud_entry_from_above":
        return bool(shallow_entry_from_above)
    if c == "shallow_cloud_entry_from_below":
        return bool(shallow_entry_from_below)
    if c == "deep_inside_cloud":
        return bool(deep_inside_cloud)
    if c == "cloud_exit_up_with_momentum":
        return bool(cloud_exit_up_momentum)
    if c == "cloud_exit_down_with_momentum":
        return bool(cloud_exit_down_momentum)
    if c == "bullish_breakout_retest_hold":
        return bool(bullish_retest_hold)
    if c == "bearish_breakdown_retest_fail":
        return bool(bearish_retest_fail)
    if c == "chikou_clears_past_cloud_bullish":
        return bool(chikou_clears_past_cloud_bull)
    if c == "chikou_clears_past_cloud_bearish":
        return bool(chikou_clears_past_cloud_bear)
    if c == "chikou_blocked_by_past_price":
        return bool(chikou_blocked)
    if c == "chikou_in_congestion_zone":
        return bool(chikou_congestion)
    if c == "cloud_expanding":
        return bool(cloud_expanding)
    if c == "cloud_contracting":
        return bool(cloud_contracting)
    if c == "cloud_thin_to_thick":
        return bool(cloud_thin_to_thick)
    if c == "bullish_to_neutral_transition":
        return bool(bullish_to_neutral)
    if c == "bearish_to_neutral_transition":
        return bool(bearish_to_neutral)
    if c == "partial_bullish_stack":
        return bull_count >= 2
    if c == "partial_bearish_stack":
        return bear_count >= 2
    if c == "full_bullish_stack":
        return bull_count == 3
    if c == "full_bearish_stack":
        return bear_count == 3
    if c == "bullish_stack_weakening":
        return bull_prev_count >= 2 and bull_count < bull_prev_count
    if c == "bearish_stack_weakening":
        return bear_prev_count >= 2 and bear_count < bear_prev_count
    if c == "kijun_bounce_bullish":
        return (
            price_above_cloud
            and tenkan_above
            and float(close_now) >= float(kijun)
            and distance_to_kijun_pct <= bounce_tol
        )
    if c == "kijun_reject_bearish":
        return (
            price_below_cloud
            and tenkan_below
            and float(close_now) <= float(kijun)
            and distance_to_kijun_pct <= bounce_tol
        )
    if c == "weak_cross_inside_cloud":
        return inside_cloud and (bool(cross_up) or bool(cross_down))
    if c == "cloud_rejection_bearish":
        return (
            close_prev is not None
            and cloud_top_prev is not None
            and float(close_prev) > float(cloud_top_prev)
            and inside_cloud
        )
    if c == "cloud_rejection_bullish":
        return (
            close_prev is not None
            and cloud_bottom_prev is not None
            and float(close_prev) < float(cloud_bottom_prev)
            and inside_cloud
        )
    return False


def _ichimoku_conditions_hit(
    conditions: List[str],
    *,
    state: Dict[str, Any],
    cloud_thickness_threshold_pct: float,
    kijun_bounce_tolerance_pct: float,
    delayed_cross_lookback: int,
    mode: str = "all",
) -> bool:
    active = [c for c in _normalize_ichi_conditions(conditions, default="hold") if c != "hold"]
    if not active:
        return False
    match_mode = _normalize_ichi_match_mode(mode, default="all")
    results = [
        _ichimoku_condition_hit(
            cond,
            state=state,
            cloud_thickness_threshold_pct=cloud_thickness_threshold_pct,
            kijun_bounce_tolerance_pct=kijun_bounce_tolerance_pct,
            delayed_cross_lookback=delayed_cross_lookback,
        )
        for cond in active
    ]
    if match_mode == "any":
        return any(results)
    return all(results)


def _ttm_sma_tail(values: List[float], length: int) -> Optional[float]:
    ln = max(1, int(length))
    if len(values) < ln:
        return None
    try:
        window = [float(v) for v in values[-ln:]]
    except Exception:
        return None
    if not window:
        return None
    return float(sum(window) / float(ln))


def _ttm_atr(
    closes: List[float],
    *,
    highs: Optional[List[float]] = None,
    lows: Optional[List[float]] = None,
    length: int = 20,
) -> Optional[float]:
    ln = max(1, int(length))
    n = len(closes)
    if n < ln:
        return None
    close_vals: List[float] = []
    high_vals: List[float] = []
    low_vals: List[float] = []
    for i, raw_close in enumerate(closes):
        close_now = _to_float_opt(raw_close)
        if close_now is None:
            return None
        c = float(close_now)
        h = _to_float_opt(highs[i]) if isinstance(highs, list) and i < len(highs) else None
        l = _to_float_opt(lows[i]) if isinstance(lows, list) and i < len(lows) else None
        close_vals.append(c)
        high_vals.append(float(h) if h is not None else c)
        low_vals.append(float(l) if l is not None else c)
    trs: List[float] = []
    for i in range(n):
        hi = float(high_vals[i])
        lo = float(low_vals[i])
        prev_close = float(close_vals[i - 1]) if i > 0 else float(close_vals[i])
        tr = max(hi - lo, abs(hi - prev_close), abs(lo - prev_close))
        trs.append(float(tr))
    if len(trs) < ln:
        return None
    return float(sum(trs[-ln:]) / float(ln))


def _ttm_linreg_endpoint(values: List[float]) -> Optional[float]:
    n = len(values)
    if n <= 0:
        return None
    try:
        ys = [float(v) for v in values]
    except Exception:
        return None
    x_sum = float((n - 1) * n / 2)
    x2_sum = float((n - 1) * n * (2 * n - 1) / 6)
    y_sum = float(sum(ys))
    xy_sum = float(sum((i * ys[i]) for i in range(n)))
    denom = (float(n) * x2_sum) - (x_sum * x_sum)
    slope = 0.0 if abs(denom) <= 1.0e-12 else ((float(n) * xy_sum) - (x_sum * y_sum)) / denom
    intercept = (y_sum - (slope * x_sum)) / float(n)
    return float(intercept + (slope * float(n - 1)))


def _ttm_momentum(
    closes: List[float],
    *,
    highs: Optional[List[float]] = None,
    lows: Optional[List[float]] = None,
    length: int = 20,
) -> Optional[float]:
    ln = max(2, int(length))
    if len(closes) < ln:
        return None
    close_vals = [float(v) for v in closes[-ln:]]
    high_vals: List[float] = []
    low_vals: List[float] = []
    for i, c in enumerate(close_vals):
        src_idx = len(closes) - ln + i
        h = _to_float_opt(highs[src_idx]) if isinstance(highs, list) and src_idx < len(highs) else None
        l = _to_float_opt(lows[src_idx]) if isinstance(lows, list) and src_idx < len(lows) else None
        high_vals.append(float(h) if h is not None else c)
        low_vals.append(float(l) if l is not None else c)
    highest = max(high_vals) if high_vals else None
    lowest = min(low_vals) if low_vals else None
    if highest is None or lowest is None:
        return None
    sma_close = sum(close_vals) / float(ln)
    ref = ((float(highest) + float(lowest)) / 2.0 + float(sma_close)) / 2.0
    centered = [float(c - ref) for c in close_vals]
    return _ttm_linreg_endpoint(centered)


def _ttm_state(
    closes: List[float],
    *,
    highs: Optional[List[float]] = None,
    lows: Optional[List[float]] = None,
    bb_length: int = 20,
    bb_mult: float = 2.0,
    kc_length: int = 20,
    kc_mult: float = 1.5,
    momentum_length: int = 20,
) -> Optional[Dict[str, Any]]:
    bb_len = max(2, int(bb_length))
    bb_mul = max(0.1, float(bb_mult))
    kc_len = max(2, int(kc_length))
    kc_mul = max(0.1, float(kc_mult))
    mom_len = max(2, int(momentum_length))
    need = max(bb_len, kc_len, mom_len)
    if len(closes) < need:
        return None

    def _single(
        closes_local: List[float],
        highs_local: Optional[List[float]],
        lows_local: Optional[List[float]],
    ) -> Optional[Dict[str, Any]]:
        bb = _bb_snapshot(closes_local, length=bb_len, std_mult=bb_mul)
        if bb is None:
            return None
        kc_mid = _ttm_sma_tail(closes_local, kc_len)
        kc_atr = _ttm_atr(closes_local, highs=highs_local, lows=lows_local, length=kc_len)
        mom = _ttm_momentum(closes_local, highs=highs_local, lows=lows_local, length=mom_len)
        if kc_mid is None or kc_atr is None or mom is None:
            return None
        kc_upper = float(kc_mid) + (kc_mul * float(kc_atr))
        kc_lower = float(kc_mid) - (kc_mul * float(kc_atr))
        bb_upper = float(bb["upper"])
        bb_lower = float(bb["lower"])
        squeeze_on = bool(bb_upper <= kc_upper and bb_lower >= kc_lower)
        squeeze_off = bool(bb_upper > kc_upper and bb_lower < kc_lower)
        return {
            "bb_upper": bb_upper,
            "bb_lower": bb_lower,
            "kc_upper": kc_upper,
            "kc_lower": kc_lower,
            "momentum": float(mom),
            "squeeze_on": bool(squeeze_on),
            "squeeze_off": bool(squeeze_off),
        }

    current = _single(closes, highs, lows)
    if current is None:
        return None
    prev_state: Optional[Dict[str, Any]] = None
    if len(closes) >= (need + 1):
        prev_state = _single(
            closes[:-1],
            highs[:-1] if isinstance(highs, list) and highs else None,
            lows[:-1] if isinstance(lows, list) and lows else None,
        )
    current["prev_momentum"] = _to_float_opt(prev_state.get("momentum")) if isinstance(prev_state, dict) else None
    current["prev_squeeze_on"] = bool(prev_state.get("squeeze_on")) if isinstance(prev_state, dict) else False
    return current


def _ttm_condition_hit(cond: str, *, state: Dict[str, Any]) -> bool:
    c = _normalize_ttm_condition(cond, default="hold")
    if c == "hold":
        return False
    momentum = float(state.get("momentum") or 0.0)
    prev_momentum = _to_float_opt(state.get("prev_momentum"))
    squeeze_on = bool(state.get("squeeze_on"))
    squeeze_off = bool(state.get("squeeze_off"))
    prev_squeeze_on = bool(state.get("prev_squeeze_on"))
    if c == "squeeze_on":
        return squeeze_on
    if c == "squeeze_off":
        return squeeze_off
    if c == "squeeze_fired":
        return prev_squeeze_on and squeeze_off
    if c == "momentum_above_zero":
        return momentum > 0.0
    if c == "momentum_below_zero":
        return momentum < 0.0
    if c == "momentum_increasing":
        return prev_momentum is not None and momentum > float(prev_momentum)
    if c == "momentum_decreasing":
        return prev_momentum is not None and momentum < float(prev_momentum)
    if c == "momentum_cross_up":
        return prev_momentum is not None and float(prev_momentum) <= 0.0 and momentum > 0.0
    if c == "momentum_cross_down":
        return prev_momentum is not None and float(prev_momentum) >= 0.0 and momentum < 0.0
    if c == "long_trend":
        return squeeze_off and (momentum > 0.0) and (prev_momentum is not None and momentum > float(prev_momentum))
    if c == "short_trend":
        return squeeze_off and (momentum < 0.0) and (prev_momentum is not None and momentum < float(prev_momentum))
    if c == "long_release":
        return (prev_squeeze_on and squeeze_off) and (momentum > 0.0)
    if c == "short_release":
        return (prev_squeeze_on and squeeze_off) and (momentum < 0.0)
    return False


def _roc_value(closes: List[float], length: int = 12) -> Optional[float]:
    ln = max(1, int(length))
    if len(closes) <= ln:
        return None
    now = _to_float_opt(closes[-1])
    base = _to_float_opt(closes[-1 - ln])
    if now is None or base is None or float(base) == 0.0:
        return None
    return ((float(now) - float(base)) / float(base)) * 100.0


def _sar_series(
    closes: List[float],
    *,
    highs: Optional[List[float]] = None,
    lows: Optional[List[float]] = None,
    step: float = 0.02,
    max_step: float = 0.2,
) -> List[Optional[float]]:
    n = len(closes)
    out: List[Optional[float]] = [None] * n
    if n < 2:
        return out
    close_vals: List[float] = []
    high_vals: List[float] = []
    low_vals: List[float] = []
    for i in range(n):
        c = _to_float_opt(closes[i] if i < len(closes) else None)
        if c is None or (not math.isfinite(float(c))):
            return out
        cc = float(c)
        close_vals.append(cc)
        h = _to_float_opt(highs[i]) if isinstance(highs, list) and i < len(highs) else None
        l = _to_float_opt(lows[i]) if isinstance(lows, list) and i < len(lows) else None
        hv = float(h) if h is not None and math.isfinite(float(h)) else cc
        lv = float(l) if l is not None and math.isfinite(float(l)) else cc
        high_vals.append(max(hv, lv))
        low_vals.append(min(hv, lv))

    af_step = max(1.0e-6, float(step))
    af_max = max(af_step, float(max_step))
    af = af_step
    uptrend = close_vals[1] >= close_vals[0]
    ep = high_vals[0] if uptrend else low_vals[0]
    sar = low_vals[0] if uptrend else high_vals[0]
    out[0] = float(sar)

    for i in range(1, n):
        sar = float(sar) + (af * (float(ep) - float(sar)))
        if uptrend:
            clamp_1 = low_vals[i - 1]
            clamp_2 = low_vals[i - 2] if i > 1 else low_vals[i - 1]
            sar = min(sar, clamp_1, clamp_2)
            if low_vals[i] < sar:
                uptrend = False
                sar = float(ep)
                ep = low_vals[i]
                af = af_step
            else:
                if high_vals[i] > ep:
                    ep = high_vals[i]
                    af = min(af + af_step, af_max)
        else:
            clamp_1 = high_vals[i - 1]
            clamp_2 = high_vals[i - 2] if i > 1 else high_vals[i - 1]
            sar = max(sar, clamp_1, clamp_2)
            if high_vals[i] > sar:
                uptrend = True
                sar = float(ep)
                ep = high_vals[i]
                af = af_step
            else:
                if low_vals[i] < ep:
                    ep = low_vals[i]
                    af = min(af + af_step, af_max)
        out[i] = float(sar)
    return out


def _roc_condition_hit(
    cond: str,
    *,
    roc: float,
    prev_roc: Optional[float],
    threshold: float,
) -> bool:
    c = _normalize_roc_condition(cond, default="hold")
    if c == "hold":
        return False
    if c == "roc_above_threshold":
        return float(roc) >= float(threshold)
    if c == "roc_below_threshold":
        return float(roc) <= float(threshold)
    if c == "roc_cross_up_zero":
        return prev_roc is not None and float(prev_roc) <= 0.0 and float(roc) > 0.0
    if c == "roc_cross_down_zero":
        return prev_roc is not None and float(prev_roc) >= 0.0 and float(roc) < 0.0
    if c == "roc_cross_up_threshold":
        return prev_roc is not None and float(prev_roc) <= float(threshold) and float(roc) > float(threshold)
    if c == "roc_cross_down_threshold":
        return prev_roc is not None and float(prev_roc) >= float(threshold) and float(roc) < float(threshold)
    if c == "roc_increasing":
        return prev_roc is not None and float(roc) > float(prev_roc)
    if c == "roc_decreasing":
        return prev_roc is not None and float(roc) < float(prev_roc)
    if c == "roc_positive":
        return float(roc) > 0.0
    if c == "roc_negative":
        return float(roc) < 0.0
    if c == "momentum_long":
        return float(roc) > 0.0 and prev_roc is not None and float(roc) > float(prev_roc)
    if c == "momentum_short":
        return float(roc) < 0.0 and prev_roc is not None and float(roc) < float(prev_roc)
    return False


def _sar_condition_hit(
    cond: str,
    *,
    close_now: float,
    close_prev: Optional[float],
    sar_now: float,
    prev_sar: Optional[float],
) -> bool:
    c = _normalize_sar_condition(cond, default="hold")
    if c == "hold":
        return False
    if c == "price_above_sar":
        return float(close_now) > float(sar_now)
    if c == "price_below_sar":
        return float(close_now) < float(sar_now)
    if c == "sar_cross_up":
        return close_prev is not None and prev_sar is not None and float(close_prev) <= float(prev_sar) and float(close_now) > float(sar_now)
    if c == "sar_cross_down":
        return close_prev is not None and prev_sar is not None and float(close_prev) >= float(prev_sar) and float(close_now) < float(sar_now)
    if c == "sar_rising":
        return prev_sar is not None and float(sar_now) > float(prev_sar)
    if c == "sar_falling":
        return prev_sar is not None and float(sar_now) < float(prev_sar)
    if c == "trend_long":
        return float(close_now) > float(sar_now) and prev_sar is not None and float(sar_now) > float(prev_sar)
    if c == "trend_short":
        return float(close_now) < float(sar_now) and prev_sar is not None and float(sar_now) < float(prev_sar)
    return False


def _normalize_donchian_condition(value: Any, *, default: str = "hold") -> str:
    raw = str(value or "").strip().lower()
    aliases = {
        "breakout": "close_above_upper",
        "close_breakout": "close_above_upper",
        "upper_break": "close_above_upper",
        "high_break": "high_above_upper",
        "use_high_break": "high_above_upper",
        "breakdown": "close_below_lower",
        "close_breakdown": "close_below_lower",
        "lower_break": "close_below_lower",
        "low_break": "low_below_lower",
    }
    allowed = {
        "hold",
        "close_above_upper",
        "high_above_upper",
        "close_below_lower",
        "low_below_lower",
        "inside_channel",
    }
    s = aliases.get(raw, raw)
    if not s:
        s = str(default or "hold").strip().lower()
    if s not in allowed:
        s = str(default or "hold").strip().lower()
    return s if s in allowed else "hold"


def _normalize_supertrend_condition(value: Any, *, default: str = "hold") -> str:
    raw = str(value or "").strip().lower()
    aliases = {
        "up": "trend_up",
        "down": "trend_down",
        "bullish": "trend_up",
        "bearish": "trend_down",
        "price_above": "close_above_trend",
        "price_below": "close_below_trend",
        "cross_up": "flip_up",
        "cross_down": "flip_down",
    }
    allowed = {
        "hold",
        "trend_up",
        "trend_down",
        "close_above_trend",
        "close_below_trend",
        "flip_up",
        "flip_down",
    }
    s = aliases.get(raw, raw)
    if not s:
        s = str(default or "hold").strip().lower()
    if s not in allowed:
        s = str(default or "hold").strip().lower()
    return s if s in allowed else "hold"


def _normalize_vwap_condition(value: Any, *, default: str = "hold") -> str:
    raw = str(value or "").strip().lower()
    aliases = {
        "above": "price_above_vwap",
        "below": "price_below_vwap",
        "filter": "within_band",
        "vwap_filter": "within_band",
        "near_vwap": "within_band",
        "not_overextended": "within_band",
        "cross_up": "cross_above",
        "cross_down": "cross_below",
        "sell_below": "exit_below",
    }
    allowed = {
        "hold",
        "price_above_vwap",
        "price_below_vwap",
        "within_band",
        "overextended_above",
        "extended_below",
        "cross_above",
        "cross_below",
        "exit_below",
    }
    s = aliases.get(raw, raw)
    if not s:
        s = str(default or "hold").strip().lower()
    if s not in allowed:
        s = str(default or "hold").strip().lower()
    return s if s in allowed else "hold"


def _normalize_relative_volume_condition(value: Any, *, default: str = "hold") -> str:
    raw = str(value or "").strip().lower()
    aliases = {
        "above": "above_threshold",
        "below": "below_threshold",
        "volume_spike": "above_threshold",
        "spike": "above_threshold",
        "increasing": "rising",
        "decreasing": "falling",
    }
    allowed = {"hold", "above_threshold", "below_threshold", "rising", "falling"}
    s = aliases.get(raw, raw)
    if not s:
        s = str(default or "hold").strip().lower()
    if s not in allowed:
        s = str(default or "hold").strip().lower()
    return s if s in allowed else "hold"


def _indicator_pct_decimal(value: Any, *, default: float) -> float:
    raw = _to_float_opt(value)
    if raw is None:
        return float(default)
    out = float(raw)
    if abs(out) > 1.0:
        out = out / 100.0
    return float(out)


def _market_donchian_channels(
    highs: Optional[List[float]],
    lows: Optional[List[float]],
    lookback: int,
) -> Tuple[List[Optional[float]], List[Optional[float]]]:
    n = min(len(highs or []), len(lows or []))
    upper: List[Optional[float]] = [None] * n
    lower: List[Optional[float]] = [None] * n
    ln = max(1, int(lookback))
    if n <= 1:
        return upper, lower
    try:
        high_vals = [float(v) for v in (highs or [])[:n]]
        low_vals = [float(v) for v in (lows or [])[:n]]
    except Exception:
        return upper, lower
    for i in range(n):
        start = max(0, i - ln)
        if i - start < 1:
            continue
        upper[i] = max(high_vals[start:i])
        lower[i] = min(low_vals[start:i])
    return upper, lower


def _donchian_condition_hit(
    cond: str,
    *,
    close_now: float,
    high_now: Optional[float],
    low_now: Optional[float],
    upper: float,
    lower: float,
) -> bool:
    c = _normalize_donchian_condition(cond, default="hold")
    if c == "close_above_upper":
        return float(close_now) > float(upper)
    if c == "high_above_upper":
        return float(high_now if high_now is not None else close_now) > float(upper)
    if c == "close_below_lower":
        return float(close_now) < float(lower)
    if c == "low_below_lower":
        return float(low_now if low_now is not None else close_now) < float(lower)
    if c == "inside_channel":
        return float(lower) <= float(close_now) <= float(upper)
    return False


def _market_true_range_series(
    highs: List[float],
    lows: List[float],
    closes: List[float],
) -> List[Optional[float]]:
    n = min(len(highs), len(lows), len(closes))
    out: List[Optional[float]] = [None] * n
    if n <= 0:
        return out
    try:
        high_vals = [float(v) for v in highs[:n]]
        low_vals = [float(v) for v in lows[:n]]
        close_vals = [float(v) for v in closes[:n]]
    except Exception:
        return out
    for i in range(n):
        prev_close = close_vals[i - 1] if i > 0 else close_vals[i]
        out[i] = max(
            high_vals[i] - low_vals[i],
            abs(high_vals[i] - prev_close),
            abs(low_vals[i] - prev_close),
        )
    return out


def _market_atr_series(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    length: int,
) -> List[Optional[float]]:
    ln = max(1, int(length))
    tr = _market_true_range_series(highs, lows, closes)
    out: List[Optional[float]] = [None] * len(tr)
    if len(tr) < ln:
        return out
    for i in range(ln - 1, len(tr)):
        window = [v for v in tr[i - ln + 1 : i + 1] if isinstance(v, (int, float))]
        if len(window) == ln:
            out[i] = float(sum(float(v) for v in window) / float(ln))
    return out


def _market_supertrend_series(
    closes: List[float],
    *,
    highs: Optional[List[float]] = None,
    lows: Optional[List[float]] = None,
    atr_length: int = 10,
    multiplier: float = 3.0,
) -> Tuple[List[Optional[float]], List[Optional[float]]]:
    n = len(closes)
    trend: List[Optional[float]] = [None] * n
    direction: List[Optional[float]] = [None] * n
    final_upper: List[Optional[float]] = [None] * n
    final_lower: List[Optional[float]] = [None] * n
    if n <= 0:
        return trend, direction
    try:
        close_vals = [float(v) for v in closes]
        high_vals = [
            float(highs[i]) if isinstance(highs, list) and i < len(highs) else float(close_vals[i])
            for i in range(n)
        ]
        low_vals = [
            float(lows[i]) if isinstance(lows, list) and i < len(lows) else float(close_vals[i])
            for i in range(n)
        ]
    except Exception:
        return trend, direction
    atr_values = _market_atr_series(high_vals, low_vals, close_vals, max(1, int(atr_length)))
    mult = max(0.1, float(multiplier))
    for i in range(n):
        atr_now = atr_values[i]
        if atr_now is None:
            continue
        hl2 = (high_vals[i] + low_vals[i]) / 2.0
        basic_upper = hl2 + (mult * float(atr_now))
        basic_lower = hl2 - (mult * float(atr_now))
        if i == 0 or final_upper[i - 1] is None or final_lower[i - 1] is None:
            final_upper[i] = basic_upper
            final_lower[i] = basic_lower
            direction[i] = 1.0
            trend[i] = final_lower[i]
            continue
        prev_close = close_vals[i - 1]
        prev_upper = float(final_upper[i - 1])
        prev_lower = float(final_lower[i - 1])
        final_upper[i] = basic_upper if basic_upper < prev_upper or prev_close > prev_upper else prev_upper
        final_lower[i] = basic_lower if basic_lower > prev_lower or prev_close < prev_lower else prev_lower
        if close_vals[i] > prev_upper:
            direction[i] = 1.0
        elif close_vals[i] < prev_lower:
            direction[i] = -1.0
        else:
            direction[i] = direction[i - 1] if direction[i - 1] is not None else 1.0
        trend[i] = final_lower[i] if float(direction[i] or 0.0) >= 0.0 else final_upper[i]
    return trend, direction


def _supertrend_condition_hit(
    cond: str,
    *,
    close_now: float,
    trend_now: float,
    direction_now: float,
    direction_prev: Optional[float],
) -> bool:
    c = _normalize_supertrend_condition(cond, default="hold")
    if c == "trend_up":
        return float(direction_now) > 0.0
    if c == "trend_down":
        return float(direction_now) < 0.0
    if c == "close_above_trend":
        return float(close_now) > float(trend_now)
    if c == "close_below_trend":
        return float(close_now) < float(trend_now)
    if c == "flip_up":
        return direction_prev is not None and float(direction_prev) <= 0.0 and float(direction_now) > 0.0
    if c == "flip_down":
        return direction_prev is not None and float(direction_prev) >= 0.0 and float(direction_now) < 0.0
    return False


def _market_timestamp_day(raw: Any) -> str:
    txt = str(raw or "").strip()
    if not txt:
        return "all"
    if txt.isdigit() and len(txt) >= 10:
        try:
            sec = int(txt)
            if sec > 10_000_000_000:
                sec = int(sec / 1000)
            return datetime.fromtimestamp(sec, tz=timezone.utc).date().isoformat()
        except Exception:
            return "all"
    try:
        parsed = datetime.fromisoformat(txt.replace("Z", "+00:00"))
        return parsed.date().isoformat()
    except Exception:
        return txt[:10] if len(txt) >= 10 else "all"


def _market_vwap_series(
    closes: List[float],
    *,
    highs: Optional[List[float]] = None,
    lows: Optional[List[float]] = None,
    volumes: Optional[List[float]] = None,
    timestamps: Optional[List[Any]] = None,
) -> List[Optional[float]]:
    n = len(closes)
    out: List[Optional[float]] = [None] * n
    if n <= 0 or not isinstance(volumes, list) or len(volumes) < n:
        return out
    try:
        close_vals = [float(v) for v in closes]
        high_vals = [
            float(highs[i]) if isinstance(highs, list) and i < len(highs) else float(close_vals[i])
            for i in range(n)
        ]
        low_vals = [
            float(lows[i]) if isinstance(lows, list) and i < len(lows) else float(close_vals[i])
            for i in range(n)
        ]
        volume_vals = [float(volumes[i]) for i in range(n)]
    except Exception:
        return out
    if not any(v > 0.0 and math.isfinite(v) for v in volume_vals):
        return out
    cum_pv = 0.0
    cum_vol = 0.0
    current_day = ""
    for i in range(n):
        day = (
            _market_timestamp_day(timestamps[i])
            if isinstance(timestamps, list) and i < len(timestamps)
            else "all"
        )
        if day != current_day:
            current_day = day
            cum_pv = 0.0
            cum_vol = 0.0
        vol = max(0.0, volume_vals[i])
        if vol > 0.0 and math.isfinite(vol):
            typical = (high_vals[i] + low_vals[i] + close_vals[i]) / 3.0
            cum_pv += typical * vol
            cum_vol += vol
        if cum_vol > 0.0:
            out[i] = cum_pv / cum_vol
    return out


def _vwap_condition_hit(
    cond: str,
    *,
    close_now: float,
    close_prev: Optional[float],
    vwap_now: float,
    vwap_prev: Optional[float],
    max_extension_pct: float,
    max_pullback_pct: float,
    exit_below_pct: float,
) -> bool:
    c = _normalize_vwap_condition(cond, default="hold")
    if c == "price_above_vwap":
        return float(close_now) >= float(vwap_now)
    if c == "price_below_vwap":
        return float(close_now) <= float(vwap_now)
    if c == "within_band":
        return (
            float(close_now) >= float(vwap_now) * (1.0 - float(max_pullback_pct))
            and float(close_now) <= float(vwap_now) * (1.0 + float(max_extension_pct))
        )
    if c == "overextended_above":
        return float(close_now) > float(vwap_now) * (1.0 + float(max_extension_pct))
    if c == "extended_below":
        return float(close_now) < float(vwap_now) * (1.0 - float(max_pullback_pct))
    if c == "cross_above":
        return (
            close_prev is not None
            and vwap_prev is not None
            and float(close_prev) <= float(vwap_prev)
            and float(close_now) > float(vwap_now)
        )
    if c == "cross_below":
        return (
            close_prev is not None
            and vwap_prev is not None
            and float(close_prev) >= float(vwap_prev)
            and float(close_now) < float(vwap_now)
        )
    if c == "exit_below":
        return float(close_now) < float(vwap_now) * (1.0 - float(exit_below_pct))
    return False


def _market_relative_volume_series(volumes: Optional[List[float]], length: int = 20) -> List[Optional[float]]:
    if not isinstance(volumes, list):
        return []
    n = len(volumes)
    ln = max(1, int(length))
    out: List[Optional[float]] = [None] * n
    try:
        vals = [max(0.0, float(v)) for v in volumes]
    except Exception:
        return out
    if n < ln or not any(v > 0.0 for v in vals):
        return out
    running = sum(vals[:ln])
    avg = running / float(ln)
    if avg > 0.0:
        out[ln - 1] = vals[ln - 1] / avg
    for i in range(ln, n):
        running += vals[i] - vals[i - ln]
        avg = running / float(ln)
        if avg > 0.0:
            out[i] = vals[i] / avg
    return out


def _relative_volume_condition_hit(
    cond: str,
    *,
    rvol: float,
    prev_rvol: Optional[float],
    threshold: float,
) -> bool:
    c = _normalize_relative_volume_condition(cond, default="hold")
    if c == "above_threshold":
        return float(rvol) >= float(threshold)
    if c == "below_threshold":
        return float(rvol) <= float(threshold)
    if c == "rising":
        return prev_rvol is not None and float(rvol) > float(prev_rvol)
    if c == "falling":
        return prev_rvol is not None and float(rvol) < float(prev_rvol)
    return False


def _eval_rule(
    rule: Dict[str, Any],
    closes: List[float],
    price: float,
    *,
    opens: Optional[List[float]] = None,
    highs: Optional[List[float]] = None,
    lows: Optional[List[float]] = None,
    volumes: Optional[List[float]] = None,
    timestamps: Optional[List[Any]] = None,
) -> Dict[str, Any]:
    kind_raw = str(rule.get("kind") or "").strip().lower()
    if kind_raw in ("bollinger", "bollinger_bands"):
        kind = "bb"
    elif kind_raw in ("ichimoku", "ichimoku_cloud", "ichi"):
        kind = "ichimoku"
    elif kind_raw in ("ttm", "ttm_squeeze", "squeeze_momentum"):
        kind = "ttm"
    elif kind_raw in ("roc", "rate_of_change"):
        kind = "roc"
    elif kind_raw in ("sar", "psar", "parabolic_sar", "parabolic"):
        kind = "sar"
    elif kind_raw in ("donchian", "donchian_breakout", "donchian_channel", "donchian_channels"):
        kind = "donchian"
    elif kind_raw in ("supertrend", "supertrend_trend"):
        kind = "supertrend"
    elif kind_raw in ("vwap", "vwap_filter"):
        kind = "vwap"
    elif kind_raw in ("relative_volume", "rvol", "rel_volume"):
        kind = "relative_volume"
    else:
        kind = kind_raw
    params = rule.get("params") if isinstance(rule.get("params"), dict) else {}
    out: Dict[str, Any] = {"buy_ok": False, "sell_ok": False, "value": "—", "detail": ""}

    if kind in ("ma", "ema"):
        if _normalize_ma_mode(params.get("mode")) == "ribbon":
            level_values: List[str] = []
            level_details: List[str] = []
            level_actions: List[str] = []
            level_checks: List[Dict[str, Any]] = []
            unavailable: List[str] = []
            base_name = str(rule.get("name") or str(rule.get("kind") or "").upper() or "MA Ribbon")
            parent_rule_id = str(params.get("rule_id") or "").strip()
            for level in _ma_ribbon_levels(params):
                ma_type = str(level["ma_type"])
                length = int(level["length"])
                line_val = _line_value(closes, ma_type=ma_type, length=length)
                line_tag = "EMA" if ma_type == "ema" else "MA"
                if line_val is None:
                    unavailable.append(f"{level['label']} {line_tag}{length}")
                    continue
                if float(price) > float(line_val):
                    action = str(level["above_action"])
                    relation = "above"
                elif float(price) < float(line_val):
                    action = str(level["below_action"])
                    relation = "below"
                else:
                    action = "hold"
                    relation = "equal"
                level_actions.append(action)
                level_value = f"{level['label']} {line_tag}{length}={_fmt(line_val, 3)}"
                level_detail = f"{level['label']} {relation}->{action.upper()}"
                level_values.append(level_value)
                level_details.append(level_detail)
                level_checks.append(
                    {
                        "name": _ma_ribbon_level_name(base_name, level),
                        "_rule_kind": kind,
                        "_rule_params": params,
                        "_rule_id": _ma_ribbon_level_rule_id(parent_rule_id, level.get("slot")),
                        "_ribbon_parent_rule_id": parent_rule_id,
                        "_ribbon_slot": str(level.get("slot") or "").strip().lower(),
                        "buy_ok": action == "buy",
                        "sell_ok": action == "sell",
                        "buy_ignored": False,
                        "sell_ignored": False,
                        "value": level_value,
                        "detail": level_detail,
                    }
                )
            if unavailable:
                out["detail"] = f"Ribbon unavailable: {', '.join(unavailable)}"
                return out
            buy_ok = bool(level_actions) and all(action == "buy" for action in level_actions)
            sell_ok = bool(level_actions) and all(action == "sell" for action in level_actions)
            agreement = "BUY" if buy_ok else ("SELL" if sell_ok else "HOLD")
            out["buy_ok"] = bool(buy_ok)
            out["sell_ok"] = bool(sell_ok)
            out["buy_ignored"] = False
            out["sell_ignored"] = False
            out["value"] = " | ".join(level_values) if level_values else "Ribbon unavailable"
            out["detail"] = "; ".join(level_details) + f" => {agreement} (all levels must agree)"
            out["_ribbon_level_checks"] = level_checks
            return out
        if _normalize_ma_mode(params.get("mode")) == "mapped":
            ma_type = _normalize_ma_type(params.get("ma_type"), "ema" if kind == "ema" else "sma")
            length = max(2, int(_to_int_opt(params.get("length")) or 30))
            line_val = _line_value(closes, ma_type=ma_type, length=length)
            line_tag = "EMA" if ma_type == "ema" else "MA"
            if line_val is None:
                out["detail"] = f"{line_tag}{length} unavailable"
                return out
            if float(price) > float(line_val):
                action = _normalize_ma_action(params.get("above_action"), "hold")
                relation = "above"
            elif float(price) < float(line_val):
                action = _normalize_ma_action(params.get("below_action"), "hold")
                relation = "below"
            else:
                action = "hold"
                relation = "equal"
            out["buy_ok"] = action == "buy"
            out["sell_ok"] = action == "sell"
            out["buy_ignored"] = False
            out["sell_ignored"] = False
            out["value"] = f"{line_tag}{length}={_fmt(line_val, 3)}"
            out["detail"] = f"{relation}->{action.upper()}"
            return out

        ma_type = _normalize_ma_type(params.get("ma_type"), "ema" if kind == "ema" else "sma")
        length = max(2, int(_to_int_opt(params.get("length")) or 30))
        line_val = _line_value(closes, ma_type=ma_type, length=length)
        line_tag = "EMA" if ma_type == "ema" else "MA"
        if line_val is None:
            out["detail"] = f"{line_tag}{length} unavailable"
            return out
        buy_rel = str(params.get("buy_relation") or "hold").strip().lower()
        sell_rel = str(params.get("sell_relation") or "hold").strip().lower()
        if buy_rel == "ignore":
            buy_rel = "hold"
        if sell_rel == "ignore":
            sell_rel = "hold"
        if buy_rel not in ("above", "below", "hold"):
            buy_rel = "hold"
        if sell_rel not in ("above", "below", "hold"):
            sell_rel = "hold"

        def _rel_ok(rel: str, cur: float, ref: float) -> bool:
            if rel == "above":
                return cur > ref
            if rel == "below":
                return cur < ref
            return False

        buy_ignored = buy_rel == "hold"
        sell_ignored = sell_rel == "hold"
        buy_ok = False if buy_ignored else _rel_ok(buy_rel, price, float(line_val))
        sell_ok = False if sell_ignored else _rel_ok(sell_rel, price, float(line_val))

        track_d = bool(int(params.get("track_derivative") or 0))
        d_val = _line_derivative(closes, ma_type=ma_type, length=length) if track_d else None
        buy_d_min = _to_float_opt(params.get("buy_derivative_min"))
        sell_d_max = _to_float_opt(params.get("sell_derivative_max"))
        if track_d:
            if d_val is None:
                if not buy_ignored:
                    buy_ok = False
                if not sell_ignored:
                    sell_ok = False
            else:
                if (not buy_ignored) and buy_d_min is not None:
                    buy_ok = buy_ok and (float(d_val) >= float(buy_d_min))
                if (not sell_ignored) and sell_d_max is not None:
                    sell_ok = sell_ok and (float(d_val) <= float(sell_d_max))

        unless_enabled = bool(int(params.get("unless_enabled") or 0))
        unless_detail = ""
        other_val: Optional[float] = None
        hit = False
        unless_rel = str(params.get("unless_relation") or "above").strip().lower()
        unless_type = _normalize_ma_type(params.get("unless_type"), "sma")
        unless_length = max(2, int(_to_int_opt(params.get("unless_length")) or 30))
        unless_action = str(params.get("unless_action") or "sell").strip().lower()
        if unless_enabled:
            other_val = _line_value(closes, ma_type=unless_type, length=unless_length)
            if other_val is not None:
                if unless_rel == "above":
                    hit = float(line_val) > float(other_val)
                else:
                    hit = float(line_val) < float(other_val)
            if hit:
                if unless_action == "buy":
                    buy_ok = True
                    buy_ignored = False
                    sell_ok = False
                elif unless_action == "sell":
                    buy_ok = False
                    sell_ok = True
                    sell_ignored = False
            utag = "EMA" if unless_type == "ema" else "MA"
            unless_detail = (
                f" unless {line_tag}{length} {unless_rel} {utag}{unless_length}->{unless_action}"
                f" (hit={'yes' if hit else 'no'})"
            )

        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = bool(buy_ignored)
        out["sell_ignored"] = bool(sell_ignored)
        out["value"] = f"{line_tag}{length}={_fmt(line_val, 3)}"
        if unless_enabled:
            utag = "EMA" if unless_type == "ema" else "MA"
            out["value"] += f" | U:{utag}{unless_length}={_fmt(other_val, 3)}"
        if track_d:
            dtag = "dEMA" if ma_type == "ema" else "dMA"
            out["detail"] = f"{dtag}{length}={_fmt(d_val, 4)}{unless_detail}"
        else:
            out["detail"] = unless_detail.strip()
        return out

    if kind == "donchian":
        n = min(len(closes), len(highs or []), len(lows or []))
        if n < 2:
            out["detail"] = "Donchian unavailable (missing OHLC)"
            return out
        lookback = max(1, int(_to_int_opt(params.get("lookback")) or 20))
        buy_cond = _normalize_donchian_condition(
            params.get("buy_condition") or ("high_above_upper" if bool(params.get("use_high_break")) else "close_above_upper"),
            default="hold",
        )
        sell_cond = _normalize_donchian_condition(params.get("sell_condition") or "close_below_lower", default="hold")
        buy_ignored = buy_cond == "hold"
        sell_ignored = sell_cond == "hold"
        upper_vals, lower_vals = _market_donchian_channels(highs[:n] if highs else None, lows[:n] if lows else None, lookback)
        upper_now = upper_vals[-1] if upper_vals else None
        lower_now = lower_vals[-1] if lower_vals else None
        if upper_now is None or lower_now is None:
            out["detail"] = f"Donchian({lookback}) unavailable"
            return out
        close_now = float(_to_float_opt(price) or closes[n - 1])
        high_now = _to_float_opt(highs[n - 1]) if highs and n <= len(highs) else None
        low_now = _to_float_opt(lows[n - 1]) if lows and n <= len(lows) else None
        buy_ok = True if buy_ignored else _donchian_condition_hit(
            buy_cond,
            close_now=close_now,
            high_now=high_now,
            low_now=low_now,
            upper=float(upper_now),
            lower=float(lower_now),
        )
        sell_ok = True if sell_ignored else _donchian_condition_hit(
            sell_cond,
            close_now=close_now,
            high_now=high_now,
            low_now=low_now,
            upper=float(upper_now),
            lower=float(lower_now),
        )
        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = bool(buy_ignored)
        out["sell_ignored"] = bool(sell_ignored)
        out["value"] = f"DON{lookback} U={_fmt(upper_now,4)} L={_fmt(lower_now,4)} P={_fmt(close_now,4)}"
        out["detail"] = f"buy={buy_cond} sell={sell_cond}"
        return out

    if kind == "supertrend":
        atr_length = max(1, int(_to_int_opt(params.get("atr_length")) or 10))
        multiplier = max(0.1, float(_to_float_opt(params.get("multiplier")) or 3.0))
        buy_cond = _normalize_supertrend_condition(params.get("buy_condition") or "trend_up", default="hold")
        sell_cond = _normalize_supertrend_condition(params.get("sell_condition") or "trend_down", default="hold")
        buy_ignored = buy_cond == "hold"
        sell_ignored = sell_cond == "hold"
        trend_vals, direction_vals = _market_supertrend_series(
            closes,
            highs=highs,
            lows=lows,
            atr_length=atr_length,
            multiplier=multiplier,
        )
        trend_now = trend_vals[-1] if trend_vals else None
        direction_now = direction_vals[-1] if direction_vals else None
        direction_prev = direction_vals[-2] if len(direction_vals) > 1 else None
        if trend_now is None or direction_now is None:
            out["detail"] = f"Supertrend({atr_length},{_fmt(multiplier,2)}) unavailable"
            return out
        close_now = float(_to_float_opt(price) or closes[-1])
        buy_ok = True if buy_ignored else _supertrend_condition_hit(
            buy_cond,
            close_now=close_now,
            trend_now=float(trend_now),
            direction_now=float(direction_now),
            direction_prev=direction_prev,
        )
        sell_ok = True if sell_ignored else _supertrend_condition_hit(
            sell_cond,
            close_now=close_now,
            trend_now=float(trend_now),
            direction_now=float(direction_now),
            direction_prev=direction_prev,
        )
        trend_txt = "up" if float(direction_now) > 0.0 else "down"
        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = bool(buy_ignored)
        out["sell_ignored"] = bool(sell_ignored)
        out["value"] = f"ST={_fmt(trend_now,4)} P={_fmt(close_now,4)} trend={trend_txt}"
        out["detail"] = f"buy={buy_cond} sell={sell_cond} atr={atr_length} mult={_fmt(multiplier,3)}"
        return out

    if kind == "vwap":
        if not isinstance(volumes, list) or not any((float(v) > 0.0) for v in volumes if _to_float_opt(v) is not None):
            out["detail"] = "VWAP unavailable: volume data unavailable"
            return out
        n = min(len(closes), len(volumes))
        if n <= 0:
            out["detail"] = "VWAP unavailable"
            return out
        buy_cond = _normalize_vwap_condition(params.get("buy_condition") or "within_band", default="hold")
        sell_cond = _normalize_vwap_condition(params.get("sell_condition") or "exit_below", default="hold")
        buy_ignored = buy_cond == "hold"
        sell_ignored = sell_cond == "hold"
        max_extension_pct = max(0.0, _indicator_pct_decimal(params.get("max_extension_pct"), default=0.015))
        max_pullback_pct = max(0.0, _indicator_pct_decimal(params.get("max_pullback_pct"), default=0.010))
        exit_below_pct = max(0.0, _indicator_pct_decimal(params.get("exit_below_pct"), default=0.012))
        vwap_vals = _market_vwap_series(
            closes[:n],
            highs=highs[:n] if isinstance(highs, list) else None,
            lows=lows[:n] if isinstance(lows, list) else None,
            volumes=volumes[:n],
            timestamps=timestamps[:n] if isinstance(timestamps, list) else None,
        )
        vwap_now = vwap_vals[-1] if vwap_vals else None
        vwap_prev = vwap_vals[-2] if len(vwap_vals) > 1 else None
        if vwap_now is None:
            out["detail"] = "VWAP unavailable: volume data unavailable"
            return out
        close_now = float(_to_float_opt(price) or closes[n - 1])
        close_prev = float(closes[n - 2]) if n >= 2 else None
        buy_ok = True if buy_ignored else _vwap_condition_hit(
            buy_cond,
            close_now=close_now,
            close_prev=close_prev,
            vwap_now=float(vwap_now),
            vwap_prev=vwap_prev,
            max_extension_pct=max_extension_pct,
            max_pullback_pct=max_pullback_pct,
            exit_below_pct=exit_below_pct,
        )
        sell_ok = True if sell_ignored else _vwap_condition_hit(
            sell_cond,
            close_now=close_now,
            close_prev=close_prev,
            vwap_now=float(vwap_now),
            vwap_prev=vwap_prev,
            max_extension_pct=max_extension_pct,
            max_pullback_pct=max_pullback_pct,
            exit_below_pct=exit_below_pct,
        )
        dist_pct = ((close_now - float(vwap_now)) / float(vwap_now) * 100.0) if float(vwap_now) else 0.0
        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = bool(buy_ignored)
        out["sell_ignored"] = bool(sell_ignored)
        out["value"] = f"VWAP={_fmt(vwap_now,4)} P={_fmt(close_now,4)} D={_fmt(dist_pct,3)}%"
        out["detail"] = (
            f"buy={buy_cond} sell={sell_cond} "
            f"pullback={_fmt(max_pullback_pct * 100.0,3)}% "
            f"extension={_fmt(max_extension_pct * 100.0,3)}% "
            f"exit={_fmt(exit_below_pct * 100.0,3)}%"
        )
        return out

    if kind == "relative_volume":
        if not isinstance(volumes, list) or not any((float(v) > 0.0) for v in volumes if _to_float_opt(v) is not None):
            out["detail"] = "RVOL unavailable: volume data unavailable"
            return out
        length = max(1, int(_to_int_opt(params.get("length")) or 20))
        threshold = max(0.0, float(_to_float_opt(params.get("threshold")) or 1.2))
        buy_cond = _normalize_relative_volume_condition(params.get("buy_condition") or "above_threshold", default="hold")
        sell_cond = _normalize_relative_volume_condition(params.get("sell_condition") or "below_threshold", default="hold")
        buy_ignored = buy_cond == "hold"
        sell_ignored = sell_cond == "hold"
        n = min(len(closes), len(volumes))
        rvol_vals = _market_relative_volume_series(volumes[:n], length=length)
        rvol_now = rvol_vals[-1] if rvol_vals else None
        prev_rvol = rvol_vals[-2] if len(rvol_vals) > 1 else None
        if rvol_now is None:
            out["detail"] = f"RVOL({length}) unavailable"
            return out
        buy_ok = True if buy_ignored else _relative_volume_condition_hit(
            buy_cond,
            rvol=float(rvol_now),
            prev_rvol=prev_rvol,
            threshold=threshold,
        )
        sell_ok = True if sell_ignored else _relative_volume_condition_hit(
            sell_cond,
            rvol=float(rvol_now),
            prev_rvol=prev_rvol,
            threshold=threshold,
        )
        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = bool(buy_ignored)
        out["sell_ignored"] = bool(sell_ignored)
        out["value"] = f"RVOL{length}={_fmt(rvol_now,4)}"
        out["detail"] = f"buy={buy_cond} sell={sell_cond} threshold={_fmt(threshold,3)} prev={_fmt(prev_rvol,4)}"
        return out

    if kind == "bb":
        length = max(2, int(_to_int_opt(params.get("length")) or 20))
        std_mult = max(0.1, float(_to_float_opt(params.get("std_mult")) or 2.0))
        buy_cond = _normalize_bb_condition(params.get("buy_condition"), default="hold")
        sell_cond = _normalize_bb_condition(params.get("sell_condition"), default="hold")
        buy_ignored = buy_cond == "hold"
        sell_ignored = sell_cond == "hold"
        squeeze_threshold_pct = float(_to_float_opt(params.get("squeeze_threshold_pct")) or 5.0)
        pb_buy = float(_to_float_opt(params.get("percent_b_buy_threshold")) or 0.2)
        pb_sell = float(_to_float_opt(params.get("percent_b_sell_threshold")) or 0.8)

        curr = _bb_snapshot(closes, length=length, std_mult=std_mult)
        if curr is None:
            out["detail"] = f"BB({length},{_fmt(std_mult,2)}σ) unavailable"
            return out
        prev = _bb_snapshot(closes[:-1], length=length, std_mult=std_mult) if len(closes) >= (length + 1) else None
        prev_close = float(closes[-2]) if len(closes) >= 2 else None
        prev_upper = float(prev["upper"]) if isinstance(prev, dict) else None
        prev_lower = float(prev["lower"]) if isinstance(prev, dict) else None
        prev_width_pct = float(prev["width_pct"]) if isinstance(prev, dict) else None

        close_now = float(_to_float_opt(price) or closes[-1])
        high_now: Optional[float] = None
        low_now: Optional[float] = None
        if isinstance(highs, list) and highs:
            high_now = _to_float_opt(highs[-1])
        if isinstance(lows, list) and lows:
            low_now = _to_float_opt(lows[-1])

        upper = float(curr["upper"])
        lower = float(curr["lower"])
        middle = float(curr["middle"])
        width_pct = float(curr["width_pct"])
        percent_b = float(curr["percent_b"])

        buy_ok = True if buy_ignored else _bb_condition_hit(
            buy_cond,
            close_now=close_now,
            high_now=high_now,
            low_now=low_now,
            upper=upper,
            lower=lower,
            middle=middle,
            prev_close=prev_close,
            prev_upper=prev_upper,
            prev_lower=prev_lower,
            width_pct=width_pct,
            prev_width_pct=prev_width_pct,
            squeeze_threshold_pct=squeeze_threshold_pct,
            percent_b=percent_b,
            percent_b_threshold=pb_buy,
        )
        sell_ok = True if sell_ignored else _bb_condition_hit(
            sell_cond,
            close_now=close_now,
            high_now=high_now,
            low_now=low_now,
            upper=upper,
            lower=lower,
            middle=middle,
            prev_close=prev_close,
            prev_upper=prev_upper,
            prev_lower=prev_lower,
            width_pct=width_pct,
            prev_width_pct=prev_width_pct,
            squeeze_threshold_pct=squeeze_threshold_pct,
            percent_b=percent_b,
            percent_b_threshold=pb_sell,
        )

        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = bool(buy_ignored)
        out["sell_ignored"] = bool(sell_ignored)
        out["value"] = (
            f"BB{length} M={_fmt(middle,3)} U={_fmt(upper,3)} "
            f"L={_fmt(lower,3)} W={_fmt(width_pct,3)}% %B={_fmt(percent_b,3)}"
        )
        out["detail"] = (
            f"buy={buy_cond} sell={sell_cond} "
            f"squeeze<={_fmt(squeeze_threshold_pct,3)}% "
            f"pb_buy={_fmt(pb_buy,3)} pb_sell={_fmt(pb_sell,3)} "
            f"prevW={_fmt(prev_width_pct,3)}%"
        )
        return out

    if kind == "ichimoku":
        tenkan_len = max(1, int(_to_int_opt(params.get("conversion_line_length", params.get("tenkan_length"))) or 9))
        kijun_len = max(1, int(_to_int_opt(params.get("base_line_length", params.get("kijun_length"))) or 26))
        senkou_b_len = max(2, int(_to_int_opt(params.get("leading_span_b_length", params.get("senkou_b_length"))) or 52))
        displacement = max(1, int(_to_int_opt(params.get("lagging_line_displacement", params.get("displacement"))) or 26))
        delayed_cross_lookback = max(1, int(_to_int_opt(params.get("delayed_cross_lookback")) or 3))
        buy_conds = _normalize_ichi_conditions(params.get("buy_conditions", params.get("buy_condition")), default="hold")
        sell_conds = _normalize_ichi_conditions(params.get("sell_conditions", params.get("sell_condition")), default="hold")
        block_conds = _normalize_ichi_conditions(params.get("block_conditions", params.get("block_condition")), default="hold")
        buy_active = [c for c in buy_conds if c != "hold"]
        sell_active = [c for c in sell_conds if c != "hold"]
        block_active = [c for c in block_conds if c != "hold"]
        buy_mode = _normalize_ichi_match_mode(params.get("buy_match_mode"), default="all")
        sell_mode = _normalize_ichi_match_mode(params.get("sell_match_mode"), default="all")
        block_mode = _normalize_ichi_match_mode(params.get("block_match_mode"), default="all")
        buy_ignored = len(buy_active) == 0
        sell_ignored = len(sell_active) == 0
        block_ignored = len(block_active) == 0
        cloud_thickness_threshold_pct = float(_to_float_opt(params.get("cloud_thickness_threshold_pct")) or 1.0)
        kijun_bounce_tolerance_pct = float(_to_float_opt(params.get("base_line_bounce_tolerance_pct", params.get("kijun_bounce_tolerance_pct"))) or 0.35)

        state = _ichimoku_state(
            closes,
            highs=highs,
            lows=lows,
            tenkan_length=tenkan_len,
            kijun_length=kijun_len,
            senkou_b_length=senkou_b_len,
            displacement=displacement,
        )
        if state is None:
            out["detail"] = f"ICHI(Conversion/Base/LeadingB={tenkan_len}/{kijun_len}/{senkou_b_len},disp={displacement}) unavailable"
            return out

        buy_ok = True if buy_ignored else _ichimoku_conditions_hit(
            buy_active,
            state=state,
            cloud_thickness_threshold_pct=cloud_thickness_threshold_pct,
            kijun_bounce_tolerance_pct=kijun_bounce_tolerance_pct,
            delayed_cross_lookback=delayed_cross_lookback,
            mode=buy_mode,
        )
        sell_ok = True if sell_ignored else _ichimoku_conditions_hit(
            sell_active,
            state=state,
            cloud_thickness_threshold_pct=cloud_thickness_threshold_pct,
            kijun_bounce_tolerance_pct=kijun_bounce_tolerance_pct,
            delayed_cross_lookback=delayed_cross_lookback,
            mode=sell_mode,
        )
        block_ok = False if block_ignored else _ichimoku_conditions_hit(
            block_active,
            state=state,
            cloud_thickness_threshold_pct=cloud_thickness_threshold_pct,
            kijun_bounce_tolerance_pct=kijun_bounce_tolerance_pct,
            delayed_cross_lookback=delayed_cross_lookback,
            mode=block_mode,
        )
        if block_ok:
            buy_ok = False
            sell_ok = False
        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = bool(buy_ignored)
        out["sell_ignored"] = bool(sell_ignored)
        out["block_ok"] = bool(block_ok)
        out["block_ignored"] = bool(block_ignored)
        out["value"] = (
            f"ICHI Conversion={_fmt(state.get('tenkan'),3)} Base={_fmt(state.get('kijun'),3)} "
            f"LiveCloudTop={_fmt(state.get('cloud_top'),3)} LiveCloudBottom={_fmt(state.get('cloud_bottom'),3)} "
            f"ProjectedA={_fmt(state.get('future_span_a'),3)} ProjectedB={_fmt(state.get('future_span_b'),3)} "
            f"Thickness={_fmt(state.get('cloud_thickness_pct'),3)}%"
        )
        out["detail"] = (
            f"buy({buy_mode})={'+'.join(buy_active) if buy_active else 'hold'} "
            f"sell({sell_mode})={'+'.join(sell_active) if sell_active else 'hold'} "
            f"block({block_mode})={'+'.join(block_active) if block_active else 'hold'} "
            f"thick_thr={_fmt(cloud_thickness_threshold_pct,3)}% "
            f"base_tol={_fmt(kijun_bounce_tolerance_pct,3)}% "
            f"delay={delayed_cross_lookback} "
            f"lagging_ref={_fmt(state.get('chikou_ref_price'),3)}"
        )
        return out

    if kind == "ttm":
        bb_len = max(2, int(_to_int_opt(params.get("bb_length")) or 20))
        bb_mult = max(0.1, float(_to_float_opt(params.get("bb_mult")) or 2.0))
        kc_len = max(2, int(_to_int_opt(params.get("kc_length")) or 20))
        kc_mult = max(0.1, float(_to_float_opt(params.get("kc_mult")) or 1.5))
        mom_len = max(2, int(_to_int_opt(params.get("momentum_length")) or 20))
        buy_cond = _normalize_ttm_condition(params.get("buy_condition"), default="hold")
        sell_cond = _normalize_ttm_condition(params.get("sell_condition"), default="hold")
        buy_ignored = buy_cond == "hold"
        sell_ignored = sell_cond == "hold"
        state = _ttm_state(
            closes,
            highs=highs,
            lows=lows,
            bb_length=bb_len,
            bb_mult=bb_mult,
            kc_length=kc_len,
            kc_mult=kc_mult,
            momentum_length=mom_len,
        )
        if state is None:
            out["detail"] = f"TTM(BB {bb_len}/{_fmt(bb_mult,2)}, KC {kc_len}/{_fmt(kc_mult,2)}, MOM {mom_len}) unavailable"
            return out
        buy_ok = True if buy_ignored else _ttm_condition_hit(buy_cond, state=state)
        sell_ok = True if sell_ignored else _ttm_condition_hit(sell_cond, state=state)
        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = bool(buy_ignored)
        out["sell_ignored"] = bool(sell_ignored)
        out["value"] = (
            f"TTM SQ={'ON' if bool(state.get('squeeze_on')) else 'OFF'} "
            f"MOM={_fmt(state.get('momentum'),4)} "
            f"BBU={_fmt(state.get('bb_upper'),3)} BBL={_fmt(state.get('bb_lower'),3)} "
            f"KCU={_fmt(state.get('kc_upper'),3)} KCL={_fmt(state.get('kc_lower'),3)}"
        )
        out["detail"] = (
            f"buy={buy_cond} sell={sell_cond} "
            f"prev_mom={_fmt(state.get('prev_momentum'),4)} "
            f"prev_sq={'ON' if bool(state.get('prev_squeeze_on')) else 'OFF'}"
        )
        return out

    if kind == "rsi":
        rsi = _rsi(closes, 14)
        if rsi is None:
            out["detail"] = "RSI unavailable"
            return out
        oversold = _to_float_opt(params.get("oversold"))
        overbought = _to_float_opt(params.get("overbought"))
        os_rel = str(params.get("oversold_relation") or "below").strip().lower()
        ob_rel = str(params.get("overbought_relation") or "above").strip().lower()
        os_action = str(params.get("oversold_action") or "buy").strip().lower()
        ob_action = str(params.get("overbought_action") or "sell").strip().lower()

        def _rel_match(value: float, threshold: float, relation: str) -> bool:
            if relation == "above":
                return value >= threshold
            return value <= threshold

        buy_checks: List[bool] = []
        sell_checks: List[bool] = []
        if oversold is not None and os_action in ("buy", "sell"):
            hit = _rel_match(float(rsi), float(oversold), os_rel)
            if os_action == "buy":
                buy_checks.append(hit)
            else:
                sell_checks.append(hit)
        if overbought is not None and ob_action in ("buy", "sell"):
            hit = _rel_match(float(rsi), float(overbought), ob_rel)
            if ob_action == "buy":
                buy_checks.append(hit)
            else:
                sell_checks.append(hit)

        buy_signal = bool(buy_checks) and all(buy_checks)
        sell_signal = bool(sell_checks) and all(sell_checks)
        out["buy_ok"] = all(buy_checks) if buy_checks else True
        out["sell_ok"] = all(sell_checks) if sell_checks else True
        out["buy_ignored"] = False
        out["sell_ignored"] = False
        out["rsi_buy_signal"] = bool(buy_signal)
        out["rsi_sell_signal"] = bool(sell_signal)
        out["value"] = f"RSI={_fmt(rsi, 2)}"
        out["detail"] = (
            f"OS={_fmt(oversold,2)} {os_rel}->{os_action} "
            f"OB={_fmt(overbought,2)} {ob_rel}->{ob_action}"
        )
        return out

    if kind == "rsi_d":
        drsi = _rsi_derivative(closes, 14)
        if drsi is None:
            out["detail"] = "dRSI unavailable"
            return out
        buy_above = _to_float_opt(params.get("buy_above"))
        sell_below = _to_float_opt(params.get("sell_below"))
        out["buy_ok"] = True if buy_above is None else (float(drsi) >= float(buy_above))
        out["sell_ok"] = True if sell_below is None else (float(drsi) <= float(sell_below))
        out["buy_ignored"] = False
        out["sell_ignored"] = False
        out["value"] = f"dRSI={_fmt(drsi, 4)}"
        out["detail"] = f"buy>={_fmt(buy_above,4)} sell<={_fmt(sell_below,4)}"
        return out

    if kind == "roc":
        length = max(1, int(_to_int_opt(params.get("length")) or 12))
        roc = _roc_value(closes, length)
        if roc is None:
            out["detail"] = f"ROC({length}) unavailable"
            return out
        prev_roc = _roc_value(closes[:-1], length) if len(closes) >= (length + 2) else None
        buy_cond = _normalize_roc_condition(params.get("buy_condition"), default="hold")
        sell_cond = _normalize_roc_condition(params.get("sell_condition"), default="hold")
        buy_thr = float(_to_float_opt(params.get("buy_threshold_pct")) or 0.0)
        sell_thr = float(_to_float_opt(params.get("sell_threshold_pct")) or 0.0)
        buy_ignored = buy_cond == "hold"
        sell_ignored = sell_cond == "hold"
        buy_ok = True if buy_ignored else _roc_condition_hit(
            buy_cond,
            roc=float(roc),
            prev_roc=prev_roc,
            threshold=buy_thr,
        )
        sell_ok = True if sell_ignored else _roc_condition_hit(
            sell_cond,
            roc=float(roc),
            prev_roc=prev_roc,
            threshold=sell_thr,
        )
        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = bool(buy_ignored)
        out["sell_ignored"] = bool(sell_ignored)
        out["value"] = f"ROC{length}={_fmt(roc,4)}%"
        out["detail"] = (
            f"buy={buy_cond} sell={sell_cond} "
            f"buy_thr={_fmt(buy_thr,3)}% sell_thr={_fmt(sell_thr,3)}% "
            f"prev={_fmt(prev_roc,4)}%"
        )
        return out

    if kind == "sar":
        step = max(0.0001, float(_to_float_opt(params.get("step")) or 0.02))
        max_step = max(step, float(_to_float_opt(params.get("max_step")) or 0.2))
        buy_cond = _normalize_sar_condition(params.get("buy_condition"), default="hold")
        sell_cond = _normalize_sar_condition(params.get("sell_condition"), default="hold")
        buy_ignored = buy_cond == "hold"
        sell_ignored = sell_cond == "hold"
        sar_vals = _sar_series(closes, highs=highs, lows=lows, step=step, max_step=max_step)
        sar_now = sar_vals[-1] if sar_vals else None
        prev_sar = sar_vals[-2] if len(sar_vals) > 1 else None
        close_now = _to_float_opt(price)
        if close_now is None:
            close_now = _to_float_opt(closes[-1] if closes else None)
        close_prev = _to_float_opt(closes[-2] if len(closes) >= 2 else None)
        if sar_now is None or close_now is None:
            out["detail"] = f"SAR(step={_fmt(step,4)},max={_fmt(max_step,4)}) unavailable"
            return out
        buy_ok = True if buy_ignored else _sar_condition_hit(
            buy_cond,
            close_now=float(close_now),
            close_prev=close_prev,
            sar_now=float(sar_now),
            prev_sar=prev_sar,
        )
        sell_ok = True if sell_ignored else _sar_condition_hit(
            sell_cond,
            close_now=float(close_now),
            close_prev=close_prev,
            sar_now=float(sar_now),
            prev_sar=prev_sar,
        )
        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = bool(buy_ignored)
        out["sell_ignored"] = bool(sell_ignored)
        out["value"] = (
            f"SAR={_fmt(sar_now,4)} P={_fmt(close_now,4)} prevSAR={_fmt(prev_sar,4)}"
        )
        out["detail"] = (
            f"buy={buy_cond} sell={sell_cond} step={_fmt(step,4)} max={_fmt(max_step,4)}"
        )
        return out

    if kind == "macd":
        fast = max(2, int(_to_int_opt(params.get("fast_length")) or 12))
        slow = max(2, int(_to_int_opt(params.get("slow_length")) or 26))
        signal_len = max(2, int(_to_int_opt(params.get("signal_length")) or 9))
        mode = str(params.get("mode") or "signal_cross").strip().lower()
        macd_s, sig_s, hist_s = _macd_series(closes, fast_len=fast, slow_len=slow, signal_len=signal_len)
        if len(macd_s) < 2:
            out["detail"] = "MACD unavailable"
            return out
        m0 = macd_s[-1]
        m1 = macd_s[-2] if len(macd_s) > 1 else None
        s0 = sig_s[-1]
        s1 = sig_s[-2] if len(sig_s) > 1 else None
        h0 = hist_s[-1]
        h1 = hist_s[-2] if len(hist_s) > 1 else None
        if None in (m0, m1, s0, s1, h0, h1):
            out["detail"] = "MACD warmup incomplete"
            return out
        m0f = float(m0)
        m1f = float(m1)
        s0f = float(s0)
        s1f = float(s1)
        h0f = float(h0)
        h1f = float(h1)

        bull_cross = m1f <= s1f and m0f > s0f
        bear_cross = m1f >= s1f and m0f < s0f

        buy_ok = False
        sell_ok = False
        buy_ignored = False
        sell_ignored = False
        derivative_buy_above_raw = _to_float_opt(params.get("derivative_buy_above"))
        derivative_sell_below_raw = _to_float_opt(params.get("derivative_sell_below"))
        derivative_scope = str(params.get("derivative_signal_scope") or "both").strip().lower()
        if derivative_scope not in ("both", "buy", "sell"):
            derivative_scope = "both"
        derivative_buy_above = float(derivative_buy_above_raw) if derivative_buy_above_raw is not None else 0.0
        derivative_sell_below = float(derivative_sell_below_raw) if derivative_sell_below_raw is not None else 0.0
        if mode == "signal_cross":
            buy_ok = m0f > s0f
            sell_ok = m0f < s0f
        elif mode == "cross_regime":
            buy_ok = bull_cross and (m0f > 0.0)
            sell_ok = bear_cross and (m0f < 0.0)
        elif mode == "hist_momentum":
            buy_ok = (h0f > 0.0) and (h0f > h1f)
            sell_ok = (h0f < 0.0) and (h0f < h1f)
        elif mode == "zero_reclaim_loss":
            buy_ok = (m0f > 0.0) and (m0f > s0f)
            sell_ok = (m0f > 0.0) and (s0f > 0.0) and (m0f < s0f)
        elif mode == "macd_derivative_sign":
            dmacd = m0f - m1f
            buy_ignored = derivative_scope == "sell"
            sell_ignored = derivative_scope == "buy"
            buy_ok = (not buy_ignored) and (dmacd > derivative_buy_above)
            sell_ok = (not sell_ignored) and (dmacd < derivative_sell_below)
        else:
            buy_ok = bull_cross
            sell_ok = bear_cross

        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = bool(buy_ignored)
        out["sell_ignored"] = bool(sell_ignored)
        out["macd_buy_signal"] = bool((not buy_ignored) and buy_ok)
        out["macd_sell_signal"] = bool((not sell_ignored) and sell_ok)
        if mode == "macd_derivative_sign":
            out["value"] = f"dMACD={_fmt(m0f - m1f, 4)}"
            out["detail"] = (
                f"{mode} ({fast}/{slow}/{signal_len}) "
                f"buy>{_fmt(derivative_buy_above,4)} "
                f"sell<{_fmt(derivative_sell_below,4)} "
                f"side={derivative_scope}"
            )
        else:
            out["value"] = f"MACD={_fmt(m0f,4)} SIG={_fmt(s0f,4)} HIST={_fmt(h0f,4)}"
            out["detail"] = f"{mode} ({fast}/{slow}/{signal_len})"
        return out

    if kind in ("heikin_ashi", "ha"):
        if not isinstance(opens, list) or not isinstance(highs, list) or not isinstance(lows, list):
            out["detail"] = "Heikin Ashi unavailable (missing OHLC)"
            return out
        n = min(len(opens), len(highs), len(lows), len(closes))
        if n < 2:
            out["detail"] = "Heikin Ashi unavailable"
            return out
        ha_open, _ha_high, _ha_low, ha_close = _heikin_ashi_series(
            opens[:n], highs[:n], lows[:n], closes[:n]
        )
        if len(ha_close) < 2:
            out["detail"] = "Heikin Ashi unavailable"
            return out
        mode = str(params.get("mode") or "transition").strip().lower()
        if mode not in ("transition", "state"):
            mode = "transition"
        doji_tol = max(0.0, float(_to_float_opt(params.get("doji_tolerance_pct")) or 0.0))
        prev_state = _ha_candle_state(ha_open[-2], ha_close[-2], doji_tolerance_pct=doji_tol)
        curr_state = _ha_candle_state(ha_open[-1], ha_close[-1], doji_tolerance_pct=doji_tol)
        if mode == "state":
            buy_ok = curr_state == "bullish"
            sell_ok = curr_state == "bearish"
        else:
            buy_ok = prev_state == "bearish" and curr_state == "bullish"
            sell_ok = prev_state == "bullish" and curr_state == "bearish"
        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = False
        out["sell_ignored"] = False
        out["ha_buy_signal"] = bool(buy_ok)
        out["ha_sell_signal"] = bool(sell_ok)
        out["value"] = f"HA_O={_fmt(ha_open[-1],4)} HA_C={_fmt(ha_close[-1],4)}"
        out["detail"] = f"{mode} prev={prev_state} curr={curr_state} doji_tol={_fmt(doji_tol,3)}%"
        return out

    out["detail"] = "unsupported rule kind"
    return out


def _build_chart_series(
    prices: List[float],
    *,
    opens: Optional[List[float]] = None,
    highs: Optional[List[float]] = None,
    lows: Optional[List[float]] = None,
    volumes: Optional[List[float]] = None,
    timestamps: Optional[List[Any]] = None,
    max_points: int = CHART_POINTS,
) -> Dict[str, List[Any]]:
    if not prices:
        return {}
    clean_prices: List[float] = []
    for raw in prices:
        try:
            v = float(raw)
        except Exception:
            continue
        if not np.isfinite(v) or v <= 0:
            continue
        clean_prices.append(v)
    if len(clean_prices) < 2:
        return {}
    prices = clean_prices

    def _ma_series(src: List[float], window: int) -> List[Optional[float]]:
        out: List[Optional[float]] = [None] * len(src)
        if len(src) < window:
            return out
        s = float(sum(src[:window]))
        out[window - 1] = s / float(window)
        for i in range(window, len(src)):
            s += float(src[i]) - float(src[i - window])
            out[i] = s / float(window)
        return out

    ma20 = _ma_series(prices, 20)
    ma78 = _ma_series(prices, 78)
    ma190 = _ma_series(prices, 190)
    chart_opens = [float(v) for v in opens] if isinstance(opens, list) and len(opens) == len(prices) else None
    chart_highs = [float(v) for v in highs] if isinstance(highs, list) and len(highs) == len(prices) else None
    chart_lows = [float(v) for v in lows] if isinstance(lows, list) and len(lows) == len(prices) else None
    chart_volumes = [float(v) for v in volumes] if isinstance(volumes, list) and len(volumes) == len(prices) else None
    chart_timestamps = list(timestamps) if isinstance(timestamps, list) and len(timestamps) == len(prices) else None

    def _payload(start: int = 0) -> Dict[str, List[Any]]:
        payload: Dict[str, List[Any]] = {
            "price": [float(p) for p in prices[start:]],
            "ma20": ma20[start:],
            "ma78": ma78[start:],
            "ma150": ma190[start:],
        }
        if chart_opens is not None and chart_highs is not None and chart_lows is not None:
            payload["open"] = chart_opens[start:]
            payload["high"] = chart_highs[start:]
            payload["low"] = chart_lows[start:]
        if chart_volumes is not None:
            payload["volume"] = chart_volumes[start:]
        if chart_timestamps is not None:
            payload["timestamp"] = chart_timestamps[start:]
        return payload

    if max_points > 0 and len(prices) > max_points:
        o = len(prices) - max_points
        return _payload(o)
    return _payload(0)


def _atr_from_historicals(rows: List[Dict[str, Any]], period: int = 14) -> Optional[float]:
    if not isinstance(rows, list) or len(rows) < period + 1:
        return None
    trs: List[float] = []
    prev_close: Optional[float] = None
    for row in rows:
        try:
            h = float(row.get("high") if row.get("high") is not None else row.get("high_price"))
            l = float(row.get("low") if row.get("low") is not None else row.get("low_price"))
            c = float(row.get("close") if row.get("close") is not None else row.get("close_price"))
        except Exception:
            continue
        if prev_close is None:
            tr = h - l
        else:
            tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        trs.append(float(tr))
        prev_close = c
    if len(trs) < period:
        return None
    return float(sum(trs[-period:]) / float(period))


def _round_to_cents(value: float) -> float:
    return max(0.01, round(float(value), 2))


def _round_up_to_cents(value: float) -> float:
    try:
        rounded = Decimal(str(float(value))).quantize(Decimal("0.01"), rounding=ROUND_CEILING)
        return max(0.01, float(rounded))
    except (InvalidOperation, ValueError, TypeError):
        return 0.01


def _resolve_trail_amount(
    *,
    trailing_stop_mode: str,
    trailing_stop_amount: float,
    trailing_stop_atr_mult: float,
    atr: Optional[float],
) -> Optional[float]:
    trail_amount = float(trailing_stop_amount)
    if str(trailing_stop_mode or "").strip().lower() == "atr":
        if atr is None or atr <= 0:
            return None
        trail_amount = float(atr) * float(trailing_stop_atr_mult)
    return _round_to_cents(trail_amount)


def _can_sell_without_loss(current_price: float, avg_buy_price: float) -> bool:
    if current_price <= 0 or avg_buy_price <= 0:
        return False
    return float(current_price) >= float(avg_buy_price)


def _order_success(resp: httpx.Response) -> bool:
    return resp.status_code < 400


def _record_trade(
    stats: Dict[str, Any],
    *,
    side: str,
    qty: float,
    price: float,
    avg_buy_price: float,
) -> None:
    stats["trades"] = int(stats.get("trades", 0)) + 1
    if side == "sell" and avg_buy_price > 0 and qty > 0:
        profit = (price - avg_buy_price) * qty
        if profit > 0:
            stats["pnl"] = float(stats.get("pnl", 0.0)) + profit


def _default_rules() -> List[Dict[str, Any]]:
    return [
        {"name": "MA30 Trend", "kind": "ma", "params": {"length": 30, "buy_relation": "above", "sell_relation": "below"}},
        {"name": "MA78 Guard", "kind": "ma", "params": {"length": 78, "buy_relation": "below", "sell_relation": "above"}},
        {"name": "MA190 Trend", "kind": "ma", "params": {"length": 190, "buy_relation": "below", "sell_relation": "above"}},
        {
            "name": "RSI Rule",
            "kind": "rsi",
            "params": {
                "oversold": 30,
                "overbought": 70,
                "oversold_relation": "below",
                "oversold_action": "buy",
                "overbought_relation": "above",
                "overbought_action": "sell",
            },
        },
        {"name": "RSI Derivative", "kind": "rsi_d", "params": {"buy_above": 0.0, "sell_below": 0.0}},
    ]


def _normalize_inline_rules(obj: Any) -> List[Dict[str, Any]]:
    if isinstance(obj, str):
        try:
            obj = json.loads(obj)
        except Exception:
            return []
    if not isinstance(obj, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in obj:
        if not isinstance(item, dict):
            continue
        kind_raw = str(item.get("kind") or "").strip().lower()
        if kind_raw in ("bollinger", "bollinger_bands"):
            kind = "bb"
        elif kind_raw in ("ichimoku", "ichimoku_cloud", "ichi"):
            kind = "ichimoku"
        elif kind_raw in ("ttm", "ttm_squeeze", "squeeze_momentum"):
            kind = "ttm"
        elif kind_raw in ("roc", "rate_of_change"):
            kind = "roc"
        elif kind_raw in ("sar", "psar", "parabolic_sar", "parabolic"):
            kind = "sar"
        elif kind_raw in ("donchian", "donchian_breakout", "donchian_channel", "donchian_channels"):
            kind = "donchian"
        elif kind_raw in ("supertrend", "supertrend_trend"):
            kind = "supertrend"
        elif kind_raw in ("vwap", "vwap_filter"):
            kind = "vwap"
        elif kind_raw in ("relative_volume", "rvol", "rel_volume"):
            kind = "relative_volume"
        else:
            kind = kind_raw
        if kind not in (
            "ma",
            "ema",
            "rsi",
            "rsi_d",
            "macd",
            "heikin_ashi",
            "ha",
            "bb",
            "ichimoku",
            "ttm",
            "roc",
            "sar",
            "donchian",
            "supertrend",
            "vwap",
            "relative_volume",
        ):
            continue
        params = item.get("params") if isinstance(item.get("params"), dict) else {}
        rule = {
            "name": str(item.get("name") or kind.upper()),
            "kind": kind,
            "params": params,
        }
        tf_raw = item.get("timeframe")
        if tf_raw in (None, "", "None"):
            tf_raw = params.get("timeframe") if isinstance(params, dict) else None
        tf = _normalize_rule_timeframe(tf_raw, default="")
        if tf:
            rule["timeframe"] = tf
        out.append(rule)
    return out


def _normalize_rule_timeframe(value: Any, default: str = "1h") -> str:
    txt = str(value or "").strip().lower()
    aliases = {
        "1min": "1m",
        "1minute": "1m",
        "5min": "5m",
        "5minute": "5m",
        "10min": "10m",
        "10minute": "10m",
        "15min": "15m",
        "15minute": "15m",
        "30min": "30m",
        "30minute": "30m",
        "60m": "1h",
        "60min": "1h",
        "1hr": "1h",
        "1hour": "1h",
        "hour": "1h",
        "daily": "1d",
        "day": "1d",
    }
    txt = aliases.get(txt, txt)
    if txt in TIMEFRAMES:
        return txt
    fallback = str(default or "").strip().lower()
    fallback = aliases.get(fallback, fallback)
    return fallback if fallback in TIMEFRAMES else ""


def _rule_timeframe(rule: Dict[str, Any], default_timeframe: str) -> str:
    params = rule.get("params") if isinstance(rule.get("params"), dict) else {}
    tf_raw = rule.get("timeframe")
    if tf_raw in (None, "", "None"):
        tf_raw = params.get("timeframe") if isinstance(params, dict) else None
    return _normalize_rule_timeframe(tf_raw, default=default_timeframe) or default_timeframe


def _rules_with_default_timeframe(rules: List[Dict[str, Any]], default_timeframe: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        r = dict(rule)
        r["timeframe"] = _rule_timeframe(r, default_timeframe)
        out.append(r)
    return out


def _rules_by_timeframe(rules: List[Dict[str, Any]], default_timeframe: str) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for rule in _rules_with_default_timeframe(rules, default_timeframe):
        grouped.setdefault(_rule_timeframe(rule, default_timeframe), []).append(rule)
    return grouped


def _resolve_rules(_db_path: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    inline_rules = _normalize_inline_rules(params.get("indicator_rules_json"))
    if inline_rules:
        return inline_rules
    return _default_rules()


def _rule_min_candles(rules: List[Dict[str, Any]]) -> int:
    longest_ma = 0
    longest_ema = 0
    longest_dma = 0
    longest_dema = 0
    longest_macd = 0
    longest_bb = 0
    longest_ichimoku = 0
    longest_ttm = 0
    longest_roc = 0
    longest_sar = 0
    longest_donchian = 0
    longest_supertrend = 0
    longest_rvol = 0
    need_rsi = False
    need_drsi = False
    need_ha = False
    need_vwap = False

    for rule in rules:
        if not isinstance(rule, dict):
            continue
        kind_raw = str(rule.get("kind") or "").strip().lower()
        if kind_raw in ("bollinger", "bollinger_bands"):
            kind = "bb"
        elif kind_raw in ("ichimoku", "ichimoku_cloud", "ichi"):
            kind = "ichimoku"
        elif kind_raw in ("ttm", "ttm_squeeze", "squeeze_momentum"):
            kind = "ttm"
        elif kind_raw in ("roc", "rate_of_change"):
            kind = "roc"
        elif kind_raw in ("sar", "psar", "parabolic_sar", "parabolic"):
            kind = "sar"
        elif kind_raw in ("donchian", "donchian_breakout", "donchian_channel", "donchian_channels"):
            kind = "donchian"
        elif kind_raw in ("supertrend", "supertrend_trend"):
            kind = "supertrend"
        elif kind_raw in ("vwap", "vwap_filter"):
            kind = "vwap"
        elif kind_raw in ("relative_volume", "rvol", "rel_volume"):
            kind = "relative_volume"
        else:
            kind = kind_raw
        params = rule.get("params") if isinstance(rule.get("params"), dict) else {}
        if kind in ("ma", "ema"):
            if _normalize_ma_mode(params.get("mode")) == "ribbon":
                for level in _ma_ribbon_levels(params):
                    ln = level["length"]
                    if level["ma_type"] == "ema":
                        longest_ema = max(longest_ema, ln)
                    else:
                        longest_ma = max(longest_ma, ln)
            else:
                ln = max(2, int(_to_int_opt(params.get("length")) or 30))
                ma_type = str(params.get("ma_type") or ("ema" if kind == "ema" else "sma")).strip().lower()
                if ma_type == "ema":
                    longest_ema = max(longest_ema, ln)
                else:
                    longest_ma = max(longest_ma, ln)
                track_d = bool(int(params.get("track_derivative") or 0))
                if track_d:
                    if ma_type == "ema":
                        longest_dema = max(longest_dema, ln)
                    else:
                        longest_dma = max(longest_dma, ln)
                unless_enabled = bool(int(params.get("unless_enabled") or 0))
                if unless_enabled:
                    ulen = max(2, int(_to_int_opt(params.get("unless_length")) or 30))
                    utype = str(params.get("unless_type") or "sma").strip().lower()
                    if utype == "ema":
                        longest_ema = max(longest_ema, ulen)
                    else:
                        longest_ma = max(longest_ma, ulen)
        elif kind == "rsi":
            need_rsi = True
        elif kind == "rsi_d":
            need_drsi = True
        elif kind == "macd":
            fast = max(2, int(_to_int_opt(params.get("fast_length")) or 12))
            slow = max(2, int(_to_int_opt(params.get("slow_length")) or 26))
            signal = max(2, int(_to_int_opt(params.get("signal_length")) or 9))
            longest_macd = max(longest_macd, max(fast, slow) + signal + 2)
        elif kind == "bb":
            ln = max(2, int(_to_int_opt(params.get("length")) or 20))
            longest_bb = max(longest_bb, ln + 1)
        elif kind == "ichimoku":
            tenkan_len = max(1, int(_to_int_opt(params.get("conversion_line_length", params.get("tenkan_length"))) or 9))
            kijun_len = max(1, int(_to_int_opt(params.get("base_line_length", params.get("kijun_length"))) or 26))
            senkou_b_len = max(2, int(_to_int_opt(params.get("leading_span_b_length", params.get("senkou_b_length"))) or 52))
            displacement = max(1, int(_to_int_opt(params.get("lagging_line_displacement", params.get("displacement"))) or 26))
            longest_ichimoku = max(
                longest_ichimoku,
                max(tenkan_len, kijun_len, senkou_b_len) + displacement + 1,
            )
        elif kind == "ttm":
            bb_len = max(2, int(_to_int_opt(params.get("bb_length")) or 20))
            kc_len = max(2, int(_to_int_opt(params.get("kc_length")) or 20))
            mom_len = max(2, int(_to_int_opt(params.get("momentum_length")) or 20))
            longest_ttm = max(longest_ttm, max(bb_len, kc_len, mom_len) + 1)
        elif kind == "roc":
            ln = max(1, int(_to_int_opt(params.get("length")) or 12))
            longest_roc = max(longest_roc, ln + 2)
        elif kind == "sar":
            longest_sar = max(longest_sar, 3)
        elif kind == "donchian":
            ln = max(1, int(_to_int_opt(params.get("lookback")) or 20))
            longest_donchian = max(longest_donchian, ln + 2)
        elif kind == "supertrend":
            atr_len = max(1, int(_to_int_opt(params.get("atr_length")) or 10))
            longest_supertrend = max(longest_supertrend, atr_len + 3)
        elif kind == "vwap":
            need_vwap = True
        elif kind == "relative_volume":
            ln = max(1, int(_to_int_opt(params.get("length")) or 20))
            longest_rvol = max(longest_rvol, ln + 1)
        elif kind in ("heikin_ashi", "ha"):
            need_ha = True

    return max(
        30,
        longest_ma,
        longest_ema,
        longest_dma + 1,
        longest_dema + 1,
        longest_macd,
        longest_bb,
        longest_ichimoku,
        longest_ttm,
        longest_roc,
        longest_sar,
        longest_donchian,
        longest_supertrend,
        longest_rvol,
        15 if need_rsi else 0,
        17 if need_drsi else 0,
        2 if need_ha else 0,
        2 if need_vwap else 0,
    )


def check_stoploss_and_sell(
    *,
    symbol: str,
    current_price: float,
    avg_buy_price: float,
    held_qty: float,
    target_gain_pct: float,
    stop_loss_pct: float,
    session_state: str,
    limit_mid: Optional[float],
    allow_extended_hours_orders: bool,
    allow_seamless_overnight_orders: bool,
    trade_stats: Optional[Dict[str, Any]] = None,
) -> None:
    if avg_buy_price <= 0:
        return
    session_norm = str(session_state or "").strip().lower()
    if session_norm not in ("regular", "extended", "overnight", "closed"):
        session_norm = "closed"

    percentage_gain = ((current_price - avg_buy_price) / avg_buy_price) * 100.0
    if symbol not in stoploss_state:
        stoploss_state[symbol] = {"armed": False}

    if not stoploss_state[symbol]["armed"] and percentage_gain >= target_gain_pct:
        stoploss_state[symbol]["armed"] = True
        print(f"Stop-loss armed for {symbol} at gain {percentage_gain:.2f}%.")

    if stoploss_state[symbol]["armed"]:
        trigger_price = avg_buy_price * (1.0 + (stop_loss_pct / 100.0))
        if current_price <= trigger_price:
            if session_norm == "closed":
                # Keep armed while market is closed; execute when a tradable session opens.
                print(f"[{symbol}] Stop-loss trigger hit while market closed; keeping stop-loss armed.")
                return
            if session_norm == "extended" and not bool(allow_extended_hours_orders):
                print(f"[{symbol}] Stop-loss trigger hit in extended hours, but extended-hours orders are disabled.")
                return
            if session_norm == "overnight" and not bool(allow_seamless_overnight_orders):
                print(f"[{symbol}] Stop-loss trigger hit overnight, but overnight orders are disabled.")
                return
            if not _can_sell_without_loss(current_price, avg_buy_price):
                stoploss_state[symbol]["armed"] = False
                print(
                    f"[{symbol}] Stop-loss trigger hit, but no-loss rule blocked sell "
                    f"({current_price:.2f} < {avg_buy_price:.2f}). Disarming stop-loss."
                )
                return
            sell_qty = int(held_qty)
            if sell_qty <= 0:
                stoploss_state[symbol]["armed"] = False
                return
            try:
                session_tag = _order_session_for_state(session_state)
                if session_norm in ("extended", "overnight"):
                    if session_norm == "overnight" and str(symbol).strip().upper() in _SEAMLESS_UNSUPPORTED_SYMBOLS:
                        print(f"[{symbol}] Overnight stop-loss skipped: symbol marked SEAMLESS-unsupported for this run.")
                        return
                    limit_price = limit_mid if limit_mid and limit_mid > 0 else current_price
                    resp = place_limit_with_session_fallback(
                        symbol=symbol,
                        qty=float(sell_qty),
                        side="sell",
                        price=float(limit_price),
                        session_state=session_norm,
                        session_tag=session_tag,
                        allow_seamless_fallback=bool(allow_seamless_overnight_orders),
                    )
                elif session_norm == "regular":
                    resp = place_market_sell(symbol, float(sell_qty), session_tag)
                else:
                    print(f"Market closed; skipping stop-loss sell for {symbol}.")
                    return
                if _order_success(resp):
                    print(f"Stop-loss SELL executed for {symbol}: {sell_qty} shares.")
                    if trade_stats is not None:
                        _record_trade(
                            trade_stats,
                            side="sell",
                            qty=float(sell_qty),
                            price=current_price,
                            avg_buy_price=avg_buy_price,
                        )
                    stoploss_state[symbol]["armed"] = False
            except Exception as e:
                print(f"[{symbol}] Stop-loss sell failed: {e}")


def _print_rule_checks(
    symbol: str,
    checks: List[Dict[str, Any]],
    rule_signal: str,
    execution_signal: str,
    execution_hold_reason: str = "",
) -> None:
    if str(rule_signal).upper() == str(execution_signal).upper():
        print(f"[{symbol}] Signal: {execution_signal}")
    elif execution_hold_reason:
        print(
            f"[{symbol}] Signal: {rule_signal} "
            f"(execution: {execution_signal}; {execution_hold_reason})"
        )
    else:
        print(f"[{symbol}] Signal: {rule_signal} (execution: {execution_signal})")
    for c in checks:
        name = str(c.get("name") or "")
        val = str(c.get("value") or "—")
        detail = str(c.get("detail") or "")
        b = "Y" if c.get("buy_ok") else "N"
        s = "Y" if c.get("sell_ok") else "N"
        print(f"  - {name}: {val} | buy={b} sell={s} {detail}".rstrip())


def _normalize_rule_name_list(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        return [s.strip() for s in raw.split(",") if s.strip()]
    if isinstance(raw, list):
        out: List[str] = []
        for item in raw:
            txt = str(item or "").strip()
            if txt:
                out.append(txt)
        return out
    return []


def _apply_rsi_signal_overrides(checks: List[Dict[str, Any]]) -> None:
    if not checks:
        return
    def _target_matches(item: Any, target_exact: set[str], target_name_set: set[str]) -> bool:
        if not isinstance(item, dict):
            return False
        item_name = str(item.get("name") or "").strip()
        item_id = str(item.get("_rule_id") or "").strip()
        return bool(item_id and item_id in target_exact) or bool(item_name and item_name.upper() in target_name_set)

    def _apply_override_to_item(
        item: Dict[str, Any],
        *,
        apply_buy: bool,
        apply_sell: bool,
        scope: str,
        forced_side: str,
        source_tag: str,
        source_name: str,
    ) -> None:
        if apply_buy:
            item["buy_ok"] = True
            if scope == "both":
                item["sell_ok"] = False
        if apply_sell:
            item["buy_ok"] = False
            item["sell_ok"] = True
        item["_override_applied"] = True
        item["_override_scope"] = scope
        item["_override_forced_side"] = forced_side
        item["_override_source_tag"] = source_tag
        item["_override_source_name"] = source_name
        note = f"{source_tag} override({scope})->{forced_side} by {source_name}"
        detail = str(item.get("detail") or "")
        if note not in detail:
            item["detail"] = f"{detail} | {note}".strip(" |")

    def _effective_state(item: Any) -> str:
        if not isinstance(item, dict):
            return "HOLD"
        forced_side = str(item.get("_override_forced_side") or "").strip().upper()
        if forced_side in ("BUY", "SELL") and bool(item.get("_override_applied")):
            return forced_side
        buy_ignored = bool(item.get("buy_ignored"))
        sell_ignored = bool(item.get("sell_ignored"))
        if bool(item.get("sell_ok")) and not sell_ignored:
            return "SELL"
        if bool(item.get("buy_ok")) and not buy_ignored:
            return "BUY"
        return "HOLD"

    for src in checks:
        src_kind = str(src.get("_rule_kind") or "").strip().lower()
        if src_kind == "ha":
            src_kind = "heikin_ashi"
        if src_kind not in ("rsi", "macd", "heikin_ashi"):
            continue
        params = src.get("_rule_params")
        if not isinstance(params, dict):
            params = {}
        if not _to_bool(params.get("signal_override_enabled", False), False):
            continue

        targets = _normalize_rule_name_list(params.get("signal_override_targets"))
        target_exact = {str(t).strip() for t in targets if str(t).strip()}
        target_name_set = {str(t).strip().upper() for t in targets if str(t).strip()}
        if not target_exact and not target_name_set:
            continue
        scope = str(params.get("signal_override_scope") or "both").strip().lower()
        if scope not in ("both", "buy", "sell"):
            scope = "both"

        if src_kind == "rsi":
            buy_signal = bool(src.get("rsi_buy_signal"))
            sell_signal = bool(src.get("rsi_sell_signal"))
            source_tag = "RSI"
        elif src_kind == "macd":
            buy_signal = bool(src.get("macd_buy_signal"))
            sell_signal = bool(src.get("macd_sell_signal"))
            source_tag = "MACD"
        else:
            buy_signal = bool(src.get("ha_buy_signal")) or (
                bool(src.get("buy_ok")) and not bool(src.get("buy_ignored"))
            )
            sell_signal = bool(src.get("ha_sell_signal")) or (
                bool(src.get("sell_ok")) and not bool(src.get("sell_ignored"))
            )
            source_tag = "HA"
        if buy_signal == sell_signal:
            continue

        apply_buy = bool(buy_signal) and scope in ("both", "buy")
        apply_sell = bool(sell_signal) and scope in ("both", "sell")
        if not (apply_buy or apply_sell):
            continue

        source_name = str(src.get("name") or source_tag)
        forced_side = "BUY" if apply_buy else "SELL"
        for dst in checks:
            if dst is src:
                continue
            if _target_matches(dst, target_exact, target_name_set):
                _apply_override_to_item(
                    dst,
                    apply_buy=apply_buy,
                    apply_sell=apply_sell,
                    scope=scope,
                    forced_side=forced_side,
                    source_tag=source_tag,
                    source_name=source_name,
                )
                if isinstance(dst.get("_ribbon_level_checks"), list):
                    dst["_ribbon_parent_override_applied"] = True
            level_checks = dst.get("_ribbon_level_checks")
            if not isinstance(level_checks, list):
                continue
            for child in level_checks:
                if not _target_matches(child, target_exact, target_name_set):
                    continue
                _apply_override_to_item(
                    child,
                    apply_buy=apply_buy,
                    apply_sell=apply_sell,
                    scope=scope,
                    forced_side=forced_side,
                    source_tag=source_tag,
                    source_name=source_name,
                )

    for parent in checks:
        level_checks = parent.get("_ribbon_level_checks")
        if not isinstance(level_checks, list) or not level_checks:
            continue
        if bool(parent.get("_ribbon_parent_override_applied")):
            continue
        states = [_effective_state(child) for child in level_checks]
        buy_ok = bool(states) and all(state == "BUY" for state in states)
        sell_ok = bool(states) and all(state == "SELL" for state in states)
        agreement = "BUY" if buy_ok else ("SELL" if sell_ok else "HOLD")
        parent["buy_ok"] = bool(buy_ok)
        parent["sell_ok"] = bool(sell_ok)
        parent["buy_ignored"] = False
        parent["sell_ignored"] = False
        value_parts = [str(child.get("value") or "").strip() for child in level_checks if str(child.get("value") or "").strip()]
        detail_parts = [str(child.get("detail") or "").strip() for child in level_checks if str(child.get("detail") or "").strip()]
        if value_parts:
            parent["value"] = " | ".join(value_parts)
        parent["detail"] = (
            "; ".join(detail_parts) + f" => {agreement} (all levels must agree)"
            if detail_parts
            else f"Ribbon => {agreement} (all levels must agree)"
        )


def _rule_consensus_state(check: Dict[str, Any]) -> str:
    forced_side = str(check.get("_override_forced_side") or "").strip().upper()
    if forced_side in ("BUY", "SELL") and bool(check.get("_override_applied")):
        return forced_side
    buy_ignored = bool(check.get("buy_ignored"))
    sell_ignored = bool(check.get("sell_ignored"))
    buy_active = bool(check.get("buy_ok")) and (not buy_ignored)
    sell_active = bool(check.get("sell_ok")) and (not sell_ignored)
    # Keep live-run rule arbitration aligned with create/edit preview:
    # SELL takes precedence when both buy/sell are true on the same rule.
    if sell_active:
        return "SELL"
    if buy_active:
        return "BUY"
    return "HOLD"


def _strict_consensus_signal(checks: List[Dict[str, Any]]) -> str:
    if not checks:
        return "HOLD"
    states = [_rule_consensus_state(c) for c in checks]
    if all(s == "BUY" for s in states):
        return "BUY"
    if all(s == "SELL" for s in states):
        return "SELL"
    return "HOLD"


# -----------------------------
# Order helpers
# -----------------------------
def _order_leg(symbol: str, qty: float, instruction: str) -> Dict[str, Any]:
    instr = _normalize_enum(instruction, INSTRUCTION_ENUM, "BUY")
    return {
        "instruction": instr,
        "quantity": float(qty),
        "instrument": {"symbol": symbol, "assetType": "EQUITY"},
    }


def _order_market(symbol: str, qty: float, instruction: str, session: str) -> Dict[str, Any]:
    sess = _normalize_enum(session, SESSION_ENUM, "NORMAL")
    duration = _normalize_enum("DAY", DURATION_ENUM, "DAY")
    order_type = _normalize_enum("MARKET", ORDER_TYPE_ENUM, "MARKET")
    strategy = _normalize_enum("SINGLE", ORDER_STRATEGY_TYPE_ENUM, "SINGLE")
    return {
        "orderType": order_type,
        "session": sess,
        "duration": duration,
        "orderStrategyType": strategy,
        "orderLegCollection": [_order_leg(symbol, qty, instruction)],
    }


def _order_limit(symbol: str, qty: float, instruction: str, session: str, price: float) -> Dict[str, Any]:
    sess = _normalize_enum(session, SESSION_ENUM, "NORMAL")
    duration = _normalize_enum("DAY", DURATION_ENUM, "DAY")
    order_type = _normalize_enum("LIMIT", ORDER_TYPE_ENUM, "LIMIT")
    strategy = _normalize_enum("SINGLE", ORDER_STRATEGY_TYPE_ENUM, "SINGLE")
    limit_price = _round_up_to_cents(price)
    return {
        "orderType": order_type,
        "session": sess,
        "duration": duration,
        "price": float(limit_price),
        "orderStrategyType": strategy,
        "orderLegCollection": [_order_leg(symbol, qty, instruction)],
    }


def _order_trailing_stop(symbol: str, qty: float, instruction: str, session: str, trail_amount: float) -> Dict[str, Any]:
    """
    Create a trailing stop order for Schwab API.

    Trailing stop orders track price movements and execute when the price moves
    unfavorably by the specified trail_amount.

    For SELL trailing stops: Activates when price drops by trail_amount from highest price.
    For BUY trailing stops: Activates when price rises by trail_amount from lowest price.

    Args:
        symbol: Stock ticker symbol
        qty: Number of shares
        instruction: "BUY" or "SELL"
        session: Trading session (e.g., "NORMAL", "AM", "PM")
        trail_amount: Dollar amount to trail (e.g., 0.50 means $0.50)

    Returns:
        Order dictionary ready for Schwab API submission
    """
    sess = _normalize_enum(session, SESSION_ENUM, "NORMAL")
    duration = _normalize_enum("GOOD_TILL_CANCEL", DURATION_ENUM, "GOOD_TILL_CANCEL")
    order_type = _normalize_enum("TRAILING_STOP", ORDER_TYPE_ENUM, "TRAILING_STOP")
    strategy = _normalize_enum("SINGLE", ORDER_STRATEGY_TYPE_ENUM, "SINGLE")
    # Use LAST as the price basis for trailing - tracks the last traded price
    link_basis = _normalize_enum("LAST", STOP_PRICE_LINK_BASIS_ENUM, "LAST")
    link_type = _normalize_enum("VALUE", STOP_PRICE_LINK_TYPE_ENUM, "VALUE")
    stop_type = _normalize_enum("STANDARD", STOP_TYPE_ENUM, "STANDARD")
    return {
        "orderType": order_type,
        "session": sess,
        "duration": duration,
        "stopPriceOffset": float(trail_amount),
        "stopPriceLinkBasis": link_basis,
        "stopPriceLinkType": link_type,
        "stopType": stop_type,
        "orderStrategyType": strategy,
        "orderLegCollection": [_order_leg(symbol, qty, instruction)],
    }


def _order_stop(symbol: str, qty: float, instruction: str, session: str, stop_price: float) -> Dict[str, Any]:
    sess = _normalize_enum(session, SESSION_ENUM, "NORMAL")
    duration = _normalize_enum("GOOD_TILL_CANCEL", DURATION_ENUM, "GOOD_TILL_CANCEL")
    order_type = _normalize_enum("STOP", ORDER_TYPE_ENUM, "STOP")
    strategy = _normalize_enum("SINGLE", ORDER_STRATEGY_TYPE_ENUM, "SINGLE")
    link_basis = _normalize_enum("MANUAL", STOP_PRICE_LINK_BASIS_ENUM, "MANUAL")
    link_type = _normalize_enum("VALUE", STOP_PRICE_LINK_TYPE_ENUM, "VALUE")
    stop_type = _normalize_enum("STANDARD", STOP_TYPE_ENUM, "STANDARD")
    return {
        "orderType": order_type,
        "session": sess,
        "duration": duration,
        "stopPrice": float(stop_price),
        "stopPriceLinkBasis": link_basis,
        "stopPriceLinkType": link_type,
        "stopType": stop_type,
        "orderStrategyType": strategy,
        "orderLegCollection": [_order_leg(symbol, qty, instruction)],
    }


def _place_order(order_obj: Dict[str, Any]) -> httpx.Response:
    try:
        session_txt = str(order_obj.get("session") or "").strip().upper()
        if session_txt in ("AM", "PM", "SEAMLESS"):
            order_type_txt = str(order_obj.get("orderType") or "").strip().upper() or "UNKNOWN"
            duration_txt = str(order_obj.get("duration") or "").strip().upper() or "UNKNOWN"
            legs = order_obj.get("orderLegCollection") if isinstance(order_obj.get("orderLegCollection"), list) else []
            leg0 = legs[0] if legs and isinstance(legs[0], dict) else {}
            inst = leg0.get("instrument") if isinstance(leg0.get("instrument"), dict) else {}
            symbol_txt = str(inst.get("symbol") or "").strip().upper() or "?"
            instr_txt = str(leg0.get("instruction") or "").strip().upper() or "?"
            qty_txt = _safe_float(leg0.get("quantity"), default=0.0)
            px = _to_float_opt(order_obj.get("price"))
            px_txt = f" price=${float(px):.2f}" if px is not None else ""
            print(
                f"[ORDER] {symbol_txt} {instr_txt} qty={qty_txt:g} "
                f"type={order_type_txt} session={session_txt} duration={duration_txt}{px_txt}"
            )
    except Exception:
        pass
    trader = _require_trader()
    account_hash = _require_account_hash()
    return trader.place_order(encrypted_account_number=account_hash, order_obj=order_obj)


def place_market_buy(symbol: str, qty: float, session: str) -> httpx.Response:
    order = _order_market(symbol, qty, "BUY", session)
    return _place_order(order)


def place_market_sell(symbol: str, qty: float, session: str) -> httpx.Response:
    order = _order_market(symbol, qty, "SELL", session)
    return _place_order(order)


def place_limit_buy(symbol: str, qty: float, session: str, price: float) -> httpx.Response:
    order = _order_limit(symbol, qty, "BUY", session, price)
    return _place_order(order)


def place_limit_sell(symbol: str, qty: float, session: str, price: float) -> httpx.Response:
    order = _order_limit(symbol, qty, "SELL", session, price)
    return _place_order(order)


def place_limit_with_session_fallback(
    *,
    symbol: str,
    qty: float,
    side: str,
    price: float,
    session_state: str,
    session_tag: str,
    allow_seamless_fallback: bool,
) -> httpx.Response:
    sym = str(symbol or "").strip().upper()
    side_norm = str(side or "").strip().lower()
    if side_norm not in ("buy", "sell"):
        raise ValueError(f"Unsupported side '{side}'.")

    def _submit(session_name: str) -> httpx.Response:
        if side_norm == "buy":
            return place_limit_buy(sym, float(qty), session_name, float(price))
        return place_limit_sell(sym, float(qty), session_name, float(price))

    primary = _normalize_enum(session_tag, SESSION_ENUM, "NORMAL")
    if str(session_state or "").strip().lower() == "overnight":
        primary = "SEAMLESS"

    if primary == "SEAMLESS" and sym in _SEAMLESS_UNSUPPORTED_SYMBOLS:
        raise RuntimeError(f"{sym} marked as SEAMLESS-unsupported in this run.")

    try:
        resp = _submit(primary)
        if primary == "SEAMLESS":
            _SEAMLESS_UNSUPPORTED_SYMBOLS.discard(sym)
        return resp
    except Exception as e:
        if primary == "SEAMLESS" and _is_session_rejection(e):
            _SEAMLESS_UNSUPPORTED_SYMBOLS.add(sym)
            print(f"[{sym}] SEAMLESS rejected by broker; skipping overnight orders for this symbol this run.")
            raise

        can_fallback = (
            str(session_state or "").strip().lower() == "extended"
            and allow_seamless_fallback
            and primary in ("AM", "PM")
            and sym not in _SEAMLESS_UNSUPPORTED_SYMBOLS
            and _is_session_rejection(e)
        )
        if not can_fallback:
            raise

        print(f"[{sym}] {primary} session rejected; retrying limit order with SEAMLESS.")
        try:
            resp2 = _submit("SEAMLESS")
            _SEAMLESS_UNSUPPORTED_SYMBOLS.discard(sym)
            return resp2
        except Exception as e2:
            if _is_session_rejection(e2):
                _SEAMLESS_UNSUPPORTED_SYMBOLS.add(sym)
                print(f"[{sym}] SEAMLESS fallback rejected; marking symbol as SEAMLESS-unsupported for this run.")
            raise


def place_trailing_stop_sell(symbol: str, qty: float, session: str, trail_amount: float, current_price: float) -> httpx.Response:
    try:
        order = _order_trailing_stop(symbol, qty, "SELL", session, trail_amount)
        return _place_order(order)
    except Exception as e:
        print(f"[WARN] Trailing stop order failed ({e}); falling back to STOP.")
        stop_price = max(0.01, current_price - trail_amount)
        order = _order_stop(symbol, qty, "SELL", session, stop_price)
        return _place_order(order)


def place_trailing_stop_buy(symbol: str, qty: float, session: str, trail_amount: float, current_price: float) -> httpx.Response:
    try:
        order = _order_trailing_stop(symbol, qty, "BUY", session, trail_amount)
        return _place_order(order)
    except Exception as e:
        print(f"[WARN] Trailing stop BUY order failed ({e}); falling back to STOP.")
        stop_price = max(0.01, current_price + trail_amount)
        order = _order_stop(symbol, qty, "BUY", session, stop_price)
        return _place_order(order)


def _candle_datetime_ms(row: Dict[str, Any]) -> Optional[int]:
    try:
        raw = row.get("datetime")
        if raw is None:
            return None
        return int(raw)
    except Exception:
        return None


def _row_has_close(row: Dict[str, Any]) -> bool:
    """Check if a candle has a valid close price. Supports both Schwab and Robinhood formats."""
    if not isinstance(row, dict):
        return False
    # Check Schwab format first, then Robinhood format
    close_val = row.get("close")
    if close_val is None or close_val in ("None", ""):
        close_val = row.get("close_price")
    return close_val not in (None, "None", "")


def _merge_pricehistory_rows(base: List[Dict[str, Any]], extra: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not base:
        return [r for r in extra if isinstance(r, dict)]
    if not extra:
        return [r for r in base if isinstance(r, dict)]

    merged: Dict[int, Dict[str, Any]] = {}
    passthrough: List[Dict[str, Any]] = []
    for row in base:
        if not isinstance(row, dict):
            continue
        ts = _candle_datetime_ms(row)
        if ts is None:
            passthrough.append(row)
            continue
        merged[int(ts)] = row

    for row in extra:
        if not isinstance(row, dict):
            continue
        ts = _candle_datetime_ms(row)
        if ts is None:
            passthrough.append(row)
            continue
        key = int(ts)
        if key in merged:
            old = merged[key]
            if _row_has_close(row) or not _row_has_close(old):
                merged[key] = row
        else:
            merged[key] = row

    out = [merged[k] for k in sorted(merged.keys())]
    if passthrough:
        out.extend(passthrough)
    return out


def _aggregate_to_hourly(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ordered = [r for r in rows if isinstance(r, dict) and _candle_datetime_ms(r) is not None]
    ordered.sort(key=lambda r: int(_candle_datetime_ms(r) or 0))
    out: List[Dict[str, Any]] = []
    bucket: List[Dict[str, Any]] = []

    def _agg_bucket(items: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not items:
            return None
        highs: List[float] = []
        lows: List[float] = []
        volumes: List[float] = []
        open_val: Optional[float] = None
        close_val: Optional[float] = None
        dt_ms = _candle_datetime_ms(items[0])
        for idx, row in enumerate(items):
            h = _to_float_opt(row.get("high"))
            l = _to_float_opt(row.get("low"))
            c = _to_float_opt(row.get("close"))
            o = _to_float_opt(row.get("open"))
            v = _to_float_opt(row.get("volume"))
            if h is not None:
                highs.append(float(h))
            if l is not None:
                lows.append(float(l))
            if v is not None:
                volumes.append(float(v))
            if idx == 0:
                open_val = o if o is not None else c
            if c is not None:
                close_val = c
        if dt_ms is None or not highs or not lows or close_val is None:
            return None
        return {
            "datetime": int(dt_ms),
            "open": float(open_val if open_val is not None else close_val),
            "high": float(max(highs)),
            "low": float(min(lows)),
            "close": float(close_val),
            "volume": float(sum(volumes)) if volumes else 0.0,
        }

    for row in ordered:
        bucket.append(row)
        if len(bucket) == 4:
            agg = _agg_bucket(bucket)
            if agg is not None:
                out.append(agg)
            bucket = []
    # Keep only full 4x15m groups for 1h synthesis.
    return out


def _fetch_historicals_like_robinhood(
    *,
    symbol: str,
    timeframe_key: str,
    include_extended_hours_data: bool,
    min_candles: int = 0,
) -> List[Dict[str, Any]]:
    """
    Fetch price history mirroring Robinhood's behavior:
    1. Fetch historical data (regular hours only for past days)
    2. Fetch TODAY'S data separately (with extended hours if enabled)
    3. Merge them to get complete data including today's candles

    This is critical because Schwab's historical endpoint doesn't include
    incomplete today's candles by default when using period-based requests.
    """
    if timeframe_key not in TIMEFRAMES:
        raise ValueError(f"Invalid timeframe '{timeframe_key}'. Choose from {list(TIMEFRAMES.keys())}.")
    tf = TIMEFRAMES[timeframe_key]
    period_type = str(tf.get("periodType") or "day")
    period = max(1, int(_to_int_opt(tf.get("period")) or 1))
    frequency_type = str(tf.get("frequencyType") or "minute")
    frequency = max(1, int(_to_int_opt(tf.get("frequency")) or 1))
    aggregate_hourly = bool(tf.get("aggregate_hourly"))
    requested_min = max(0, int(min_candles or 0))
    if aggregate_hourly:
        # For 1h mode, min_candles refers to synthesized 1h candles.
        target_bars = max(30, requested_min)
    else:
        target_bars = max(30, int(_to_int_opt(tf.get("target_bars")) or 30), requested_min)
    fetch_scale = max(1, int(_to_int_opt(tf.get("fetch_scale")) or 1))
    fetch_target = target_bars * fetch_scale

    # fetch_price_history_with_min_candles now handles current day inclusion automatically
    rows = fetch_price_history_with_min_candles(
        fetch_fn=get_price_history,
        symbol=symbol,
        period_type=period_type,
        period=period,
        frequency_type=frequency_type,
        frequency=frequency,
        need_extended=include_extended_hours_data,
        min_candles=fetch_target,
    )
    rows = [r for r in rows if isinstance(r, dict)]

    if aggregate_hourly:
        rows = _aggregate_to_hourly(rows)

    if len(rows) > target_bars:
        rows = rows[-target_bars:]
    return rows


def _extract_ohlcv(
    rows: List[Dict[str, Any]],
) -> Tuple[List[float], List[float], List[float], List[float], List[float], List[Any]]:
    """
    Extract open, high, low, close, volume, and timestamp arrays from price history candles.
    Schwab API returns: {"datetime": <ms>, "open": float, "high": float, "low": float, "close": float, "volume": float}
    """
    opens: List[float] = []
    highs: List[float] = []
    lows: List[float] = []
    closes: List[float] = []
    volumes: List[float] = []
    timestamps: List[Any] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        # Try Schwab format first ("open", "high", "low", "close")
        o = row.get("open")
        h = row.get("high")
        l = row.get("low")
        c = row.get("close")

        # Fallback to Robinhood format if needed ("open_price", "high_price", "low_price", "close_price")
        if o is None:
            o = row.get("open_price")
        if h is None:
            h = row.get("high_price")
        if l is None:
            l = row.get("low_price")
        if c is None:
            c = row.get("close_price")

        # Only append if all values are valid numbers
        try:
            o_val = float(o)
            h_val = float(h)
            l_val = float(l)
            c_val = float(c)

            # Sanity check: prices should be positive and within reasonable range
            if (
                o_val > 0
                and h_val > 0
                and l_val > 0
                and c_val > 0
                and h_val >= l_val
                and c_val >= l_val
                and c_val <= h_val
                and o_val >= l_val
                and o_val <= h_val
            ):
                opens.append(o_val)
                highs.append(h_val)
                lows.append(l_val)
                closes.append(c_val)
                volume_val = 0.0
                for key in ("volume", "volume_traded", "volume_traded_units", "session_volume", "total_volume"):
                    parsed_volume = _to_float_opt(row.get(key))
                    if parsed_volume is not None:
                        parsed_volume_f = float(parsed_volume)
                        volume_val = max(0.0, parsed_volume_f) if math.isfinite(parsed_volume_f) else 0.0
                        break
                volumes.append(volume_val)
                timestamps.append(
                    row.get("datetime")
                    or row.get("begins_at")
                    or row.get("beginsAt")
                    or row.get("time")
                    or row.get("timestamp")
                    or ""
                )
        except (TypeError, ValueError):
            # Skip candles with invalid/missing data
            continue

    return opens, highs, lows, closes, volumes, timestamps


def _extract_ohlc(rows: List[Dict[str, Any]]) -> Tuple[List[float], List[float], List[float], List[float]]:
    opens, highs, lows, closes, _volumes, _timestamps = _extract_ohlcv(rows)
    return opens, highs, lows, closes


def main_trading_loop(
    *,
    run_dir: Path,
    db_path: str,
    connection_id: int,
    symbols: List[str],
    shares_per_trade: int,
    trailing_stop_amount: float,
    trailing_stop_mode: str,
    trailing_stop_atr_mult: float,
    buy_order_type: str,
    sell_order_type: str,
    target_gain_pct: float,
    stop_loss_pct: float,
    stoploss_enabled: bool,
    allow_extended_hours_orders: bool,
    allow_seamless_overnight_orders: bool,
    portfolio_cap_rule_enabled: bool,
    portfolio_cap_mode: str,
    portfolio_cap_percent_by_symbol: Dict[str, float],
    portfolio_cap_percent: float,
    portfolio_cap_divisor: int,
    portfolio_cash_percent: float,
    timeframe: str,
    sleep_duration: float,
    include_extended_hours_data: bool,
    rules: List[Dict[str, Any]],
    trade_stats: Optional[Dict[str, Any]] = None,
    status_writer: Optional[Any] = None,
) -> None:
    if timeframe not in TIMEFRAMES:
        raise ValueError(f"Invalid timeframe '{timeframe}'. Choose from {list(TIMEFRAMES.keys())}.")
    print(f"Using timeframe: {timeframe}")
    print(f"Symbols: {symbols}")
    print(f"Rules loaded: {len(rules)}")
    print(f"BUY order type: {buy_order_type} | SELL order type: {sell_order_type}")
    print(f"History include extended hours: {'YES' if include_extended_hours_data else 'NO'}")
    rules = _rules_with_default_timeframe(rules, timeframe)
    rules_by_tf = _rules_by_timeframe(rules, timeframe)
    rule_min_candles = _rule_min_candles(rules)
    rule_min_candles_by_tf = {tf: _rule_min_candles(tf_rules) for tf, tf_rules in rules_by_tf.items()}
    print(f"Rule candle requirement: {rule_min_candles}")
    print(
        "Rule timeframes: "
        + ", ".join(f"{tf}({rule_min_candles_by_tf[tf]} candles)" for tf in rules_by_tf)
    )

    session_state = ""
    market_state = ""
    while True:
        next_market_state = _market_session_state()
        next_state = _execution_state_from_market(
            next_market_state,
            allow_extended_hours_orders=allow_extended_hours_orders,
            allow_seamless_overnight_orders=allow_seamless_overnight_orders,
        )
        if next_state != session_state or next_market_state != market_state:
            session_state = next_state
            market_state = next_market_state
            if session_state == "regular":
                print("Session: regular market hours (order routing enabled).")
            elif session_state == "extended":
                print("Session: extended hours (limit orders at bid/ask midpoint).")
            elif session_state == "overnight":
                print("Session: overnight enabled (SEAMLESS limit orders).")
            elif market_state == "extended":
                print("Session: extended hours detected but disabled (orders skipped).")
            else:
                print("Session: market closed (orders skipped).")

        tickers_status: List[Dict[str, Any]] = []
        account = get_account_snapshot()
        positions = get_open_stock_positions(account)
        positions_map = build_positions_map(positions)
        day_trade_metrics = _day_trade_metrics(account)
        day_trade_round_trips = day_trade_metrics.get("round_trips_used")
        day_trade_remaining = day_trade_metrics.get("remaining")
        day_trader_flag = bool(day_trade_metrics.get("is_day_trader"))

        divisor = max(2, int(portfolio_cap_divisor))
        cash_target_pct = max(0.0, float(portfolio_cash_percent))
        if cash_target_pct <= 0.0:
            cash_target_pct = 100.0 / float(divisor)
        cash_target_pct = min(100.0, float(cash_target_pct))
        portfolio_value: Optional[float] = None
        available_cash: Optional[float] = None
        buying_power: Optional[float] = None
        cap_pct: Optional[float] = None
        cash_target_value: Optional[float] = None
        cash_pct: Optional[float] = None
        try:
            buying_power = float(_pick_buying_power(account))
        except Exception as e:
            print(f"[WARN] Buying power unavailable; buy power gate will not be enforced: {e}")
            buying_power = None
        if portfolio_cap_rule_enabled:
            try:
                portfolio_value = float(_pick_equity(account))
                available_cash = float(_pick_available_cash(account))
                cap_pct = None if portfolio_cap_mode == "percent" else 100.0 / float(divisor)
                if portfolio_value > 0:
                    cash_target_value = float(portfolio_value) * (float(cash_target_pct) / 100.0)
                    cash_pct = (float(available_cash) / float(portfolio_value)) * 100.0
            except Exception as e:
                print(f"[WARN] Portfolio cap enabled but portfolio value unavailable: {e}")
                portfolio_value = None
                available_cash = None
                cap_pct = None
                cash_target_value = None
                cash_pct = None

        quotes = get_quotes_map(symbols)

        for symbol in symbols:
            try:
                quote = _get_quote_for_symbol(quotes, symbol)
                current_price = _price_from_quote(quote, prefer_extended=bool(include_extended_hours_data))
                if current_price is None:
                    raise RuntimeError(f"Missing price for {symbol}")
                quote_root = quote.get("quote") if isinstance(quote, dict) else None
                if not isinstance(quote_root, dict):
                    quote_root = quote if isinstance(quote, dict) else {}
                bid_price = _safe_float(quote_root.get("bidPrice"), default=0.0)
                ask_price = _safe_float(quote_root.get("askPrice"), default=0.0)
                mid_price = _round_up_to_cents(_mid_price(bid_price, ask_price, current_price))

                fetch_timeframes = list(dict.fromkeys([timeframe] + list(rules_by_tf.keys())))
                hist_by_tf: Dict[str, List[Dict[str, Any]]] = {}
                ohlc_by_tf: Dict[str, Tuple[List[float], List[float], List[float], List[float], List[float], List[Any]]] = {}
                missing_tf_reasons: List[str] = []
                for tf_key in fetch_timeframes:
                    tf_min = int(rule_min_candles_by_tf.get(tf_key, 30 if tf_key == timeframe else rule_min_candles))
                    hist_tf = _fetch_historicals_like_robinhood(
                        symbol=symbol,
                        timeframe_key=tf_key,
                        include_extended_hours_data=bool(include_extended_hours_data),
                        min_candles=tf_min,
                    )
                    tf_opens, tf_highs, tf_lows, tf_closes, tf_volumes, tf_timestamps = _extract_ohlcv(hist_tf)
                    if len(tf_closes) < tf_min:
                        reason = f"{tf_key}: got {len(tf_closes)} closes from {len(hist_tf)} raw candles, need {tf_min}"
                        missing_tf_reasons.append(reason)
                        if hist_tf:
                            sample = hist_tf[0] if isinstance(hist_tf[0], dict) else None
                            if sample:
                                print(f"[{symbol}] {tf_key} sample candle keys: {list(sample.keys())}")
                        continue

                    prev_close = float(tf_closes[-1])
                    cur = float(current_price)
                    tf_opens.append(prev_close)
                    tf_highs.append(max(prev_close, cur))
                    tf_lows.append(min(prev_close, cur))
                    tf_closes.append(cur)
                    tf_volumes.append(0.0)
                    tf_timestamps.append(iso_now())
                    hist_by_tf[tf_key] = hist_tf
                    ohlc_by_tf[tf_key] = (tf_opens, tf_highs, tf_lows, tf_closes, tf_volumes, tf_timestamps)

                if missing_tf_reasons:
                    print(f"[{symbol}] Not enough historical candles by timeframe: {'; '.join(missing_tf_reasons)}.")
                    tickers_status.append({"symbol": symbol, "signal": "NO_DATA", "timeframes": list(rules_by_tf.keys())})
                    continue

                default_ohlc = ohlc_by_tf.get(timeframe) or next(iter(ohlc_by_tf.values()))
                opens, highs, lows, closes, volumes, timestamps = default_ohlc
                hist = hist_by_tf.get(timeframe) or next(iter(hist_by_tf.values()))

                pos_info = positions_map.get(symbol, {})
                pos_qty = _safe_float(pos_info.get("quantity"))
                avg_buy_price = _safe_float(pos_info.get("average_buy_price"))
                open_order_qty = 0.0
                open_order_value = 0.0
                open_order_count = 0
                held_pct: Optional[float] = None
                cap_delta_pct: Optional[float] = None
                buy_order_price = float(current_price)
                buy_order_cost = float(shares_per_trade) * buy_order_price
                symbol_cap_pct = float(portfolio_cap_percent_by_symbol.get(str(symbol).strip().upper(), portfolio_cap_percent))
                if symbol_cap_pct <= 0:
                    symbol_cap_pct = portfolio_cap_percent
                row_cap_pct = max(0.01, float(symbol_cap_pct)) if portfolio_cap_mode == "percent" else cap_pct
                if row_cap_pct is not None and portfolio_value is not None and portfolio_value > 0:
                    held_value = float(pos_qty) * float(current_price)
                    held_pct = (held_value / float(portfolio_value)) * 100.0
                    cap_delta_pct = held_pct - row_cap_pct

                checks: List[Dict[str, Any]] = []
                for r in rules:
                    rule_tf = _rule_timeframe(r, timeframe)
                    rule_opens, rule_highs, rule_lows, rule_closes, rule_volumes, rule_timestamps = ohlc_by_tf.get(rule_tf, default_ohlc)
                    c = _eval_rule(
                        r,
                        rule_closes,
                        float(rule_closes[-1]) if rule_closes else float(current_price),
                        opens=rule_opens,
                        highs=rule_highs,
                        lows=rule_lows,
                        volumes=rule_volumes,
                        timestamps=rule_timestamps,
                    )
                    rule_params = r.get("params") if isinstance(r.get("params"), dict) else {}
                    base_name = str(c.get("name") or r.get("name") or str(r.get("kind") or "").upper())
                    base_kind = str(r.get("kind") or "").strip().lower()
                    base_rule_id = str(rule_params.get("rule_id") or "").strip()
                    level_checks = c.get("_ribbon_level_checks")
                    if isinstance(level_checks, list) and level_checks:
                        for child in level_checks:
                            if not isinstance(child, dict):
                                continue
                            child["name"] = str(child.get("name") or _ma_ribbon_level_name(base_name, child))
                            child["_rule_kind"] = str(child.get("_rule_kind") or base_kind)
                            child["_rule_params"] = rule_params
                            if not str(child.get("_rule_id") or "").strip():
                                child["_rule_id"] = _ma_ribbon_level_rule_id(base_rule_id, child.get("_ribbon_slot"))
                            child["_timeframe"] = rule_tf
                            checks.append(child)
                        continue
                    c["name"] = base_name
                    c["_rule_kind"] = base_kind
                    c["_rule_params"] = rule_params
                    c["_rule_id"] = base_rule_id
                    c["_timeframe"] = rule_tf
                    checks.append(c)
                _apply_rsi_signal_overrides(checks)
                rule_consensus_signal = _strict_consensus_signal(checks)

                buy_cap_blocked = False
                buy_power_blocked = False
                cash_slice_blocked = False
                if rule_consensus_signal == "BUY" and buying_power is not None and float(buying_power) < float(buy_order_cost):
                    buy_power_blocked = True
                    print(
                        f"[{symbol}] BUY blocked by buying power: need ${float(buy_order_cost):.2f}, "
                        f"buying power ${float(buying_power):.2f}."
                    )
                if rule_consensus_signal == "BUY" and portfolio_cap_rule_enabled:
                    if portfolio_value is not None and portfolio_value > 0:
                        ticker_value = float(pos_qty) * float(current_price)
                        if portfolio_cap_mode == "percent":
                            ticker_cap_pct = float(portfolio_cap_percent_by_symbol.get(str(symbol).strip().upper(), portfolio_cap_percent))
                            ticker_cap_pct = max(0.01, ticker_cap_pct)
                            ticker_cap_value = float(portfolio_value) * (ticker_cap_pct / 100.0)
                            if ticker_value > ticker_cap_value:
                                buy_cap_blocked = True
                                print(
                                    f"[{symbol}] BUY blocked by portfolio percent cap: current holdings "
                                    f"${ticker_value:.2f} exceed {ticker_cap_pct:.2f}% "
                                    f"of portfolio (${ticker_cap_value:.2f})."
                                )
                        elif ticker_value > (float(portfolio_value) / float(divisor)):
                            buy_cap_blocked = True
                            print(
                                f"[{symbol}] BUY blocked by portfolio cap: holdings ${ticker_value:.2f} exceed "
                                f"1/{divisor} of portfolio (${float(portfolio_value):.2f})."
                            )
                        if (
                            available_cash is not None
                            and cash_target_value is not None
                            and float(available_cash) < float(cash_target_value)
                        ):
                            cash_slice_blocked = True
                            print(
                                f"[{symbol}] BUY blocked by cash target: current available cash "
                                f"${float(available_cash):.2f} is below "
                                f"{float(cash_target_pct):.2f}% cash target (${float(cash_target_value):.2f})."
                            )
                    else:
                        buy_cap_blocked = True
                        print(f"[{symbol}] BUY blocked by portfolio cap: portfolio value unavailable.")

                can_sell_now = pos_qty > 0 and _can_sell_without_loss(float(current_price), avg_buy_price)
                execution_hold_reason = ""
                if rule_consensus_signal == "SELL" and can_sell_now:
                    execution_signal = "SELL"
                elif rule_consensus_signal == "BUY" and not (buy_cap_blocked or buy_power_blocked or cash_slice_blocked):
                    execution_signal = "BUY"
                else:
                    execution_signal = "HOLD"
                    if rule_consensus_signal == "SELL":
                        if pos_qty <= 0:
                            execution_hold_reason = "sell blocked: no shares held"
                        else:
                            execution_hold_reason = "sell blocked: no-loss rule"
                    elif rule_consensus_signal == "BUY":
                        if buy_power_blocked:
                            execution_hold_reason = "buy blocked: buying power"
                        elif cash_slice_blocked:
                            execution_hold_reason = "buy blocked: cash target"
                        elif buy_cap_blocked:
                            execution_hold_reason = "buy blocked: portfolio cap"

                overnight_symbol_blocked = (
                    session_state == "overnight"
                    and str(symbol).strip().upper() in _SEAMLESS_UNSUPPORTED_SYMBOLS
                )
                if overnight_symbol_blocked and execution_signal != "HOLD":
                    execution_signal = "HOLD"
                    execution_hold_reason = "overnight blocked: symbol not SEAMLESS-eligible"

                signal = execution_signal
                _print_rule_checks(
                    symbol,
                    checks,
                    rule_consensus_signal,
                    execution_signal,
                    execution_hold_reason,
                )

                pnl_pct: Optional[float] = None
                if avg_buy_price > 0:
                    pnl_pct = ((float(current_price) - avg_buy_price) / avg_buy_price) * 100.0

                ma20 = _ma_value(closes, 20)
                ma78 = _ma_value(closes, 78)
                ma190 = _ma_value(closes, 190)
                rsi = _rsi(closes, 14)
                drsi = _rsi_derivative(closes, 14)
                atr = _atr_from_historicals(hist, period=14)

                tickers_status.append(
                    {
                        "symbol": symbol,
                        "signal": signal,
                        "rule_signal": rule_consensus_signal,
                        "execution_signal": execution_signal,
                        "execution_hold_reason": execution_hold_reason,
                        "overnight_seamless_blocked": bool(overnight_symbol_blocked),
                        "price": float(current_price),
                        "qty": pos_qty,
                        "avg_buy": avg_buy_price,
                        "pnl_pct": pnl_pct,
                        "cap_pct": row_cap_pct,
                        "cash_pct": cash_pct,
                        "cash_target_pct": cash_target_pct if portfolio_cap_rule_enabled else None,
                        "cash_target_value": cash_target_value,
                        "available_cash": available_cash,
                        "buying_power": buying_power,
                        "held_pct": held_pct,
                        "cap_delta_pct": cap_delta_pct,
                        "alloc_pct": held_pct,
                        "delta_pct": cap_delta_pct,
                        "portfolio_cap_divisor": divisor if portfolio_cap_rule_enabled else None,
                        "portfolio_cap_mode": portfolio_cap_mode if portfolio_cap_rule_enabled else None,
                        "portfolio_cash_percent": cash_target_pct if portfolio_cap_rule_enabled else None,
                        "portfolio_cap_percent": (
                            symbol_cap_pct
                            if portfolio_cap_rule_enabled and portfolio_cap_mode == "percent"
                            else (portfolio_cap_percent if portfolio_cap_rule_enabled else None)
                        ),
                        "buy_cap_rule_enabled": bool(portfolio_cap_rule_enabled),
                        "buy_cap_blocked": bool(buy_cap_blocked),
                        "buy_power_blocked": bool(buy_power_blocked),
                        "cash_slice_blocked": bool(cash_slice_blocked),
                        "buy_order_cost": buy_order_cost,
                        "buy_order_price": buy_order_price,
                        "open_buy_order_value": open_order_value,
                        "open_buy_order_shares": open_order_qty,
                        "open_buy_order_count": open_order_count,
                        "day_trades_used": day_trade_round_trips,
                        "day_trades_remaining": day_trade_remaining,
                        "is_day_trader": bool(day_trader_flag),
                        "session_state": session_state,
                        "seamless_supported": str(symbol).strip().upper() not in _SEAMLESS_UNSUPPORTED_SYMBOLS,
                        "buy_order_type": buy_order_type,
                        "sell_order_type": sell_order_type,
                        "ma20": ma20,
                        "ma78": ma78,
                        "ma150": ma190,
                        "rsi": rsi,
                        "rsi_d": drsi,
                        "atr": atr,
                        "chart": (
                            _build_chart_series(
                                closes,
                                opens=opens,
                                highs=highs,
                                lows=lows,
                                volumes=volumes,
                                timestamps=timestamps,
                            )
                            if status_writer is not None
                            else {}
                        ),
                        "charts_by_timeframe": (
                            {
                                tf_key: _build_chart_series(
                                    tf_ohlc[3],
                                    opens=tf_ohlc[0],
                                    highs=tf_ohlc[1],
                                    lows=tf_ohlc[2],
                                    volumes=tf_ohlc[4],
                                    timestamps=tf_ohlc[5],
                                )
                                for tf_key, tf_ohlc in ohlc_by_tf.items()
                                if tf_key in rules_by_tf
                            }
                            if status_writer is not None
                            else {}
                        ),
                        "timeframes": list(rules_by_tf.keys()),
                        "rule_summary": [
                            {
                                "name": str(c.get("name") or ""),
                                "kind": str(c.get("_rule_kind") or ""),
                                "rule_id": str(c.get("_rule_id") or ""),
                                "timeframe": str(c.get("_timeframe") or timeframe),
                                "buy_ok": bool(c.get("buy_ok")),
                                "sell_ok": bool(c.get("sell_ok")),
                                "buy_ignored": bool(c.get("buy_ignored")),
                                "sell_ignored": bool(c.get("sell_ignored")),
                                "block_ok": bool(c.get("block_ok")),
                                "block_ignored": bool(c.get("block_ignored")),
                                "value": str(c.get("value") or "—"),
                                "detail": str(c.get("detail") or ""),
                            }
                            for c in checks
                        ],
                    }
                )

                if stoploss_enabled:
                    check_stoploss_and_sell(
                        symbol=symbol,
                        current_price=float(current_price),
                        avg_buy_price=avg_buy_price,
                        held_qty=pos_qty,
                        target_gain_pct=target_gain_pct,
                        stop_loss_pct=stop_loss_pct,
                        session_state=session_state,
                        limit_mid=mid_price,
                        allow_extended_hours_orders=allow_extended_hours_orders,
                        allow_seamless_overnight_orders=allow_seamless_overnight_orders,
                        trade_stats=trade_stats,
                    )

                stop_arm_price: Optional[float] = None
                stop_trigger_price: Optional[float] = None
                stop_arm_gap_pct: Optional[float] = None
                stop_trigger_gap_pct: Optional[float] = None
                if avg_buy_price > 0:
                    stop_arm_price = avg_buy_price * (1.0 + (target_gain_pct / 100.0))
                    stop_trigger_price = avg_buy_price * (1.0 + (stop_loss_pct / 100.0))
                    if stop_arm_price > 0:
                        stop_arm_gap_pct = ((float(current_price) - float(stop_arm_price)) / float(stop_arm_price)) * 100.0
                    if stop_trigger_price > 0:
                        stop_trigger_gap_pct = ((float(current_price) - float(stop_trigger_price)) / float(stop_trigger_price)) * 100.0
                stop_armed = bool(stoploss_state.get(symbol, {}).get("armed"))
                tickers_status[-1].update(
                    {
                        "stoploss_enabled": bool(stoploss_enabled),
                        "stoploss_armed": stop_armed,
                        "stoploss_arm_price": stop_arm_price,
                        "stoploss_trigger": stop_trigger_price,
                        "stoploss_arm_gap_pct": stop_arm_gap_pct,
                        "stoploss_trigger_gap_pct": stop_trigger_gap_pct,
                    }
                )

                if execution_signal == "BUY":
                    try:
                        session_tag = _order_session_for_state(session_state)
                        resp: Optional[httpx.Response] = None

                        # Determine if we can place orders based on session state
                        can_place_order = False
                        if session_state == "regular":
                            can_place_order = True
                        elif session_state == "extended" and allow_extended_hours_orders:
                            can_place_order = True
                        elif session_state == "overnight" and allow_seamless_overnight_orders:
                            can_place_order = True

                        if can_place_order:
                            # During extended and overnight sessions, force midpoint limit orders.
                            if session_state in ("extended", "overnight"):
                                print(
                                    f"[{symbol}] BUY signal ({session_state}) -> placing midpoint limit buy for {shares_per_trade} shares "
                                    f"at ${float(mid_price):.2f}..."
                                )
                                resp = place_limit_with_session_fallback(
                                    symbol=symbol,
                                    qty=float(shares_per_trade),
                                    side="buy",
                                    price=float(mid_price),
                                    session_state=session_state,
                                    session_tag=session_tag,
                                    allow_seamless_fallback=bool(allow_seamless_overnight_orders),
                                )
                            # During regular hours, use configured order type
                            elif buy_order_type == "market":
                                print(f"[{symbol}] BUY signal -> placing market buy for {shares_per_trade} shares...")
                                resp = place_market_buy(symbol, float(shares_per_trade), session_tag)
                            elif buy_order_type == "limit_midpoint":
                                print(
                                    f"[{symbol}] BUY signal -> placing midpoint limit buy for {shares_per_trade} shares "
                                    f"at ${float(mid_price):.2f}..."
                                )
                                resp = place_limit_buy(symbol, float(shares_per_trade), session_tag, float(mid_price))
                            elif buy_order_type == "trailing_stop":
                                trail_amount = _resolve_trail_amount(
                                    trailing_stop_mode=trailing_stop_mode,
                                    trailing_stop_amount=trailing_stop_amount,
                                    trailing_stop_atr_mult=trailing_stop_atr_mult,
                                    atr=atr,
                                )
                                if trail_amount is None:
                                    print(f"[{symbol}] ATR unavailable; skipping trailing stop buy.")
                                    resp = None
                                else:
                                    print(
                                        f"[{symbol}] BUY signal -> placing trailing stop buy for {shares_per_trade} shares, "
                                        f"trail=${float(trail_amount):.2f} ({trailing_stop_mode})..."
                                    )
                                    resp = place_trailing_stop_buy(
                                        symbol,
                                        float(shares_per_trade),
                                        session_tag,
                                        float(trail_amount),
                                        float(current_price),
                                    )
                            else:
                                print(f"[{symbol}] BUY skipped: unsupported order type '{buy_order_type}'.")
                        else:
                            if session_state == "extended":
                                print(f"[{symbol}] Extended hours detected but allow_extended_hours_orders=False; skipping buy order.")
                            elif session_state == "overnight":
                                print(f"[{symbol}] Overnight detected but allow_seamless_overnight_orders=False; skipping buy order.")
                            else:
                                print(f"[{symbol}] Market closed; skipping buy order.")
                        if resp is not None and trade_stats is not None and _order_success(resp):
                            _record_trade(
                                trade_stats,
                                side="buy",
                                qty=float(shares_per_trade),
                                price=float(current_price),
                                avg_buy_price=0.0,
                            )
                    except Exception as e:
                        print(f"[{symbol}] Buy failed: {e}")

                if execution_signal == "SELL" and pos_qty > 0:
                    if not _can_sell_without_loss(float(current_price), avg_buy_price):
                        print(
                            f"[{symbol}] SELL blocked by no-loss rule "
                            f"({float(current_price):.2f} < {avg_buy_price:.2f})."
                        )
                        continue
                    try:
                        session_tag = _order_session_for_state(session_state)
                        resp: Optional[httpx.Response] = None

                        # Determine if we can place orders based on session state
                        can_place_order = False
                        if session_state == "regular":
                            can_place_order = True
                        elif session_state == "extended" and allow_extended_hours_orders:
                            can_place_order = True
                        elif session_state == "overnight" and allow_seamless_overnight_orders:
                            can_place_order = True

                        if can_place_order:
                            # During extended and overnight sessions, force midpoint limit orders.
                            if session_state in ("extended", "overnight"):
                                if float(mid_price) < float(avg_buy_price):
                                    print(
                                        f"[{symbol}] SELL blocked by no-loss rule: {session_state} midpoint "
                                        f"({float(mid_price):.2f}) is below avg buy ({avg_buy_price:.2f})."
                                    )
                                    continue
                                print(
                                    f"[{symbol}] SELL signal ({session_state}) -> placing midpoint limit sell for {shares_per_trade} shares "
                                    f"at ${float(mid_price):.2f}..."
                                )
                                resp = place_limit_with_session_fallback(
                                    symbol=symbol,
                                    qty=float(shares_per_trade),
                                    side="sell",
                                    price=float(mid_price),
                                    session_state=session_state,
                                    session_tag=session_tag,
                                    allow_seamless_fallback=bool(allow_seamless_overnight_orders),
                                )
                            # During regular hours, use configured order type
                            elif sell_order_type == "market":
                                print(f"[{symbol}] SELL signal -> placing market sell for {shares_per_trade} shares...")
                                resp = place_market_sell(symbol, float(shares_per_trade), session_tag)
                            elif sell_order_type == "limit_midpoint":
                                if float(mid_price) < float(avg_buy_price):
                                    print(
                                        f"[{symbol}] SELL blocked by no-loss rule: midpoint "
                                        f"({float(mid_price):.2f}) is below avg buy ({avg_buy_price:.2f})."
                                    )
                                    continue
                                print(
                                    f"[{symbol}] SELL signal -> placing midpoint limit sell for {shares_per_trade} shares "
                                    f"at ${float(mid_price):.2f}..."
                                )
                                resp = place_limit_sell(symbol, float(shares_per_trade), session_tag, float(mid_price))
                            elif sell_order_type == "trailing_stop":
                                trail_amount = _resolve_trail_amount(
                                    trailing_stop_mode=trailing_stop_mode,
                                    trailing_stop_amount=trailing_stop_amount,
                                    trailing_stop_atr_mult=trailing_stop_atr_mult,
                                    atr=atr,
                                )
                                if trail_amount is None:
                                    print(f"[{symbol}] ATR unavailable; skipping trailing stop sell.")
                                    continue
                                if (float(current_price) - float(trail_amount)) < float(avg_buy_price):
                                    print(
                                        f"[{symbol}] SELL blocked by no-loss rule: trailing trigger "
                                        f"({float(current_price) - float(trail_amount):.2f}) would be below "
                                        f"avg buy ({avg_buy_price:.2f})."
                                    )
                                    continue
                                print(
                                    f"[{symbol}] SELL signal -> placing trailing stop sell for {shares_per_trade} shares, "
                                    f"trail=${float(trail_amount):.2f} ({trailing_stop_mode})..."
                                )
                                resp = place_trailing_stop_sell(
                                    symbol,
                                    float(shares_per_trade),
                                    session_tag,
                                    float(trail_amount),
                                    float(current_price),
                                )
                            else:
                                print(f"[{symbol}] SELL skipped: unsupported order type '{sell_order_type}'.")
                        else:
                            if session_state == "extended":
                                print(f"[{symbol}] Extended hours detected but allow_extended_hours_orders=False; skipping sell order.")
                            elif session_state == "overnight":
                                print(f"[{symbol}] Overnight detected but allow_seamless_overnight_orders=False; skipping sell order.")
                            else:
                                print(f"[{symbol}] SELL signal but market closed; skipping order.")

                        if resp is not None and trade_stats is not None and _order_success(resp):
                            _record_trade(
                                trade_stats,
                                side="sell",
                                qty=float(shares_per_trade),
                                price=float(current_price),
                                avg_buy_price=avg_buy_price,
                            )
                    except Exception as e:
                        print(f"[{symbol}] Sell order failed: {e}")

            except Exception as e:
                print(f"[{symbol}] ERROR: {e}")
                tickers_status.append({"symbol": symbol, "signal": "ERROR"})

        if status_writer is not None:
            try:
                status_writer(
                    {
                        "phase": "loop",
                        "timeframe": timeframe,
                        "market_state": market_state,
                        "session_state": session_state,
                        "buy_order_type": buy_order_type,
                        "sell_order_type": sell_order_type,
                        "allow_extended_hours_orders": bool(allow_extended_hours_orders),
                        "allow_seamless_overnight_orders": bool(allow_seamless_overnight_orders),
                        "day_trades_used": day_trade_round_trips,
                        "day_trades_remaining": day_trade_remaining,
                        "is_day_trader": bool(day_trader_flag),
                        "tickers": tickers_status,
                    }
                )
            except Exception:
                pass

        print(f"Sleeping {sleep_duration} seconds...\n")
        time.sleep(float(sleep_duration))


def load_params(params_path: str) -> dict[str, Any]:
    p = Path(params_path)
    obj = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError("params-json must be a JSON object.")
    return obj


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True, help="Run directory allocated by the web app.")
    ap.add_argument("--params-json", required=True, help="Path to params.json provided by the web app.")
    ap.add_argument("--db-path", required=True, help="Path to Cryptid Exchange sqlite DB.")
    ap.add_argument("--connection-id", required=True, type=int, help="broker_connections.id for Schwab.")
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    status_path = run_dir / "status.json"
    last_status: Dict[str, Any] = {}
    trade_stats: Dict[str, Any] = {"pnl": 0.0, "trades": 0}

    def write_status(payload: Dict[str, Any]) -> None:
        payload = dict(payload)
        payload["ts"] = iso_now()
        payload["script"] = "IndicatorForge.Schwab"
        payload["pnl"] = round(float(trade_stats.get("pnl", 0.0)), 2)
        payload["trades"] = int(trade_stats.get("trades", 0))
        try:
            status_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            last_status.clear()
            last_status.update(payload)
        except Exception:
            pass

    params = load_params(args.params_json)

    sleep_duration = float(params.get("sleep_duration", 30))
    symbols = params.get("symbols", [])
    if isinstance(symbols, str):
        symbols = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not isinstance(symbols, list) or not symbols:
        raise ValueError("params.symbols must be a non-empty list of tickers.")
    symbols = [str(s).strip().upper() for s in symbols if str(s).strip()]

    shares_per_trade = int(params.get("shares_per_trade", 1))
    trailing_stop_amount = float(params.get("trailing_stop_amount", 0.10))
    trailing_stop_mode = str(params.get("trailing_stop_mode", "fixed")).strip().lower()
    if trailing_stop_mode not in ("fixed", "atr"):
        trailing_stop_mode = "fixed"
    trailing_stop_atr_mult = float(params.get("trailing_stop_atr_mult", 3.0))
    if trailing_stop_atr_mult <= 0:
        trailing_stop_atr_mult = 3.0
    buy_order_type = _normalize_order_type(params.get("buy_order_type", "market"), default="market")
    sell_order_type = _normalize_order_type(params.get("sell_order_type", "trailing_stop"), default="trailing_stop")
    target_gain_pct = float(params.get("target_gain_pct", 0.5))
    stop_loss_pct = float(params.get("stop_loss_pct", -0.5))
    stoploss_enabled = _to_bool(params.get("stoploss_enabled", False), False)
    allow_extended_hours_orders = _to_bool(params.get("allow_extended_hours_orders", False), False)
    allow_seamless_overnight_orders = _to_bool(params.get("allow_seamless_overnight_orders", False), False)
    include_extended_hours_data = _to_bool(
        params.get("include_extended_hours_data", params.get("history_include_extended", True)),
        True,
    )
    portfolio_cap_rule_enabled = _to_bool(params.get("portfolio_cap_rule_enabled", False), False)
    portfolio_cap_mode = str(params.get("portfolio_cap_mode", "divisor_cash_slice")).strip().lower()
    if portfolio_cap_mode not in ("divisor_cash_slice", "percent"):
        portfolio_cap_mode = "divisor_cash_slice"
    portfolio_cap_percent = float(params.get("portfolio_cap_percent", 20.0))
    if portfolio_cap_percent <= 0:
        portfolio_cap_percent = 20.0
    portfolio_cap_percent_by_symbol = _parse_symbol_cap_map(params.get("portfolio_cap_percent_by_symbol"))
    portfolio_cap_divisor = int(params.get("portfolio_cap_divisor", 6))
    if portfolio_cap_divisor < 2:
        portfolio_cap_divisor = 2
    portfolio_cash_percent = float(params.get("portfolio_cash_percent", 0.0) or 0.0)
    if portfolio_cash_percent < 0:
        portfolio_cash_percent = 0.0
    timeframe = str(params.get("timeframe", "30m")).strip().lower()
    if timeframe not in TIMEFRAMES:
        print(f"[WARN] Invalid timeframe '{timeframe}'. Defaulting to '30m'.")
        timeframe = "30m"

    rules = _resolve_rules(str(args.db_path), params)
    if not rules:
        rules = _default_rules()
    print(f"Loaded {len(rules)} indicator rules.")

    db_path = _resolve_db_path(args.db_path)
    connection_id = _resolve_connection_id(str(db_path), args.connection_id)

    row = get_broker_connection(str(db_path), int(connection_id))
    if not row:
        print(f"[ERROR] Schwab connection_id {connection_id} not found in broker_connections.")
        return 2

    broker = str(row["broker"])
    status = str(row["status"] or "")
    if broker != "schwab":
        print(f"[ERROR] connection_id {connection_id} is broker='{broker}', expected 'schwab'.")
        return 2

    metadata = read_connection_metadata(row)
    if status not in ("connected", "ok", ""):
        _set_connection_error(
            db_path=str(db_path),
            connection_id=int(connection_id),
            status="needs_auth",
            metadata=metadata,
            error=f"Schwab connection status '{status}' is not connected.",
        )
        print(f"[ERROR] Schwab connection_id {connection_id} status='{status}'. Re-link via Broker page.")
        return 2

    token_path = _resolve_token_path(db_path=db_path, metadata=metadata)
    if not token_path.exists():
        _set_connection_error(
            db_path=str(db_path),
            connection_id=int(connection_id),
            status="needs_auth",
            metadata=metadata,
            error=f"Schwab token file not found at {token_path}",
        )
        print(f"[ERROR] Schwab token not found at {token_path}.")
        return 2

    trader_base = str(
        params.get("trader_base")
        or params.get("schwab_trader_api_base")
        or os.environ.get("SCHWAB_TRADER_API_BASE", "")
    ).strip()
    if not trader_base:
        print("[ERROR] Missing SCHWAB_TRADER_API_BASE env var.")
        return 2

    market_base = str(
        params.get("market_data_base")
        or params.get("schwab_market_data_base")
        or os.environ.get("SCHWAB_MARKET_DATA_BASE", "")
        or trader_base
    ).strip()
    market_base = _normalize_market_base(trader_base, market_base)
    if "/marketdata/" not in market_base:
        print(f"[WARN] Schwab market data base looks unusual: {market_base}")

    oauth = SchwabOAuth(token_path=token_path)
    try:
        oauth.ensure_access_token()
    except SchwabAuthError as e:
        _set_connection_error(
            db_path=str(db_path),
            connection_id=int(connection_id),
            status="needs_attention",
            metadata=metadata,
            error=str(e),
        )
        print(f"[ERROR] Schwab auth error: {e}")
        return 2

    global _TRADER, _MARKET, _ACCOUNT_HASH, _ACCOUNT_NUMBER
    _TRADER = SchwabTraderClient(base_url=trader_base, oauth=oauth)
    _MARKET = SchwabMarketDataClient(base_url=market_base, oauth=oauth)

    explicit_hash = str(
        params.get("account_hash")
        or params.get("schwab_account_hash")
        or os.environ.get("SCHWAB_ACCOUNT_HASH", "")
    ).strip() or None
    account_index_raw = params.get("account_index", 0)
    try:
        account_index = int(account_index_raw)
    except Exception:
        account_index = 0

    try:
        _ACCOUNT_HASH, _ACCOUNT_NUMBER = _resolve_account_hash(
            _TRADER,
            explicit_hash=explicit_hash,
            account_index=account_index,
        )
    except Exception as e:
        print(f"[ERROR] Failed to resolve Schwab account hash: {e}")
        return 2

    main_trading_loop(
        run_dir=run_dir,
        db_path=str(db_path),
        connection_id=int(connection_id),
        symbols=symbols,
        shares_per_trade=shares_per_trade,
        trailing_stop_amount=trailing_stop_amount,
        trailing_stop_mode=trailing_stop_mode,
        trailing_stop_atr_mult=trailing_stop_atr_mult,
        buy_order_type=buy_order_type,
        sell_order_type=sell_order_type,
        target_gain_pct=target_gain_pct,
        stop_loss_pct=stop_loss_pct,
        stoploss_enabled=stoploss_enabled,
        allow_extended_hours_orders=allow_extended_hours_orders,
        allow_seamless_overnight_orders=allow_seamless_overnight_orders,
        portfolio_cap_rule_enabled=portfolio_cap_rule_enabled,
        portfolio_cap_mode=portfolio_cap_mode,
        portfolio_cap_percent_by_symbol=portfolio_cap_percent_by_symbol,
        portfolio_cap_percent=portfolio_cap_percent,
        portfolio_cap_divisor=portfolio_cap_divisor,
        portfolio_cash_percent=portfolio_cash_percent,
        timeframe=timeframe,
        sleep_duration=sleep_duration,
        include_extended_hours_data=include_extended_hours_data,
        rules=rules,
        trade_stats=trade_stats,
        status_writer=write_status,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
