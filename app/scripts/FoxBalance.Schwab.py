#!/usr/bin/env python3
"""
FoxBalance.Schwab.py (Web-App Compatible)

Clone of the FoxBalance.Robinhood strategy using Schwab Trader + Market Data APIs.

Refactor goals:
- No interactive input() prompts
- Uses Cryptid Exchange broker_connections + Schwab token file
- Reads runtime parameters from --params-json
- Preserves FoxBalance ranking + calculus + ATR trailing-stop logic

Required CLI args (expected by the web app launcher):
  --run-dir <path>
  --params-json <path>
  --db-path <path_to_sqlite>
  --connection-id <int>   (broker_connections.id for Schwab)
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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
from app.schwab_history import fetch_price_history_with_min_candles, required_candles_for_lookbacks  # noqa: E402

# -----------------------------
# Policy knobs (ranking/eligibility)
# -----------------------------
PROFIT_LIQ_THRESHOLD_PCT = 0.00
INCLUDE_OVERWEIGHT_IN_LIQ_POOL = True

# -----------------------------
# Trading knobs (rebalance sizing)
# -----------------------------
MIN_ORDER_DOLLARS = 1.00
MIN_QTY_EPS = 1e-6

# -----------------------------
# Whole-share trade sizing (PARAM-DRIVEN)
# -----------------------------
DEFAULT_SHARES_PER_TRADE_LOOP = 1

# -----------------------------
# Trailing stop ATR settings
# -----------------------------
ATR_PERIOD = 14
ATR_MULTIPLIER = 2.0
MIN_TRAIL_AMOUNT_USD = 0.01
MIN_REQUIRED_CANDLES = required_candles_for_lookbacks([190], baseline=600, extra_candles=12)

# -----------------------------
# Mini chart sampling
# -----------------------------
CHART_POINTS = 90

# -----------------------------
# Calculus thresholds (keep logic + printing consistent)
# -----------------------------
BUY_RSI_LOW = 30.0
BUY_RSI_HIGH = 55.0
BUY_RSI_D_MIN = 1.0

SELL_RSI_MIN = 70.0
SELL_RSI_D_MAX = 0.5

# -----------------------------
# ANSI color helpers (terminal-only)
# -----------------------------
ANSI_RESET = "\033[0m"
ANSI_RED = "\033[31m"
ANSI_GREEN = "\033[32m"

ENABLE_ANSI_COLORS = True

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
# Market state cache
# -----------------------------
MARKET_STATE_TTL = 60
_MARKET_STATE_CACHE: Dict[str, Any] = {"ts": 0, "state": "regular", "session_tag": "NORMAL"}

# -----------------------------
# Globals initialized at runtime
# -----------------------------
_TRADER: Optional["SchwabTraderClient"] = None
_MARKET: Optional["SchwabMarketDataClient"] = None
_ACCOUNT_HASH: Optional[str] = None
_ACCOUNT_NUMBER: Optional[str] = None


# -----------------------------
# Timeframe config (Schwab PriceHistory)
# -----------------------------
TIMEFRAMES: Dict[str, Dict[str, Any]] = {
    "1m": {
        "periodType": "day",
        "period": 10,
        "frequencyType": "minute",
        "frequency": 1,
        "window": 52,
        "label": "52x1m",
        "fallbacks": [],
    },
    "5m": {
        "periodType": "day",
        "period": 10,
        "frequencyType": "minute",
        "frequency": 5,
        "window": 52,
        "label": "52x5m",
        "fallbacks": ["1m"],
    },
    "10m": {
        "periodType": "day",
        "period": 10,
        "frequencyType": "minute",
        "frequency": 10,
        "window": 52,
        "label": "52x10m",
        "fallbacks": ["5m", "1m"],
    },
    "15m": {
        "periodType": "day",
        "period": 10,
        "frequencyType": "minute",
        "frequency": 15,
        "window": 52,
        "label": "52x15m",
        "fallbacks": ["10m", "5m", "1m"],
    },
    "30m": {
        "periodType": "day",
        "period": 10,
        "frequencyType": "minute",
        "frequency": 30,
        "window": 52,
        "label": "52x30m",
        "fallbacks": ["15m", "10m", "5m", "1m"],
    },
    "1h": {
        "periodType": "day",
        "period": 20,
        "frequencyType": "minute",
        "frequency": 30,
        "window": 52,
        "label": "52x1h",
        "fallbacks": ["30m", "15m", "10m", "5m", "1m"],
    },
    "1d": {
        "periodType": "year",
        "period": 1,
        "frequencyType": "daily",
        "frequency": 1,
        "window": 52,
        "label": "52-day",
        "fallbacks": [],
    },
    "1w": {
        "periodType": "year",
        "period": 5,
        "frequencyType": "weekly",
        "frequency": 1,
        "window": 52,
        "label": "52-week",
        "fallbacks": [],
    },
    "1mo": {
        "periodType": "year",
        "period": 20,
        "frequencyType": "monthly",
        "frequency": 1,
        "window": 52,
        "label": "52-month",
        "fallbacks": [],
    },
}


# -----------------------------
# Formatting helpers
# -----------------------------

def colorize(text: str, color_code: str) -> str:
    if not ENABLE_ANSI_COLORS or not sys.stdout.isatty():
        return text
    return f"{color_code}{text}{ANSI_RESET}"


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def mark_condition(label: str, ok: bool, color_code: str) -> str:
    status = "OK" if ok else "NO"
    base = f"{label:<40} : {status}"
    return colorize(base, color_code) if ok else base


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
            "fields": "quote,regular",
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


def safe_sleep(seconds: float) -> None:
    print(f"[RATE LIMIT] Cooling down for {seconds:.2f}s...")
    time.sleep(max(0.0, float(seconds)))


def safe_call(fn, retries: int = 3, backoff: float = 0.8, name: str = "call"):
    for attempt in range(retries):
        try:
            return fn()
        except httpx.HTTPStatusError as e:
            status = _status_code(e)
            if status in (429, 500, 502, 503) and attempt < retries - 1:
                safe_sleep(backoff * (2 ** attempt))
                continue
            raise
        except httpx.RequestError:
            if attempt < retries - 1:
                safe_sleep(backoff * (2 ** attempt))
                continue
            raise
        except Exception:
            if attempt < retries - 1:
                safe_sleep(backoff * (2 ** attempt))
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
        return str(_MARKET_STATE_CACHE.get("state", "regular"))

    state = "regular"
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
        state = "regular"
        session_tag = "NORMAL"

    # Note: Extended hours support enabled - controlled by allow_extended_hours_orders parameter
    # state is kept as "extended" when in pre/post market hours

    _MARKET_STATE_CACHE["ts"] = now
    _MARKET_STATE_CACHE["state"] = state
    _MARKET_STATE_CACHE["session_tag"] = session_tag
    return state


def _order_session_for_state(state: str) -> str:
    if state == "regular":
        session = "NORMAL"
    elif state == "extended":
        session = str(_MARKET_STATE_CACHE.get("session_tag") or "SEAMLESS")
    else:
        session = "NORMAL"
    return _normalize_enum(session, SESSION_ENUM, "NORMAL")


def _normalize_enum(value: Any, allowed: set[str], default: str) -> str:
    text = str(value or "").strip().upper()
    if text in allowed:
        return text
    return default


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        if val in (None, "None"):
            return default
        return float(val)
    except Exception:
        return default


def _to_bool(val: Any, default: bool = False) -> bool:
    if isinstance(val, bool):
        return val
    if val is None:
        return default
    text = str(val).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off", ""}:
        return False
    return default


def _price_from_quote(quote_obj: Dict[str, Any], *, prefer_extended: bool) -> Optional[float]:
    if not isinstance(quote_obj, dict):
        return None
    quote = quote_obj.get("quote") or quote_obj
    regular = quote_obj.get("regular") or {}
    price = 0.0
    if isinstance(regular, dict):
        price = _safe_float(regular.get("regularMarketLastPrice"), default=0.0)
    if price <= 0:
        price = _safe_float(quote.get("lastPrice"), default=0.0)
    if price <= 0:
        price = _safe_float(quote.get("closePrice"), default=0.0)
    if price <= 0:
        price = _safe_float(quote.get("askPrice"), default=0.0)
    if price <= 0:
        price = _safe_float(quote.get("bidPrice"), default=0.0)
    return price if price > 0 else None


def _mid_price(bid: float, ask: float, fallback: float) -> float:
    if bid > 0 and ask > 0:
        return (bid + ask) / 2.0
    if bid > 0:
        return bid
    if ask > 0:
        return ask
    return fallback


# -----------------------------
# DreadFox calculus (ported)
# -----------------------------

def calculate_moving_average(prices: List[float], window_size: int) -> Optional[float]:
    if len(prices) < window_size:
        return None
    return float(np.mean(prices[-window_size:]))


def _ma_series(prices: List[float], window: int) -> List[Optional[float]]:
    out: List[Optional[float]] = [None] * len(prices)
    if len(prices) < window:
        return out
    window_sum = float(sum(prices[:window]))
    out[window - 1] = window_sum / window
    for i in range(window, len(prices)):
        window_sum += float(prices[i]) - float(prices[i - window])
        out[i] = window_sum / window
    return out


def _build_chart_series(prices: List[float], max_points: int = CHART_POINTS) -> Dict[str, List[Optional[float]]]:
    if not prices:
        return {}
    ma20_full = _ma_series(prices, 20)
    ma78_full = _ma_series(prices, 78)
    ma150_full = _ma_series(prices, 190)
    if max_points > 0 and len(prices) > max_points:
        offset = len(prices) - max_points
        return {
            "price": [float(p) for p in prices[-max_points:]],
            "ma20": ma20_full[offset:],
            "ma78": ma78_full[offset:],
            "ma150": ma150_full[offset:],
        }
    return {
        "price": [float(p) for p in prices],
        "ma20": ma20_full,
        "ma78": ma78_full,
        "ma150": ma150_full,
    }


def calculate_rsi(prices: List[float], period: int = 14) -> Optional[float]:
    if len(prices) < period + 1:
        return None

    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)

    avg_gain = float(np.sum(gains[:period]) / period)
    avg_loss = float(np.sum(losses[:period]) / period)

    for i in range(period, len(deltas)):
        avg_gain = ((avg_gain * (period - 1)) + float(gains[i])) / period
        avg_loss = ((avg_loss * (period - 1)) + float(losses[i])) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return float(100.0 - (100.0 / (1.0 + rs)))


def calculate_rsi_and_derivative(prices: List[float], period: int = 14) -> Tuple[Optional[float], Optional[float]]:
    if len(prices) < period + 3:
        return None, None

    rsi_t2 = calculate_rsi(prices[:-2], period)
    rsi_t1 = calculate_rsi(prices[:-1], period)
    rsi_t0 = calculate_rsi(prices, period)

    if rsi_t0 is None or rsi_t1 is None or rsi_t2 is None:
        return rsi_t0, None

    d_rsi = (rsi_t0 - rsi_t2) / 2.0
    return rsi_t0, float(d_rsi)


def dreadfox_signal(
    prices: List[float],
    current_price: float,
    avg_buy_price: float,
    held_qty: float,
) -> Tuple[str, Dict[str, Optional[float]]]:
    ma20 = calculate_moving_average(prices, 20)
    ma78 = calculate_moving_average(prices, 78)
    ma150 = calculate_moving_average(prices, 190)

    rsi, rsi_d = calculate_rsi_and_derivative(prices, period=14)

    meta: Dict[str, Optional[float]] = {
        "ma20": ma20,
        "ma78": ma78,
        "ma150": ma150,
        "rsi": rsi,
        "rsi_d": rsi_d,
    }

    if None in (ma20, ma78, ma150, rsi, rsi_d):
        return "HOLD", meta

    buy = (
        current_price > ma20
        and current_price < ma78
        and current_price < ma150
        and (BUY_RSI_LOW < float(rsi) < BUY_RSI_HIGH)
        and (float(rsi_d) > BUY_RSI_D_MIN)
    )

    sell = (
        held_qty > 0
        and current_price > ma20
        and current_price > ma78
        and current_price > ma150
        and (float(rsi_d) < SELL_RSI_D_MAX)
        and (float(rsi) > SELL_RSI_MIN)
        and (avg_buy_price > 0 and current_price > avg_buy_price)
    )

    if buy and not sell:
        return "BUY", meta
    if sell and not buy:
        return "SELL", meta
    return "HOLD", meta


# -----------------------------
# ATR
# -----------------------------

def extract_hlc(hist: List[Dict[str, Any]]) -> Tuple[List[float], List[float], List[float]]:
    highs: List[float] = []
    lows: List[float] = []
    closes: List[float] = []
    for row in hist:
        hp = row.get("high")
        lp = row.get("low")
        cp = row.get("close")
        if hp in (None, "None") or lp in (None, "None") or cp in (None, "None"):
            continue
        highs.append(float(hp))
        lows.append(float(lp))
        closes.append(float(cp))
    return highs, lows, closes


def calculate_atr_wilder(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    period: int = 14,
) -> Optional[float]:
    if len(highs) != len(lows) or len(lows) != len(closes):
        return None
    n = len(closes)
    if n < period + 1:
        return None

    trs: List[float] = []
    for i in range(1, n):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        trs.append(float(tr))

    if len(trs) < period:
        return None

    atr = float(np.mean(trs[:period]))
    for tr in trs[period:]:
        atr = ((atr * (period - 1)) + float(tr)) / period

    return float(atr)


# -----------------------------
# Portfolio model
# -----------------------------
@dataclass
class PositionRow:
    symbol: str
    quantity: float
    avg_buy_price: float
    current_price: float
    bid_price: float
    ask_price: float
    market_value: float
    pnl_percent: float
    alloc_percent: float
    delta_percent: float
    window_pos: float
    calc_signal: str
    calc_meta: Dict[str, Optional[float]]
    atr: Optional[float]
    chart: Dict[str, List[Optional[float]]]
    asset_type: str = ""


def build_status_rows(
    rows: List[PositionRow],
    *,
    top_liq: Optional[PositionRow],
    top_acq: Optional[PositionRow],
) -> List[Dict[str, Any]]:
    top_liq_sym = top_liq.symbol if top_liq is not None else ""
    top_acq_sym = top_acq.symbol if top_acq is not None else ""
    out: List[Dict[str, Any]] = []
    for r in rows:
        role = "LIQ" if r.symbol == top_liq_sym else ("ACQ" if r.symbol == top_acq_sym else "")
        out.append(
            {
                "symbol": r.symbol,
                "role": role,
                "signal": r.calc_signal,
                "price": r.current_price,
                "avg_buy": r.avg_buy_price,
                "qty": r.quantity,
                "market_value": r.market_value,
                "pnl_pct": r.pnl_percent,
                "alloc_pct": r.alloc_percent,
                "delta_pct": r.delta_percent,
                "window_pos": r.window_pos,
                "ma20": r.calc_meta.get("ma20"),
                "ma78": r.calc_meta.get("ma78"),
                "ma150": r.calc_meta.get("ma150"),
                "rsi": r.calc_meta.get("rsi"),
                "rsi_d": r.calc_meta.get("rsi_d"),
                "atr": r.atr,
                "chart": r.chart,
            }
        )
    return out


# -----------------------------
# Schwab portfolio helpers
# -----------------------------

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


def get_portfolio_equity() -> float:
    trader = _require_trader()
    accounts = trader.get_accounts()
    sec = _select_account(accounts)
    if not sec:
        raise RuntimeError("No Schwab account data available.")
    return float(_pick_equity(sec))


def get_reported_cash_field() -> float:
    trader = _require_trader()
    accounts = trader.get_accounts()
    sec = _select_account(accounts)
    if not sec:
        raise RuntimeError("No Schwab account data available.")
    return float(_pick_balance(sec, "cashBalance", "cashAvailableForTrading"))


def get_open_stock_positions() -> List[Dict[str, Any]]:
    trader = _require_trader()
    accounts = trader.get_accounts()
    sec = _select_account(accounts)
    if not sec:
        return []
    positions = sec.get("positions") or []
    if not isinstance(positions, list):
        return []
    return positions


def _instrument_type(inst: Dict[str, Any]) -> str:
    return str(inst.get("assetType") or inst.get("type") or "").upper()


def _is_option(inst: Dict[str, Any]) -> bool:
    return "OPTION" in _instrument_type(inst)


def get_symbols_and_qty_from_positions(positions: List[Dict[str, Any]]) -> Tuple[List[str], Dict[str, float], Dict[str, float]]:
    symbols: List[str] = []
    qty_by: Dict[str, float] = {}
    avg_by: Dict[str, float] = {}

    for p in positions:
        inst = p.get("instrument") or {}
        sym = str(inst.get("symbol") or "").upper().strip()
        if not sym:
            continue
        if _is_option(inst):
            continue

        qty = _safe_float(p.get("longQuantity"), default=0.0)
        if qty <= 0:
            qty = _safe_float(p.get("quantity"), default=0.0)
        if qty <= 0:
            continue

        avg_buy = _safe_float(
            p.get("averagePrice")
            or p.get("averageLongPrice")
            or p.get("taxLotAverageLongPrice"),
            default=0.0,
        )

        symbols.append(sym)
        qty_by[sym] = qty_by.get(sym, 0.0) + qty
        avg_by[sym] = avg_buy

    uniq = sorted(set(symbols))
    return uniq, qty_by, avg_by


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


def window_pos(prices: List[float], current_price: float, window: int) -> float:
    if len(prices) < max(10, window):
        return 0.0
    recent = prices[-window:]
    low = min(recent)
    high = max(recent)
    if high <= low:
        return 0.0
    x = (current_price - low) / (high - low)
    return float(max(0.0, min(1.0, x)))


def _resolve_timeframe_chain(timeframe_key: str) -> List[str]:
    if timeframe_key not in TIMEFRAMES:
        raise ValueError(f"Invalid timeframe '{timeframe_key}'. Choose from {list(TIMEFRAMES.keys())}.")
    tf = TIMEFRAMES[timeframe_key]
    return [timeframe_key] + list(tf.get("fallbacks") or [])


def get_position_rows(
    portfolio_equity: float,
    target_slice: float,
    timeframe_key: str,
    session_state: str,
    include_extended_hours_data: bool,
) -> List[PositionRow]:
    positions = get_open_stock_positions()
    symbols, qty_by, avg_by = get_symbols_and_qty_from_positions(positions)
    quotes = get_quotes_map(symbols)

    rows: List[PositionRow] = []
    tf_chain = _resolve_timeframe_chain(timeframe_key)

    for sym in symbols:
        try:
            qty = float(qty_by[sym])
            avg_buy = float(avg_by.get(sym, 0.0))

            qobj = _get_quote_for_symbol(quotes, sym)
            current = _price_from_quote(qobj, prefer_extended=False)
            if current is None:
                raise RuntimeError(f"Missing price for {sym}")
            quote_root = qobj.get("quote") if isinstance(qobj, dict) else None
            if not isinstance(quote_root, dict):
                quote_root = qobj if isinstance(qobj, dict) else {}

            bid_price = _safe_float(quote_root.get("bidPrice"), default=0.0)
            ask_price = _safe_float(quote_root.get("askPrice"), default=0.0)

            used_tf_key = None
            hist: List[Dict[str, Any]] = []
            closes_hist: List[float] = []
            highs: List[float] = []
            lows: List[float] = []

            for tf_key in tf_chain:
                tf = TIMEFRAMES[tf_key]
                need_extended = bool(include_extended_hours_data)

                # fetch_price_history_with_min_candles now handles current day inclusion automatically
                hist = fetch_price_history_with_min_candles(
                    fetch_fn=get_price_history,
                    symbol=sym,
                    period_type=str(tf["periodType"]),
                    period=int(tf["period"]),
                    frequency_type=str(tf["frequencyType"]),
                    frequency=int(tf["frequency"]),
                    need_extended=need_extended,
                    min_candles=MIN_REQUIRED_CANDLES,
                )

                highs, lows, closes_hist = extract_hlc(hist)
                if (len(closes_hist) + 1) >= MIN_REQUIRED_CANDLES:
                    used_tf_key = tf_key
                    break

            if used_tf_key is None:
                used_tf_key = tf_chain[-1]

            closes_for_calc = list(closes_hist)
            if closes_for_calc and closes_for_calc[-1] != current:
                closes_for_calc.append(current)

            if len(closes_for_calc) < MIN_REQUIRED_CANDLES:
                print(
                    f"[{sym}] Requested {MIN_REQUIRED_CANDLES}+ candles for timeframe "
                    f"{timeframe_key}; received {len(closes_hist)}."
                )
                continue

            sig, meta = dreadfox_signal(
                closes_for_calc, current_price=current, avg_buy_price=avg_buy, held_qty=qty
            )

            mv = _safe_float(qty * current, default=0.0)
            alloc = (mv / portfolio_equity) * 100 if portfolio_equity > 0 else 0.0
            delta = alloc - target_slice
            pnl = ((current - avg_buy) / avg_buy) * 100 if avg_buy > 0 else 0.0
            tf_win = TIMEFRAMES[used_tf_key]["window"]
            wp = window_pos(closes_for_calc, current, window=tf_win)

            atr = calculate_atr_wilder(highs, lows, closes_hist, period=ATR_PERIOD)
            chart = _build_chart_series(closes_for_calc)

            rows.append(
                PositionRow(
                    symbol=sym,
                    quantity=qty,
                    avg_buy_price=avg_buy,
                    current_price=current,
                    bid_price=bid_price,
                    ask_price=ask_price,
                    market_value=mv,
                    pnl_percent=pnl,
                    alloc_percent=alloc,
                    delta_percent=delta,
                    window_pos=wp,
                    calc_signal=sig,
                    calc_meta=meta,
                    atr=atr,
                    chart=chart,
                )
            )
        except Exception as e:
            print(f"[WARN] Failed processing {sym}: {e}")

    rows.sort(key=lambda r: r.alloc_percent, reverse=True)
    return rows


# -----------------------------
# Options market value (best-effort)
# -----------------------------

def get_options_market_value_total() -> float:
    positions = get_open_stock_positions()
    total = 0.0
    for p in positions:
        inst = p.get("instrument") or {}
        if not _is_option(inst):
            continue
        mv = _safe_float(p.get("marketValue"), default=0.0)
        if mv > 0:
            total += mv
    return float(total)


# -----------------------------
# Pretty printing
# -----------------------------

def _fmt_money(x: float) -> str:
    return f"${x:,.2f}"


def _fmt_price(x: float) -> str:
    return f"${x:,.2f}"


def _fmt_pct(x: float) -> str:
    return f"{x:+.2f}%"


def print_kv_table(rows: List[Tuple[str, str]], title: str = "") -> None:
    if title:
        print(title)
    w = max((len(k) for k, _ in rows), default=10)
    for k, v in rows:
        print(f"  {k:<{w}} : {v}")
    print()


def print_positions_table(rows: List[Tuple[PositionRow, float]], target_slice: float, label: str, title: str) -> None:
    safe_label = str(label or "window")
    if label is None:
        print("[WARN] Missing timeframe label; defaulting to 'window'.")
    print(title)
    header = (
        f"{'SYM':<6}"
        f"{'PX':>10} "
        f"{'QTY':>12} "
        f"{'MV':>12} "
        f"{'ALLOC%':>8} "
        f"{'TGT%':>7} "
        f"{'DELTA':>8} "
        f"{'PNL%':>8} "
        f"{(safe_label + '_pos'):>10} "
        f"{'ATR':>8} "
        f"{'RANK':>9} "
        f"{'SIG':>6}"
    )
    print(header)
    print("-" * len(header))

    if not rows:
        print("(none)\n")
        return

    for r, score in rows:
        atr_s = "--" if r.atr is None else f"{r.atr:.2f}"
        print(
            f"{r.symbol:<6}"
            f"{_fmt_price(r.current_price):>10} "
            f"{r.quantity:>12.6f} "
            f"{_fmt_money(r.market_value):>12} "
            f"{r.alloc_percent:>8.2f} "
            f"{target_slice:>7.2f} "
            f"{r.delta_percent:>8.2f} "
            f"{r.pnl_percent:>+8.2f} "
            f"{r.window_pos:>10.3f} "
            f"{atr_s:>8} "
            f"{score:>9.3f} "
            f"{r.calc_signal:>6}"
        )
    print()


def print_gating_table(liq: Optional[PositionRow], acq: Optional[PositionRow]) -> None:
    print("[CALCULUS GATING] (top-ranked only)")
    header = (
        f"{'TYPE':<4} {'SYM':<6} {'SIG':<5} {'NEED':<5} {'PASS':<5} "
        f"{'MA20':>10} {'MA78':>10} {'MA190':>10} {'RSI':>7} {'RSI_d':>7} {'ATR':>7}"
    )
    print(header)
    print("-" * len(header))

    def _num(x: Optional[float], width: int, prec: int) -> str:
        if x is None:
            return f"{'--':>{width}}"
        return f"{x:>{width}.{prec}f}"

    def row_line(tp: str, r: PositionRow, need: str) -> str:
        ok = (r.calc_signal == need)
        ma20 = r.calc_meta.get("ma20")
        ma78 = r.calc_meta.get("ma78")
        ma150 = r.calc_meta.get("ma150")
        rsi = r.calc_meta.get("rsi")
        rsi_d = r.calc_meta.get("rsi_d")

        return (
            f"{tp:<4} {r.symbol:<6} {r.calc_signal:<5} {need:<5} {('YES' if ok else 'NO'):<5} "
            f"{_num(ma20, 10, 2)} {_num(ma78, 10, 2)} {_num(ma150, 10, 2)} "
            f"{_num(rsi, 7, 2)} {_num(rsi_d, 7, 2)} {_num(r.atr, 7, 2)}"
        )

    if liq is None:
        print("LIQ  (none)")
    else:
        print(row_line("LIQ", liq, "SELL"))

    if acq is None:
        print("ACQ  (none)")
    else:
        print(row_line("ACQ", acq, "BUY"))
    print()


# -----------------------------
# Trailing amount formatting (CENT-PRECISE)
# -----------------------------

def round_to_cent(x: float) -> float:
    return float(f"{x:.2f}")


def atr_based_trail_amount(r: PositionRow) -> Optional[float]:
    if r.atr is None or r.atr <= 0:
        return None

    amt = ATR_MULTIPLIER * r.atr
    if amt < MIN_TRAIL_AMOUNT_USD:
        amt = MIN_TRAIL_AMOUNT_USD

    amt = round_to_cent(amt)
    if amt < MIN_TRAIL_AMOUNT_USD:
        amt = round_to_cent(MIN_TRAIL_AMOUNT_USD)

    return amt


# -----------------------------
# Condition explanations
# -----------------------------

def _safe_gt(a: Optional[float], b: Optional[float]) -> bool:
    return (a is not None) and (b is not None) and (a > b)


def _safe_lt(a: Optional[float], b: Optional[float]) -> bool:
    return (a is not None) and (b is not None) and (a < b)


def _safe_between(x: Optional[float], lo: float, hi: float) -> bool:
    return (x is not None) and (lo < x < hi)


def print_calculus_conditions(tp: str, r: PositionRow) -> None:
    ma20 = r.calc_meta.get("ma20")
    ma78 = r.calc_meta.get("ma78")
    ma150 = r.calc_meta.get("ma150")
    rsi = r.calc_meta.get("rsi")
    rsi_d = r.calc_meta.get("rsi_d")
    cp = r.current_price
    avg = r.avg_buy_price
    qty = r.quantity

    if tp == "LIQ":
        color = ANSI_RED
        print("[CALCULUS CONDITIONS] LIQ (SELL rules) - red means condition satisfied")
        conds = [
            ("held_qty > 0", qty > 0),
            ("current_price > MA20", _safe_gt(cp, ma20)),
            ("current_price > MA78", _safe_gt(cp, ma78)),
            ("current_price > MA190", _safe_gt(cp, ma150)),
            (f"RSI > {SELL_RSI_MIN:.0f}", (rsi is not None and rsi > SELL_RSI_MIN)),
            (f"RSI_d < {SELL_RSI_D_MAX:.2f}", (rsi_d is not None and rsi_d < SELL_RSI_D_MAX)),
            ("current_price > avg_buy_price", (avg is not None and avg > 0 and cp > avg)),
        ]
    else:
        color = ANSI_GREEN
        print("[CALCULUS CONDITIONS] ACQ (BUY rules) - green means condition satisfied")
        conds = [
            ("current_price > MA20", _safe_gt(cp, ma20)),
            ("current_price < MA78", _safe_lt(cp, ma78)),
            ("current_price < MA190", _safe_lt(cp, ma150)),
            (f"{BUY_RSI_LOW:.0f} < RSI < {BUY_RSI_HIGH:.0f}", _safe_between(rsi, BUY_RSI_LOW, BUY_RSI_HIGH)),
            (f"RSI_d > {BUY_RSI_D_MIN:.2f}", (rsi_d is not None and rsi_d > BUY_RSI_D_MIN)),
        ]

    for label, ok in conds:
        print("  " + mark_condition(label, ok, color))
    print()


def print_target_details(tp: str, r: PositionRow, target_slice: float, effective_cash: float) -> None:
    ma20 = r.calc_meta.get("ma20")
    ma78 = r.calc_meta.get("ma78")
    ma150 = r.calc_meta.get("ma150")
    rsi = r.calc_meta.get("rsi")
    rsi_d = r.calc_meta.get("rsi_d")

    trail_amt = atr_based_trail_amount(r) if tp == "LIQ" else None

    def n2(x: Optional[float]) -> str:
        return "--" if x is None else f"{x:.2f}"

    print(f"[TARGET DETAILS] {tp} -> {r.symbol}")
    print(f"  current_price         : {_fmt_price(r.current_price)}")
    print(f"  qty                  : {r.quantity:.6f}")
    print(f"  mv                   : {_fmt_money(r.market_value)}")
    print(f"  alloc% / tgt%         : {r.alloc_percent:.2f}% / {target_slice:.2f}%  (delta {r.delta_percent:+.2f}%)")
    print(f"  pnl%                 : {r.pnl_percent:+.2f}%")
    print(f"  window_pos           : {r.window_pos:.3f}")
    print(f"  calculus signal      : {r.calc_signal}")
    print(f"  MA20 / MA78 / MA190  : {n2(ma20)} / {n2(ma78)} / {n2(ma150)}")
    print(f"  RSI / RSI_d          : {n2(rsi)} / {n2(rsi_d)}")
    print(f"  ATR({ATR_PERIOD})            : {n2(r.atr)}")
    if tp == "LIQ":
        print(f"  trailAmount (2xATR)  : {('--' if trail_amt is None else f'${trail_amt:.2f}')}")
    else:
        print(f"  effective_cash       : {_fmt_money(effective_cash)}")
    print()

    print_calculus_conditions(tp, r)


# -----------------------------
# Ranking rules
# -----------------------------

def is_profit_liquidation_candidate(r: PositionRow) -> bool:
    return r.pnl_percent > PROFIT_LIQ_THRESHOLD_PCT


def is_overweight_liquidation_candidate(r: PositionRow) -> bool:
    return r.delta_percent > 0.0 and r.pnl_percent > PROFIT_LIQ_THRESHOLD_PCT


def liquidation_rank_score(r: PositionRow) -> float:
    return float((max(r.pnl_percent, 0.0) * 10000.0) + r.delta_percent)


def acquisition_rank_score(r: PositionRow) -> float:
    return float((1.0 - r.window_pos) + (abs(r.delta_percent) / 100.0))


# -----------------------------
# Whole-share execution checks
# -----------------------------

def clamp_whole_share_qty(requested_shares: int, available_shares: float) -> Optional[float]:
    if requested_shares < 1:
        return None
    if available_shares < float(requested_shares):
        return None
    return float(requested_shares)


def can_afford_whole_share_buy(requested_shares: int, effective_cash: float, price: float) -> bool:
    if requested_shares < 1 or price <= 0:
        return False
    return effective_cash >= (float(requested_shares) * price)


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


# -----------------------------
# Trading execution
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
    return {
        "orderType": order_type,
        "session": sess,
        "duration": duration,
        "price": float(price),
        "orderStrategyType": strategy,
        "orderLegCollection": [_order_leg(symbol, qty, instruction)],
    }


def _order_trailing_stop(symbol: str, qty: float, session: str, trail_amount: float) -> Dict[str, Any]:
    sess = _normalize_enum(session, SESSION_ENUM, "NORMAL")
    duration = _normalize_enum("GOOD_TILL_CANCEL", DURATION_ENUM, "GOOD_TILL_CANCEL")
    order_type = _normalize_enum("TRAILING_STOP", ORDER_TYPE_ENUM, "TRAILING_STOP")
    strategy = _normalize_enum("SINGLE", ORDER_STRATEGY_TYPE_ENUM, "SINGLE")
    link_basis = _normalize_enum("MANUAL", STOP_PRICE_LINK_BASIS_ENUM, "MANUAL")
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
        "orderLegCollection": [_order_leg(symbol, qty, "SELL")],
    }


def _order_stop(symbol: str, qty: float, session: str, stop_price: float) -> Dict[str, Any]:
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
        "orderLegCollection": [_order_leg(symbol, qty, "SELL")],
    }


def _place_order(order_obj: Dict[str, Any]) -> httpx.Response:
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


def place_trailing_stop_sell(symbol: str, qty: float, session: str, trail_amount: float, current_price: float) -> httpx.Response:
    try:
        order = _order_trailing_stop(symbol, qty, session, trail_amount)
        return _place_order(order)
    except Exception as e:
        print(f"[WARN] Trailing stop order failed ({e}); falling back to STOP.")
        stop_price = max(0.01, current_price - trail_amount)
        order = _order_stop(symbol, qty, session, stop_price)
        return _place_order(order)


# -----------------------------
# Full analysis + trade cycle
# -----------------------------

def fox_balance_cycle(
    timeframe_key: str,
    top_n: int,
    trading_enabled: bool,
    shares_per_loop: int,
    session_state: str,
    include_extended_hours_data: bool,
    *,
    trade_stats: Optional[Dict[str, Any]] = None,
    status_writer: Optional[Any] = None,
    status_reason: str = "",
) -> Dict[str, float]:
    if timeframe_key not in TIMEFRAMES and timeframe_key != "1h":
        raise ValueError(f"Invalid timeframe '{timeframe_key}'.")

    tf_key = "30m" if timeframe_key == "1h" else timeframe_key
    tf = TIMEFRAMES[tf_key]
    label = str(tf.get("label") or tf.get("window") or "window")

    equity = get_portfolio_equity()
    reported_cash = get_reported_cash_field()

    rows = get_position_rows(
        equity,
        target_slice=0.0,
        timeframe_key=tf_key,
        session_state=session_state,
        include_extended_hours_data=include_extended_hours_data,
    )

    slices = len(rows) + 1
    target_slice = (100.0 / slices) if slices > 0 else 0.0
    for r in rows:
        r.delta_percent = r.alloc_percent - target_slice

    stock_total = sum(r.market_value for r in rows)
    options_total = get_options_market_value_total()

    effective_cash = equity - stock_total - options_total
    if -1.0 < effective_cash < 0.0:
        effective_cash = 0.0

    cash_alloc = (effective_cash / equity) * 100 if equity > 0 else 0.0
    cash_delta = cash_alloc - target_slice

    liq_pool: List[PositionRow] = []
    for r in rows:
        if is_profit_liquidation_candidate(r) or (
            INCLUDE_OVERWEIGHT_IN_LIQ_POOL and is_overweight_liquidation_candidate(r)
        ):
            liq_pool.append(r)

    liq_symbols = {r.symbol for r in liq_pool}
    acq_pool = [
        r
        for r in rows
        if (r.symbol not in liq_symbols) and (r.pnl_percent <= PROFIT_LIQ_THRESHOLD_PCT)
    ]

    liq_ranked = sorted(
        [(r, liquidation_rank_score(r)) for r in liq_pool],
        key=lambda t: (t[0].pnl_percent, t[0].delta_percent),
        reverse=True,
    )
    acq_ranked = sorted(
        [(r, acquisition_rank_score(r)) for r in acq_pool],
        key=lambda t: t[1],
        reverse=True,
    )

    print_kv_table(
        [
            (
                "Timeframe",
                f"{timeframe_key} (periodType={tf['periodType']}, period={tf['period']}, "
                f"frequencyType={tf['frequencyType']}, frequency={tf['frequency']}, window={label})",
            ),
            ("Broker-reported equity", _fmt_money(equity)),
            ("Reported cash field (ref only)", _fmt_money(reported_cash)),
            ("Stock market value total", _fmt_money(stock_total)),
            ("Options market value total", _fmt_money(options_total)),
            ("Effective cash (equity - stocks - options)", _fmt_money(effective_cash)),
            ("Effective cash alloc", f"{cash_alloc:.2f}%"),
            ("Target slice", f"{target_slice:.4f}%"),
            ("Cash delta", _fmt_pct(cash_delta)),
            ("Held tickers (N)", str(len(rows))),
            ("Slices", f"{len(rows)} tickers + 1 cash = {len(rows) + 1}"),
            ("Ranking rule (LIQ)", "PnL>0 enters LIQ pool; LIQ ranked by PNL% then DELTA%"),
            ("Ranking rule (ACQ)", "PnL<=0 enters ACQ pool; ranked by 52-window lows then delta magnitude"),
            ("Options policy", "Options included in value math only; not included in slice/ranking pools"),
            (
                "Trailing stop policy",
                f"trailAmount = {ATR_MULTIPLIER:.1f}x ATR({ATR_PERIOD}), rounded to cents, floor=${MIN_TRAIL_AMOUNT_USD:.2f}",
            ),
            ("Trading enabled", "YES" if trading_enabled else "NO"),
            ("ANSI colors", "ON" if ENABLE_ANSI_COLORS else "OFF"),
            ("SELL RSI threshold", f">{SELL_RSI_MIN:.0f} (printing matches logic)"),
            ("Shares per trade loop", f"{shares_per_loop} (whole shares, user-configured)"),
        ],
        title="[PORTFOLIO SUMMARY]",
    )

    print_positions_table(
        liq_ranked[:top_n],
        target_slice,
        label,
        "[LIQUIDATION CANDIDATES] (profit-taking, ranked by PNL% then DELTA%)",
    )
    print_positions_table(
        acq_ranked[:top_n],
        target_slice,
        label,
        "[ACQUISITION CANDIDATES] (loss pool, ranked by 52-low bias)",
    )

    top_liq = liq_ranked[0][0] if liq_ranked else None
    top_acq = acq_ranked[0][0] if acq_ranked else None

    print_gating_table(top_liq, top_acq)

    if top_liq is not None:
        print_target_details("LIQ", top_liq, target_slice=target_slice, effective_cash=effective_cash)
    if top_acq is not None:
        print_target_details("ACQ", top_acq, target_slice=target_slice, effective_cash=effective_cash)

    if trading_enabled:
        if session_state != "regular":
            print("Market not open (regular hours only); skipping all orders.\n")
        else:
            session_tag = _order_session_for_state(session_state)

            if top_liq is not None and top_liq.calc_signal == "SELL":
                sell_qty = clamp_whole_share_qty(shares_per_loop, top_liq.quantity)
                if sell_qty is None:
                    print(
                        f"[TRADE] LIQ passed calculus, but insufficient shares for requested sell "
                        f"(need {shares_per_loop}, have {top_liq.quantity:.6f}); no order placed.\n"
                    )
                else:
                    trail_amt = atr_based_trail_amount(top_liq)
                    if trail_amt is None:
                        print(
                            "[TRADE] LIQ passed calculus, but ATR unavailable -> cannot compute trailAmount; no order placed.\n"
                        )
                    else:
                        print(
                            f"[TRADE] LIQ SELL (trailing stop) {top_liq.symbol} "
                            f"qty={sell_qty:.0f} trail=${trail_amt:.2f}"
                        )
                        resp = place_trailing_stop_sell(
                            top_liq.symbol,
                            sell_qty,
                            session_tag,
                            trail_amt,
                            top_liq.current_price,
                        )
                        print(f"[TRADE] Response: status={resp.status_code} location={resp.headers.get('Location')}\n")
                        if trade_stats is not None and _order_success(resp):
                            _record_trade(
                                trade_stats,
                                side="sell",
                                qty=float(sell_qty),
                                price=top_liq.current_price,
                                avg_buy_price=top_liq.avg_buy_price,
                            )
            else:
                if top_liq is None:
                    print("[TRADE] No LIQ candidate.\n")
                else:
                    print("[TRADE] Top LIQ did not pass SELL calculus; no order placed.\n")

            if top_acq is not None and top_acq.calc_signal == "BUY":
                if not can_afford_whole_share_buy(shares_per_loop, effective_cash, top_acq.current_price):
                    need = float(shares_per_loop) * top_acq.current_price
                    print(
                        f"[TRADE] ACQ passed calculus, but insufficient effective_cash for requested buy "
                        f"(need {_fmt_money(need)}, have {_fmt_money(effective_cash)}); no order placed.\n"
                    )
                else:
                    buy_qty = float(shares_per_loop)
                    print(
                        f"[TRADE] ACQ BUY (market) {top_acq.symbol} "
                        f"qty={buy_qty:.0f} (user-configured whole-share mode)"
                    )
                    resp = place_market_buy(top_acq.symbol, buy_qty, session_tag)
                    print(f"[TRADE] Response: status={resp.status_code} location={resp.headers.get('Location')}\n")
                    if trade_stats is not None and _order_success(resp):
                        _record_trade(
                            trade_stats,
                            side="buy",
                            qty=float(buy_qty),
                            price=top_acq.current_price,
                            avg_buy_price=0.0,
                        )
            else:
                if top_acq is None:
                    print("[TRADE] No ACQ candidate.\n")
                else:
                    print("[TRADE] Top ACQ did not pass BUY calculus; no order placed.\n")
    else:
        print("Trading disabled (analysis mode). No orders placed.\n")

    if status_writer is not None:
        try:
            status_writer(
                {
                    "phase": "cycle",
                    "timeframe": timeframe_key,
                    "trading_enabled": bool(trading_enabled),
                    "reason": status_reason,
                    "equity": equity,
                    "effective_cash": effective_cash,
                    "cash_alloc_pct": cash_alloc,
                    "target_slice_pct": target_slice,
                    "top_liq": top_liq.symbol if top_liq else "",
                    "top_acq": top_acq.symbol if top_acq else "",
                    "tickers": build_status_rows(rows, top_liq=top_liq, top_acq=top_acq),
                }
            )
        except Exception:
            pass

    reconstructed = stock_total + options_total + effective_cash
    print("[RECONCILIATION]")
    print(f"  Stock total + options total + effective cash = {_fmt_money(reconstructed)}")
    print(f"  Equity (reported)                           = {_fmt_money(equity)}")
    print(f"  Difference                                  = {_fmt_money(equity - reconstructed)}")
    print()

    return {
        "equity": equity,
        "stock_total": stock_total,
        "options_total": options_total,
        "effective_cash": effective_cash,
        "n_tickers": float(len(rows)),
    }


# -----------------------------
# Watch tier + triggers
# -----------------------------

def watch_snapshot(session_state: str) -> Dict[str, Any]:
    equity = get_portfolio_equity()
    positions = get_open_stock_positions()
    symbols, qty_by, _avg_by = get_symbols_and_qty_from_positions(positions)

    prices: Dict[str, float] = {}
    quotes = get_quotes_map(symbols)
    for sym in symbols:
        q = _get_quote_for_symbol(quotes, sym)
        price = _price_from_quote(q, prefer_extended=False)
        if price is None:
            price = _safe_float((q.get("quote") or q).get("lastPrice"), default=0.0)
        prices[sym] = float(price)

    return {"equity": equity, "symbols": symbols, "qty": qty_by, "prices": prices, "ts": time.time()}


def should_trigger(
    prev: Optional[Dict[str, Any]],
    curr: Dict[str, Any],
    price_move_trigger_pct: float,
    equity_move_trigger_usd: float,
) -> Tuple[bool, str]:
    if prev is None:
        return True, "initial"

    if prev["symbols"] != curr["symbols"]:
        return True, "symbols_changed"

    if abs(curr["equity"] - prev["equity"]) >= equity_move_trigger_usd:
        return True, f"equity_moved_${abs(curr['equity'] - prev['equity']):.2f}"

    for sym in curr["symbols"]:
        p0 = prev["prices"].get(sym)
        p1 = curr["prices"].get(sym)
        if p0 is None or p1 is None or p0 <= 0:
            continue
        move_pct = abs((p1 - p0) / p0) * 100.0
        if move_pct >= price_move_trigger_pct:
            return True, f"{sym}_moved_{move_pct:.2f}%"

    return False, "no_change"


# -----------------------------
# IO helpers
# -----------------------------

def load_params(params_path: str) -> Dict[str, Any]:
    p = Path(params_path)
    obj = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError("params-json must be a JSON object.")
    return obj


def _normalize_market_base(trader_base: str, market_base: str) -> str:
    base = market_base or trader_base
    if "/trader/" in base:
        base = base.replace("/trader/", "/marketdata/")
    if base.rstrip("/").endswith("/trader/v1"):
        base = base.rsplit("/trader/v1", 1)[0] + "/marketdata/v1"
    return base


# -----------------------------
# Main
# -----------------------------

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
        payload["script"] = "FoxBalance.Schwab"
        payload["pnl"] = round(float(trade_stats.get("pnl", 0.0)), 2)
        payload["trades"] = int(trade_stats.get("trades", 0))
        try:
            status_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            last_status.clear()
            last_status.update(payload)
        except Exception:
            pass

    def bump_heartbeat(reason: str) -> None:
        payload = dict(last_status) if last_status else {"phase": "watching"}
        payload["reason"] = reason
        write_status(payload)

    params = load_params(args.params_json)

    global ENABLE_ANSI_COLORS
    ENABLE_ANSI_COLORS = bool(params.get("enable_ansi_colors", True))

    timeframe_key = str(params.get("timeframe", "30m")).strip()
    if timeframe_key not in TIMEFRAMES and timeframe_key != "1h":
        print(f"[WARN] Invalid timeframe '{timeframe_key}'. Defaulting to '30m'.")
        timeframe_key = "30m"

    shares_per_loop = int(params.get("shares_per_trade_loop", DEFAULT_SHARES_PER_TRADE_LOOP))
    if shares_per_loop < 1:
        print(f"[WARN] shares_per_trade_loop must be >= 1. Defaulting to {DEFAULT_SHARES_PER_TRADE_LOOP}.")
        shares_per_loop = DEFAULT_SHARES_PER_TRADE_LOOP

    trading_enabled = bool(params.get("trading_enabled", False))
    top_n = int(params.get("top_n", 5))
    include_extended_hours_data = _to_bool(
        params.get("include_extended_hours_data", params.get("history_include_extended", True)),
        True,
    )

    price_move_trigger_pct = float(params.get("price_move_trigger_pct", 0.25))
    equity_move_trigger_usd = float(params.get("equity_move_trigger_usd", 5.0))
    max_silent_seconds = int(params.get("max_silent_seconds", 300))
    watch_interval_seconds = int(params.get("watch_interval_seconds", 10))

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

    print(f"Using timeframe: {timeframe_key}")
    print(f"Shares per trade loop: {shares_per_loop} (whole shares)")
    print(f"Trading enabled: {'YES' if trading_enabled else 'NO'}")
    print(f"History include extended hours: {'YES' if include_extended_hours_data else 'NO'}")

    prev_snap: Optional[Dict[str, Any]] = None
    last_full_run = 0.0

    session_state = ""
    while True:
        try:
            next_state = _market_session_state()
            if next_state != session_state:
                session_state = next_state
                if session_state == "regular":
                    print("Session: regular market hours (trailing stop sells enabled).")
                elif session_state == "extended":
                    print("Session: extended hours (limit orders at bid/ask midpoint).")
                else:
                    print("Session: market closed (orders skipped).")

            curr_snap = watch_snapshot(session_state)
            trigger, reason = should_trigger(
                prev_snap,
                curr_snap,
                price_move_trigger_pct=price_move_trigger_pct,
                equity_move_trigger_usd=equity_move_trigger_usd,
            )

            if (time.time() - last_full_run) >= float(max_silent_seconds):
                trigger = True
                reason = f"max_silent_{max_silent_seconds}s"

            if trigger:
                print(f"\n[TRIGGER] Running FoxBalance cycle (reason: {reason})\n")
                fox_balance_cycle(
                    timeframe_key=timeframe_key,
                    top_n=top_n,
                    trading_enabled=trading_enabled,
                    shares_per_loop=shares_per_loop,
                    session_state=session_state,
                    include_extended_hours_data=include_extended_hours_data,
                    trade_stats=trade_stats,
                    status_writer=write_status,
                    status_reason=reason,
                )
                last_full_run = time.time()
            else:
                bump_heartbeat(reason)

            prev_snap = curr_snap
            time.sleep(max(0.0, float(watch_interval_seconds)))

        except KeyboardInterrupt:
            print("\nExiting FoxBalance loop.")
            return 0
        except Exception as e:
            print(f"[WARN] Loop error: {e} (retrying in 15s)")
            print(traceback.format_exc())
            time.sleep(15)


if __name__ == "__main__":
    raise SystemExit(main())
