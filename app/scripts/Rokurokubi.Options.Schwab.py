#!/usr/bin/env python3
"""
Rokurokubi.Options.Schwab.py (Web-App Compatible)

Schwab-backed clone of Rokurokubi.Options.Robinhood:
- MA20/78/190 + RSI-derivative signals for stock buy/sell intent
- On sell intent, attempts covered-call harvest (sell-to-open calls)
- Optional stop-loss can sell whole shares and disarms after liquidation
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

stoploss_state: Dict[str, Dict[str, Any]] = {}
_TRADER: Optional["SchwabTraderClient"] = None
_MARKET: Optional["SchwabMarketDataClient"] = None
_ACCOUNT_HASH: Optional[str] = None
_ACCOUNT_NUMBER: Optional[str] = None

CHART_POINTS = 90
ATR_PERIOD = 14
MIN_TRAIL_AMOUNT_USD = 0.01
CC_SHORTLIST_MAX = 8
MIN_REQUIRED_CANDLES = required_candles_for_lookbacks([190], baseline=600, extra_candles=12)

MARKET_STATE_TTL = 60
_MARKET_STATE_CACHE: Dict[str, Any] = {"ts": 0, "state": "regular", "session_tag": "NORMAL"}

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
    "5m": {"periodType": "day", "period": 10, "frequencyType": "minute", "frequency": 5},
    "10m": {"periodType": "day", "period": 10, "frequencyType": "minute", "frequency": 10},
    "30m": {"periodType": "day", "period": 10, "frequencyType": "minute", "frequency": 30},
    "1d": {"periodType": "year", "period": 1, "frequencyType": "daily", "frequency": 1},
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

    def get_order(self, *, encrypted_account_number: str, order_id: int) -> Dict[str, Any]:
        url = self._url(f"/accounts/{encrypted_account_number}/orders/{order_id}")

        def _call():
            with httpx.Client(timeout=30.0) as c:
                r = c.get(url, headers=self._headers())
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, dict):
                raise SchwabTraderError("Unexpected response for order lookup.")
            return data

        return safe_call(_call, name="get_order")

    def cancel_order(self, *, encrypted_account_number: str, order_id: int) -> httpx.Response:
        url = self._url(f"/accounts/{encrypted_account_number}/orders/{order_id}")

        def _call():
            with httpx.Client(timeout=30.0) as c:
                r = c.delete(url, headers=self._headers())
            r.raise_for_status()
            return r

        return safe_call(_call, name="cancel_order")


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

    def get_option_chain(self, params: Dict[str, Any]) -> Dict[str, Any]:
        url = self._url("/chains")

        def _call():
            with httpx.Client(timeout=30.0) as c:
                r = c.get(url, headers=self._headers(), params=params)
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, dict):
                raise SchwabTraderError("Unexpected response for /chains")
            return data

        return safe_call(_call, name="option_chain")


# -----------------------------
# Retry helpers
# -----------------------------
def _status_code(exc: Exception) -> Optional[int]:
    return getattr(getattr(exc, "response", None), "status_code", None)


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

    # Hard gate: Schwab orders only during regular session.
    if state != "regular":
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
    else:
        session = "NORMAL"
    return _normalize_enum(session, SESSION_ENUM, "NORMAL")


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
def calculate_moving_average(prices: List[float], window: int) -> Optional[float]:
    if len(prices) < window:
        return None
    return float(np.mean(prices[-window:]))


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


def calculate_ma_derivative(prices: List[float], window_size: int) -> Optional[float]:
    if len(prices) < window_size + 1:
        return None
    ma_now = calculate_moving_average(prices, window_size)
    ma_prev = calculate_moving_average(prices[:-1], window_size)
    if ma_now is None or ma_prev is None:
        return None
    return float(ma_now - ma_prev)


def calculate_atr_wilder(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    period: int = ATR_PERIOD,
) -> Optional[float]:
    n = min(len(highs), len(lows), len(closes))
    if n < period + 1:
        return None
    trs: List[float] = []
    for i in range(1, n):
        h = float(highs[i])
        l = float(lows[i])
        pc = float(closes[i - 1])
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    if len(trs) < period:
        return None
    atr = float(sum(trs[:period]) / period)
    for tr in trs[period:]:
        atr = ((atr * (period - 1)) + float(tr)) / period
    return float(atr)


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
    ma30_full = _ma_series(prices, 30)
    ma78_full = _ma_series(prices, 78)
    ma150_full = _ma_series(prices, 190)
    if max_points > 0 and len(prices) > max_points:
        offset = len(prices) - max_points
        return {
            "price": [float(p) for p in prices[-max_points:]],
            "ma20": ma30_full[offset:],
            "ma78": ma78_full[offset:],
            "ma150": ma150_full[offset:],
        }
    return {
        "price": [float(p) for p in prices],
        "ma20": ma30_full,
        "ma78": ma78_full,
        "ma150": ma150_full,
    }


def should_buy(
    current_price: float,
    ma30: Optional[float],
    ma78: Optional[float],
    ma190: Optional[float],
    rsi_derivative: Optional[float],
    ma30_derivative: Optional[float],
) -> bool:
    if rsi_derivative is None or ma30_derivative is None:
        return False
    if None in (ma30, ma78, ma190):
        return False
    return (
        current_price > ma30
        and current_price < ma78
        and current_price < ma190
        and rsi_derivative > 0
        and ma30_derivative > 0
    )


def should_sell(
    current_price: float,
    ma30: Optional[float],
    ma78: Optional[float],
    ma190: Optional[float],
    rsi: Optional[float],
    rsi_derivative: Optional[float],
    ma30_derivative: Optional[float],
    quantity: float,
) -> bool:
    if rsi is None or rsi_derivative is None or ma30_derivative is None:
        return False
    if None in (ma30, ma78, ma190):
        return False
    sell_condition_a = (
        quantity > 0
        and current_price > ma30
        and current_price > ma78
        and current_price > ma190
        and rsi > 70
        and rsi_derivative < 0
    )
    sell_condition_b = (
        quantity > 0
        and current_price > ma190
        and current_price > ma78
        and ma30_derivative < 0
    )
    return sell_condition_a or sell_condition_b


def print_indicator_signals(
    ticker: str,
    current_price: float,
    ma30: Optional[float],
    ma78: Optional[float],
    ma190: Optional[float],
    rsi: Optional[float],
    rsi_derivative: Optional[float],
    ma30_derivative: Optional[float],
    quantity: float,
    avg_buy_price: float,
) -> str:
    action = "Hold"
    percentage_gain_loss: Optional[float] = None

    if avg_buy_price > 0:
        percentage_gain_loss = ((current_price - avg_buy_price) / avg_buy_price) * 100

    if current_price > 0 and None not in (ma30, ma78, ma190, rsi, rsi_derivative, ma30_derivative):
        if should_buy(current_price, ma30, ma78, ma190, rsi_derivative, ma30_derivative):
            action = "Buy"
        elif should_sell(current_price, ma30, ma78, ma190, rsi, rsi_derivative, ma30_derivative, quantity):
            action = "Sell"

    print(
        f"Ticker: {ticker}\n"
        f"Current Price: {current_price}\n"
        f"30-Period Moving Average: {ma30}\n"
        f"78-Period Moving Average: {ma78}\n"
        f"190-Period Moving Average: {ma190}\n"
        f"RSI: {rsi}\n"
        f"RSI Derivative (3-candle): {rsi_derivative}\n"
        f"30-MA Derivative: {ma30_derivative}\n"
        f"Quantity Held: {quantity}\n"
        f"Average Buy Price: {avg_buy_price}\n"
        f"Percentage Gain/Loss: {'N/A' if percentage_gain_loss is None else f'{percentage_gain_loss:.2f}%'}\n"
        f"Determined Action: {action}\n"
        f"------------------------------"
    )
    return action


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
# Option chain helpers
# -----------------------------
def _to_date(s: str) -> datetime.date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def round_down_to_tick(price: float, tick: float) -> float:
    if tick <= 0:
        raise ValueError("Tick must be > 0")
    return math.floor(price / tick) * tick


def _option_bid_ask(opt: Dict[str, Any]) -> Tuple[float, float]:
    bid = _safe_float(opt.get("bid") or opt.get("bidPrice"), default=0.0)
    ask = _safe_float(opt.get("ask") or opt.get("askPrice"), default=0.0)
    return bid, ask


def _extract_call_map(chain: Dict[str, Any]) -> Dict[str, Any]:
    return chain.get("callExpDateMap") or {}


def list_candidate_calls(chain: Dict[str, Any], spot: float, max_dte: int, strike_below: int, strike_above: int):
    call_map = _extract_call_map(chain)
    today = datetime.utcnow().date()

    min_strike = math.floor(spot) - int(strike_below)
    max_strike = math.floor(spot) + int(strike_above)

    for exp_key, strikes in call_map.items():
        if not isinstance(strikes, dict):
            continue
        exp_date = exp_key.split(":")[0]
        try:
            dte = (_to_date(exp_date) - today).days
        except Exception:
            continue
        if dte < 0 or dte > max_dte:
            continue
        for strike_key, opts in strikes.items():
            try:
                strike = float(strike_key)
            except Exception:
                continue
            if strike < min_strike or strike > max_strike:
                continue
            if isinstance(opts, list) and opts:
                opt = opts[0]
            elif isinstance(opts, dict):
                opt = opts
            else:
                continue
            yield exp_date, strike, opt


def score_call(opt: Dict[str, Any], exp: str, strike: float, avg_cost: float, min_bid: float):
    result = {
        "expiration": exp,
        "strike": float(strike),
        "bid": None,
        "ask": None,
        "spread": None,
        "breakeven": None,
        "premium_per_contract": None,
        "capped_gain_per_share": None,
        "capped_gain_pct": None,
        "score": None,
        "qualifies": False,
        "reject_reason": None,
        "option_symbol": opt.get("symbol"),
    }
    bid, ask = _option_bid_ask(opt)
    result["bid"] = bid
    result["ask"] = ask
    result["spread"] = ask - bid

    if ask <= 0:
        result["reject_reason"] = "ask_nonpositive"
        return result
    if bid < min_bid:
        result["reject_reason"] = "bid_below_min"
        return result

    breakeven = float(strike) + bid
    capped_gain_per_share = breakeven - float(avg_cost)
    capped_gain_pct = (capped_gain_per_share / float(avg_cost)) * 100.0 if avg_cost > 0 else 0.0

    result["breakeven"] = breakeven
    result["premium_per_contract"] = bid * 100.0
    result["capped_gain_per_share"] = capped_gain_per_share
    result["capped_gain_pct"] = capped_gain_pct

    if breakeven < float(avg_cost):
        result["reject_reason"] = "breakeven_below_avg_cost"
        return result
    if not result.get("option_symbol"):
        result["reject_reason"] = "missing_option_symbol"
        return result

    result["score"] = (breakeven, capped_gain_pct, bid)
    result["qualifies"] = True
    return result


def choose_best_call(
    chain: Dict[str, Any],
    spot: float,
    max_dte: int,
    strike_below: int,
    strike_above: int,
    avg_cost: float,
    min_bid: float,
):
    best = None
    checked = 0
    qualified = 0
    rejected_reasons: Dict[str, int] = {}
    qualified_rows: List[Dict[str, Any]] = []
    scanned_rows: List[Dict[str, Any]] = []

    for exp, strike, opt in list_candidate_calls(chain, spot, max_dte, strike_below, strike_above):
        checked += 1
        scored = score_call(opt, exp, strike, avg_cost, min_bid)
        scanned_rows.append(scored)
        if not scored.get("qualifies"):
            reason = str(scored.get("reject_reason") or "rejected_other")
            rejected_reasons[reason] = int(rejected_reasons.get(reason, 0)) + 1
            continue
        qualified += 1
        qualified_rows.append(scored)
        if (best is None) or (scored["score"] > best["score"]):
            best = scored

    qualified_rows.sort(
        key=lambda r: (
            float(r.get("breakeven") or 0.0),
            float(r.get("capped_gain_pct") or 0.0),
            float(r.get("bid") or 0.0),
        ),
        reverse=True,
    )
    top_qualified = qualified_rows[:5]
    sorted_scanned = sorted(
        scanned_rows,
        key=lambda r: (
            float(r.get("breakeven") or -1e9),
            float(r.get("strike") or 0.0),
        ),
        reverse=True,
    )
    shortlist = sorted_scanned[:CC_SHORTLIST_MAX]
    rejection_list = sorted(rejected_reasons.items(), key=lambda kv: kv[1], reverse=True)
    top_rejection = rejection_list[0][0] if rejection_list else None
    top_rejection_count = int(rejection_list[0][1]) if rejection_list else 0
    meta = {
        "spot": spot,
        "candidates_checked": checked,
        "qualified_count": qualified,
        "rejection_counts": rejected_reasons,
        "top_qualified": top_qualified,
        "shortlist": shortlist,
        "top_rejection_reason": top_rejection,
        "top_rejection_count": top_rejection_count,
    }
    return best, meta


def _reject_reason_label(reason: Optional[str]) -> str:
    mapping = {
        "missing_option_symbol": "Missing option symbol",
        "ask_nonpositive": "Ask <= 0",
        "bid_below_min": "Bid below min",
        "breakeven_below_avg_cost": "BE < avg cost",
        "rejected_other": "Other reject",
    }
    key = str(reason or "")
    return mapping.get(key, key or "Unknown")


def _shortlist_text(meta: Dict[str, Any], max_items: int = 4) -> str:
    rows = meta.get("shortlist") or []
    if not isinstance(rows, list) or not rows:
        return "No in-range options scanned."
    parts: List[str] = []
    for row in rows[:max_items]:
        exp = str(row.get("expiration") or "?")
        strike = float(row.get("strike") or 0.0)
        be = row.get("breakeven")
        be_txt = f"{float(be):.2f}" if be is not None else "--"
        if row.get("qualifies"):
            state = "OK"
        else:
            state = f"X:{_reject_reason_label(row.get('reject_reason'))}"
        parts.append(f"{exp} {strike:.2f}C BE {be_txt} {state}")
    return " | ".join(parts)


def print_call_scan_summary(
    ticker: str,
    *,
    avg_buy_price: float,
    meta: Dict[str, Any],
    best: Optional[Dict[str, Any]],
) -> None:
    checked = int(meta.get("candidates_checked") or 0)
    qualified = int(meta.get("qualified_count") or 0)
    print(f"[CC] {ticker}: Scan summary -> checked={checked}, qualified={qualified}, avg_cost={avg_buy_price:.2f}")

    shortlist = meta.get("shortlist") or []
    if isinstance(shortlist, list) and shortlist:
        print("[CC] In-range shortlist (shows why contracts are/aren't eligible yet):")
        for i, row in enumerate(shortlist, start=1):
            exp = str(row.get("expiration") or "?")
            strike = float(row.get("strike") or 0.0)
            bid = row.get("bid")
            ask = row.get("ask")
            be = row.get("breakeven")
            bid_txt = f"{float(bid):.2f}" if bid is not None else "--"
            ask_txt = f"{float(ask):.2f}" if ask is not None else "--"
            be_txt = f"{float(be):.2f}" if be is not None else "--"
            if row.get("qualifies"):
                reason_txt = "eligible"
            else:
                reason_txt = _reject_reason_label(row.get("reject_reason"))
            print(
                f"  #{i:<2} {exp} {strike:.2f}C | bid={bid_txt} ask={ask_txt} "
                f"BE={be_txt} | {reason_txt}"
            )

    top_qualified = meta.get("top_qualified") or []
    if isinstance(top_qualified, list) and top_qualified:
        print("[CC] Top qualifying calls:")
        for i, row in enumerate(top_qualified, start=1):
            print(
                f"  #{i:<2} {row.get('expiration')} {float(row.get('strike') or 0.0):.2f}C | "
                f"bid={float(row.get('bid') or 0.0):.2f} ask={float(row.get('ask') or 0.0):.2f} "
                f"BE={float(row.get('breakeven') or 0.0):.2f} "
                f"cap={float(row.get('capped_gain_per_share') or 0.0):.2f}/sh "
                f"({float(row.get('capped_gain_pct') or 0.0):.2f}%)"
            )
    else:
        top_reason = meta.get("top_rejection_reason")
        top_reason_count = int(meta.get("top_rejection_count") or 0)
        if top_reason:
            print(
                f"[CC] No qualifying calls. Most common reject reason: {top_reason} ({top_reason_count} contracts)."
            )
        else:
            print("[CC] No qualifying calls and no rejection detail available.")

    if best is not None:
        print(
            f"[CC] Selected contract -> {best.get('expiration')} {float(best.get('strike') or 0.0):.2f}C "
            f"BE={float(best.get('breakeven') or 0.0):.2f} "
            f"cap={float(best.get('capped_gain_per_share') or 0.0):.2f}/sh "
            f"({float(best.get('capped_gain_pct') or 0.0):.2f}%)"
        )


def get_option_chain(symbol: str, *, contract_type: str = "CALL", strike: Optional[float] = None,
                     from_date: Optional[str] = None, to_date: Optional[str] = None) -> Dict[str, Any]:
    market = _require_market()
    params: Dict[str, Any] = {
        "symbol": symbol,
        "contractType": contract_type,
        "includeUnderlyingQuote": "true",
        "strategy": "SINGLE",
    }
    if strike is not None:
        params["strike"] = strike
    if from_date:
        params["fromDate"] = from_date
    if to_date:
        params["toDate"] = to_date
    return market.get_option_chain(params)


def get_option_quote(symbol: str, exp: str, strike: float) -> Optional[Dict[str, Any]]:
    chain = get_option_chain(symbol, strike=strike, from_date=exp, to_date=exp)
    call_map = _extract_call_map(chain)
    target_key = None
    for key in call_map.keys():
        if str(key).startswith(exp):
            target_key = key
            break
    if target_key is None:
        return None
    strikes = call_map.get(target_key) or {}
    for strike_key, opts in strikes.items():
        try:
            if abs(float(strike_key) - float(strike)) > 1e-6:
                continue
        except Exception:
            continue
        if isinstance(opts, list) and opts:
            return opts[0]
        if isinstance(opts, dict):
            return opts
    return None


# -----------------------------
# Order helpers
# -----------------------------
def _order_leg(symbol: str, qty: float, instruction: str, asset_type: str = "EQUITY") -> Dict[str, Any]:
    instr = _normalize_enum(instruction, INSTRUCTION_ENUM, "BUY")
    return {
        "instruction": instr,
        "quantity": float(qty),
        "instrument": {"symbol": symbol, "assetType": asset_type},
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
        "orderLegCollection": [_order_leg(symbol, qty, instruction, "EQUITY")],
    }


def _order_limit(symbol: str, qty: float, instruction: str, session: str, price: float, asset_type: str) -> Dict[str, Any]:
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
        "orderLegCollection": [_order_leg(symbol, qty, instruction, asset_type)],
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
        "orderLegCollection": [_order_leg(symbol, qty, "SELL", "EQUITY")],
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
        "orderLegCollection": [_order_leg(symbol, qty, "SELL", "EQUITY")],
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
    order = _order_limit(symbol, qty, "BUY", session, price, "EQUITY")
    return _place_order(order)


def place_limit_sell(symbol: str, qty: float, session: str, price: float) -> httpx.Response:
    order = _order_limit(symbol, qty, "SELL", session, price, "EQUITY")
    order["duration"] = _normalize_enum("GOOD_TILL_CANCEL", DURATION_ENUM, "GOOD_TILL_CANCEL")
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


def _extract_order_id(resp: httpx.Response) -> Optional[int]:
    location = resp.headers.get("Location") or resp.headers.get("location")
    if not location:
        return None
    try:
        return int(str(location).rstrip("/").split("/")[-1])
    except Exception:
        return None


def place_sell_to_open_limit(option_symbol: str, limit_price: float, qty_contracts: int, tif: str) -> Optional[int]:
    order = _order_limit(option_symbol, qty_contracts, "SELL_TO_OPEN", "NORMAL", limit_price, "OPTION")
    order["duration"] = _normalize_enum("GOOD_TILL_CANCEL", DURATION_ENUM, "GOOD_TILL_CANCEL")
    resp = _place_order(order)
    if not _order_success(resp):
        return None
    return _extract_order_id(resp)


def is_option_order_open(order_id: int) -> bool:
    try:
        trader = _require_trader()
        account_hash = _require_account_hash()
        order = trader.get_order(encrypted_account_number=account_hash, order_id=order_id)
        status = str(order.get("status") or "").upper()
        open_status = {
            "AWAITING_PARENT_ORDER",
            "AWAITING_CONDITION",
            "AWAITING_STOP_CONDITION",
            "AWAITING_MANUAL_REVIEW",
            "ACCEPTED",
            "AWAITING_UR_OUT",
            "PENDING_ACTIVATION",
            "QUEUED",
            "WORKING",
            "PENDING_CANCEL",
            "PENDING_REPLACE",
            "NEW",
            "AWAITING_RELEASE_TIME",
            "PENDING_ACKNOWLEDGEMENT",
            "PENDING_RECALL",
        }
        return status in open_status
    except Exception as e:
        print(f"[WARN] Could not fetch option order {order_id}: {e}")
        return True


def cancel_option_order(order_id: int) -> None:
    try:
        trader = _require_trader()
        account_hash = _require_account_hash()
        trader.cancel_order(encrypted_account_number=account_hash, order_id=order_id)
        print(f"[ORDER] Canceled option order {order_id}")
    except Exception as e:
        print(f"[WARN] Cancel failed for {order_id}: {e}")


def execute_covered_call_harvest(
    *,
    ticker: str,
    spot: float,
    quantity: float,
    avg_buy_price: float,
    max_dte: int,
    strike_below: int,
    strike_above: int,
    option_tick: float,
    ask_undercut: float,
    spread_narrow_threshold: float,
    min_bid: float,
    poll_seconds: float,
    tif: str,
    max_reprices: int,
    order_timeout_seconds: int,
    session_state: str,
    trade_stats: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    if session_state != "regular":
        print(f"[CC] {ticker}: Options orders only during regular hours. Skipping.")
        return None, None

    if avg_buy_price <= 0:
        print(f"[CC] {ticker}: Missing avg buy price; skipping covered call.")
        return None, None
    lots = int(math.floor(quantity / 100.0))
    if lots <= 0:
        print(f"[CC] {ticker}: Sell trigger hit, but you have {quantity:.2f} shares (<100).")
        return None, None

    chain = get_option_chain(ticker)
    best, meta = choose_best_call(
        chain,
        spot=spot,
        max_dte=max_dte,
        strike_below=strike_below,
        strike_above=strike_above,
        avg_cost=avg_buy_price,
        min_bid=min_bid,
    )
    print_call_scan_summary(ticker, avg_buy_price=avg_buy_price, meta=meta, best=best)

    if best is None:
        print(f"[CC] {ticker}: No qualifying calls found. Checked {meta.get('candidates_checked')} candidates.")
        return None, {
            "cc_candidates_checked": int(meta.get("candidates_checked") or 0),
            "cc_qualified_count": int(meta.get("qualified_count") or 0),
            "cc_top_reject_reason": meta.get("top_rejection_reason"),
            "cc_top_reject_count": int(meta.get("top_rejection_count") or 0),
            "cc_shortlist_text": _shortlist_text(meta),
        }

    capped_gain_per_share = float(best["breakeven"]) - float(avg_buy_price)
    capped_gain_pct = (capped_gain_per_share / float(avg_buy_price)) * 100.0 if avg_buy_price > 0 else 0.0
    capped_gain_per_contract = capped_gain_per_share * 100.0
    summary = {
        "cc_best_exp": best["expiration"],
        "cc_best_strike": float(best["strike"]),
        "cc_best_bid": float(best["bid"]),
        "cc_best_ask": float(best["ask"]),
        "cc_breakeven": float(best["breakeven"]),
        "cc_capped_gain": capped_gain_per_share,
        "cc_capped_gain_pct": capped_gain_pct,
        "cc_capped_gain_contract": capped_gain_per_contract,
        "cc_candidates_checked": int(meta.get("candidates_checked") or 0),
        "cc_qualified_count": int(meta.get("qualified_count") or 0),
        "cc_top_reject_reason": meta.get("top_rejection_reason"),
        "cc_top_reject_count": int(meta.get("top_rejection_count") or 0),
        "cc_shortlist_text": _shortlist_text(meta),
    }

    print(
        f"[CC] {ticker}: Best call selected: {best['expiration']} {best['strike']}C "
        f"bid={best['bid']:.2f} ask={best['ask']:.2f} BE={best['breakeven']:.2f} "
        f"(avg_cost={avg_buy_price:.2f}, lots={lots}, checked={meta.get('candidates_checked')})"
    )

    order_id: Optional[int] = None
    working_limit: Optional[float] = None
    mode: Optional[str] = None
    reprices = 0
    started = time.time()

    while True:
        if order_timeout_seconds > 0 and (time.time() - started) >= order_timeout_seconds:
            if order_id and is_option_order_open(order_id):
                cancel_option_order(order_id)
            print(f"[CC] {ticker}: Timeout reached; stopping order management.")
            return None, summary

        if order_id is not None and not is_option_order_open(order_id):
            print(f"[CC] {ticker}: Order no longer open (likely filled/closed). Done.")
            return f"Sold {lots} covered call(s) on {ticker}.", summary

        md = get_option_quote(ticker, best["expiration"], best["strike"])
        if not md:
            time.sleep(poll_seconds)
            continue

        bid, ask = _option_bid_ask(md)
        if bid <= 0 or ask <= 0:
            time.sleep(poll_seconds)
            continue

        breakeven_now = best["strike"] + bid
        if breakeven_now < avg_buy_price:
            if order_id and is_option_order_open(order_id):
                cancel_option_order(order_id)
            print(f"[CC] {ticker}: BE safety failed (BE={breakeven_now:.2f} < avg_cost={avg_buy_price:.2f}).")
            return None, summary

        spread = ask - bid

        if spread <= spread_narrow_threshold:
            desired = bid
            new_mode = "BID_MODE"
        else:
            desired = max(bid, ask - ask_undercut)
            new_mode = "ASK_UNDERCUT"

        new_limit = round_down_to_tick(desired, option_tick)
        stepped_in_front = (working_limit is not None) and (ask < (working_limit + option_tick))

        need_new_order = (
            order_id is None
            or mode != new_mode
            or stepped_in_front
            or (working_limit != new_limit)
        )

        if need_new_order:
            if max_reprices >= 0 and reprices > max_reprices:
                if order_id and is_option_order_open(order_id):
                    cancel_option_order(order_id)
                print(f"[CC] {ticker}: Max reprices exceeded; stopping.")
                return None, summary

            if order_id and is_option_order_open(order_id):
                cancel_option_order(order_id)

            order_id = place_sell_to_open_limit(
                option_symbol=best["option_symbol"],
                limit_price=new_limit,
                qty_contracts=lots,
                tif=tif,
            )
            working_limit = new_limit
            mode = new_mode
            reprices += 1
            if trade_stats is not None and order_id is not None:
                trade_stats["trades"] = int(trade_stats.get("trades", 0)) + 1

            print(
                f"[CC] {ticker}: Placed sell-to-open {lots}x {best['expiration']} {best['strike']}C "
                f"limit={working_limit:.2f} mode={mode} (bid={bid:.2f} ask={ask:.2f} spread={spread:.2f})"
            )

        time.sleep(poll_seconds)


def preview_covered_call_scan(
    *,
    ticker: str,
    spot: float,
    quantity: float,
    avg_buy_price: float,
    max_dte: int,
    strike_below: int,
    strike_above: int,
    min_bid: float,
    session_state: str,
) -> Dict[str, Any]:
    lots = int(math.floor(float(quantity) / 100.0))
    out: Dict[str, Any] = {
        "cc_best_exp": None,
        "cc_best_strike": None,
        "cc_best_bid": None,
        "cc_best_ask": None,
        "cc_breakeven": None,
        "cc_capped_gain": None,
        "cc_capped_gain_pct": None,
        "cc_capped_gain_contract": None,
        "cc_candidates_checked": 0,
        "cc_qualified_count": 0,
        "cc_top_reject_reason": None,
        "cc_top_reject_count": 0,
        "cc_shortlist_text": "No CC scan.",
    }
    if quantity <= 0:
        out["cc_shortlist_text"] = "No shares held."
        return out
    if avg_buy_price <= 0:
        out["cc_shortlist_text"] = "Missing avg buy price."
        return out

    print(
        f"[CC] {ticker}: Preview scan (session={session_state}, qty={quantity:.2f}, lots={lots}, "
        f"spot={spot:.2f}, avg_cost={avg_buy_price:.2f})"
    )
    chain = get_option_chain(ticker)
    best, meta = choose_best_call(
        chain,
        spot=spot,
        max_dte=max_dte,
        strike_below=strike_below,
        strike_above=strike_above,
        avg_cost=avg_buy_price,
        min_bid=min_bid,
    )
    print_call_scan_summary(ticker, avg_buy_price=avg_buy_price, meta=meta, best=best)
    out.update(
        {
            "cc_candidates_checked": int(meta.get("candidates_checked") or 0),
            "cc_qualified_count": int(meta.get("qualified_count") or 0),
            "cc_top_reject_reason": meta.get("top_rejection_reason"),
            "cc_top_reject_count": int(meta.get("top_rejection_count") or 0),
            "cc_shortlist_text": _shortlist_text(meta),
        }
    )
    if best is not None:
        capped_gain_per_share = float(best["breakeven"]) - float(avg_buy_price)
        capped_gain_pct = (capped_gain_per_share / float(avg_buy_price)) * 100.0 if avg_buy_price > 0 else 0.0
        capped_gain_per_contract = capped_gain_per_share * 100.0
        out.update(
            {
                "cc_best_exp": best["expiration"],
                "cc_best_strike": float(best["strike"]),
                "cc_best_bid": float(best["bid"]),
                "cc_best_ask": float(best["ask"]),
                "cc_breakeven": float(best["breakeven"]),
                "cc_capped_gain": capped_gain_per_share,
                "cc_capped_gain_pct": capped_gain_pct,
                "cc_capped_gain_contract": capped_gain_per_contract,
            }
        )
    return out


def check_stoploss_and_sell(
    *,
    current_price: float,
    avg_buy_price: float,
    ticker: str,
    held_qty: float,
    target_gain_for_stoploss: float,
    stoploss_percentage: float,
    session_state: str,
    limit_mid: Optional[float],
    trade_stats: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    if avg_buy_price <= 0:
        return None
    if str(session_state or "").strip().lower() != "regular":
        return None

    percentage_gain = ((current_price - avg_buy_price) / avg_buy_price) * 100.0

    if ticker not in stoploss_state:
        stoploss_state[ticker] = {"armed": False}

    if not stoploss_state[ticker]["armed"] and percentage_gain >= target_gain_for_stoploss:
        stoploss_state[ticker]["armed"] = True
        print(f"Stop-loss armed for {ticker} with a percentage gain of {percentage_gain:.2f}%.")

    if stoploss_state[ticker]["armed"]:
        stoploss_trigger_price = avg_buy_price * (1.0 + stoploss_percentage / 100.0)

        if current_price <= stoploss_trigger_price:
            sell_qty = int(held_qty)

            if sell_qty <= 0:
                print(f"Cannot sell {ticker}. No shares available.")
                stoploss_state[ticker]["armed"] = False
                print(f"Stop-loss DISARMED for {ticker} (quantity < 1).")
                return None

            try:
                session_tag = _order_session_for_state(session_state)
                if session_state == "extended":
                    limit_price = limit_mid if limit_mid and limit_mid > 0 else current_price
                    response = place_limit_sell(ticker, float(sell_qty), session_tag, limit_price)
                elif session_state == "regular":
                    response = place_market_sell(ticker, float(sell_qty), session_tag)
                else:
                    print(f"Market closed; skipping stop-loss sell for {ticker}.")
                    return None

                if _order_success(response):
                    print(f"Sold {sell_qty} shares of {ticker}.")
                    if trade_stats is not None:
                        _record_trade(
                            trade_stats,
                            side="sell",
                            qty=float(sell_qty),
                            price=current_price,
                            avg_buy_price=avg_buy_price,
                        )
                    stoploss_state[ticker]["armed"] = False
                    print(f"Stop-loss DISARMED for {ticker} (quantity < 1).")
                    return f"Sold {sell_qty} shares of {ticker} due to stop-loss."
                print(f"Order status {response.status_code}.")
            except Exception as e:
                print(f"Error placing sell order for {ticker}: {e}")

    return None


def _effective_timeframe_key(timeframe: str, session_state: str) -> str:
    base = "10m" if timeframe == "1h" else timeframe
    return base


def _load_hlc(
    symbol: str,
    tf_key: str,
    session_state: str,
    include_extended_hours_data: bool,
) -> Tuple[List[float], List[float], List[float]]:
    tf = TIMEFRAMES[tf_key]
    need_extended = bool(include_extended_hours_data)

    # fetch_price_history_with_min_candles now handles current day inclusion automatically
    candles = fetch_price_history_with_min_candles(
        fetch_fn=get_price_history,
        symbol=symbol,
        period_type=str(tf["periodType"]),
        period=int(tf["period"]),
        frequency_type=str(tf["frequencyType"]),
        frequency=int(tf["frequency"]),
        need_extended=need_extended,
        min_candles=MIN_REQUIRED_CANDLES,
    )
    highs: List[float] = []
    lows: List[float] = []
    closes_out: List[float] = []
    for row in candles:
        try:
            highs.append(float(row.get("high")))
            lows.append(float(row.get("low")))
            closes_out.append(float(row.get("close")))
        except Exception:
            continue
    return highs, lows, closes_out


def main_trading_loop(
    *,
    symbols: List[str],
    shares_per_trade: int,
    enable_stock_buys: bool,
    target_gain_pct: float,
    stop_loss_pct: float,
    stoploss_enabled: bool,
    timeframe: str,
    sleep_duration: float,
    include_extended_hours_data: bool,
    cc_max_dte: int,
    cc_strike_below: int,
    cc_strike_above: int,
    cc_option_tick: float,
    cc_ask_undercut: float,
    cc_spread_narrow_threshold: float,
    cc_min_bid: float,
    cc_poll_seconds: float,
    cc_time_in_force: str,
    cc_max_reprices: int,
    cc_order_timeout_seconds: int,
    trade_stats: Optional[Dict[str, Any]] = None,
    status_writer: Optional[Any] = None,
) -> None:
    if timeframe not in TIMEFRAMES and timeframe != "1h":
        raise ValueError(f"Invalid timeframe '{timeframe}'. Choose from {list(TIMEFRAMES.keys())} or '1h'.")

    print(f"Using timeframe: {timeframe}")
    print(f"Symbols: {symbols}")
    print(f"History include extended hours: {'YES' if include_extended_hours_data else 'NO'}")

    session_state = ""
    while True:
        next_state = _market_session_state()
        if next_state != session_state:
            session_state = next_state
            if session_state == "regular":
                print("Session: regular market hours (covered calls enabled).")
            elif session_state == "extended":
                print("Session: extended hours (limit orders at bid/ask midpoint).")
            else:
                print("Session: market closed (orders skipped).")

        account = get_account_snapshot()
        positions = get_open_stock_positions(account)
        positions_map = build_positions_map(positions)

        quotes = get_quotes_map(symbols)
        tickers_status: List[Dict[str, Any]] = []

        for symbol in symbols:
            try:
                quote = _get_quote_for_symbol(quotes, symbol)
                current_price = _price_from_quote(quote, prefer_extended=False)
                if current_price is None:
                    raise RuntimeError(f"Missing price for {symbol}")
                quote_root = quote.get("quote") if isinstance(quote, dict) else None
                if not isinstance(quote_root, dict):
                    quote_root = quote if isinstance(quote, dict) else {}
                bid_price = _safe_float(quote_root.get("bidPrice"), default=0.0)
                ask_price = _safe_float(quote_root.get("askPrice"), default=0.0)
                mid_price = _mid_price(bid_price, ask_price, current_price)

                tf_key = _effective_timeframe_key(timeframe, session_state)
                highs, lows, closes = _load_hlc(symbol, tf_key, session_state, include_extended_hours_data)

                if (len(closes) + 1) < MIN_REQUIRED_CANDLES:
                    print(
                        f"[{symbol}] Requested {MIN_REQUIRED_CANDLES}+ candles for timeframe "
                        f"{tf_key}; received {len(closes)}."
                    )
                    tickers_status.append({"symbol": symbol, "signal": "NO_DATA"})
                    continue

                closes.append(current_price)
                atr = calculate_atr_wilder(highs, lows, closes, period=ATR_PERIOD)

                ma30 = calculate_moving_average(closes, 30)
                ma78 = calculate_moving_average(closes, 78)
                ma190 = calculate_moving_average(closes, 190)

                rsi, rsi_deriv = calculate_rsi_and_derivative(closes, 14)
                ma30_deriv = calculate_ma_derivative(closes, 30)
                pos_info = positions_map.get(symbol)
                if pos_info:
                    pos_qty = _safe_float(pos_info.get("quantity"))
                    avg_buy_price = _safe_float(pos_info.get("average_buy_price"))
                else:
                    pos_qty = 0.0
                    avg_buy_price = 0.0

                action = print_indicator_signals(
                    symbol,
                    current_price,
                    ma30,
                    ma78,
                    ma190,
                    rsi,
                    rsi_deriv,
                    ma30_deriv,
                    pos_qty,
                    avg_buy_price,
                )

                buy_signal = action == "Buy"
                sell_signal = action == "Sell"
                signal = "SELL" if sell_signal else ("BUY" if buy_signal else "HOLD")
                pnl_pct: Optional[float] = None
                if avg_buy_price > 0:
                    pnl_pct = ((current_price - avg_buy_price) / avg_buy_price) * 100.0

                status_entry = {
                    "symbol": symbol,
                    "signal": signal,
                    "price": current_price,
                    "qty": pos_qty,
                    "avg_buy": avg_buy_price,
                    "pnl_pct": pnl_pct,
                    "ma20": ma30,
                    "ma78": ma78,
                    "ma150": ma190,
                    "rsi": rsi,
                    "rsi_d": rsi_deriv,
                    "ma30_d": ma30_deriv,
                    "atr": atr,
                    "chart": _build_chart_series(closes) if status_writer is not None else {},
                    "cc_best_exp": None,
                    "cc_best_strike": None,
                    "cc_best_bid": None,
                    "cc_best_ask": None,
                    "cc_breakeven": None,
                    "cc_capped_gain": None,
                    "cc_capped_gain_pct": None,
                    "cc_capped_gain_contract": None,
                    "cc_candidates_checked": None,
                    "cc_qualified_count": None,
                    "cc_top_reject_reason": None,
                    "cc_top_reject_count": None,
                    "cc_shortlist_text": None,
                }
                if pos_qty > 0:
                    preview = preview_covered_call_scan(
                        ticker=symbol,
                        spot=current_price,
                        quantity=pos_qty,
                        avg_buy_price=avg_buy_price,
                        max_dte=cc_max_dte,
                        strike_below=cc_strike_below,
                        strike_above=cc_strike_above,
                        min_bid=cc_min_bid,
                        session_state=session_state,
                    )
                    status_entry.update(preview)
                tickers_status.append(status_entry)

                if stoploss_enabled and avg_buy_price > 0:
                    check_stoploss_and_sell(
                        current_price=current_price,
                        avg_buy_price=avg_buy_price,
                        ticker=symbol,
                        held_qty=pos_qty,
                        target_gain_for_stoploss=target_gain_pct,
                        stoploss_percentage=stop_loss_pct,
                        session_state=session_state,
                        limit_mid=mid_price,
                        trade_stats=trade_stats,
                    )

                if buy_signal:
                    if not enable_stock_buys:
                        print(f"[{symbol}] BUY signal but stock buys disabled.")
                    else:
                        print(f"[{symbol}] BUY signal -> buying {shares_per_trade} shares...")
                        try:
                            session_tag = _order_session_for_state(session_state)
                            if session_state == "extended":
                                resp = place_limit_buy(symbol, float(shares_per_trade), session_tag, mid_price)
                            elif session_state == "regular":
                                resp = place_market_buy(symbol, float(shares_per_trade), session_tag)
                            else:
                                print(f"[{symbol}] Market closed; skipping buy order.")
                                resp = None
                            if resp is not None and trade_stats is not None and _order_success(resp):
                                _record_trade(
                                    trade_stats,
                                    side="buy",
                                    qty=float(shares_per_trade),
                                    price=current_price,
                                    avg_buy_price=0.0,
                                )
                        except Exception as e:
                            print(f"[{symbol}] Buy failed: {e}")

                if pos_qty > 0 and sell_signal:
                    result, cc_summary = execute_covered_call_harvest(
                        ticker=symbol,
                        spot=current_price,
                        quantity=pos_qty,
                        avg_buy_price=avg_buy_price,
                        max_dte=cc_max_dte,
                        strike_below=cc_strike_below,
                        strike_above=cc_strike_above,
                        option_tick=cc_option_tick,
                        ask_undercut=cc_ask_undercut,
                        spread_narrow_threshold=cc_spread_narrow_threshold,
                        min_bid=cc_min_bid,
                        poll_seconds=cc_poll_seconds,
                        tif=cc_time_in_force,
                        max_reprices=cc_max_reprices,
                        order_timeout_seconds=cc_order_timeout_seconds,
                        session_state=session_state,
                        trade_stats=trade_stats,
                    )
                    if cc_summary:
                        status_entry.update(cc_summary)
                    if result:
                        print(result)

            except Exception as e:
                print(f"[{symbol}] ERROR: {e}")
                tickers_status.append({"symbol": symbol, "signal": "ERROR"})

        if status_writer is not None:
            try:
                status_writer(
                    {
                        "phase": "loop",
                        "timeframe": timeframe,
                        "tickers": tickers_status,
                    }
                )
            except Exception:
                pass

        print(
            r"""  
                                                                   ,-,
                                                             _.-=;~ /_
                                                          _-~   '     ;.
                                                      _.-~     '   .-~-~`-._
                                                _.--~~:.             --.____88
                              ____.........--~~~. .' .  .        _..-------~~
                     _..--~~~~               .' .'             ,'
                 _.-~                        .       .     ` ,'
               .'                                    :.    ./
             .:     ,/          `                   ::.   ,'
           .:'     ,(            ;.                ::. ,-'
          .'     ./'.`.     . . /:::._______.... _/:.o/
         /     ./'. . .)  . _.,'               `88;?88|
       ,'  . .,/'._,-~ /_.o8P'                  88P ?8b
    _,'' . .,/',-~    d888P'                    88'  88|
 _.'~  . .,:oP'        ?88b              _..--- 88.--'8b.--..__
:     ...' 88o __,------.88o ...__..._.=~- .    `~~   `~~      ~-._ Rokurokubi.Options _.
`.;;;:='    ~~            ~~~                ~-    -       -   -
"""
        )
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
        payload["script"] = "Rokurokubi.Options.Schwab"
        payload["pnl"] = round(float(trade_stats.get("pnl", 0.0)), 2)
        payload["trades"] = int(trade_stats.get("trades", 0))
        try:
            status_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            last_status.clear()
            last_status.update(payload)
        except Exception:
            pass

    params = load_params(args.params_json)

    sleep_duration = float(params.get("sleep_duration", 60))
    symbols = params.get("symbols", [])
    if isinstance(symbols, str):
        symbols = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not isinstance(symbols, list) or not symbols:
        raise ValueError("params.symbols must be a non-empty list of tickers.")
    symbols = [str(s).strip().upper() for s in symbols if str(s).strip()]

    shares_per_trade = int(params.get("shares_per_trade", 1))
    enable_stock_buys = bool(params.get("enable_stock_buys", True))
    target_gain_pct = float(params.get("target_gain_pct", 0.5))
    stop_loss_pct = float(params.get("stop_loss_pct", -0.5))
    stoploss_enabled = bool(params.get("stoploss_enabled", False))
    include_extended_hours_data = _to_bool(
        params.get("include_extended_hours_data", params.get("history_include_extended", True)),
        True,
    )
    timeframe = str(params.get("timeframe", "10m")).strip()

    cc_max_dte = int(params.get("cc_max_dte", 30))
    cc_strike_below = int(params.get("cc_strike_below", 0))
    cc_strike_above = int(params.get("cc_strike_above", 2))
    cc_option_tick = float(params.get("cc_option_tick", 0.05))
    cc_ask_undercut = float(params.get("cc_ask_undercut", 0.05))
    cc_spread_narrow_threshold = float(params.get("cc_spread_narrow_threshold", 0.05))
    cc_min_bid = float(params.get("cc_min_bid", 0.05))
    cc_poll_seconds = float(params.get("cc_poll_seconds", 5.0))
    cc_time_in_force = str(params.get("cc_time_in_force", "gtc")).strip().lower() or "gtc"
    cc_max_reprices = int(params.get("cc_max_reprices", 10))
    cc_order_timeout_seconds = int(params.get("cc_order_timeout_seconds", 900))

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
        symbols=symbols,
        shares_per_trade=shares_per_trade,
        enable_stock_buys=enable_stock_buys,
        target_gain_pct=target_gain_pct,
        stop_loss_pct=stop_loss_pct,
        stoploss_enabled=stoploss_enabled,
        timeframe=timeframe,
        sleep_duration=sleep_duration,
        include_extended_hours_data=include_extended_hours_data,
        cc_max_dte=cc_max_dte,
        cc_strike_below=cc_strike_below,
        cc_strike_above=cc_strike_above,
        cc_option_tick=cc_option_tick,
        cc_ask_undercut=cc_ask_undercut,
        cc_spread_narrow_threshold=cc_spread_narrow_threshold,
        cc_min_bid=cc_min_bid,
        cc_poll_seconds=cc_poll_seconds,
        cc_time_in_force=cc_time_in_force,
        cc_max_reprices=cc_max_reprices,
        cc_order_timeout_seconds=cc_order_timeout_seconds,
        trade_stats=trade_stats,
        status_writer=write_status,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
