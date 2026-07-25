#!/usr/bin/env python3
"""
DreadFox.Stock.Schwab.py (Web-App Compatible)

Schwab-backed clone of DreadFox.Stock.Robinhood with the same indicator + trading logic:
- MA20/78/190 + RSI derivative signals
- Stop-loss arming + trigger logic (sell whole shares, disarm)
- Extended-hours limit orders at bid/ask midpoint
- Regular-hours trailing stop sells

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
    "1h": {"periodType": "day", "period": 20, "frequencyType": "minute", "frequency": 30},
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


def should_buy(
    current_price: float,
    ma20: Optional[float],
    ma78: Optional[float],
    ma150: Optional[float],
    rsi: Optional[float],
    rsi_deriv: Optional[float],
) -> bool:
    if rsi is None or rsi_deriv is None:
        return False
    if None in (ma20, ma78, ma150):
        return False
    return (
        current_price > ma20
        and current_price < ma78
        and current_price < ma150
        and rsi < 55
        and rsi > 30
        and rsi_deriv > 1
    )


def should_sell(
    current_price: float,
    ma20: Optional[float],
    ma78: Optional[float],
    ma150: Optional[float],
    rsi: Optional[float],
    rsi_deriv: Optional[float],
    qty: float,
    avg_buy_price: float,
) -> bool:
    if rsi is None or rsi_deriv is None:
        return False
    if None in (ma20, ma78, ma150):
        return False
    return (
        qty > 0
        and current_price > ma20
        and current_price > ma78
        and current_price > ma150
        and rsi_deriv < 1
        and rsi > 69
        and current_price > avg_buy_price
    )


def print_indicator_signals(
    symbol: str,
    current_price: float,
    ma20: Optional[float],
    ma78: Optional[float],
    ma150: Optional[float],
    rsi: Optional[float],
    rsi_deriv: Optional[float],
    qty: float,
    avg_buy_price: float,
) -> str:
    action = "Hold"
    percentage_gain_loss: Optional[float] = None

    if avg_buy_price > 0:
        percentage_gain_loss = ((current_price - avg_buy_price) / avg_buy_price) * 100

    if current_price > 0 and None not in (ma20, ma78, ma150, rsi, rsi_deriv):
        if should_buy(current_price, ma20, ma78, ma150, rsi, rsi_deriv):
            action = "Buy"
        elif should_sell(current_price, ma20, ma78, ma150, rsi, rsi_deriv, qty, avg_buy_price):
            action = "Sell"

    print(
        f"Ticker: {symbol}\n"
        f"Current Price: {current_price}\n"
        f"20-Period Moving Average: {ma20}\n"
        f"78-Period Moving Average: {ma78}\n"
        f"190-Period Moving Average: {ma150}\n"
        f"RSI: {rsi}\n"
        f"RSI Derivative (3-candle): {rsi_deriv}\n"
        f"Quantity Held: {qty}\n"
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
    trade_stats: Optional[Dict[str, Any]] = None,
) -> None:
    if avg_buy_price <= 0:
        return
    if str(session_state or "").strip().lower() != "regular":
        return

    percentage_gain = ((current_price - avg_buy_price) / avg_buy_price) * 100.0

    if symbol not in stoploss_state:
        stoploss_state[symbol] = {"armed": False}

    if not stoploss_state[symbol]["armed"] and percentage_gain >= target_gain_pct:
        stoploss_state[symbol]["armed"] = True
        print(f"Stop-loss armed for {symbol} with a percentage gain of {percentage_gain:.2f}%.")

    if stoploss_state[symbol]["armed"]:
        stop_trigger_price = avg_buy_price * (1.0 + (stop_loss_pct / 100.0))
        if current_price <= stop_trigger_price:
            sell_qty = int(held_qty)
            if sell_qty <= 0:
                print(f"Cannot sell {symbol}. No shares available.")
                stoploss_state[symbol]["armed"] = False
                print(f"Stop-loss DISARMED for {symbol} (quantity < 1).")
                return

            try:
                session_tag = _order_session_for_state(session_state)
                if session_state == "extended":
                    limit_price = limit_mid if limit_mid and limit_mid > 0 else current_price
                    resp = place_limit_sell(symbol, float(sell_qty), session_tag, limit_price)
                elif session_state == "regular":
                    resp = place_market_sell(symbol, float(sell_qty), session_tag)
                else:
                    print(f"Market closed; skipping stop-loss sell for {symbol}.")
                    return
                if _order_success(resp):
                    print(f"Sold {sell_qty} shares of {symbol}.")
                    if trade_stats is not None:
                        _record_trade(
                            trade_stats,
                            side="sell",
                            qty=float(sell_qty),
                            price=current_price,
                            avg_buy_price=avg_buy_price,
                        )
                    stoploss_state[symbol]["armed"] = False
                    print(f"Stop-loss DISARMED for {symbol} (quantity < 1).")
                else:
                    print(f"Order status {resp.status_code}. Sell might not have executed.")
            except Exception as e:
                print(f"Error placing sell order for {symbol}: {e}")


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


def _effective_timeframe_key(timeframe: str, session_state: str) -> str:
    return timeframe


def _load_closes(symbol: str, tf_key: str, session_state: str, include_extended_hours_data: bool) -> List[float]:
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

    out: List[float] = []
    for row in candles:
        try:
            out.append(float(row.get("close")))
        except Exception:
            continue
    return out


def main_trading_loop(
    *,
    run_dir: Path,
    db_path: str,
    connection_id: int,
    symbols: List[str],
    shares_per_trade: int,
    trailing_stop_amount: float,
    target_gain_pct: float,
    stop_loss_pct: float,
    timeframe: str,
    sleep_duration: float,
    include_extended_hours_data: bool,
    allow_extended_hours_orders: bool,
    trade_stats: Optional[Dict[str, Any]] = None,
    status_writer: Optional[Any] = None,
) -> None:
    if timeframe not in TIMEFRAMES:
        raise ValueError(f"Invalid timeframe '{timeframe}'. Choose from {list(TIMEFRAMES.keys())}.")

    session_state = ""
    print(f"History include extended hours: {'YES' if include_extended_hours_data else 'NO'}")
    print(f"Allow extended hours orders: {'YES' if allow_extended_hours_orders else 'NO'}")
    while True:
        next_state = _market_session_state()
        if next_state != session_state:
            session_state = next_state
            if session_state == "regular":
                print("Session: regular market hours (trailing stop sells enabled).")
            elif session_state == "extended":
                if allow_extended_hours_orders:
                    print("Session: extended hours (limit orders at bid/ask midpoint).")
                else:
                    print("Session: extended hours detected but allow_extended_hours_orders=False (orders skipped).")
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
                closes = _load_closes(symbol, tf_key, session_state, include_extended_hours_data)

                if (len(closes) + 1) < MIN_REQUIRED_CANDLES:
                    print(
                        f"[{symbol}] Requested {MIN_REQUIRED_CANDLES}+ candles for timeframe "
                        f"{tf_key}; received {len(closes)}."
                    )
                    tickers_status.append({"symbol": symbol, "signal": "NO_DATA"})
                    continue

                closes.append(current_price)

                ma20 = calculate_moving_average(closes, 20)
                ma78 = calculate_moving_average(closes, 78)
                ma150 = calculate_moving_average(closes, 190)
                rsi, rsi_deriv = calculate_rsi_and_derivative(closes, 14)

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
                    ma20,
                    ma78,
                    ma150,
                    rsi,
                    rsi_deriv,
                    pos_qty,
                    avg_buy_price,
                )

                buy_signal = action == "Buy"
                sell_signal = action == "Sell"
                signal = "SELL" if sell_signal else ("BUY" if buy_signal else "HOLD")
                pnl_pct: Optional[float] = None
                if avg_buy_price > 0:
                    pnl_pct = ((current_price - avg_buy_price) / avg_buy_price) * 100.0

                tickers_status.append(
                    {
                        "symbol": symbol,
                        "signal": signal,
                        "price": current_price,
                        "qty": pos_qty,
                        "avg_buy": avg_buy_price,
                        "pnl_pct": pnl_pct,
                        "ma20": ma20,
                        "ma78": ma78,
                        "ma150": ma150,
                        "rsi": rsi,
                        "rsi_d": rsi_deriv,
                        "chart": _build_chart_series(closes) if status_writer is not None else {},
                    }
                )

                check_stoploss_and_sell(
                    symbol=symbol,
                    current_price=current_price,
                    avg_buy_price=avg_buy_price,
                    held_qty=pos_qty,
                    target_gain_pct=target_gain_pct,
                    stop_loss_pct=stop_loss_pct,
                    session_state=session_state,
                    limit_mid=mid_price,
                    trade_stats=trade_stats,
                )

                if buy_signal:
                    try:
                        session_tag = _order_session_for_state(session_state)
                        resp = None

                        # Determine if we can place orders based on session state
                        can_place_order = False
                        if session_state == "regular":
                            can_place_order = True
                        elif session_state == "extended" and allow_extended_hours_orders:
                            can_place_order = True

                        if can_place_order:
                            if session_state == "extended":
                                print(f"[{symbol}] BUY signal (extended hours) -> placing midpoint limit buy for {shares_per_trade} shares at ${float(mid_price):.2f}...")
                                resp = place_limit_buy(symbol, float(shares_per_trade), session_tag, mid_price)
                            else:
                                print(f"[{symbol}] BUY signal -> buying {shares_per_trade} shares...")
                                resp = place_market_buy(symbol, float(shares_per_trade), session_tag)
                        else:
                            if session_state == "extended":
                                print(f"[{symbol}] Extended hours detected but allow_extended_hours_orders=False; skipping buy order.")
                            else:
                                print(f"[{symbol}] Market closed; skipping buy order.")
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
                    try:
                        session_tag = _order_session_for_state(session_state)
                        resp = None

                        # Determine if we can place orders based on session state
                        can_place_order = False
                        if session_state == "regular":
                            can_place_order = True
                        elif session_state == "extended" and allow_extended_hours_orders:
                            can_place_order = True

                        if can_place_order:
                            if session_state == "extended":
                                print(f"[{symbol}] SELL signal (extended hours) -> placing midpoint limit sell for {shares_per_trade} shares at ${float(mid_price):.2f}...")
                                resp = place_limit_sell(symbol, float(shares_per_trade), session_tag, mid_price)
                            else:
                                print(f"[{symbol}] SELL signal -> placing trailing stop sell for {shares_per_trade} shares, trail=${trailing_stop_amount}...")
                                resp = place_trailing_stop_sell(
                                    symbol,
                                    float(shares_per_trade),
                                    session_tag,
                                    float(trailing_stop_amount),
                                    current_price,
                                )
                        else:
                            if session_state == "extended":
                                print(f"[{symbol}] Extended hours detected but allow_extended_hours_orders=False; skipping sell order.")
                            else:
                                print(f"[{symbol}] SELL signal but market closed; skipping order.")
                        if resp is not None and trade_stats is not None and _order_success(resp):
                            _record_trade(
                                trade_stats,
                                side="sell",
                                qty=float(shares_per_trade),
                                price=current_price,
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
        :     ...' 88o __,------.88o ...__..._.=~- .    `~~   `~~      ~-._ DreadFox.Stock _.
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
        payload["script"] = "DreadFox.Stock.Schwab"
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
    target_gain_pct = float(params.get("target_gain_pct", 0.5))
    stop_loss_pct = float(params.get("stop_loss_pct", -0.5))
    timeframe = str(params.get("timeframe", "10m")).strip()
    include_extended_hours_data = _to_bool(
        params.get("include_extended_hours_data", params.get("history_include_extended", True)),
        True,
    )
    allow_extended_hours_orders = _to_bool(params.get("allow_extended_hours_orders", False), False)
    if timeframe not in TIMEFRAMES:
        print(f"[WARN] Invalid timeframe '{timeframe}'. Defaulting to '10m'.")
        timeframe = "10m"

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
        target_gain_pct=target_gain_pct,
        stop_loss_pct=stop_loss_pct,
        timeframe=timeframe,
        sleep_duration=sleep_duration,
        include_extended_hours_data=include_extended_hours_data,
        allow_extended_hours_orders=allow_extended_hours_orders,
        trade_stats=trade_stats,
        status_writer=write_status,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
