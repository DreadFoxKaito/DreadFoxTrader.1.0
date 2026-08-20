#!/usr/bin/env python3
"""
IndicatorForge.Crypto.Robinhood.py

Robinhood crypto IndicatorForge variant that keeps the same indicator-rule semantics
as IndicatorForge.Robinhood (MA/EMA + derivative + unless, RSI, dRSI, MACD), while
running crypto-native execution:
- dollar-sized crypto orders
- optional stop-loss arming/trigger logic
- local trailing-stop tracking for BOTH buy and sell directions

Robinhood does not provide trailing orders for crypto, so this script manages trailing
state locally and submits normal crypto orders when local trails trigger.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

try:
    import robin_stocks.robinhood as rh  # type: ignore
except Exception as e:
    raise RuntimeError("robin_stocks is required. Install with: pip install robin_stocks") from e


THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.brokers.robinhood_connector import (  # noqa: E402
    _pickle_debug_info,
    _resolve_pickle_config,
    _restore_session_from_pickle,
)
from app.brokers.robin_stocks_adapter import (  # noqa: E402
    get_crypto_historicals as adapter_get_crypto_historicals,
    place_crypto_order,
)
from app.db import get_broker_connection, read_connection_metadata, read_connection_secrets, set_broker_status  # noqa: E402
from app.indicator_pipeline import heikin_ashi_series as shared_heikin_ashi_series  # noqa: E402

try:
    from strategy_forge.pivot_points import calculate_pivot_points, pivot_target_above_price  # noqa: E402
except Exception:
    calculate_pivot_points = None  # type: ignore
    pivot_target_above_price = None  # type: ignore


stoploss_state: Dict[str, Dict[str, Any]] = {}
local_trailing_orders: Dict[str, Dict[str, Dict[str, Any]]] = {}
CHART_POINTS = 90
LOCAL_TRAIL_STATE_VERSION = 1
PRINT_ORDER_EVENTS = False
COLORIZE_INDICATOR_LOGS = True
MIN_CRYPTO_ORDER_AMOUNT_DOLLARS = 0.25

ANSI_RESET = "\033[0m"
ANSI_BUY = "\033[92m"
ANSI_SELL = "\033[91m"
ANSI_HOLD = "\033[93m"
ANSI_IGNORED = "\033[90m"

TIMEFRAMES: Dict[str, Dict[str, str]] = {
    "5m": {"interval": "5minute", "span": "day"},
    "10m": {"interval": "10minute", "span": "week"},
    "1h": {"interval": "hour", "span": "3month"},
    "1d": {"interval": "day", "span": "year"},
}
HISTORICAL_BOUNDS = "24_7"


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


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
        symbol = str(key or "").strip().upper()
        if not symbol:
            continue
        try:
            pct = float(val)
        except Exception:
            continue
        if pct > 0:
            out[symbol] = pct
    return out


def _normalize_order_type(val: Any, default: str = "local_trailing") -> str:
    txt = str(val or "").strip().lower()
    if txt in ("market", "mkt"):
        return "market"
    if txt in ("local_trailing", "trailing", "local_trail", "trail"):
        return "local_trailing"
    return "local_trailing" if str(default).strip().lower() != "market" else "market"


def _fmt(val: Any, digits: int = 4) -> str:
    try:
        if val is None:
            return "—"
        return f"{float(val):.{digits}f}"
    except Exception:
        return "—"


def _log(level: str, message: str) -> None:
    print(message)


def _log_info(message: str) -> None:
    _log("INFO", message)


def _log_warn(message: str) -> None:
    if not PRINT_ORDER_EVENTS:
        return
    _log("WARN", message)


def _log_action(message: str) -> None:
    if not PRINT_ORDER_EVENTS:
        return
    _log("ACTION", message)


def _log_error(message: str) -> None:
    _log("ERROR", message)


def _indicator_color(state: str) -> str:
    s = str(state or "").strip().upper()
    if s == "BUY":
        return ANSI_BUY
    if s == "SELL":
        return ANSI_SELL
    if s == "IGNORED":
        return ANSI_IGNORED
    if s == "HOLD":
        return ANSI_HOLD
    return ""


def _paint_indicator(text: str, state: str) -> str:
    if not COLORIZE_INDICATOR_LOGS:
        return text
    color = _indicator_color(state)
    if not color:
        return text
    return f"{color}{text}{ANSI_RESET}"


def ensure_robinhood_session(db_path: str, connection_id: int) -> None:
    row = get_broker_connection(db_path, connection_id)
    if not row:
        raise RuntimeError(f"Robinhood connection_id {connection_id} not found in broker_connections.")

    broker = str(row["broker"])
    status = str(row["status"] or "")
    if broker != "robinhood":
        raise RuntimeError(f"connection_id {connection_id} is broker='{broker}', expected 'robinhood'.")
    if status != "connected":
        raise RuntimeError(
            f"Robinhood connection_id {connection_id} status='{status}'. Re-link Robinhood in Broker page."
        )

    meta = read_connection_metadata(row)
    secrets = read_connection_secrets(row, default={})
    _, _, pickle_file = _resolve_pickle_config(db_path=db_path, connection_id=int(connection_id), secrets=secrets)
    if not pickle_file.exists():
        debug = _pickle_debug_info(pickle_file)
        set_broker_status(
            db_path=db_path,
            connection_id=connection_id,
            status="needs_auth",
            metadata={**meta, "error": "Missing Robinhood session pickle.", "debug": debug},
        )
        raise RuntimeError("Robinhood session pickle missing; relink required.")

    restored = _restore_session_from_pickle(pickle_file, expires_in=86400, scope="internal", validate=True)
    if not restored:
        debug = _pickle_debug_info(pickle_file)
        set_broker_status(
            db_path=db_path,
            connection_id=connection_id,
            status="needs_auth",
            metadata={**meta, "error": "Failed to restore Robinhood session from pickle.", "debug": debug},
        )
        raise RuntimeError("Robinhood session restore failed; relink required.")


def _status_code(exc: BaseException) -> Optional[int]:
    return getattr(getattr(exc, "response", None), "status_code", None)


def safe_sleep(seconds: float) -> None:
    time.sleep(max(0.0, float(seconds)))


def safe_crypto_quote(symbol: str, retries: int = 3, backoff: float = 0.8) -> Any:
    for attempt in range(retries):
        try:
            q = rh.crypto.get_crypto_quote(symbol)
            if q:
                return q
        except requests.HTTPError as e:
            if _status_code(e) == 429 and attempt < retries - 1:
                safe_sleep(backoff * (2**attempt))
                continue
            raise
        except Exception:
            if attempt == retries - 1:
                raise
            safe_sleep(backoff * (2**attempt))
    return None


def safe_crypto_historicals(
    symbol: str,
    interval: str,
    span: str,
    *,
    bounds: str = HISTORICAL_BOUNDS,
    retries: int = 3,
    backoff: float = 0.8,
) -> Any:
    for attempt in range(retries):
        try:
            h = adapter_get_crypto_historicals(symbol, interval=interval, span=span, bounds=bounds)
            if h:
                return h
        except requests.HTTPError as e:
            if _status_code(e) == 429 and attempt < retries - 1:
                safe_sleep(backoff * (2**attempt))
                continue
            raise
        except Exception:
            if attempt == retries - 1:
                raise
            safe_sleep(backoff * (2**attempt))
    return None


def _price_from_quote(quote: dict) -> Optional[float]:
    if not isinstance(quote, dict):
        return None
    for key in ("mark_price", "ask_price", "bid_price", "open_price"):
        val = _to_float_opt(quote.get(key))
        if val is not None and val > 0:
            return float(val)
    return None


def _order_success(resp: Any) -> bool:
    if hasattr(resp, "accepted") and hasattr(resp, "submitted"):
        return bool(resp.accepted and resp.submitted and not getattr(resp, "blocked", False))
    if isinstance(resp, list):
        if not resp:
            return False
        return _order_success(resp[0])
    if isinstance(resp, dict):
        http_status = _to_int_opt(resp.get("_http_status"))
        if http_status is not None and http_status >= 400 and not resp.get("id"):
            return False
        state = str(resp.get("state") or "").lower()
        if any(k in resp for k in ("detail", "error", "errors", "non_field_errors")):
            # Keep state/id checks below; some responses include both metadata and order payload.
            pass
        if state in ("rejected", "cancelled", "canceled", "failed", "error", "voided", "expired"):
            return False
        if resp.get("id"):
            return True
        if state in ("queued", "confirmed", "filled", "partially_filled", "unconfirmed", "submitted", "new"):
            return True
        if state:
            return False
        if any(k in resp for k in ("detail", "error", "errors", "non_field_errors")):
            return False
        return bool(resp)
    return bool(resp)


def _order_failure_reason(resp: Any) -> str:
    if hasattr(resp, "reason"):
        return str(getattr(resp, "reason") or "adapter rejected order")
    if isinstance(resp, list):
        if not resp:
            return "empty response list"
        return _order_failure_reason(resp[0])
    if isinstance(resp, dict):
        for key in ("detail", "error"):
            val = resp.get(key)
            if val:
                return str(val)
        for key in ("price", "quantity"):
            val = resp.get(key)
            if isinstance(val, list) and val:
                return "; ".join(str(x) for x in val)
            if val:
                return str(val)
        for key in ("non_field_errors", "errors"):
            val = resp.get(key)
            if isinstance(val, list) and val:
                return "; ".join(str(x) for x in val)
            if val:
                return str(val)
        state = str(resp.get("state") or "").strip()
        if state:
            return f"state={state}"
        http_status = _to_int_opt(resp.get("_http_status"))
        if http_status is not None:
            return f"http_status={http_status}"
        if resp.get("id"):
            return "order id present but status unknown"
        return "unknown dict response"
    if resp is None:
        return "no response"
    return str(resp)


def _is_reprice_rejection(resp: Any) -> bool:
    if hasattr(resp, "raw_response"):
        return _is_reprice_rejection(getattr(resp, "raw_response"))
    if isinstance(resp, list):
        return bool(resp) and _is_reprice_rejection(resp[0])
    if not isinstance(resp, dict):
        return False

    messages: List[str] = []
    for key in ("price", "non_field_errors", "errors", "detail", "error"):
        val = resp.get(key)
        if isinstance(val, list):
            for item in val:
                if item:
                    messages.append(str(item).lower())
        elif val:
            messages.append(str(val).lower())

    for msg in messages:
        if "more than 1 percent since submission" in msg:
            return True
    return False


def _normalize_order_response(resp: Any) -> Any:
    if hasattr(resp, "accepted") and hasattr(resp, "raw_response"):
        return resp
    if isinstance(resp, list):
        return [_normalize_order_response(item) for item in resp]
    status_code = _to_int_opt(getattr(resp, "status_code", None))
    if status_code is None:
        return resp

    body: Any = None
    parse_json = getattr(resp, "json", None)
    if callable(parse_json):
        try:
            body = parse_json()
        except Exception:
            body = None
    if body is None:
        text_val = str(getattr(resp, "text", "") or "").strip()
        body = {"detail": text_val or f"HTTP {status_code}"}

    if isinstance(body, dict):
        out = dict(body)
        out["_http_status"] = int(status_code)
        return out
    if isinstance(body, list):
        return {"_http_status": int(status_code), "results": body}
    return {"_http_status": int(status_code), "detail": str(body)}


def reconnect_if_needed(func: Callable[..., Any]) -> Callable[..., Any]:
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        db_path = kwargs.pop("_db_path", None)
        connection_id = kwargs.pop("_connection_id", None)
        try:
            return func(*args, **kwargs)
        except (requests.RequestException, ConnectionError):
            if db_path is not None and connection_id is not None:
                ensure_robinhood_session(str(db_path), int(connection_id))
            safe_sleep(2.0)
            return func(*args, **kwargs)
        except Exception:
            raise

    return wrapper


@reconnect_if_needed
def get_crypto_price(symbol: str) -> float:
    quote = safe_crypto_quote(symbol)
    if not isinstance(quote, dict):
        raise RuntimeError(f"Failed to get quote for {symbol}")
    price = _price_from_quote(quote)
    if price is None:
        raise RuntimeError(f"Failed to get price for {symbol}")
    return float(price)


@reconnect_if_needed
def get_crypto_historicals(symbol: str, interval: str, span: str) -> List[Dict[str, Any]]:
    data = safe_crypto_historicals(symbol, interval=interval, span=span, bounds=HISTORICAL_BOUNDS)
    if not isinstance(data, list):
        raise RuntimeError(f"Failed to get historicals for {symbol}")
    return data


def _extract_ohlcv(
    rows: List[Dict[str, Any]],
) -> Tuple[List[float], List[float], List[float], List[float], List[float], List[Any]]:
    opens: List[float] = []
    highs: List[float] = []
    lows: List[float] = []
    closes: List[float] = []
    volumes: List[float] = []
    timestamps: List[Any] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        c = row.get("close_price")
        if c is None:
            c = row.get("close")
        try:
            close_val = float(c)
        except Exception:
            continue
        if close_val <= 0:
            continue
        prev_close = closes[-1] if closes else close_val
        o = row.get("open_price")
        h = row.get("high_price")
        l = row.get("low_price")
        if o is None:
            o = row.get("open")
        if h is None:
            h = row.get("high")
        if l is None:
            l = row.get("low")
        try:
            open_val = float(o)
            high_val = float(h)
            low_val = float(l)
        except Exception:
            open_val = float(prev_close)
            high_val = max(float(prev_close), float(close_val))
            low_val = min(float(prev_close), float(close_val))
        if open_val <= 0 or high_val <= 0 or low_val <= 0:
            continue
        high_val = max(float(high_val), float(open_val), float(close_val))
        low_val = min(float(low_val), float(open_val), float(close_val))
        volume_val = 0.0
        for key in ("volume", "volume_traded", "volume_traded_units", "session_volume", "total_volume"):
            parsed_volume = _to_float_opt(row.get(key))
            if parsed_volume is not None:
                parsed_volume_f = float(parsed_volume)
                volume_val = max(0.0, parsed_volume_f) if math.isfinite(parsed_volume_f) else 0.0
                break
        opens.append(float(open_val))
        highs.append(float(high_val))
        lows.append(float(low_val))
        closes.append(float(close_val))
        volumes.append(volume_val)
        timestamps.append(
            row.get("begins_at")
            or row.get("beginsAt")
            or row.get("datetime")
            or row.get("time")
            or row.get("timestamp")
            or ""
        )
    return opens, highs, lows, closes, volumes, timestamps


@reconnect_if_needed
def get_open_crypto_positions() -> List[Dict[str, Any]]:
    positions = rh.crypto.get_crypto_positions()
    return positions if isinstance(positions, list) else []


@reconnect_if_needed
def get_portfolio_value() -> float:
    portfolio_data = rh.profiles.load_portfolio_profile()
    if isinstance(portfolio_data, dict):
        for key in ("equity", "portfolio_equity", "market_value"):
            val = portfolio_data.get(key)
            if val is not None:
                return float(val)
    return 0.0


@reconnect_if_needed
def get_buying_power() -> float:
    acct = rh.profiles.load_account_profile()
    if isinstance(acct, dict):
        for key in ("crypto_buying_power", "buying_power", "cash_available_for_withdrawal", "cash"):
            val = acct.get(key)
            if val is not None:
                return float(val)
    return 0.0


@reconnect_if_needed
def get_available_cash() -> float:
    acct = rh.profiles.load_account_profile()
    if isinstance(acct, dict):
        for key in ("cash", "cash_available_for_withdrawal", "crypto_buying_power", "buying_power"):
            val = acct.get(key)
            if val is not None:
                return float(val)
    return 0.0


def _position_avg_buy_price(pos: Dict[str, Any], quantity: float) -> float:
    if quantity <= 0:
        return 0.0
    cost_bases = pos.get("cost_bases")
    if not isinstance(cost_bases, list) or not cost_bases:
        return 0.0
    first = cost_bases[0] if isinstance(cost_bases[0], dict) else {}
    direct_cost = _safe_float(first.get("direct_cost_basis"), 0.0)
    if direct_cost <= 0:
        return 0.0
    return float(direct_cost / quantity)


def build_positions_map(positions: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}
    for pos in positions:
        if not isinstance(pos, dict):
            continue
        cur = pos.get("currency") if isinstance(pos.get("currency"), dict) else {}
        code = cur.get("code")
        if not code:
            continue
        sym = str(code).strip().upper()
        qty = _safe_float(pos.get("quantity_available") or pos.get("quantity"), 0.0)
        avg_buy = _position_avg_buy_price(pos, qty)
        out[sym] = {
            "quantity": qty,
            "average_buy_price": avg_buy,
        }
    return out


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
    return shared_heikin_ashi_series(opens, highs, lows, closes)


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
        return float(close_now) >= float(upper)
    if c == "touch_lower":
        return float(close_now) <= float(lower)
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
        window = close_vals[start : idx + 1]
        if not window:
            return None
        return (max(window) + min(window)) / 2.0

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
    step: float = 0.02,
    max_step: float = 0.2,
) -> List[Optional[float]]:
    n = len(closes)
    out: List[Optional[float]] = [None] * n
    if n < 2:
        return out
    close_vals: List[float] = []
    for i in range(n):
        c = _to_float_opt(closes[i] if i < len(closes) else None)
        if c is None or (not math.isfinite(float(c))):
            return out
        close_vals.append(float(c))
    high_vals = list(close_vals)
    low_vals = list(close_vals)

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
        "above_middle": "above_mid_inside",
        "above_mid": "above_mid_inside",
        "close_above_mid": "above_mid_inside",
        "close_above_middle": "above_mid_inside",
        "midpoint_long": "above_mid_inside",
        "below_middle": "below_mid_inside",
        "below_mid": "below_mid_inside",
        "close_below_mid": "below_mid_inside",
        "close_below_middle": "below_mid_inside",
        "midpoint_short": "below_mid_inside",
        "slope_up": "channel_slope_up",
        "bands_rising": "channel_slope_up",
        "channel_up": "channel_slope_up",
        "rising_channel": "channel_slope_up",
        "slope_down": "channel_slope_down",
        "bands_falling": "channel_slope_down",
        "channel_down": "channel_slope_down",
        "falling_channel": "channel_slope_down",
        "channel_up_above_mid": "slope_up_above_mid_inside",
        "rising_above_mid": "slope_up_above_mid_inside",
        "channel_up_below_mid": "slope_up_below_mid_inside",
        "rising_below_mid": "slope_up_below_mid_inside",
        "channel_down_above_mid": "slope_down_above_mid_inside",
        "falling_above_mid": "slope_down_above_mid_inside",
        "channel_down_below_mid": "slope_down_below_mid_inside",
        "falling_below_mid": "slope_down_below_mid_inside",
    }
    allowed = {
        "hold",
        "close_above_upper",
        "high_above_upper",
        "close_below_lower",
        "low_below_lower",
        "inside_channel",
        "above_mid_inside",
        "below_mid_inside",
        "channel_slope_up",
        "channel_slope_down",
        "slope_up_above_mid_inside",
        "slope_up_below_mid_inside",
        "slope_down_above_mid_inside",
        "slope_down_below_mid_inside",
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
) -> Tuple[List[Optional[float]], List[Optional[float]], List[Optional[float]]]:
    n = min(len(highs or []), len(lows or []))
    upper: List[Optional[float]] = [None] * n
    lower: List[Optional[float]] = [None] * n
    middle: List[Optional[float]] = [None] * n
    ln = max(1, int(lookback))
    if n <= 1:
        return upper, lower, middle
    try:
        high_vals = [float(v) for v in (highs or [])[:n]]
        low_vals = [float(v) for v in (lows or [])[:n]]
    except Exception:
        return upper, lower, middle
    for i in range(n):
        start = max(0, i - ln)
        if i - start < 1:
            continue
        upper[i] = max(high_vals[start:i])
        lower[i] = min(low_vals[start:i])
        middle[i] = (float(upper[i]) + float(lower[i])) / 2.0
    return upper, lower, middle


def _donchian_condition_hit(
    cond: str,
    *,
    close_now: float,
    high_now: Optional[float],
    low_now: Optional[float],
    upper: float,
    lower: float,
    middle: Optional[float] = None,
    prev_upper: Optional[float] = None,
    prev_lower: Optional[float] = None,
) -> bool:
    c = _normalize_donchian_condition(cond, default="hold")
    mid = (float(upper) + float(lower)) / 2.0 if middle is None else float(middle)
    inside = float(lower) <= float(close_now) <= float(upper)
    slope_up = (
        prev_upper is not None
        and prev_lower is not None
        and float(upper) > float(prev_upper)
        and float(lower) > float(prev_lower)
    )
    slope_down = (
        prev_upper is not None
        and prev_lower is not None
        and float(upper) < float(prev_upper)
        and float(lower) < float(prev_lower)
    )
    if c == "close_above_upper":
        return float(close_now) > float(upper)
    if c == "high_above_upper":
        return float(high_now if high_now is not None else close_now) > float(upper)
    if c == "close_below_lower":
        return float(close_now) < float(lower)
    if c == "low_below_lower":
        return float(low_now if low_now is not None else close_now) < float(lower)
    if c == "inside_channel":
        return inside
    if c == "above_mid_inside":
        return inside and float(close_now) > mid
    if c == "below_mid_inside":
        return inside and float(close_now) < mid
    if c == "channel_slope_up":
        return slope_up
    if c == "channel_slope_down":
        return slope_down
    if c == "slope_up_above_mid_inside":
        return slope_up and inside and float(close_now) > mid
    if c == "slope_up_below_mid_inside":
        return slope_up and inside and float(close_now) < mid
    if c == "slope_down_above_mid_inside":
        return slope_down and inside and float(close_now) > mid
    if c == "slope_down_below_mid_inside":
        return slope_down and inside and float(close_now) < mid
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
    if kind_raw == "ha":
        kind = "heikin_ashi"
    elif kind_raw in ("bollinger", "bollinger_bands"):
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
        upper_vals, lower_vals, middle_vals = _market_donchian_channels(highs[:n] if highs else None, lows[:n] if lows else None, lookback)
        upper_now = upper_vals[-1] if upper_vals else None
        lower_now = lower_vals[-1] if lower_vals else None
        middle_now = middle_vals[-1] if middle_vals else None
        prev_upper = upper_vals[-2] if len(upper_vals) >= 2 else None
        prev_lower = lower_vals[-2] if len(lower_vals) >= 2 else None
        if upper_now is None or lower_now is None or middle_now is None:
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
            middle=float(middle_now),
            prev_upper=prev_upper,
            prev_lower=prev_lower,
        )
        sell_ok = True if sell_ignored else _donchian_condition_hit(
            sell_cond,
            close_now=close_now,
            high_now=high_now,
            low_now=low_now,
            upper=float(upper_now),
            lower=float(lower_now),
            middle=float(middle_now),
            prev_upper=prev_upper,
            prev_lower=prev_lower,
        )
        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = bool(buy_ignored)
        out["sell_ignored"] = bool(sell_ignored)
        out["value"] = f"DON{lookback} U={_fmt(upper_now,4)} M={_fmt(middle_now,4)} L={_fmt(lower_now,4)} P={_fmt(close_now,4)}"
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
        upper = float(curr["upper"])
        lower = float(curr["lower"])
        middle = float(curr["middle"])
        width_pct = float(curr["width_pct"])
        percent_b = float(curr["percent_b"])

        buy_ok = True if buy_ignored else _bb_condition_hit(
            buy_cond,
            close_now=close_now,
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
        sar_vals = _sar_series(closes, step=step, max_step=max_step)
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
        out["value"] = f"SAR={_fmt(sar_now,4)} P={_fmt(close_now,4)} prevSAR={_fmt(prev_sar,4)}"
        out["detail"] = f"buy={buy_cond} sell={sell_cond} step={_fmt(step,4)} max={_fmt(max_step,4)}"
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
        cross_up_zero = m1f <= 0.0 and m0f > 0.0
        cross_down_zero = m1f >= 0.0 and m0f < 0.0

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

    if kind == "heikin_ashi":
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
            h = float(row.get("high_price"))
            l = float(row.get("low_price"))
            c = float(row.get("close_price"))
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


def _round_trail_amount(value: float) -> float:
    return max(0.0001, round(float(value), 6))


def _floor_to_cents(value: float) -> float:
    return max(0.0, math.floor(float(value) * 100.0) / 100.0)


def _round_up_crypto_price(value: float) -> float:
    if value <= 0:
        return 0.00000001
    return max(0.00000001, math.ceil(float(value) * 100000000.0) / 100000000.0)


def _round_down_crypto_price(value: float) -> float:
    if value <= 0:
        return 0.00000001
    return max(0.00000001, math.floor(float(value) * 100000000.0) / 100000000.0)


def _pivot_preorder_target(
    highs: List[float],
    lows: List[float],
    closes: List[float],
    current_price: float,
    *,
    offset: float,
    include_half_levels: bool,
    fallback_pct: float,
) -> Optional[Tuple[str, float]]:
    if calculate_pivot_points is not None and pivot_target_above_price is not None and len(closes) >= 2:
        levels = calculate_pivot_points(highs, lows, closes, source_index=-2)
        target = pivot_target_above_price(
            levels,
            float(current_price),
            offset=max(0.5, float(offset)),
            include_half_levels=bool(include_half_levels),
        )
        if target is not None:
            label, raw_price = target
            price = _round_down_crypto_price(float(raw_price))
            if price > float(current_price):
                return str(label), price
    if float(fallback_pct) > 0.0:
        price = _round_up_crypto_price(float(current_price) * (1.0 + (float(fallback_pct) / 100.0)))
        if price > float(current_price):
            return f"fallback +{float(fallback_pct):g}%", price
    return None


def _estimated_avg_after_buy(current_qty: float, avg_buy_price: float, buy_qty: float, buy_price: float) -> float:
    if buy_price <= 0:
        return 0.0
    if current_qty > 0 and avg_buy_price > 0 and buy_qty > 0:
        total_qty = float(current_qty) + float(buy_qty)
        if total_qty > 0:
            return ((float(current_qty) * float(avg_buy_price)) + (float(buy_qty) * float(buy_price))) / total_qty
    return float(avg_buy_price) if avg_buy_price > 0 else float(buy_price)


def _preorder_profit_target(
    current_price: float,
    avg_buy_price: float,
    profit_pct: float,
) -> Optional[Tuple[str, float]]:
    basis = max(float(avg_buy_price) if avg_buy_price > 0 else 0.0, float(current_price))
    if basis <= 0 or float(profit_pct) <= 0:
        return None
    price = _round_up_crypto_price(basis * (1.0 + (float(profit_pct) / 100.0)))
    if price <= basis or price <= float(current_price):
        return None
    return f"profit +{float(profit_pct):g}%", price


def _preorder_target_allowed(target_price: Optional[float], current_price: float, avg_buy_price: float) -> bool:
    if target_price is None or target_price <= 0:
        return False
    basis = float(avg_buy_price) if avg_buy_price > 0 else float(current_price)
    return float(target_price) > float(current_price) and float(target_price) > basis


def _record_trade(stats: Dict[str, Any], *, side: str, qty: float, price: float, avg_buy_price: float) -> None:
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
        elif kind_raw in ("heikin_ashi", "ha"):
            kind = "heikin_ashi"
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
    longest_ha = 0
    need_rsi = False
    need_drsi = False
    need_vwap = False

    for rule in rules:
        if not isinstance(rule, dict):
            continue
        kind_raw = str(rule.get("kind") or "").strip().lower()
        if kind_raw in ("bollinger", "bollinger_bands"):
            kind = "bb"
        elif kind_raw in ("heikin_ashi", "ha"):
            kind = "heikin_ashi"
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
        elif kind == "heikin_ashi":
            longest_ha = max(longest_ha, 2)

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
        longest_ha,
        15 if need_rsi else 0,
        17 if need_drsi else 0,
        2 if need_vwap else 0,
    )


def _sanitize_trail_order(order: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(order, dict):
        return None
    side = str(order.get("side") or "").strip().lower()
    symbol = str(order.get("symbol") or "").strip().upper()
    trail_amount = _safe_float(order.get("trail_amount"), 0.0)
    best_price = _safe_float(order.get("best_price"), 0.0)
    amount_dollars = _safe_float(order.get("amount_dollars"), 0.0)
    if side not in ("buy", "sell") or not symbol or trail_amount <= 0 or best_price <= 0:
        return None
    trigger = _safe_float(order.get("trigger_price"), 0.0)
    if trigger <= 0:
        trigger = best_price + trail_amount if side == "buy" else max(0.0, best_price - trail_amount)
    return {
        "id": str(order.get("id") or f"{symbol}:{side}"),
        "symbol": symbol,
        "side": side,
        "trail_amount": float(trail_amount),
        "amount_dollars": float(amount_dollars),
        "mode": str(order.get("mode") or "fixed").strip().lower(),
        "best_price": float(best_price),
        "trigger_price": float(trigger),
        "created_at": str(order.get("created_at") or iso_now()),
        "updated_at": str(order.get("updated_at") or iso_now()),
        "last_error": str(order.get("last_error") or ""),
    }


def load_local_trailing_state(path: Path) -> None:
    global local_trailing_orders
    local_trailing_orders = {}
    if not path.exists():
        return
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        _log_warn(f"Failed to read local trailing state ({path}): {e}")
        return
    orders = obj.get("orders") if isinstance(obj, dict) else None
    if not isinstance(orders, dict):
        return
    for raw_symbol, sides in orders.items():
        symbol = str(raw_symbol or "").strip().upper()
        if not symbol or not isinstance(sides, dict):
            continue
        sym_store: Dict[str, Dict[str, Any]] = {}
        for side in ("buy", "sell"):
            san = _sanitize_trail_order(sides.get(side))
            if san is not None:
                sym_store[side] = san
        if sym_store:
            local_trailing_orders[symbol] = sym_store


def save_local_trailing_state(path: Path) -> None:
    payload = {
        "version": LOCAL_TRAIL_STATE_VERSION,
        "saved_at": iso_now(),
        "orders": local_trailing_orders,
    }
    try:
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception as e:
        _log_warn(f"Failed to persist local trailing state ({path}): {e}")


def _remove_symbol_if_empty(symbol: str) -> None:
    sides = local_trailing_orders.get(symbol)
    if isinstance(sides, dict) and not sides:
        local_trailing_orders.pop(symbol, None)


def _cancel_local_trail(symbol: str, side: str, reason: str) -> None:
    symbol = str(symbol).strip().upper()
    side = str(side).strip().lower()
    sides = local_trailing_orders.get(symbol)
    if not isinstance(sides, dict):
        return
    if side in sides:
        sides.pop(side, None)
        _log_action(f"[{symbol}] Canceled local trailing {side.upper()} ({reason}).")
    _remove_symbol_if_empty(symbol)


def _trail_snapshot(symbol: str) -> List[Dict[str, Any]]:
    symbol = str(symbol).strip().upper()
    sides = local_trailing_orders.get(symbol)
    if not isinstance(sides, dict):
        return []
    out: List[Dict[str, Any]] = []
    for side in ("buy", "sell"):
        o = sides.get(side)
        if not isinstance(o, dict):
            continue
        out.append(
            {
                "side": side,
                "trail_amount": _safe_float(o.get("trail_amount")),
                "best_price": _safe_float(o.get("best_price")),
                "trigger_price": _safe_float(o.get("trigger_price")),
                "amount_dollars": _safe_float(o.get("amount_dollars")),
                "mode": str(o.get("mode") or "fixed"),
            }
        )
    return out


def _resolve_trail_amount(
    *,
    trailing_stop_mode: str,
    trailing_stop_amount: float,
    trailing_stop_atr_mult: float,
    atr: Optional[float],
) -> Optional[float]:
    trail_amount = float(trailing_stop_amount)
    if trailing_stop_mode == "atr":
        if atr is None or atr <= 0:
            return None
        trail_amount = float(atr) * float(trailing_stop_atr_mult)
    if trail_amount <= 0:
        return None
    return _round_trail_amount(trail_amount)


def _open_or_refresh_local_trail(
    *,
    symbol: str,
    side: str,
    current_price: float,
    trail_amount: float,
    amount_dollars: float,
    mode: str,
) -> None:
    symbol = str(symbol).strip().upper()
    side = str(side).strip().lower()
    if side not in ("buy", "sell"):
        return
    if current_price <= 0 or trail_amount <= 0:
        return

    sides = local_trailing_orders.setdefault(symbol, {})
    existing = sides.get(side)
    now = iso_now()
    if isinstance(existing, dict):
        best_price = _safe_float(existing.get("best_price"), current_price)
        trigger = best_price + trail_amount if side == "buy" else max(0.0, best_price - trail_amount)
        existing["trail_amount"] = float(trail_amount)
        existing["amount_dollars"] = float(amount_dollars)
        existing["mode"] = str(mode)
        existing["trigger_price"] = float(trigger)
        existing["updated_at"] = now
        return

    trigger = current_price + trail_amount if side == "buy" else max(0.0, current_price - trail_amount)
    sides[side] = {
        "id": f"{symbol}:{side}:{int(time.time())}",
        "symbol": symbol,
        "side": side,
        "trail_amount": float(trail_amount),
        "amount_dollars": float(amount_dollars),
        "mode": str(mode),
        "best_price": float(current_price),
        "trigger_price": float(trigger),
        "created_at": now,
        "updated_at": now,
        "last_error": "",
    }
    _log_action(
        f"[{symbol}] Armed LOCAL trailing {side.upper()} "
        f"trail={trail_amount:.6f} trigger={trigger:.6f} amount=${amount_dollars:.2f} ({mode})."
    )


def _update_local_trail(order: Dict[str, Any], current_price: float) -> bool:
    side = str(order.get("side") or "").strip().lower()
    trail_amount = _safe_float(order.get("trail_amount"), 0.0)
    best = _safe_float(order.get("best_price"), current_price)
    if side not in ("buy", "sell") or current_price <= 0 or trail_amount <= 0:
        return False

    if side == "buy":
        if current_price < best:
            best = float(current_price)
        trigger = best + trail_amount
        hit = current_price >= trigger
    else:
        if current_price > best:
            best = float(current_price)
        trigger = max(0.0, best - trail_amount)
        hit = current_price <= trigger

    order["best_price"] = float(best)
    order["trigger_price"] = float(trigger)
    order["updated_at"] = iso_now()
    return bool(hit)


def _normalize_order_amount_dollars(value: float) -> Optional[float]:
    amt = _floor_to_cents(value)
    if amt < MIN_CRYPTO_ORDER_AMOUNT_DOLLARS:
        return None
    return float(amt)


def _min_crypto_order_amount_label() -> str:
    return f"${MIN_CRYPTO_ORDER_AMOUNT_DOLLARS:.2f}"


def _to_limit_price(side: str, base_price: float) -> Optional[float]:
    side_norm = str(side or "").strip().lower()
    if base_price <= 0:
        return None
    if side_norm == "buy":
        return float(base_price * 1.01)
    if side_norm == "sell":
        return float(max(0.00000001, base_price * 0.99))
    return None


def _side_price_from_quote(quote: Any, side: str) -> Optional[float]:
    if not isinstance(quote, dict):
        return None
    side_norm = str(side or "").strip().lower()
    if side_norm == "buy":
        keys = ("ask_price", "mark_price", "bid_price", "open_price")
    else:
        keys = ("bid_price", "mark_price", "ask_price", "open_price")
    for key in keys:
        val = _to_float_opt(quote.get(key))
        if val is not None and val > 0:
            return float(val)
    return None


def _compute_reprice_limit(symbol: str, side: str, fallback_price: float) -> Optional[float]:
    side_norm = str(side or "").strip().lower()
    base = float(fallback_price) if fallback_price > 0 else 0.0
    try:
        quote = safe_crypto_quote(symbol, retries=2, backoff=0.25)
        quote_price = _side_price_from_quote(quote, side_norm)
        if quote_price is not None and quote_price > 0:
            base = float(quote_price)
    except Exception:
        pass
    return _to_limit_price(side_norm, base)


def _place_crypto_buy_limit_by_price(symbol: str, amount_dollars: float, limit_price: float) -> Tuple[Any, float]:
    amount = _normalize_order_amount_dollars(amount_dollars)
    if amount is None:
        raise RuntimeError(f"Buy amount is below crypto order minimum ({_min_crypto_order_amount_label()}).")
    if limit_price <= 0:
        raise RuntimeError("Invalid buy limit price.")

    resp = place_crypto_order(
        symbol=symbol,
        side="buy",
        order_type="limit",
        amountInDollars=float(amount),
        limitPrice=float(limit_price),
        jsonify=False,
    )
    return _normalize_order_response(resp), float(amount)


def _place_crypto_sell_limit_by_price(symbol: str, amount_dollars: float, limit_price: float) -> Tuple[Any, float]:
    amount = _normalize_order_amount_dollars(amount_dollars)
    if amount is None:
        raise RuntimeError(f"Sell amount is below crypto order minimum ({_min_crypto_order_amount_label()}).")
    if limit_price <= 0:
        raise RuntimeError("Invalid sell limit price.")

    resp = place_crypto_order(
        symbol=symbol,
        side="sell",
        order_type="limit",
        amountInDollars=float(amount),
        limitPrice=float(limit_price),
        timeInForce="gtc",
        jsonify=False,
    )
    return _normalize_order_response(resp), float(amount)


def _place_crypto_buy(symbol: str, amount_dollars: float, current_price: float) -> Tuple[Any, float]:
    amount = _normalize_order_amount_dollars(amount_dollars)
    if amount is None:
        raise RuntimeError(f"Buy amount is below crypto order minimum ({_min_crypto_order_amount_label()}).")

    resp = place_crypto_order(
        symbol=symbol,
        side="buy",
        order_type="market",
        amountInDollars=float(amount),
        jsonify=False,
    )
    return _normalize_order_response(resp), float(amount)


def _place_crypto_sell(
    symbol: str,
    amount_dollars: float,
    *,
    current_price: float,
    quantity_available: float,
) -> Tuple[Any, float]:
    amount = _normalize_order_amount_dollars(amount_dollars)
    if amount is None:
        raise RuntimeError(f"Sell amount is below crypto order minimum ({_min_crypto_order_amount_label()}).")

    resp = place_crypto_order(
        symbol=symbol,
        side="sell",
        order_type="market",
        amountInDollars=float(amount),
        jsonify=False,
    )
    return _normalize_order_response(resp), float(amount)


def _compute_sell_order_amount(pos_qty: float, current_price: float, desired_amount: float) -> Optional[float]:
    if pos_qty <= 0 or current_price <= 0:
        return None
    max_value = float(pos_qty) * float(current_price)
    if max_value < MIN_CRYPTO_ORDER_AMOUNT_DOLLARS:
        return None
    amount = min(float(desired_amount), max_value)
    amount = _floor_to_cents(amount)
    if amount < MIN_CRYPTO_ORDER_AMOUNT_DOLLARS:
        amount = _floor_to_cents(max_value)
    if amount < MIN_CRYPTO_ORDER_AMOUNT_DOLLARS:
        return None
    return float(amount)


def _can_sell_without_loss(current_price: float, avg_buy_price: float) -> bool:
    if current_price <= 0 or avg_buy_price <= 0:
        return False
    return float(current_price) >= float(avg_buy_price)


def _process_local_trails_for_symbol(
    *,
    symbol: str,
    current_price: float,
    pos_qty: float,
    avg_buy_price: float,
    buy_order_guard: Optional[Callable[[float], Optional[str]]] = None,
    trade_stats: Optional[Dict[str, Any]] = None,
) -> List[str]:
    symbol = str(symbol).strip().upper()
    triggered: List[str] = []
    sides = local_trailing_orders.get(symbol)
    if not isinstance(sides, dict):
        return triggered

    for side in ("sell", "buy"):
        order = sides.get(side)
        if not isinstance(order, dict):
            continue

        if side == "sell" and pos_qty <= 0:
            _cancel_local_trail(symbol, "sell", "no position")
            continue
        if side == "sell" and (not _can_sell_without_loss(current_price, avg_buy_price)):
            _cancel_local_trail(symbol, "sell", "no-loss rule (price below avg)")
            continue

        if not _update_local_trail(order, current_price):
            continue

        try:
            if side == "buy":
                desired = _safe_float(order.get("amount_dollars"), 0.0)
                guard_reason = buy_order_guard(float(desired)) if buy_order_guard is not None else None
                if guard_reason:
                    order["last_error"] = guard_reason
                    _log_error(f"[{symbol}] Local trailing BUY blocked: {guard_reason}")
                    continue
                resp, used_amount = _place_crypto_buy(symbol, desired, current_price)
                if _order_success(resp):
                    est_qty = used_amount / current_price if current_price > 0 else 0.0
                    _log_action(
                        f"[{symbol}] LOCAL trailing BUY triggered at {current_price:.6f}. "
                        f"Submitted buy for ~${used_amount:.2f}."
                    )
                    if trade_stats is not None:
                        _record_trade(
                            trade_stats,
                            side="buy",
                            qty=float(est_qty),
                            price=float(current_price),
                            avg_buy_price=0.0,
                        )
                    _cancel_local_trail(symbol, "buy", "triggered")
                    triggered.append("buy")
                else:
                    reason = _order_failure_reason(resp)
                    order["last_error"] = reason
                    _log_error(f"[{symbol}] Local trailing BUY rejected: {reason} | resp={resp}")
            else:
                desired = _safe_float(order.get("amount_dollars"), 0.0)
                sell_amount = _compute_sell_order_amount(pos_qty, current_price, desired)
                if sell_amount is None:
                    _cancel_local_trail(symbol, "sell", f"position value below {_min_crypto_order_amount_label()}")
                    continue
                resp, used_amount = _place_crypto_sell(
                    symbol,
                    sell_amount,
                    current_price=current_price,
                    quantity_available=pos_qty,
                )
                if _order_success(resp):
                    sold_qty = min(pos_qty, (used_amount / current_price) if current_price > 0 else 0.0)
                    _log_action(
                        f"[{symbol}] LOCAL trailing SELL triggered at {current_price:.6f}. "
                        f"Submitted sell for ~${used_amount:.2f}."
                    )
                    if trade_stats is not None:
                        _record_trade(
                            trade_stats,
                            side="sell",
                            qty=float(sold_qty),
                            price=float(current_price),
                            avg_buy_price=avg_buy_price,
                        )
                    _cancel_local_trail(symbol, "sell", "triggered")
                    triggered.append("sell")
                else:
                    reason = _order_failure_reason(resp)
                    order["last_error"] = reason
                    _log_error(f"[{symbol}] Local trailing SELL rejected: {reason} | resp={resp}")
        except Exception as e:
            order["last_error"] = str(e)
            _log_error(f"[{symbol}] Local trailing {side.upper()} execution failed: {e}")

    return triggered


def check_stoploss_and_sell(
    symbol: str,
    current_price: float,
    avg_buy_price: float,
    held_qty: float,
    target_gain_pct: float,
    stop_loss_pct: float,
    *,
    trade_stats: Optional[Dict[str, Any]] = None,
) -> bool:
    if avg_buy_price <= 0 or held_qty <= 0:
        return False

    percentage_gain = ((current_price - avg_buy_price) / avg_buy_price) * 100.0
    if symbol not in stoploss_state:
        stoploss_state[symbol] = {"armed": False}

    if not stoploss_state[symbol]["armed"] and percentage_gain >= target_gain_pct:
        stoploss_state[symbol]["armed"] = True
        _log_action(f"[{symbol}] Stop-loss armed at gain {percentage_gain:.2f}%.")

    if stoploss_state[symbol]["armed"]:
        trigger_price = avg_buy_price * (1.0 + (stop_loss_pct / 100.0))
        if current_price <= trigger_price:
            if not _can_sell_without_loss(current_price, avg_buy_price):
                stoploss_state[symbol]["armed"] = False
                _log_error(
                    f"[{symbol}] Stop-loss trigger hit, but no-loss rule blocked sell "
                    f"({current_price:.6f} < {avg_buy_price:.6f}). Disarming stop-loss."
                )
                return False
            liquidate_amount = _compute_sell_order_amount(held_qty, current_price, held_qty * current_price)
            if liquidate_amount is None:
                stoploss_state[symbol]["armed"] = False
                return False
            try:
                resp, used_amount = _place_crypto_sell(
                    symbol,
                    liquidate_amount,
                    current_price=current_price,
                    quantity_available=held_qty,
                )
                if _order_success(resp):
                    _log_action(f"[{symbol}] Stop-loss SELL executed: ~${used_amount:.2f}.")
                    if trade_stats is not None:
                        sold_qty = min(held_qty, used_amount / current_price if current_price > 0 else 0.0)
                        _record_trade(
                            trade_stats,
                            side="sell",
                            qty=float(sold_qty),
                            price=current_price,
                            avg_buy_price=avg_buy_price,
                        )
                    stoploss_state[symbol]["armed"] = False
                    return True
            except Exception as e:
                _log_error(f"[{symbol}] Stop-loss sell failed: {e}")
    return False


def _is_ichimoku_rule_item(item: Any) -> bool:
    if not isinstance(item, dict):
        return False
    kind = str(item.get("_rule_kind") or item.get("kind") or "").strip().lower()
    return kind in ("ichimoku", "ichimoku_cloud", "ichi")


def _rule_consensus_state(check: Dict[str, Any]) -> str:
    if not _is_ichimoku_rule_item(check):
        forced_side = str(check.get("_override_forced_side") or "").strip().upper()
        if forced_side in ("BUY", "SELL") and bool(check.get("_override_applied")):
            return forced_side
    buy_ignored = bool(check.get("buy_ignored"))
    sell_ignored = bool(check.get("sell_ignored"))
    buy_active = bool(check.get("buy_ok")) and (not buy_ignored)
    sell_active = bool(check.get("sell_ok")) and (not sell_ignored)
    # Match create/edit arbitration: SELL wins if both sides are active.
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


def _print_rule_checks(symbol: str, checks: List[Dict[str, Any]]) -> None:
    _log_info(f"[{symbol}] Rule checks:")
    for idx, c in enumerate(checks, start=1):
        name = str(c.get("name") or "")
        val = str(c.get("value") or "—")
        detail = str(c.get("detail") or "")
        state = _rule_consensus_state(c)
        name_col = _paint_indicator(name, state)
        state_col = _paint_indicator(state, state)
        val_col = _paint_indicator(val, state)
        line = f"  {idx:02d}. {name_col} -> {state_col} | {val_col}"
        if detail:
            line += f" | {detail}"
        _log_info(f"[{symbol}] {line}")


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
        if not _is_ichimoku_rule_item(item):
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
            if _target_matches(dst, target_exact, target_name_set) and not _is_ichimoku_rule_item(dst):
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
                if _is_ichimoku_rule_item(child):
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


def _print_symbol_snapshot(
    *,
    symbol: str,
    signal: str,
    price: float,
    qty: float,
    avg_buy: float,
    pnl_pct: Optional[float],
    ma20: Optional[float],
    ma78: Optional[float],
    ma190: Optional[float],
    rsi: Optional[float],
    drsi: Optional[float],
    atr: Optional[float],
    buy_order_type: str,
    sell_order_type: str,
    trails: List[Dict[str, Any]],
) -> None:
    _ = (qty, avg_buy, pnl_pct, buy_order_type, sell_order_type, trails)
    _log_info(f"[{symbol}] Signal={signal} Price={price:.6f}")
    _log_info(
        f"[{symbol}] MA20={_fmt(ma20,6)} MA78={_fmt(ma78,6)} MA190={_fmt(ma190,6)} "
        f"RSI={_fmt(rsi,2)} dRSI={_fmt(drsi,4)} ATR={_fmt(atr,6)}"
    )


def load_params(params_path: str) -> Dict[str, Any]:
    obj = json.loads(Path(params_path).read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError("params-json must be a JSON object.")
    return obj


def main_trading_loop(
    *,
    db_path: str,
    connection_id: int,
    symbols: List[str],
    trade_amount: float,
    trailing_stop_amount: float,
    trailing_stop_mode: str,
    trailing_stop_atr_mult: float,
    buy_order_type: str,
    sell_order_type: str,
    pivot_preorder_enabled: bool,
    pivot_preorder_profit_enabled: bool,
    pivot_preorder_profit_pct: float,
    pivot_preorder_offset: float,
    pivot_preorder_include_half_levels: bool,
    pivot_preorder_fallback_pct: float,
    target_gain_pct: float,
    stop_loss_pct: float,
    stoploss_enabled: bool,
    portfolio_cap_rule_enabled: bool,
    portfolio_cap_mode: str,
    portfolio_cap_percent_by_symbol: Dict[str, float],
    portfolio_cap_percent: float,
    portfolio_cap_divisor: int,
    portfolio_cash_percent: float,
    timeframe: str,
    sleep_duration: float,
    rules: List[Dict[str, Any]],
    local_state_path: Optional[Path] = None,
    trade_stats: Optional[Dict[str, Any]] = None,
    status_writer: Optional[Any] = None,
) -> None:
    if timeframe not in TIMEFRAMES:
        raise ValueError(f"Invalid timeframe '{timeframe}'. Choose from {list(TIMEFRAMES.keys())}.")
    interval = TIMEFRAMES[timeframe]["interval"]
    span = TIMEFRAMES[timeframe]["span"]
    rules = _rules_with_default_timeframe(rules, timeframe)
    rules_by_tf = _rules_by_timeframe(rules, timeframe)
    rule_min_candles = _rule_min_candles(rules)
    rule_min_candles_by_tf = {tf: _rule_min_candles(tf_rules) for tf, tf_rules in rules_by_tf.items()}
    _log_info(f"Rule candle requirement: {rule_min_candles}")
    _log_info(
        "Rule timeframes: "
        + ", ".join(f"{tf}({rule_min_candles_by_tf[tf]} candles)" for tf in rules_by_tf)
    )

    while True:
        tickers_status: List[Dict[str, Any]] = []
        try:
            positions = get_open_crypto_positions(_db_path=db_path, _connection_id=connection_id)
            positions_map = build_positions_map(positions)
        except Exception:
            positions_map = {}
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
            buying_power = float(get_buying_power(_db_path=db_path, _connection_id=connection_id))
        except Exception as e:
            _log_warn(f"Buying power unavailable; buy power gate will not be enforced: {e}")
            buying_power = None
        if portfolio_cap_rule_enabled:
            try:
                portfolio_value = float(get_portfolio_value(_db_path=db_path, _connection_id=connection_id))
                available_cash = float(get_available_cash(_db_path=db_path, _connection_id=connection_id))
                cap_pct = None if portfolio_cap_mode == "percent" else 100.0 / float(divisor)
                if portfolio_value > 0:
                    cash_target_value = float(portfolio_value) * (float(cash_target_pct) / 100.0)
                    cash_pct = (float(available_cash) / float(portfolio_value)) * 100.0
            except Exception as e:
                _log_warn(f"Portfolio cap enabled but portfolio value unavailable: {e}")
                portfolio_value = None
                available_cash = None
                cap_pct = None
                cash_target_value = None
                cash_pct = None

        for symbol in symbols:
            try:
                current_price = get_crypto_price(symbol, _db_path=db_path, _connection_id=connection_id)
                fetch_timeframes = list(dict.fromkeys([timeframe] + list(rules_by_tf.keys())))
                hist_by_tf: Dict[str, List[Dict[str, Any]]] = {}
                ohlc_by_tf: Dict[str, Tuple[List[float], List[float], List[float], List[float], List[float], List[Any]]] = {}
                missing_tf_reasons: List[str] = []

                for tf_key in fetch_timeframes:
                    tf_cfg = TIMEFRAMES[tf_key]
                    tf_min = int(rule_min_candles_by_tf.get(tf_key, 30 if tf_key == timeframe else rule_min_candles))
                    hist_tf = get_crypto_historicals(
                        symbol,
                        tf_cfg["interval"],
                        tf_cfg["span"],
                        _db_path=db_path,
                        _connection_id=connection_id,
                    )
                    tf_opens, tf_highs, tf_lows, tf_closes, tf_volumes, tf_timestamps = _extract_ohlcv(hist_tf)
                    if len(tf_closes) < tf_min:
                        missing_tf_reasons.append(f"{tf_key}: got {len(tf_closes)}, need {tf_min}")
                        continue
                    prev_close = float(tf_closes[-1])
                    current_price_f = float(current_price)
                    tf_opens.append(prev_close)
                    tf_highs.append(max(prev_close, current_price_f))
                    tf_lows.append(min(prev_close, current_price_f))
                    tf_closes.append(current_price_f)
                    tf_volumes.append(0.0)
                    tf_timestamps.append(iso_now())
                    hist_by_tf[tf_key] = hist_tf
                    ohlc_by_tf[tf_key] = (tf_opens, tf_highs, tf_lows, tf_closes, tf_volumes, tf_timestamps)

                if missing_tf_reasons:
                    _log_warn(f"[{symbol}] Not enough historical candles by timeframe: {'; '.join(missing_tf_reasons)}.")
                    tickers_status.append({"symbol": symbol, "signal": "NO_DATA", "timeframes": list(rules_by_tf.keys())})
                    continue

                default_ohlc = ohlc_by_tf.get(timeframe) or next(iter(ohlc_by_tf.values()))
                opens, highs, lows, closes, volumes, timestamps = default_ohlc
                hist = hist_by_tf.get(timeframe) or next(iter(hist_by_tf.values()))

                pos_info = positions_map.get(symbol, {})
                pos_qty = _safe_float(pos_info.get("quantity"))
                avg_buy_price = _safe_float(pos_info.get("average_buy_price"))
                buy_order_cost = _normalize_order_amount_dollars(trade_amount) or 0.0
                symbol_cap_pct = float(portfolio_cap_percent_by_symbol.get(str(symbol).strip().upper(), portfolio_cap_percent))
                if symbol_cap_pct <= 0:
                    symbol_cap_pct = portfolio_cap_percent
                row_cap_pct = max(0.01, float(symbol_cap_pct)) if portfolio_cap_mode == "percent" else cap_pct
                held_pct: Optional[float] = None
                cap_delta_pct: Optional[float] = None
                if row_cap_pct is not None and portfolio_value is not None and portfolio_value > 0:
                    held_value = float(pos_qty) * float(current_price)
                    held_pct = (held_value / float(portfolio_value)) * 100.0
                    cap_delta_pct = held_pct - row_cap_pct

                def evaluate_buy_gate(buy_cost: float) -> Tuple[bool, bool, bool, str]:
                    buy_cap_blocked = False
                    buy_power_blocked = False
                    cash_slice_blocked = False
                    reason = ""
                    if buying_power is not None and float(buying_power) < float(buy_cost):
                        buy_power_blocked = True
                        reason = (
                            f"buying power ${float(buying_power):.2f} below order cost "
                            f"${float(buy_cost):.2f}"
                        )
                    if portfolio_cap_rule_enabled:
                        if portfolio_value is not None and portfolio_value > 0:
                            ticker_value = float(pos_qty) * float(current_price)
                            if portfolio_cap_mode == "percent":
                                ticker_cap_pct = max(0.01, float(symbol_cap_pct))
                                ticker_cap_value = float(portfolio_value) * (ticker_cap_pct / 100.0)
                                if ticker_value > ticker_cap_value:
                                    buy_cap_blocked = True
                                    if not reason:
                                        reason = (
                                            f"current holdings ${ticker_value:.2f} exceed "
                                            f"{ticker_cap_pct:.2f}% cap (${ticker_cap_value:.2f})"
                                        )
                            elif ticker_value > (float(portfolio_value) / float(divisor)):
                                buy_cap_blocked = True
                                if not reason:
                                    reason = (
                                        f"holdings ${ticker_value:.2f} exceed 1/{divisor} "
                                        f"of portfolio (${float(portfolio_value):.2f})"
                                    )
                            if (
                                available_cash is not None
                                and cash_target_value is not None
                                and float(available_cash) < float(cash_target_value)
                            ):
                                cash_slice_blocked = True
                                if not reason:
                                    reason = (
                                        f"current available cash ${float(available_cash):.2f} "
                                        f"is below {float(cash_target_pct):.2f}% target "
                                        f"(${float(cash_target_value):.2f})"
                                    )
                        else:
                            buy_cap_blocked = True
                            if not reason:
                                reason = "portfolio value unavailable"
                    return buy_cap_blocked, buy_power_blocked, cash_slice_blocked, reason

                buy_cap_blocked = False
                buy_power_blocked = False
                cash_slice_blocked = False
                buy_block_reason = ""

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
                rule_consensus_signal = _strict_consensus_signal(checks)
                if rule_consensus_signal == "BUY":
                    buy_cap_blocked, buy_power_blocked, cash_slice_blocked, buy_block_reason = evaluate_buy_gate(
                        float(buy_order_cost)
                    )
                    if buy_power_blocked:
                        _log_warn(
                            f"[{symbol}] BUY blocked by buying power: need ${float(buy_order_cost):.2f}, "
                            f"buying power ${float(buying_power or 0.0):.2f}."
                        )
                    if buy_cap_blocked and portfolio_cap_rule_enabled and not buy_power_blocked:
                        _log_warn(f"[{symbol}] BUY blocked by portfolio cap: {buy_block_reason}.")
                    if cash_slice_blocked:
                        _log_warn(f"[{symbol}] BUY blocked by cash target: {buy_block_reason}.")
                can_sell_now = pos_qty > 0 and _can_sell_without_loss(float(current_price), avg_buy_price)
                execution_hold_reason = ""
                if rule_consensus_signal == "SELL" and can_sell_now:
                    signal = "SELL"
                elif rule_consensus_signal == "BUY" and not (buy_cap_blocked or buy_power_blocked or cash_slice_blocked):
                    signal = "BUY"
                else:
                    signal = "HOLD"
                    if rule_consensus_signal == "SELL":
                        if pos_qty <= 0:
                            execution_hold_reason = "sell blocked: no position"
                        else:
                            execution_hold_reason = "sell blocked: no-loss rule"
                    elif rule_consensus_signal == "BUY":
                        if buy_power_blocked:
                            execution_hold_reason = "buy blocked: buying power"
                        elif cash_slice_blocked:
                            execution_hold_reason = "buy blocked: cash target"
                        elif buy_cap_blocked:
                            execution_hold_reason = "buy blocked: portfolio cap"

                pnl_pct: Optional[float] = None
                if avg_buy_price > 0:
                    pnl_pct = ((float(current_price) - avg_buy_price) / avg_buy_price) * 100.0

                estimated_buy_qty = float(buy_order_cost) / float(current_price) if float(current_price) > 0 else 0.0
                estimated_preorder_avg = _estimated_avg_after_buy(
                    float(pos_qty),
                    float(avg_buy_price),
                    float(estimated_buy_qty) if signal == "BUY" else 0.0,
                    float(current_price),
                )
                pivot_preorder_target = (
                    _preorder_profit_target(
                        float(current_price),
                        float(estimated_preorder_avg),
                        float(pivot_preorder_profit_pct),
                    )
                    if bool(pivot_preorder_profit_enabled)
                    else None
                ) or (
                    _pivot_preorder_target(
                        highs,
                        lows,
                        closes,
                        float(current_price),
                        offset=float(pivot_preorder_offset),
                        include_half_levels=bool(pivot_preorder_include_half_levels),
                        fallback_pct=float(pivot_preorder_fallback_pct),
                    )
                    if bool(pivot_preorder_enabled)
                    else None
                )
                if pivot_preorder_target is not None and not _preorder_target_allowed(
                    pivot_preorder_target[1],
                    float(current_price),
                    float(estimated_preorder_avg),
                ):
                    pivot_preorder_target = None
                pivot_preorder_target_label = pivot_preorder_target[0] if pivot_preorder_target is not None else None
                pivot_preorder_target_price = pivot_preorder_target[1] if pivot_preorder_target is not None else None
                pivot_preorder_margin_pct = None
                pivot_preorder_margin_per_unit = None
                pivot_preorder_margin_total = None
                if pivot_preorder_target_price is not None and estimated_preorder_avg > 0.0:
                    pivot_preorder_margin_per_unit = float(pivot_preorder_target_price) - float(estimated_preorder_avg)
                    pivot_preorder_margin_pct = (pivot_preorder_margin_per_unit / float(estimated_preorder_avg)) * 100.0
                    pivot_preorder_margin_total = pivot_preorder_margin_per_unit * float(estimated_buy_qty)

                ma20 = _ma_value(closes, 20)
                ma78 = _ma_value(closes, 78)
                ma190 = _ma_value(closes, 190)
                rsi = _rsi(closes, 14)
                drsi = _rsi_derivative(closes, 14)
                atr = _atr_from_historicals(hist, period=14)

                stoploss_sold = False
                if stoploss_enabled:
                    stoploss_sold = check_stoploss_and_sell(
                        symbol=symbol,
                        current_price=float(current_price),
                        avg_buy_price=avg_buy_price,
                        held_qty=pos_qty,
                        target_gain_pct=target_gain_pct,
                        stop_loss_pct=stop_loss_pct,
                        trade_stats=trade_stats,
                    )
                    if stoploss_sold:
                        pos_qty = 0.0
                        _cancel_local_trail(symbol, "sell", "stop-loss filled")

                if buy_order_type != "local_trailing":
                    _cancel_local_trail(symbol, "buy", "BUY order type is market")
                if sell_order_type != "local_trailing":
                    _cancel_local_trail(symbol, "sell", "SELL order type is market")

                triggered_sides: List[str] = []
                if buy_order_type == "local_trailing" or sell_order_type == "local_trailing":
                    def local_buy_guard(amount: float) -> Optional[str]:
                        gate = evaluate_buy_gate(float(amount))
                        return gate[3] if any(gate[:3]) else None

                    triggered_sides = _process_local_trails_for_symbol(
                        symbol=symbol,
                        current_price=float(current_price),
                        pos_qty=pos_qty,
                        avg_buy_price=avg_buy_price,
                        buy_order_guard=local_buy_guard,
                        trade_stats=trade_stats,
                    )

                trails_now = _trail_snapshot(symbol)
                _print_symbol_snapshot(
                    symbol=symbol,
                    signal=signal,
                    price=float(current_price),
                    qty=pos_qty,
                    avg_buy=avg_buy_price,
                    pnl_pct=pnl_pct,
                    ma20=ma20,
                    ma78=ma78,
                    ma190=ma190,
                    rsi=rsi,
                    drsi=drsi,
                    atr=atr,
                    buy_order_type=buy_order_type,
                    sell_order_type=sell_order_type,
                    trails=trails_now,
                )
                _print_rule_checks(symbol, checks)

                trail_amount = _resolve_trail_amount(
                    trailing_stop_mode=trailing_stop_mode,
                    trailing_stop_amount=trailing_stop_amount,
                    trailing_stop_atr_mult=trailing_stop_atr_mult,
                    atr=atr,
                )
                pivot_preorder_order_status = (
                    "preview"
                    if pivot_preorder_target is not None
                    else ("no target" if (bool(pivot_preorder_enabled) or bool(pivot_preorder_profit_enabled)) else None)
                )
                pivot_preorder_order_reason = None

                if signal == "BUY":
                    _cancel_local_trail(symbol, "sell", "BUY signal")
                    if buy_order_type == "local_trailing" and "buy" not in triggered_sides:
                        if trail_amount is None:
                            _log_warn(f"[{symbol}] Trail amount unavailable; skipping local trailing BUY arm.")
                        else:
                            buy_amount = _normalize_order_amount_dollars(trade_amount)
                            if buy_amount is None:
                                _log_warn(
                                    f"[{symbol}] Trade amount < {_min_crypto_order_amount_label()}; "
                                    "skipping local trailing BUY arm."
                                )
                            else:
                                _open_or_refresh_local_trail(
                                    symbol=symbol,
                                    side="buy",
                                    current_price=float(current_price),
                                    trail_amount=float(trail_amount),
                                    amount_dollars=float(buy_amount),
                                    mode=trailing_stop_mode,
                                )
                    elif buy_order_type == "market":
                        _cancel_local_trail(symbol, "buy", "BUY signal market order")
                        buy_amount = _normalize_order_amount_dollars(trade_amount)
                        if buy_amount is None:
                            _log_warn(
                                f"[{symbol}] Trade amount < {_min_crypto_order_amount_label()}; "
                                "skipping BUY market order."
                            )
                        else:
                            try:
                                resp, used_amount = _place_crypto_buy(symbol, buy_amount, float(current_price))
                                if (not _order_success(resp)) and _is_reprice_rejection(resp):
                                    safe_sleep(0.35)
                                    retry_resp, retry_amount = _place_crypto_buy(symbol, buy_amount, float(current_price))
                                    resp, used_amount = retry_resp, retry_amount
                                if (not _order_success(resp)) and _is_reprice_rejection(resp):
                                    limit_price = _compute_reprice_limit(symbol, "buy", float(current_price))
                                    if limit_price is not None:
                                        limit_resp, limit_amount = _place_crypto_buy_limit_by_price(
                                            symbol,
                                            buy_amount,
                                            float(limit_price),
                                        )
                                        resp, used_amount = limit_resp, limit_amount
                                if _order_success(resp):
                                    est_qty = used_amount / float(current_price) if current_price > 0 else 0.0
                                    _log_action(f"[{symbol}] BUY signal -> order submitted for ~${used_amount:.2f}.")
                                    if trade_stats is not None:
                                        _record_trade(
                                            trade_stats,
                                            side="buy",
                                            qty=float(est_qty),
                                            price=float(current_price),
                                            avg_buy_price=0.0,
                                        )
                                    if bool(pivot_preorder_enabled) or bool(pivot_preorder_profit_enabled):
                                        preorder_avg = _estimated_avg_after_buy(
                                            float(pos_qty),
                                            float(avg_buy_price),
                                            float(est_qty),
                                            float(current_price),
                                        )
                                        preorder_target = _preorder_profit_target(
                                            float(current_price),
                                            float(preorder_avg),
                                            float(pivot_preorder_profit_pct),
                                        ) if bool(pivot_preorder_profit_enabled) else None
                                        if preorder_target is None and bool(pivot_preorder_enabled):
                                            preorder_target = _pivot_preorder_target(
                                                highs,
                                                lows,
                                                closes,
                                                float(current_price),
                                                offset=float(pivot_preorder_offset),
                                                include_half_levels=bool(pivot_preorder_include_half_levels),
                                                fallback_pct=float(pivot_preorder_fallback_pct),
                                            )
                                        if preorder_target is None or not _preorder_target_allowed(
                                            preorder_target[1],
                                            float(current_price),
                                            float(preorder_avg),
                                        ):
                                            _log_warn(f"[{symbol}] Pre-sale order skipped: no profitable target above held average.")
                                            pivot_preorder_order_status = "no target"
                                        else:
                                            target_label, target_price = preorder_target
                                            sell_amount = _normalize_order_amount_dollars(float(est_qty) * float(target_price))
                                            if sell_amount is None:
                                                _log_warn(
                                                    f"[{symbol}] Pre-sale order skipped: target value below "
                                                    f"{_min_crypto_order_amount_label()}."
                                                )
                                                pivot_preorder_order_status = "too small"
                                            else:
                                                _log_action(
                                                    f"[{symbol}] Pre-sale order -> placing limit SELL for "
                                                    f"~${float(sell_amount):.2f} at ${float(target_price):.8f} ({target_label})."
                                                )
                                                sell_resp, used_sell_amount = _place_crypto_sell_limit_by_price(
                                                    symbol,
                                                    float(sell_amount),
                                                    float(target_price),
                                                )
                                                pivot_preorder_target_label = target_label
                                                pivot_preorder_target_price = float(target_price)
                                                pivot_preorder_margin_pct = (
                                                    ((float(target_price) - float(preorder_avg)) / float(preorder_avg)) * 100.0
                                                    if preorder_avg > 0
                                                    else None
                                                )
                                                pivot_preorder_margin_per_unit = (
                                                    float(target_price) - float(preorder_avg)
                                                    if preorder_avg > 0
                                                    else None
                                                )
                                                pivot_preorder_margin_total = float(used_sell_amount) - float(used_amount)
                                                if _order_success(sell_resp):
                                                    _log_action(f"[{symbol}] Pre-sale limit SELL accepted: resp={sell_resp}")
                                                    pivot_preorder_order_status = "accepted"
                                                else:
                                                    reason = _order_failure_reason(sell_resp)
                                                    _log_error(
                                                        f"[{symbol}] Pre-sale limit SELL rejected: "
                                                        f"{reason} | resp={sell_resp}"
                                                    )
                                                    pivot_preorder_order_status = "rejected"
                                                    pivot_preorder_order_reason = reason
                                else:
                                    reason = _order_failure_reason(resp)
                                    _log_error(f"[{symbol}] BUY market order rejected: {reason} | resp={resp}")
                            except Exception as e:
                                _log_error(f"[{symbol}] BUY market order failed: {e}")

                elif signal == "SELL" and pos_qty > 0:
                    if not _can_sell_without_loss(float(current_price), avg_buy_price):
                        _cancel_local_trail(symbol, "sell", "no-loss rule (price below avg)")
                        _log_error(
                            f"[{symbol}] SELL blocked by no-loss rule "
                            f"({float(current_price):.6f} < {avg_buy_price:.6f})."
                        )
                        continue
                    _cancel_local_trail(symbol, "buy", "SELL signal")
                    if sell_order_type == "local_trailing" and "sell" not in triggered_sides:
                        if trail_amount is None:
                            _log_warn(f"[{symbol}] Trail amount unavailable; skipping local trailing SELL arm.")
                        elif (float(current_price) - float(trail_amount)) < float(avg_buy_price):
                            _cancel_local_trail(symbol, "sell", "no-loss rule (trail below avg)")
                            _log_error(
                                f"[{symbol}] SELL blocked by no-loss rule: trailing trigger "
                                f"({float(current_price) - float(trail_amount):.6f}) would be below "
                                f"avg buy ({avg_buy_price:.6f})."
                            )
                        else:
                            sell_amount = _compute_sell_order_amount(pos_qty, float(current_price), trade_amount)
                            if sell_amount is None:
                                _log_warn(f"[{symbol}] Position value too small; skipping local trailing SELL arm.")
                            else:
                                _open_or_refresh_local_trail(
                                    symbol=symbol,
                                    side="sell",
                                    current_price=float(current_price),
                                    trail_amount=float(trail_amount),
                                    amount_dollars=float(sell_amount),
                                    mode=trailing_stop_mode,
                                )
                    elif sell_order_type == "market":
                        _cancel_local_trail(symbol, "sell", "SELL signal market order")
                        sell_amount = _compute_sell_order_amount(pos_qty, float(current_price), trade_amount)
                        if sell_amount is None:
                            _log_warn(f"[{symbol}] Position value too small; skipping SELL market order.")
                        else:
                            try:
                                resp, used_amount = _place_crypto_sell(
                                    symbol,
                                    sell_amount,
                                    current_price=float(current_price),
                                    quantity_available=pos_qty,
                                )
                                if (not _order_success(resp)) and _is_reprice_rejection(resp):
                                    safe_sleep(0.35)
                                    retry_resp, retry_amount = _place_crypto_sell(
                                        symbol,
                                        sell_amount,
                                        current_price=float(current_price),
                                        quantity_available=pos_qty,
                                    )
                                    resp, used_amount = retry_resp, retry_amount
                                if (not _order_success(resp)) and _is_reprice_rejection(resp):
                                    limit_price = _compute_reprice_limit(symbol, "sell", float(current_price))
                                    if limit_price is not None:
                                        limit_resp, limit_amount = _place_crypto_sell_limit_by_price(
                                            symbol,
                                            sell_amount,
                                            float(limit_price),
                                        )
                                        resp, used_amount = limit_resp, limit_amount
                                if _order_success(resp):
                                    sold_qty = min(pos_qty, (used_amount / float(current_price)) if current_price > 0 else 0.0)
                                    _log_action(f"[{symbol}] SELL signal -> order submitted for ~${used_amount:.2f}.")
                                    if trade_stats is not None:
                                        _record_trade(
                                            trade_stats,
                                            side="sell",
                                            qty=float(sold_qty),
                                            price=float(current_price),
                                            avg_buy_price=avg_buy_price,
                                        )
                                else:
                                    reason = _order_failure_reason(resp)
                                    _log_error(f"[{symbol}] SELL market order rejected: {reason} | resp={resp}")
                            except Exception as e:
                                _log_error(f"[{symbol}] SELL market order failed: {e}")

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

                tickers_status.append(
                    {
                        "symbol": symbol,
                        "signal": signal,
                        "rule_signal": rule_consensus_signal,
                        "execution_signal": signal,
                        "execution_hold_reason": execution_hold_reason,
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
                        "ma20": ma20,
                        "ma78": ma78,
                        "ma150": ma190,
                        "rsi": rsi,
                        "rsi_d": drsi,
                        "atr": atr,
                        "buy_order_type": buy_order_type,
                        "sell_order_type": sell_order_type,
                        "pivot_preorder_enabled": bool(pivot_preorder_enabled),
                        "pivot_preorder_profit_enabled": bool(pivot_preorder_profit_enabled),
                        "pivot_preorder_profit_pct": float(pivot_preorder_profit_pct),
                        "pivot_preorder_offset": float(pivot_preorder_offset),
                        "pivot_preorder_include_half_levels": bool(pivot_preorder_include_half_levels),
                        "pivot_preorder_fallback_pct": float(pivot_preorder_fallback_pct),
                        "pivot_preorder_target_label": pivot_preorder_target_label,
                        "pivot_preorder_target_price": pivot_preorder_target_price,
                        "pivot_preorder_margin_pct": pivot_preorder_margin_pct,
                        "pivot_preorder_margin_per_share": pivot_preorder_margin_per_unit,
                        "pivot_preorder_margin_total": pivot_preorder_margin_total,
                        "pivot_preorder_shares": float(estimated_buy_qty) if (bool(pivot_preorder_enabled) or bool(pivot_preorder_profit_enabled)) else None,
                        "pivot_preorder_order_status": pivot_preorder_order_status,
                        "pivot_preorder_order_reason": pivot_preorder_order_reason,
                        "stoploss_enabled": bool(stoploss_enabled),
                        "stoploss_armed": stop_armed,
                        "stoploss_arm_price": stop_arm_price,
                        "stoploss_trigger": stop_trigger_price,
                        "stoploss_arm_gap_pct": stop_arm_gap_pct,
                        "stoploss_trigger_gap_pct": stop_trigger_gap_pct,
                        "local_trails": _trail_snapshot(symbol),
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

            except Exception as e:
                _log_error(f"[{symbol}] Unhandled symbol error: {e}")
                tickers_status.append({"symbol": symbol, "signal": "ERROR"})

        if status_writer is not None:
            try:
                status_writer({"phase": "loop", "timeframe": timeframe, "tickers": tickers_status})
            except Exception:
                pass

        if local_state_path is not None:
            save_local_trailing_state(local_state_path)

        time.sleep(float(sleep_duration))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--params-json", required=True)
    ap.add_argument("--db-path", required=True)
    ap.add_argument("--connection-id", required=True, type=int)
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    status_path = run_dir / "status.json"
    local_state_path = run_dir / "local_trailing_orders.json"
    last_status: Dict[str, Any] = {}
    trade_stats: Dict[str, Any] = {"pnl": 0.0, "trades": 0}

    def write_status(payload: Dict[str, Any]) -> None:
        p = dict(payload)
        p["ts"] = iso_now()
        p["script"] = "IndicatorForge.Crypto.Robinhood"
        p["pnl"] = round(float(trade_stats.get("pnl", 0.0)), 2)
        p["trades"] = int(trade_stats.get("trades", 0))
        try:
            status_path.write_text(json.dumps(p, indent=2), encoding="utf-8")
            last_status.clear()
            last_status.update(p)
        except Exception:
            pass

    params = load_params(args.params_json)
    global COLORIZE_INDICATOR_LOGS
    global PRINT_ORDER_EVENTS
    COLORIZE_INDICATOR_LOGS = _to_bool(params.get("colorize_indicator_logs", True), True)
    PRINT_ORDER_EVENTS = _to_bool(params.get("print_order_events", False), False)

    symbols = params.get("symbols", [])
    if isinstance(symbols, str):
        symbols = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not isinstance(symbols, list) or not symbols:
        raise ValueError("params.symbols must be a non-empty list.")
    symbols = [str(s).strip().upper() for s in symbols if str(s).strip()]

    sleep_duration = float(params.get("sleep_duration", 30))
    trade_amount = float(params.get("trade_amount", params.get("shares_per_trade", 10.0)))
    trailing_stop_amount = float(params.get("trailing_stop_amount", 0.10))
    trailing_stop_mode = str(params.get("trailing_stop_mode", "fixed")).strip().lower()
    if trailing_stop_mode not in ("fixed", "atr"):
        trailing_stop_mode = "fixed"
    trailing_stop_atr_mult = float(params.get("trailing_stop_atr_mult", 3.0))
    if trailing_stop_atr_mult <= 0:
        trailing_stop_atr_mult = 3.0

    buy_order_type_raw = params.get("buy_order_type", None)
    if buy_order_type_raw is None:
        legacy_buy_local = _to_bool(params.get("local_trailing_buy_enabled", True), True)
        buy_order_type = "local_trailing" if legacy_buy_local else "market"
    else:
        buy_order_type = _normalize_order_type(buy_order_type_raw, default="local_trailing")

    sell_order_type_raw = params.get("sell_order_type", None)
    if sell_order_type_raw is None:
        legacy_sell_local = _to_bool(params.get("local_trailing_sell_enabled", True), True)
        sell_order_type = "local_trailing" if legacy_sell_local else "market"
    else:
        sell_order_type = _normalize_order_type(sell_order_type_raw, default="local_trailing")

    target_gain_pct = float(params.get("target_gain_pct", 0.5))
    stop_loss_pct = float(params.get("stop_loss_pct", -0.5))
    stoploss_enabled = _to_bool(params.get("stoploss_enabled", False), False)
    pivot_preorder_enabled = _to_bool(params.get("pivot_preorder_enabled", False), False)
    pivot_preorder_profit_enabled = _to_bool(params.get("pivot_preorder_profit_enabled", False), False)
    pivot_preorder_profit_pct = max(0.0, float(params.get("pivot_preorder_profit_pct", 0) or 0))
    pivot_preorder_offset = max(0.5, float(params.get("pivot_preorder_offset", 1) or 1))
    pivot_preorder_include_half_levels = _to_bool(params.get("pivot_preorder_include_half_levels", False), False)
    pivot_preorder_fallback_pct = max(0.0, float(params.get("pivot_preorder_fallback_pct", 0) or 0))
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
    timeframe = str(params.get("timeframe", "1h")).strip().lower()

    rules = _resolve_rules(args.db_path, params)
    if not rules:
        rules = _default_rules()

    ensure_robinhood_session(args.db_path, int(args.connection_id))
    load_local_trailing_state(local_state_path)

    main_trading_loop(
        db_path=args.db_path,
        connection_id=int(args.connection_id),
        symbols=symbols,
        trade_amount=trade_amount,
        trailing_stop_amount=trailing_stop_amount,
        trailing_stop_mode=trailing_stop_mode,
        trailing_stop_atr_mult=trailing_stop_atr_mult,
        buy_order_type=buy_order_type,
        sell_order_type=sell_order_type,
        pivot_preorder_enabled=pivot_preorder_enabled,
        pivot_preorder_profit_enabled=pivot_preorder_profit_enabled,
        pivot_preorder_profit_pct=pivot_preorder_profit_pct,
        pivot_preorder_offset=pivot_preorder_offset,
        pivot_preorder_include_half_levels=pivot_preorder_include_half_levels,
        pivot_preorder_fallback_pct=pivot_preorder_fallback_pct,
        target_gain_pct=target_gain_pct,
        stop_loss_pct=stop_loss_pct,
        stoploss_enabled=stoploss_enabled,
        portfolio_cap_rule_enabled=portfolio_cap_rule_enabled,
        portfolio_cap_mode=portfolio_cap_mode,
        portfolio_cap_percent_by_symbol=portfolio_cap_percent_by_symbol,
        portfolio_cap_percent=portfolio_cap_percent,
        portfolio_cap_divisor=portfolio_cap_divisor,
        portfolio_cash_percent=portfolio_cash_percent,
        timeframe=timeframe,
        sleep_duration=sleep_duration,
        rules=rules,
        local_state_path=local_state_path,
        trade_stats=trade_stats,
        status_writer=write_status,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
