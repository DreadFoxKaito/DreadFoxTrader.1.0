#!/usr/bin/env python3
"""
EntangledTickers.Robinhood.py

Robinhood entangled-ticker script built from IndicatorForge semantics.
Primary ticker evaluates indicator rules; inverse ticker executes opposite side:
- primary BUY => inverse SELL
- primary SELL => inverse BUY
"""

from __future__ import annotations

import argparse
import json
import sqlite3
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
from app.brokers.robin_stocks_adapter import place_stock_order  # noqa: E402
from app.db import get_broker_connection, read_connection_metadata, read_connection_secrets, set_broker_status  # noqa: E402


_ACCOUNT_NUMBER: Optional[str] = None
stoploss_state: Dict[str, Dict[str, Any]] = {}
CHART_POINTS = 90

TIMEFRAMES: Dict[str, Dict[str, str]] = {
    "5m": {"interval": "5minute", "span": "week"},
    "10m": {"interval": "10minute", "span": "week"},
    "1h": {"interval": "hour", "span": "3month"},
    "1d": {"interval": "day", "span": "year"},
}
HISTORICAL_BOUNDS = "extended"


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


def _inverse_signal(signal: str) -> str:
    side = str(signal or "").strip().upper()
    if side == "BUY":
        return "SELL"
    if side == "SELL":
        return "BUY"
    return "HOLD"


def _fmt(val: Any, digits: int = 4) -> str:
    try:
        if val is None:
            return "—"
        return f"{float(val):.{digits}f}"
    except Exception:
        return "—"


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


def _resolve_account_number() -> Optional[str]:
    global _ACCOUNT_NUMBER
    if _ACCOUNT_NUMBER:
        return _ACCOUNT_NUMBER
    try:
        acct = rh.profiles.load_account_profile()
    except Exception:
        return None
    if isinstance(acct, dict):
        acct_num = acct.get("account_number") or acct.get("rhs_account_number")
        if acct_num:
            _ACCOUNT_NUMBER = str(acct_num)
            return _ACCOUNT_NUMBER
    return None


def safe_sleep(seconds: float) -> None:
    time.sleep(max(0.0, float(seconds)))


def safe_stock_quote(symbol: str, retries: int = 3, backoff: float = 1.5) -> Any:
    for i in range(retries):
        try:
            q = rh.stocks.get_stock_quote_by_symbol(symbol)
            if q:
                return q
        except Exception:
            if i == retries - 1:
                raise
            safe_sleep(backoff * (i + 1))
    return None


def safe_stock_historicals(
    symbol: str,
    interval: str,
    span: str,
    *,
    bounds: Optional[str] = None,
    retries: int = 3,
    backoff: float = 1.5,
) -> Any:
    for i in range(retries):
        try:
            if bounds:
                h = rh.stocks.get_stock_historicals(symbol, interval=interval, span=span, bounds=bounds)
            else:
                h = rh.stocks.get_stock_historicals(symbol, interval=interval, span=span)
            if h:
                return h
        except Exception:
            if i == retries - 1:
                raise
            safe_sleep(backoff * (i + 1))
    return None


def _price_from_quote(quote: dict, *, prefer_extended: bool) -> Optional[float]:
    if not isinstance(quote, dict):
        return None
    last = _safe_float(quote.get("last_trade_price"), default=0.0)
    last_ext = _safe_float(
        quote.get("last_extended_hours_trade_price") or quote.get("extended_hours_market_price"),
        default=0.0,
    )
    if prefer_extended:
        if last_ext > 0:
            return last_ext
        if last > 0:
            return last
    if last > 0:
        return last
    if last_ext > 0:
        return last_ext
    return None


def _merge_historicals(base: List[Dict[str, Any]], extra: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not base:
        return extra
    if not extra:
        return base

    def _has_close(row: Dict[str, Any]) -> bool:
        return row.get("close_price") not in (None, "None", "")

    merged: Dict[str, Dict[str, Any]] = {}
    for row in base:
        if not isinstance(row, dict):
            continue
        key = row.get("begins_at")
        if key:
            merged[str(key)] = row
    for row in extra:
        if not isinstance(row, dict):
            continue
        key = row.get("begins_at")
        if not key:
            continue
        k = str(key)
        if k in merged:
            if _has_close(row) or not _has_close(merged[k]):
                merged[k] = row
        else:
            merged[k] = row
    return [merged[k] for k in sorted(merged.keys())] if merged else base


def _order_success(resp: Any) -> bool:
    if hasattr(resp, "accepted") and hasattr(resp, "submitted"):
        return bool(resp.accepted and resp.submitted and not getattr(resp, "blocked", False))
    if isinstance(resp, dict):
        state = str(resp.get("state") or "").lower()
        if state:
            return state in ("queued", "confirmed", "filled")
        return bool(resp.get("id"))
    return bool(resp)


def reconnect_if_needed(func: Callable[..., Any]) -> Callable[..., Any]:
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        db_path = kwargs.pop("_db_path", None)
        connection_id = kwargs.pop("_connection_id", None)
        try:
            return func(*args, **kwargs)
        except (requests.RequestException, ConnectionError) as e:
            if db_path is not None and connection_id is not None:
                ensure_robinhood_session(str(db_path), int(connection_id))
            safe_sleep(2.0)
            return func(*args, **kwargs)
        except Exception:
            raise

    return wrapper


@reconnect_if_needed
def get_stock_price(symbol: str, *, prefer_extended: bool = False) -> float:
    quote = safe_stock_quote(symbol)
    if not isinstance(quote, dict):
        raise RuntimeError(f"Failed to get quote for {symbol}")
    price = _price_from_quote(quote, prefer_extended=prefer_extended)
    if price is None:
        raise RuntimeError(f"Failed to get price for {symbol}")
    return float(price)


@reconnect_if_needed
def get_stock_historicals(symbol: str, interval: str, span: str) -> List[Dict[str, Any]]:
    data = safe_stock_historicals(symbol, interval=interval, span=span, bounds="regular")
    if not isinstance(data, list):
        raise RuntimeError(f"Failed to get historicals for {symbol}")
    try:
        extra = safe_stock_historicals(
            symbol, interval=interval, span="day", bounds=HISTORICAL_BOUNDS, retries=1, backoff=0.5
        )
        if isinstance(extra, list) and extra:
            data = _merge_historicals(data, extra)
    except Exception:
        pass
    return data


@reconnect_if_needed
def get_open_stock_positions() -> List[Dict[str, Any]]:
    account_number = _resolve_account_number()
    if not account_number:
        raise RuntimeError("Robinhood account_number missing; cannot fetch positions.")
    positions = rh.account.get_open_stock_positions(account_number=account_number)
    return positions if isinstance(positions, list) else []


@reconnect_if_needed
def get_portfolio_value() -> float:
    portfolio_data = rh.profiles.load_portfolio_profile()
    return float(portfolio_data["equity"])


@reconnect_if_needed
def get_buying_power() -> float:
    acct = rh.profiles.load_account_profile()
    if isinstance(acct, dict):
        for key in ("buying_power", "cash_available_for_withdrawal", "cash"):
            val = acct.get(key)
            if val is not None:
                return float(val)
    return 0.0


@reconnect_if_needed
def get_available_cash() -> float:
    acct = rh.profiles.load_account_profile()
    if isinstance(acct, dict):
        for key in ("cash", "cash_available_for_withdrawal", "buying_power"):
            val = acct.get(key)
            if val is not None:
                return float(val)
    return 0.0


@reconnect_if_needed
def get_open_stock_orders() -> List[Dict[str, Any]]:
    account_number = _resolve_account_number()
    if not account_number:
        raise RuntimeError("Robinhood account_number missing; cannot fetch open orders.")
    try:
        orders = rh.orders.get_all_open_stock_orders(account_number=account_number)
    except TypeError:
        # Backwards compatibility with older robin_stocks signatures.
        orders = rh.orders.get_all_open_stock_orders()
    return orders if isinstance(orders, list) else []


def _order_remaining_qty(order: Dict[str, Any]) -> float:
    rem = _to_float_opt(order.get("remaining_quantity"))
    if rem is not None:
        return max(0.0, float(rem))
    qty = _to_float_opt(order.get("quantity")) or 0.0
    filled = _to_float_opt(order.get("cumulative_quantity"))
    if filled is None:
        filled = _to_float_opt(order.get("executed_quantity"))
    if filled is None:
        filled = _to_float_opt(order.get("filled_quantity"))
    remaining = float(qty) - float(filled or 0.0)
    return max(0.0, remaining)


def _order_symbol(order: Dict[str, Any]) -> str:
    symbol = str(order.get("symbol") or "").strip().upper()
    if symbol:
        return symbol
    inst_url = order.get("instrument") or order.get("instrument_url")
    if isinstance(inst_url, str) and inst_url:
        try:
            symbol = str(rh.stocks.get_symbol_by_url(inst_url) or "").strip().upper()
        except Exception:
            symbol = ""
    return symbol


def _open_buy_order_allocation_maps(
    *,
    _db_path: Optional[str] = None,
    _connection_id: Optional[int] = None,
) -> Tuple[Dict[str, float], Dict[str, float], Dict[str, int]]:
    """
    Returns:
      (priced_notional_by_symbol, qty_without_price_by_symbol, open_order_count_by_symbol)
    """
    priced_notional: Dict[str, float] = {}
    qty_without_price: Dict[str, float] = {}
    order_count: Dict[str, int] = {}
    orders = get_open_stock_orders(_db_path=_db_path, _connection_id=_connection_id)
    for order in orders:
        if not isinstance(order, dict):
            continue
        side = str(order.get("side") or order.get("direction") or "").strip().lower()
        if side != "buy":
            continue
        remaining_qty = _order_remaining_qty(order)
        if remaining_qty <= 0:
            continue
        symbol = _order_symbol(order)
        if not symbol:
            continue
        order_count[symbol] = int(order_count.get(symbol, 0)) + 1
        limit_price = _to_float_opt(order.get("price"))
        if limit_price is None or limit_price <= 0:
            limit_price = _to_float_opt(order.get("stop_price") or order.get("stopPrice"))
        if limit_price is not None and limit_price > 0:
            priced_notional[symbol] = float(priced_notional.get(symbol, 0.0)) + (remaining_qty * float(limit_price))
        else:
            qty_without_price[symbol] = float(qty_without_price.get(symbol, 0.0)) + remaining_qty
    return priced_notional, qty_without_price, order_count


def build_positions_map(positions: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}
    for pos in positions:
        if not isinstance(pos, dict):
            continue
        inst_url = pos.get("instrument")
        if not inst_url:
            continue
        try:
            inst = rh.account.get_instrument_by_url(inst_url)
        except Exception:
            inst = {}
        symbol = (inst or {}).get("symbol") or pos.get("symbol")
        if not symbol:
            continue
        sym = str(symbol).strip().upper()
        out[sym] = {
            "quantity": _safe_float(pos.get("quantity")),
            "average_buy_price": _safe_float(pos.get("average_buy_price")),
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
    if len(prices) < period + 3:
        return None
    r0 = _rsi(prices, period)
    r2 = _rsi(prices[:-2], period)
    if r0 is None or r2 is None:
        return None
    return float((r0 - r2) / 2.0)


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
        "cloud_breakout_bullish",
        "cloud_breakout_bearish",
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

    tenkan_now = _midpoint(tenkan_len, curr_idx)
    kijun_now = _midpoint(kijun_len, curr_idx)
    if tenkan_now is None or kijun_now is None:
        return None
    tenkan_prev = _midpoint(tenkan_len, prev_idx) if prev_idx >= 0 else None
    kijun_prev = _midpoint(kijun_len, prev_idx) if prev_idx >= 0 else None

    span_a = _span_a_raw(cloud_idx) if cloud_idx >= 0 else None
    span_b = _span_b_raw(cloud_idx) if cloud_idx >= 0 else None
    future_span_a = _span_a_raw(curr_idx)
    future_span_b = _span_b_raw(curr_idx)
    if span_a is None or span_b is None or future_span_a is None or future_span_b is None:
        return None
    prev_span_a = _span_a_raw(prev_cloud_idx) if (prev_cloud_idx is not None and prev_cloud_idx >= 0) else None
    prev_span_b = _span_b_raw(prev_cloud_idx) if (prev_cloud_idx is not None and prev_cloud_idx >= 0) else None

    close_now = float(close_vals[curr_idx])
    close_prev = float(close_vals[prev_idx]) if prev_idx >= 0 else None
    cloud_top = max(float(span_a), float(span_b))
    cloud_bottom = min(float(span_a), float(span_b))
    cloud_top_prev = max(float(prev_span_a), float(prev_span_b)) if (prev_span_a is not None and prev_span_b is not None) else None
    cloud_bottom_prev = min(float(prev_span_a), float(prev_span_b)) if (prev_span_a is not None and prev_span_b is not None) else None
    cloud_mid = (float(span_a) + float(span_b)) / 2.0
    cloud_thickness_pct = (abs(float(span_a) - float(span_b)) / max(abs(cloud_mid), 1.0e-9)) * 100.0
    chikou_ref_idx = curr_idx - disp
    chikou_ref_price = float(close_vals[chikou_ref_idx]) if chikou_ref_idx >= 0 else None

    return {
        "close_now": float(close_now),
        "close_prev": float(close_prev) if close_prev is not None else None,
        "tenkan": float(tenkan_now),
        "tenkan_prev": float(tenkan_prev) if tenkan_prev is not None else None,
        "kijun": float(kijun_now),
        "kijun_prev": float(kijun_prev) if kijun_prev is not None else None,
        "span_a": float(span_a),
        "span_b": float(span_b),
        "future_span_a": float(future_span_a),
        "future_span_b": float(future_span_b),
        "cloud_top": float(cloud_top),
        "cloud_bottom": float(cloud_bottom),
        "cloud_top_prev": float(cloud_top_prev) if cloud_top_prev is not None else None,
        "cloud_bottom_prev": float(cloud_bottom_prev) if cloud_bottom_prev is not None else None,
        "cloud_thickness_pct": float(cloud_thickness_pct),
        "chikou_ref_price": float(chikou_ref_price) if chikou_ref_price is not None else None,
    }


def _ichimoku_condition_hit(
    cond: str,
    *,
    state: Dict[str, Any],
    cloud_thickness_threshold_pct: float,
    kijun_bounce_tolerance_pct: float,
) -> bool:
    c = _normalize_ichi_condition(cond, default="hold")
    if c == "hold":
        return False

    close_now = float(state.get("close_now") or 0.0)
    close_prev = _to_float_opt(state.get("close_prev"))
    tenkan = float(state.get("tenkan") or 0.0)
    tenkan_prev = _to_float_opt(state.get("tenkan_prev"))
    kijun = float(state.get("kijun") or 0.0)
    kijun_prev = _to_float_opt(state.get("kijun_prev"))
    cloud_top = float(state.get("cloud_top") or 0.0)
    cloud_bottom = float(state.get("cloud_bottom") or 0.0)
    cloud_top_prev = _to_float_opt(state.get("cloud_top_prev"))
    cloud_bottom_prev = _to_float_opt(state.get("cloud_bottom_prev"))
    span_a = float(state.get("span_a") or 0.0)
    span_b = float(state.get("span_b") or 0.0)
    future_span_a = float(state.get("future_span_a") or 0.0)
    future_span_b = float(state.get("future_span_b") or 0.0)
    cloud_thickness_pct = float(state.get("cloud_thickness_pct") or 0.0)
    chikou_ref_price = _to_float_opt(state.get("chikou_ref_price"))

    inside_cloud = float(cloud_bottom) <= float(close_now) <= float(cloud_top)
    price_above_cloud = float(close_now) > float(cloud_top)
    price_below_cloud = float(close_now) < float(cloud_bottom)
    cloud_bullish = float(span_a) > float(span_b)
    cloud_bearish = float(span_a) < float(span_b)
    tenkan_above = float(tenkan) > float(kijun)
    tenkan_below = float(tenkan) < float(kijun)
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


def _eval_rule(rule: Dict[str, Any], closes: List[float], price: float) -> Dict[str, Any]:
    kind_raw = str(rule.get("kind") or "").strip().lower()
    if kind_raw in ("bollinger", "bollinger_bands"):
        kind = "bb"
    elif kind_raw in ("ichimoku", "ichimoku_cloud", "ichi"):
        kind = "ichimoku"
    elif kind_raw in ("ttm", "ttm_squeeze", "squeeze_momentum"):
        kind = "ttm"
    elif kind_raw in ("roc", "rate_of_change"):
        kind = "roc"
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
        buy_cond = _normalize_ichi_condition(params.get("buy_condition"), default="hold")
        sell_cond = _normalize_ichi_condition(params.get("sell_condition"), default="hold")
        buy_ignored = buy_cond == "hold"
        sell_ignored = sell_cond == "hold"
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

        buy_ok = True if buy_ignored else _ichimoku_condition_hit(
            buy_cond,
            state=state,
            cloud_thickness_threshold_pct=cloud_thickness_threshold_pct,
            kijun_bounce_tolerance_pct=kijun_bounce_tolerance_pct,
        )
        sell_ok = True if sell_ignored else _ichimoku_condition_hit(
            sell_cond,
            state=state,
            cloud_thickness_threshold_pct=cloud_thickness_threshold_pct,
            kijun_bounce_tolerance_pct=kijun_bounce_tolerance_pct,
        )
        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = bool(buy_ignored)
        out["sell_ignored"] = bool(sell_ignored)
        out["value"] = (
            f"ICHI Conversion={_fmt(state.get('tenkan'),3)} Base={_fmt(state.get('kijun'),3)} "
            f"LiveCloudTop={_fmt(state.get('cloud_top'),3)} LiveCloudBottom={_fmt(state.get('cloud_bottom'),3)} "
            f"ProjectedA={_fmt(state.get('future_span_a'),3)} ProjectedB={_fmt(state.get('future_span_b'),3)} "
            f"Thickness={_fmt(state.get('cloud_thickness_pct'),3)}%"
        )
        out["detail"] = (
            f"buy={buy_cond} sell={sell_cond} "
            f"thick_thr={_fmt(cloud_thickness_threshold_pct,3)}% "
            f"base_tol={_fmt(kijun_bounce_tolerance_pct,3)}% "
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

    out["detail"] = "unsupported rule kind"
    return out


def _build_chart_series(prices: List[float], max_points: int = CHART_POINTS) -> Dict[str, List[Optional[float]]]:
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
    if max_points > 0 and len(prices) > max_points:
        o = len(prices) - max_points
        return {
            "price": [float(p) for p in prices[-max_points:]],
            "ma20": ma20[o:],
            "ma78": ma78[o:],
            "ma150": ma190[o:],
        }
    return {"price": [float(p) for p in prices], "ma20": ma20, "ma78": ma78, "ma150": ma190}


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


def _round_to_cents(value: float) -> float:
    return max(0.01, round(float(value), 2))


def _can_sell_without_loss(current_price: float, avg_buy_price: float) -> bool:
    if current_price <= 0 or avg_buy_price <= 0:
        return False
    return float(current_price) >= float(avg_buy_price)


def _record_trade(stats: Dict[str, Any], *, side: str, qty: float, price: float, avg_buy_price: float) -> None:
    stats["trades"] = int(stats.get("trades", 0)) + 1
    if side == "sell" and avg_buy_price > 0 and qty > 0:
        profit = (price - avg_buy_price) * qty
        if profit > 0:
            stats["pnl"] = float(stats.get("pnl", 0.0)) + profit


def _load_rules_from_db(db_path: str) -> List[Dict[str, Any]]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    try:
        cur.execute("SELECT id, name, kind, params_json, enabled FROM markets_indicator_rules ORDER BY id ASC")
        rows = cur.fetchall()
    except Exception:
        conn.close()
        return []
    conn.close()
    out: List[Dict[str, Any]] = []
    for r in rows:
        if int(r["enabled"] or 0) != 1:
            continue
        try:
            params = json.loads(str(r["params_json"] or "{}"))
        except Exception:
            params = {}
        out.append(
            {
                "id": int(r["id"]),
                "name": str(r["name"] or ""),
                "kind": str(r["kind"] or "").lower(),
                "params": params if isinstance(params, dict) else {},
            }
        )
    return out


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
        else:
            kind = kind_raw
        if kind not in ("ma", "ema", "rsi", "rsi_d", "macd", "bb", "ichimoku", "ttm", "roc"):
            continue
        params = item.get("params") if isinstance(item.get("params"), dict) else {}
        out.append(
            {
                "name": str(item.get("name") or kind.upper()),
                "kind": kind,
                "params": params,
            }
        )
    return out


def _resolve_rules(db_path: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
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
    need_rsi = False
    need_drsi = False

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
        15 if need_rsi else 0,
        17 if need_drsi else 0,
    )


def check_stoploss_and_sell(
    symbol: str,
    current_price: float,
    avg_buy_price: float,
    held_qty: float,
    target_gain_pct: float,
    stop_loss_pct: float,
    *,
    trade_stats: Optional[Dict[str, Any]] = None,
) -> None:
    if avg_buy_price <= 0:
        return

    percentage_gain = ((current_price - avg_buy_price) / avg_buy_price) * 100.0
    if symbol not in stoploss_state:
        stoploss_state[symbol] = {"armed": False}

    if not stoploss_state[symbol]["armed"] and percentage_gain >= target_gain_pct:
        stoploss_state[symbol]["armed"] = True
        print(f"Stop-loss armed for {symbol} at gain {percentage_gain:.2f}%.")

    if stoploss_state[symbol]["armed"]:
        trigger_price = avg_buy_price * (1.0 + (stop_loss_pct / 100.0))
        if current_price <= trigger_price:
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
                resp = place_stock_order(
                    symbol=symbol,
                    side="sell",
                    order_type="market",
                    quantity=sell_qty,
                    timeInForce="gfd",
                )
                if _order_success(resp):
                    print(f"Stop-loss SELL executed for {symbol}: {sell_qty} shares.")
                    if trade_stats is not None:
                        _record_trade(
                            trade_stats, side="sell", qty=float(sell_qty), price=current_price, avg_buy_price=avg_buy_price
                        )
                    stoploss_state[symbol]["armed"] = False
            except Exception as e:
                print(f"[{symbol}] Stop-loss sell failed: {e}")


def _print_rule_checks(symbol: str, checks: List[Dict[str, Any]], signal: str) -> None:
    print(f"[{symbol}] Signal: {signal}")
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
    shares_per_trade: int,
    trailing_stop_amount: float,
    trailing_stop_mode: str,
    trailing_stop_atr_mult: float,
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
    primary_symbol: str,
    inverse_symbol: str,
    trade_stats: Optional[Dict[str, Any]] = None,
    status_writer: Optional[Any] = None,
) -> None:
    if timeframe not in TIMEFRAMES:
        raise ValueError(f"Invalid timeframe '{timeframe}'. Choose from {list(TIMEFRAMES.keys())}.")
    interval = TIMEFRAMES[timeframe]["interval"]
    span = TIMEFRAMES[timeframe]["span"]
    print(f"Using timeframe: {timeframe} ({interval}, {span})")
    print(f"Entangled pair: primary={primary_symbol}, inverse={inverse_symbol}")
    print(f"Rules loaded: {len(rules)}")
    rule_min_candles = _rule_min_candles(rules)
    print(f"Rule candle requirement: {rule_min_candles}")

    while True:
        tickers_status: List[Dict[str, Any]] = []
        primary_loop_signal = "HOLD"
        try:
            positions = get_open_stock_positions(_db_path=db_path, _connection_id=connection_id)
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
            print(f"[WARN] Buying power unavailable; buy power gate will not be enforced: {e}")
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
                print(f"[WARN] Portfolio cap enabled but portfolio value unavailable: {e}")
                portfolio_value = None
                available_cash = None
                cap_pct = None
                cash_target_value = None
                cash_pct = None
        open_order_notional_by_symbol: Dict[str, float] = {}
        open_order_qty_by_symbol: Dict[str, float] = {}
        open_order_count_by_symbol: Dict[str, int] = {}
        if portfolio_cap_rule_enabled:
            try:
                (
                    open_order_notional_by_symbol,
                    open_order_qty_by_symbol,
                    open_order_count_by_symbol,
                ) = _open_buy_order_allocation_maps(
                    _db_path=db_path, _connection_id=connection_id
                )
            except Exception as e:
                print(f"[WARN] Portfolio cap enabled but open-order fetch failed: {e}")

        for symbol in symbols:
            try:
                current_price = get_stock_price(symbol, _db_path=db_path, _connection_id=connection_id)
                hist = get_stock_historicals(symbol, interval, span, _db_path=db_path, _connection_id=connection_id)
                closes: List[float] = []
                for row in hist:
                    try:
                        closes.append(float(row.get("close_price")))
                    except Exception:
                        continue
                if len(closes) < rule_min_candles:
                    print(f"[{symbol}] Not enough historical candles.")
                    tickers_status.append({"symbol": symbol, "signal": "NO_DATA"})
                    continue
                closes.append(float(current_price))

                pos_info = positions_map.get(symbol, {})
                pos_qty = _safe_float(pos_info.get("quantity"))
                avg_buy_price = _safe_float(pos_info.get("average_buy_price"))
                open_order_qty = float(open_order_qty_by_symbol.get(symbol, 0.0))
                open_order_value = float(open_order_notional_by_symbol.get(symbol, 0.0))
                open_order_count = int(open_order_count_by_symbol.get(symbol, 0))
                if open_order_qty > 0:
                    open_order_value += open_order_qty * float(current_price)
                if portfolio_cap_rule_enabled and open_order_count > 0:
                    print(f"[{symbol}] Open BUY orders: count={open_order_count} total=${open_order_value:.2f}")
                held_pct: Optional[float] = None
                cap_delta_pct: Optional[float] = None
                buy_order_price = float(current_price)
                buy_order_cost = float(shares_per_trade) * buy_order_price
                symbol_cap_pct = float(portfolio_cap_percent_by_symbol.get(str(symbol).strip().upper(), portfolio_cap_percent))
                if symbol_cap_pct <= 0:
                    symbol_cap_pct = portfolio_cap_percent
                row_cap_pct = max(0.01, float(symbol_cap_pct)) if portfolio_cap_mode == "percent" else cap_pct
                if row_cap_pct is not None and portfolio_value is not None and portfolio_value > 0:
                    held_value = (float(pos_qty) * float(current_price)) + open_order_value
                    held_pct = (held_value / float(portfolio_value)) * 100.0
                    cap_delta_pct = held_pct - row_cap_pct

                checks: List[Dict[str, Any]] = []
                for r in rules:
                    c = _eval_rule(r, closes, float(current_price))
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
                            checks.append(child)
                        continue
                    c["name"] = base_name
                    c["_rule_kind"] = base_kind
                    c["_rule_params"] = rule_params
                    c["_rule_id"] = base_rule_id
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
                        ticker_value = (float(pos_qty) * float(current_price)) + open_order_value
                        if portfolio_cap_mode == "percent":
                            ticker_cap_pct = max(0.01, float(symbol_cap_pct))
                            ticker_cap_value = float(portfolio_value) * (ticker_cap_pct / 100.0)
                            if ticker_value > ticker_cap_value:
                                buy_cap_blocked = True
                                print(
                                    f"[{symbol}] BUY blocked by portfolio percent cap: current holdings + open buys "
                                    f"${ticker_value:.2f} exceed {ticker_cap_pct:.2f}% "
                                    f"of portfolio (${ticker_cap_value:.2f})."
                                )
                        elif ticker_value > (float(portfolio_value) / float(divisor)):
                            buy_cap_blocked = True
                            print(
                                f"[{symbol}] BUY blocked by portfolio cap: holdings + open buys ${ticker_value:.2f} exceed "
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
                if rule_consensus_signal == "SELL" and can_sell_now:
                    signal = "SELL"
                elif rule_consensus_signal == "BUY" and not (buy_cap_blocked or buy_power_blocked or cash_slice_blocked):
                    signal = "BUY"
                else:
                    signal = "HOLD"
                if symbol == primary_symbol:
                    primary_loop_signal = signal
                elif symbol == inverse_symbol:
                    desired_signal = _inverse_signal(primary_loop_signal)
                    signal = desired_signal
                    if signal == "BUY" and (buy_cap_blocked or buy_power_blocked or cash_slice_blocked):
                        print(f"[{symbol}] Inverse BUY blocked by portfolio cap / funds gate.")
                        signal = "HOLD"
                    elif signal == "SELL" and not can_sell_now:
                        print(f"[{symbol}] Inverse SELL blocked (position/no-loss constraint).")
                        signal = "HOLD"
                    print(f"[{symbol}] Inverse action from primary {primary_symbol}={primary_loop_signal} -> {signal}")
                _print_rule_checks(symbol, checks, signal)

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
                        "entangled_mode_enabled": True,
                        "entangled_role": ("primary" if symbol == primary_symbol else ("inverse" if symbol == inverse_symbol else "")),
                        "entangled_primary_symbol": primary_symbol,
                        "entangled_inverse_symbol": inverse_symbol,
                        "entangled_primary_signal": primary_loop_signal if symbol == inverse_symbol else "",
                        "ma20": ma20,
                        "ma78": ma78,
                        "ma150": ma190,
                        "rsi": rsi,
                        "rsi_d": drsi,
                        "atr": atr,
                        "chart": _build_chart_series(closes) if status_writer is not None else {},
                        "rule_summary": [
                            {
                                "name": str(c.get("name") or ""),
                                "buy_ok": bool(c.get("buy_ok")),
                                "sell_ok": bool(c.get("sell_ok")),
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

                if signal == "BUY":
                    print(f"[{symbol}] BUY signal -> buying {shares_per_trade} shares...")
                    try:
                        resp = place_stock_order(
                            symbol=symbol,
                            side="buy",
                            order_type="market",
                            quantity=int(shares_per_trade),
                            timeInForce="gfd",
                        )
                        if resp is not None and trade_stats is not None and _order_success(resp):
                            _record_trade(
                                trade_stats, side="buy", qty=float(shares_per_trade), price=float(current_price), avg_buy_price=0.0
                            )
                    except Exception as e:
                        print(f"[{symbol}] Buy failed: {e}")

                if signal == "SELL" and pos_qty > 0:
                    if not _can_sell_without_loss(float(current_price), avg_buy_price):
                        print(
                            f"[{symbol}] SELL blocked by no-loss rule "
                            f"({float(current_price):.2f} < {avg_buy_price:.2f})."
                        )
                        continue
                    trail_amount = float(trailing_stop_amount)
                    if trailing_stop_mode == "atr":
                        if atr is None or atr <= 0:
                            print(f"[{symbol}] ATR unavailable; skipping trailing stop sell.")
                            continue
                        trail_amount = float(atr) * float(trailing_stop_atr_mult)
                    trail_amount = _round_to_cents(trail_amount)
                    if (float(current_price) - float(trail_amount)) < float(avg_buy_price):
                        print(
                            f"[{symbol}] SELL blocked by no-loss rule: trailing trigger "
                            f"({float(current_price) - float(trail_amount):.2f}) would be below "
                            f"avg buy ({avg_buy_price:.2f})."
                        )
                        continue
                    print(
                        f"[{symbol}] SELL signal -> placing trailing stop sell for {shares_per_trade} shares, "
                        f"trail=${trail_amount:.2f} ({trailing_stop_mode})..."
                    )
                    try:
                        resp = place_stock_order(
                            symbol=symbol,
                            side="sell",
                            order_type="trailing_stop",
                            quantity=int(shares_per_trade),
                            trailAmount=float(trail_amount),
                            trailType="amount",
                            timeInForce="gtc",
                        )
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
                status_writer({"phase": "loop", "timeframe": timeframe, "tickers": tickers_status})
            except Exception:
                pass

        print(f"Sleeping {sleep_duration} seconds...\n")
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
    last_status: Dict[str, Any] = {}
    trade_stats: Dict[str, Any] = {"pnl": 0.0, "trades": 0}

    def write_status(payload: Dict[str, Any]) -> None:
        p = dict(payload)
        p["ts"] = iso_now()
        p["script"] = "EntangledTickers.Robinhood"
        p["pnl"] = round(float(trade_stats.get("pnl", 0.0)), 2)
        p["trades"] = int(trade_stats.get("trades", 0))
        try:
            status_path.write_text(json.dumps(p, indent=2), encoding="utf-8")
            last_status.clear()
            last_status.update(p)
        except Exception:
            pass

    params = load_params(args.params_json)
    primary_symbol = str(
        params.get("primary_symbol")
        or params.get("entangled_primary_symbol")
        or ""
    ).strip().upper()
    inverse_symbol = str(
        params.get("inverse_symbol")
        or params.get("entangled_inverse_symbol")
        or ""
    ).strip().upper()
    if not primary_symbol or not inverse_symbol:
        raise ValueError("EntangledTickers requires params.primary_symbol and params.inverse_symbol.")
    if primary_symbol == inverse_symbol:
        raise ValueError("params.primary_symbol and params.inverse_symbol must be different tickers.")
    symbols = [primary_symbol, inverse_symbol]

    sleep_duration = float(params.get("sleep_duration", 30))
    shares_per_trade = int(params.get("shares_per_trade", 1))
    trailing_stop_amount = float(params.get("trailing_stop_amount", 0.10))
    trailing_stop_mode = str(params.get("trailing_stop_mode", "fixed")).strip().lower()
    if trailing_stop_mode not in ("fixed", "atr"):
        trailing_stop_mode = "fixed"
    trailing_stop_atr_mult = float(params.get("trailing_stop_atr_mult", 3.0))
    if trailing_stop_atr_mult <= 0:
        trailing_stop_atr_mult = 3.0
    target_gain_pct = float(params.get("target_gain_pct", 0.5))
    stop_loss_pct = float(params.get("stop_loss_pct", -0.5))
    stoploss_enabled = bool(params.get("stoploss_enabled", False))
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
    print(f"Loaded {len(rules)} indicator rules.")

    ensure_robinhood_session(args.db_path, int(args.connection_id))
    main_trading_loop(
        db_path=args.db_path,
        connection_id=int(args.connection_id),
        symbols=symbols,
        shares_per_trade=shares_per_trade,
        trailing_stop_amount=trailing_stop_amount,
        trailing_stop_mode=trailing_stop_mode,
        trailing_stop_atr_mult=trailing_stop_atr_mult,
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
        primary_symbol=primary_symbol,
        inverse_symbol=inverse_symbol,
        trade_stats=trade_stats,
        status_writer=write_status,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
