#!/usr/bin/env python3
"""
IndicatorForge.Robinhood.py

Robinhood stock base script that reuses Markets scanner indicator-rule semantics
for BUY/SELL signals, while preserving DreadFox.Stock-style execution controls:
- shares_per_trade
- sleep_duration
- fixed-dollar trailing stop sells
- optional stop-loss arming/trigger logic
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from decimal import ROUND_FLOOR
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

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
    get_10m_stock_historicals as adapter_get_10m_stock_historicals,
    get_stock_historicals as adapter_get_stock_historicals,
    place_stock_order,
)
from app.db import get_broker_connection, read_connection_metadata, read_connection_secrets, set_broker_status  # noqa: E402
from app.indicator_pipeline import apply_final_candle_policy, heikin_ashi_series as shared_heikin_ashi_series, log_indicator_policy  # noqa: E402
try:
    from strategy_forge.pivot_points import calculate_pivot_points, pivot_target_above_price  # noqa: E402
except Exception:  # pragma: no cover
    calculate_pivot_points = None  # type: ignore
    pivot_target_above_price = None  # type: ignore


_ACCOUNT_NUMBER: Optional[str] = None
stoploss_state: Dict[str, Dict[str, Any]] = {}
CHART_POINTS = 90
OVERNIGHT_HISTORY_STATE_VERSION = 1
OVERNIGHT_HISTORY_MAX_ROWS = 1500
OVERNIGHT_QUOTE_STALE_MULTIPLIER = 3
MIN_TRAIL_AMOUNT_USD = 0.01
NON_PDT_DAY_TRADE_LIMIT = 3
MARKET_STATE_TTL = 60
ORDER_TYPE_CHOICES = {"market", "trailing_stop", "limit_midpoint"}
_ET_TZ = ZoneInfo("America/New_York")
_MARKET_STATE_CACHE: Dict[str, Any] = {"ts": 0.0, "state": "closed"}
_INSTRUMENT_ID_CACHE: Dict[str, str] = {}
BONFIRE_LIVE_QUOTE_URL = "https://bonfire.robinhood.com/instruments/{instrument_id}/detail-page-live-updating-data/"
BONFIRE_LIVE_QUOTE_HEADERS = {
    "accept": "*/*",
    "origin": "https://robinhood.com",
    "referer": "https://robinhood.com/",
    "user-agent": "Mozilla/5.0",
    "x-hyper-ex": "enabled",
}

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


def _normalize_portfolio_cash_source(value: Any) -> str:
    txt = str(value or "buying_power").strip().lower().replace("-", "_").replace(" ", "_")
    if txt in ("cash", "available_cash", "cash_position", "cash_positions"):
        return "cash"
    return "buying_power"


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


def _normalize_order_type(val: Any, default: str = "market") -> str:
    txt = str(val or default or "market").strip().lower()
    if txt in ORDER_TYPE_CHOICES:
        return txt
    return default if default in ORDER_TYPE_CHOICES else "market"


def _fmt(val: Any, digits: int = 4) -> str:
    try:
        if val is None:
            return "—"
        return f"{float(val):.{digits}f}"
    except Exception:
        return "—"


def _mid_price(bid: float, ask: float, fallback: float) -> float:
    if bid > 0 and ask > 0 and ask >= bid:
        return (float(bid) + float(ask)) / 2.0
    return float(fallback)


def _parse_iso_ts(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _aware_utc_dt(value: Optional[datetime]) -> Optional[datetime]:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _et_session_parts(now_dt: Optional[datetime] = None) -> Tuple[datetime, int, int]:
    now = _aware_utc_dt(now_dt or datetime.now(timezone.utc)) or datetime.now(timezone.utc)
    now_et = now.astimezone(_ET_TZ)
    minute = int(now_et.hour) * 60 + int(now_et.minute)
    weekday = int(now_et.weekday())
    return now_et, minute, weekday


def _common_extended_hours_label(now_dt: Optional[datetime] = None) -> Optional[str]:
    _, minute, weekday = _et_session_parts(now_dt)
    if weekday >= 5:
        return None
    if (4 * 60) <= minute < (9 * 60 + 30):
        return "premarket"
    if (16 * 60) <= minute < (20 * 60):
        return "after_hours"
    return None


def _common_equity_market_state(now_dt: Optional[datetime] = None) -> str:
    _, minute, weekday = _et_session_parts(now_dt)
    if weekday >= 5:
        return "closed"
    if (9 * 60 + 30) <= minute < (16 * 60):
        return "regular"
    return _common_extended_hours_label(now_dt) or "closed"


def _extended_label_for_regular_session(
    now_dt: datetime,
    regular_open: Optional[datetime],
    regular_close: Optional[datetime],
) -> Optional[str]:
    now = _aware_utc_dt(now_dt)
    open_dt = _aware_utc_dt(regular_open)
    close_dt = _aware_utc_dt(regular_close)
    if not (now and open_dt and close_dt):
        return None

    now_et = now.astimezone(_ET_TZ)
    open_et = open_dt.astimezone(_ET_TZ)
    close_et = close_dt.astimezone(_ET_TZ)
    if now_et.date() != open_et.date() or now_et.date() != close_et.date() or now_et.weekday() >= 5:
        return None

    minute = int(now_et.hour) * 60 + int(now_et.minute)
    open_minute = int(open_et.hour) * 60 + int(open_et.minute)
    close_minute = int(close_et.hour) * 60 + int(close_et.minute)
    if (4 * 60) <= minute < open_minute:
        return "premarket"
    if close_minute < minute < (20 * 60):
        return "after_hours"
    return None


def _classify_market_hours(hours: Any, now_dt: datetime) -> str:
    if not isinstance(hours, dict):
        return "closed"

    now = _aware_utc_dt(now_dt) or now_dt
    regular_open = _aware_utc_dt(_parse_iso_ts(hours.get("opens_at")))
    regular_close = _aware_utc_dt(_parse_iso_ts(hours.get("closes_at")))
    extended_open = _aware_utc_dt(_parse_iso_ts(hours.get("extended_opens_at")))
    extended_close = _aware_utc_dt(_parse_iso_ts(hours.get("extended_closes_at")))

    if regular_open and regular_close and regular_open <= now <= regular_close:
        return "regular"
    if extended_open and extended_close and extended_open <= now <= extended_close:
        if regular_open and now < regular_open:
            return "premarket"
        if regular_close and now > regular_close:
            return "after_hours"
        return "extended"

    if not (extended_open and extended_close):
        fallback_label = _extended_label_for_regular_session(now, regular_open, regular_close)
        if fallback_label:
            return fallback_label
    return "closed"


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


def _instrument_id_from_quote(quote: Any) -> Optional[str]:
    if not isinstance(quote, dict):
        return None
    instrument_id = quote.get("instrument_id")
    if instrument_id:
        return str(instrument_id).strip()
    instrument_url = str(quote.get("instrument") or "").strip().rstrip("/")
    if instrument_url:
        candidate = instrument_url.split("/")[-1].strip()
        if candidate:
            return candidate
    return None


def _resolve_instrument_id(symbol: str, base_quote: Any = None) -> Optional[str]:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return None
    cached = _INSTRUMENT_ID_CACHE.get(sym)
    if cached:
        return cached

    instrument_id = _instrument_id_from_quote(base_quote)
    if not instrument_id:
        try:
            instruments = rh.stocks.get_instruments_by_symbols(sym)
            if isinstance(instruments, list) and instruments:
                first = instruments[0]
                if isinstance(first, dict):
                    instrument_id = str(first.get("id") or "").strip() or None
        except Exception:
            instrument_id = None

    if instrument_id:
        _INSTRUMENT_ID_CACHE[sym] = instrument_id
    return instrument_id


def _extract_live_quote(payload: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return None
    chart_section = payload.get("chart_section")
    quote = chart_section.get("quote") if isinstance(chart_section, dict) else None
    if not isinstance(quote, dict):
        quote = payload.get("quote")
    return dict(quote) if isinstance(quote, dict) else None


def _merge_quote_data(base_quote: Any, live_quote: Any, *, source: str) -> Dict[str, Any]:
    merged: Dict[str, Any] = dict(base_quote) if isinstance(base_quote, dict) else {}
    if isinstance(live_quote, dict):
        for key, value in live_quote.items():
            if value not in (None, ""):
                merged[key] = value
    merged["_quote_source"] = source
    merged["_quote_updated_at"] = str(
        merged.get("updated_at")
        or merged.get("venue_last_non_reg_trade_time")
        or merged.get("venue_last_trade_time")
        or ""
    )
    return merged


def safe_live_overnight_quote(
    symbol: str,
    *,
    base_quote: Any = None,
    retries: int = 2,
    backoff: float = 0.5,
) -> Optional[Dict[str, Any]]:
    instrument_id = _resolve_instrument_id(symbol, base_quote)
    if not instrument_id:
        return None
    url = BONFIRE_LIVE_QUOTE_URL.format(instrument_id=instrument_id)
    params = {"display_span": "day", "hide_extended_hours": "false"}
    for i in range(retries):
        try:
            response = requests.get(
                url,
                headers=BONFIRE_LIVE_QUOTE_HEADERS,
                params=params,
                timeout=8,
            )
            response.raise_for_status()
            quote = _extract_live_quote(response.json())
            if quote:
                merged = _merge_quote_data(base_quote, quote, source="robinhood_bonfire_live")
                merged["symbol"] = str(symbol or merged.get("symbol") or "").strip().upper()
                merged["instrument_id"] = instrument_id
                return merged
        except Exception:
            if i == retries - 1:
                return None
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
                h = adapter_get_stock_historicals(symbol, interval=interval, span=span, bounds=bounds)
            else:
                h = adapter_get_stock_historicals(symbol, interval=interval, span=span)
            if h:
                return h
        except Exception:
            if i == retries - 1:
                raise
            safe_sleep(backoff * (i + 1))
    return None


def get_quotes_map(symbols: List[str], *, prefer_live_overnight: bool = False) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for symbol in symbols:
        sym = str(symbol or "").strip().upper()
        if not sym:
            continue
        try:
            q = safe_stock_quote(sym, retries=2, backoff=0.5)
            if bool(prefer_live_overnight):
                live_q = safe_live_overnight_quote(sym, base_quote=q, retries=1, backoff=0.25)
                if live_q:
                    q = live_q
            if isinstance(q, dict):
                out[sym] = q
        except Exception:
            continue
    return out


def _get_quote_for_symbol(quotes: Dict[str, Any], symbol: str) -> Dict[str, Any]:
    sym = str(symbol or "").strip().upper()
    q = quotes.get(sym) if isinstance(quotes, dict) else None
    return q if isinstance(q, dict) else {}


def _price_from_quote(quote: dict, *, prefer_extended: bool) -> Optional[float]:
    if not isinstance(quote, dict):
        return None
    last = _safe_float(quote.get("last_trade_price"), default=0.0)
    last_ext = _safe_float(
        quote.get("last_non_reg_trade_price")
        or quote.get("last_extended_hours_trade_price")
        or quote.get("extended_hours_market_price"),
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


def _quote_bid_ask(quote: Dict[str, Any]) -> Tuple[float, float]:
    bid = _safe_float(
        quote.get("bid_price")
        or quote.get("bidPrice")
        or quote.get("bid"),
        default=0.0,
    )
    ask = _safe_float(
        quote.get("ask_price")
        or quote.get("askPrice")
        or quote.get("ask"),
        default=0.0,
    )
    return bid, ask


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


def _historical_row_ts(row: Dict[str, Any]) -> str:
    return str(row.get("begins_at") or row.get("beginsAt") or row.get("time") or "")


def _historical_session_counts(rows: List[Dict[str, Any]]) -> Tuple[int, int]:
    pre = 0
    post = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        sess = str(row.get("session") or "").strip().lower()
        if sess in ("pre", "premarket", "pre_market"):
            pre += 1
        elif sess in ("post", "afterhours", "after_hours", "postmarket"):
            post += 1
    return pre, post


def _log_historical_candles(
    *,
    symbol: str,
    timeframe: str,
    extended_enabled: bool,
    requested_bounds: str,
    rows: List[Dict[str, Any]],
    chart_count: int,
    indicator_count: int,
    synthetically_modified: bool,
) -> None:
    pre_count, post_count = _historical_session_counts(rows)
    first_ts = _historical_row_ts(rows[0]) if rows else ""
    latest_ts = _historical_row_ts(rows[-1]) if rows else ""
    print(
        f"[{symbol}] Historical candles timeframe={timeframe} extended_hours_enabled={bool(extended_enabled)} "
        f"requested_bounds={requested_bounds} raw_candle_count={len(rows)} chart_candle_count={chart_count} "
        f"indicator_candle_count={indicator_count} first_ts={first_ts} latest_ts={latest_ts} "
        f"premarket_candles={pre_count} after_hours_candles={post_count} "
        f"source=robin_stocks synthetically_modified={bool(synthetically_modified)}"
    )
    if (
        _should_note_missing_extended_candles(
            timeframe=timeframe,
            extended_enabled=extended_enabled,
            requested_bounds=requested_bounds,
        )
        and pre_count == 0
        and post_count == 0
    ):
        print(
            f"[{symbol}] Extended-hours candles not returned; using regular-session candles "
            f"timeframe={timeframe} requested_bounds={requested_bounds} raw_candle_count={len(rows)}"
        )


def _should_note_missing_extended_candles(
    *,
    timeframe: str,
    extended_enabled: bool,
    requested_bounds: str,
) -> bool:
    if not bool(extended_enabled):
        return False
    requested = str(requested_bounds or "").strip().lower()
    if requested in ("", "regular", "24_7", "synthetic_from_non_robinhood_closes"):
        return False
    cfg = TIMEFRAMES.get(str(timeframe or "").strip().lower()) or {}
    interval = str(cfg.get("interval") or timeframe or "").strip().lower()
    if interval in ("day", "week"):
        return False
    return True


def _order_success(resp: Any) -> bool:
    if hasattr(resp, "accepted") and hasattr(resp, "submitted"):
        return bool(resp.accepted and resp.submitted and not getattr(resp, "blocked", False))
    if isinstance(resp, dict):
        state = str(resp.get("state") or "").lower()
        if state:
            return state in ("queued", "confirmed", "filled")
        return bool(resp.get("id"))
    return bool(resp)


def _order_failure_reason(resp: Any) -> str:
    if resp is None:
        return "no response"
    if isinstance(resp, dict):
        for key in ("detail", "message", "error", "reject_reason", "reason", "state"):
            value = resp.get(key)
            if value:
                return str(value)
        return str(resp)
    return str(resp)


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
def get_stock_price(symbol: str, *, prefer_extended: bool = False, prefer_live_overnight: bool = False) -> float:
    quote = safe_stock_quote(symbol)
    if bool(prefer_live_overnight):
        quote = safe_live_overnight_quote(symbol, base_quote=quote, retries=1, backoff=0.25) or quote
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


def _market_session_state() -> str:
    now = time.time()
    if now - float(_MARKET_STATE_CACHE.get("ts", 0)) < MARKET_STATE_TTL:
        return str(_MARKET_STATE_CACHE.get("state", "closed"))

    state = "closed"
    try:
        hours = rh.markets.get_market_today_hours("XNYS")
        now_dt = datetime.now(timezone.utc)
        state = _classify_market_hours(hours, now_dt)
    except Exception:
        # Fallback to common US equity hours when Robinhood market-hours is unavailable.
        try:
            state = _common_equity_market_state(datetime.now(timezone.utc))
        except Exception:
            state = "closed"

    _MARKET_STATE_CACHE["ts"] = now
    _MARKET_STATE_CACHE["state"] = state
    return state


def _execution_state_from_market(
    market_state: str,
    *,
    allow_extended_hours_orders: bool,
    allow_seamless_overnight_orders: bool,
    now_dt: Optional[datetime] = None,
) -> str:
    state = str(market_state or "closed").strip().lower()
    if state == "regular":
        return "regular"
    if _is_overnight_et_window(now_dt):
        return "overnight" if bool(allow_seamless_overnight_orders) else "closed"
    if state in ("extended", "premarket", "after_hours"):
        return "extended"
    if state == "closed" and bool(allow_extended_hours_orders) and _common_extended_hours_label(now_dt):
        return "extended"
    return "closed"


def _is_overnight_et_window(now_dt: Optional[datetime] = None) -> bool:
    now = now_dt or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    now_et = now.astimezone(_ET_TZ)
    minute = (int(now_et.hour) * 60) + int(now_et.minute)
    weekday = int(now_et.weekday())  # Monday=0, Sunday=6.
    # Robinhood's all-day/overnight order route bridges the gap until its
    # extended-hours order session starts at 7 AM ET.
    # Include Sunday evening through Friday morning, but exclude Friday evening
    # and Saturday because the 24h equity session is closed for the weekend.
    if minute >= (20 * 60):
        return weekday in {6, 0, 1, 2, 3}
    if minute < (7 * 60):
        return weekday in {0, 1, 2, 3, 4}
    return False


def _order_extended_hours_for_state(state: str) -> bool:
    return str(state or "").strip().lower() in ("extended", "premarket", "after_hours", "overnight")


def _order_market_hours_for_state(state: str) -> Optional[str]:
    session = str(state or "").strip().lower()
    if session == "overnight":
        return "all_day_hours"
    if session in ("extended", "premarket", "after_hours"):
        return "extended_hours"
    return None


def _fetch_historicals_like_schwab(
    *,
    symbol: str,
    timeframe_key: str,
    include_extended_hours_data: bool,
    min_candles: int,
    _db_path: Optional[str] = None,
    _connection_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    cfg = TIMEFRAMES[timeframe_key]
    interval = cfg["interval"]
    span = cfg["span"]
    # robin_stocks only permits bounds=extended/trading when span="day".
    # Extended-hours daily/weekly candles are not meaningful and can be rejected.
    can_fetch_extended = include_extended_hours_data and interval not in ("day", "week")
    # For longer intraday spans, use regular history plus a day of extended candles.
    bounds = HISTORICAL_BOUNDS if can_fetch_extended and span == "day" else "regular"
    requested_bounds = HISTORICAL_BOUNDS if can_fetch_extended else "regular"
    if timeframe_key == "10m":
        try:
            rows = adapter_get_10m_stock_historicals(
                symbol,
                span=span,
                bounds=bounds,
                min_candles=int(min_candles),
                allow_partial=True,
            )
        except RuntimeError as exc:
            if str(exc) == "INSUFFICIENT_CANDLES_FOR_10M_CALCULATION":
                print(f"[{symbol}] INSUFFICIENT_CANDLES_FOR_10M_CALCULATION")
                return []
            raise
    else:
        rows = safe_stock_historicals(symbol, interval=interval, span=span, bounds=bounds)
    if not isinstance(rows, list):
        rows = []
    if can_fetch_extended:
        try:
            if timeframe_key == "10m":
                extended_day = adapter_get_10m_stock_historicals(
                    symbol,
                    span="day",
                    bounds=HISTORICAL_BOUNDS,
                    min_candles=1,
                    allow_partial=True,
                )
            else:
                extended_day = safe_stock_historicals(
                    symbol,
                    interval=interval,
                    span="day",
                    bounds=HISTORICAL_BOUNDS,
                    retries=1,
                    backoff=0.5,
                )
            if isinstance(extended_day, list) and extended_day:
                rows = _merge_historicals(rows, extended_day)
        except Exception:
            pass
    _log_historical_candles(
        symbol=symbol,
        timeframe=timeframe_key,
        extended_enabled=bool(include_extended_hours_data),
        requested_bounds=requested_bounds,
        rows=rows,
        chart_count=len(rows),
        indicator_count=len(rows),
        synthetically_modified=False,
    )
    return rows


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
        o = row.get("open_price")
        h = row.get("high_price")
        l = row.get("low_price")
        c = row.get("close_price")
        if o is None:
            o = row.get("open")
        if h is None:
            h = row.get("high")
        if l is None:
            l = row.get("low")
        if c is None:
            c = row.get("close")
        try:
            o_val = float(o)
            h_val = float(h)
            l_val = float(l)
            c_val = float(c)
        except Exception:
            continue
        if o_val <= 0 or h_val <= 0 or l_val <= 0 or c_val <= 0:
            continue
        if h_val < l_val:
            continue
        if c_val < l_val or c_val > h_val:
            continue
        if o_val < l_val or o_val > h_val:
            continue
        volume_val = 0.0
        for key in ("volume", "volume_traded", "volume_traded_units", "session_volume", "total_volume"):
            raw_volume = row.get(key)
            parsed_volume = _to_float_opt(raw_volume)
            if parsed_volume is not None:
                parsed_volume_f = float(parsed_volume)
                volume_val = max(0.0, parsed_volume_f) if math.isfinite(parsed_volume_f) else 0.0
                break
        ts_val = (
            row.get("begins_at")
            or row.get("beginsAt")
            or row.get("datetime")
            or row.get("time")
            or row.get("timestamp")
            or ""
        )
        opens.append(o_val)
        highs.append(h_val)
        lows.append(l_val)
        closes.append(c_val)
        volumes.append(volume_val)
        timestamps.append(ts_val)
    return opens, highs, lows, closes, volumes, timestamps


def _extract_ohlc(rows: List[Dict[str, Any]]) -> Tuple[List[float], List[float], List[float], List[float]]:
    opens, highs, lows, closes, _volumes, _timestamps = _extract_ohlcv(rows)
    return opens, highs, lows, closes


def _dt_iso_z(value: datetime) -> str:
    dt = _aware_utc_dt(value) or datetime.now(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def _timeframe_seconds(timeframe_key: str) -> int:
    tf = str(timeframe_key or "").strip().lower()
    if tf.endswith("m"):
        raw = _to_int_opt(tf[:-1])
        if raw is not None and raw > 0:
            return int(raw) * 60
    if tf.endswith("h"):
        raw = _to_int_opt(tf[:-1])
        if raw is not None and raw > 0:
            return int(raw) * 60 * 60
    if tf.endswith("d"):
        raw = _to_int_opt(tf[:-1])
        if raw is not None and raw > 0:
            return int(raw) * 24 * 60 * 60
    return 60 * 60


def _bucket_start_dt(value: datetime, timeframe_key: str) -> datetime:
    dt = _aware_utc_dt(value) or datetime.now(timezone.utc)
    seconds = max(60, _timeframe_seconds(timeframe_key))
    bucket_epoch = int(dt.timestamp()) - (int(dt.timestamp()) % seconds)
    return datetime.fromtimestamp(bucket_epoch, tz=timezone.utc)


def _historical_row_dt(row: Dict[str, Any]) -> Optional[datetime]:
    if not isinstance(row, dict):
        return None
    for key in ("begins_at", "beginsAt", "time"):
        parsed = _aware_utc_dt(_parse_iso_ts(row.get(key)))
        if parsed is not None:
            return parsed
    raw_ms = _to_int_opt(row.get("datetime"))
    if raw_ms is not None:
        try:
            val = int(raw_ms)
            if val > 10_000_000_000:
                val = val // 1000
            return datetime.fromtimestamp(val, tz=timezone.utc)
        except Exception:
            return None
    return None


def _quote_age_seconds(quote_updated_at: Any, now_dt: Optional[datetime] = None) -> Optional[float]:
    qdt = _aware_utc_dt(_parse_iso_ts(quote_updated_at))
    ndt = _aware_utc_dt(now_dt or datetime.now(timezone.utc))
    if qdt is None or ndt is None:
        return None
    return max(0.0, (ndt - qdt).total_seconds())


def _overnight_quote_stale(
    quote_updated_at: Any,
    *,
    timeframe_key: str,
    now_dt: Optional[datetime] = None,
) -> Tuple[bool, Optional[float], float]:
    age = _quote_age_seconds(quote_updated_at, now_dt)
    threshold = max(15 * 60.0, float(_timeframe_seconds(timeframe_key) * OVERNIGHT_QUOTE_STALE_MULTIPLIER))
    if age is None:
        return False, None, threshold
    return bool(age > threshold), float(age), threshold


def load_overnight_synthetic_history(path: Path) -> Dict[str, Any]:
    if not isinstance(path, Path) or not path.exists():
        return {"version": OVERNIGHT_HISTORY_STATE_VERSION, "symbols": {}}
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"version": OVERNIGHT_HISTORY_STATE_VERSION, "symbols": {}}
    if not isinstance(obj, dict):
        return {"version": OVERNIGHT_HISTORY_STATE_VERSION, "symbols": {}}
    symbols = obj.get("symbols")
    if not isinstance(symbols, dict):
        symbols = {}
    return {"version": OVERNIGHT_HISTORY_STATE_VERSION, "symbols": symbols}


def save_overnight_synthetic_history(path: Path, state: Dict[str, Any]) -> None:
    if not isinstance(path, Path):
        return
    try:
        payload = {
            "version": OVERNIGHT_HISTORY_STATE_VERSION,
            "updated_at": iso_now(),
            "symbols": state.get("symbols") if isinstance(state.get("symbols"), dict) else {},
        }
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    except Exception:
        pass


def _overnight_symbol_rows(state: Dict[str, Any], symbol: str, timeframe_key: str) -> List[Dict[str, Any]]:
    symbols_obj = state.setdefault("symbols", {})
    if not isinstance(symbols_obj, dict):
        state["symbols"] = {}
        symbols_obj = state["symbols"]
    sym = str(symbol or "").strip().upper()
    tf = str(timeframe_key or "").strip().lower()
    symbol_obj = symbols_obj.setdefault(sym, {})
    if not isinstance(symbol_obj, dict):
        symbol_obj = {}
        symbols_obj[sym] = symbol_obj
    rows = symbol_obj.setdefault(tf, [])
    if not isinstance(rows, list):
        rows = []
        symbol_obj[tf] = rows
    return rows


def _normalize_overnight_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_dt = _historical_row_dt(row)
        if row_dt is None:
            continue
        try:
            o = float(row.get("open_price"))
            h = float(row.get("high_price"))
            l = float(row.get("low_price"))
            c = float(row.get("close_price"))
        except Exception:
            continue
        if min(o, h, l, c) <= 0 or h < l:
            continue
        key = _dt_iso_z(row_dt)
        normalized[key] = {
            **row,
            "begins_at": key,
            "open_price": float(o),
            "high_price": float(h),
            "low_price": float(l),
            "close_price": float(c),
        }
    return [normalized[k] for k in sorted(normalized.keys())][-OVERNIGHT_HISTORY_MAX_ROWS:]


def record_overnight_price_sample(
    *,
    state: Dict[str, Any],
    symbol: str,
    timeframe_key: str,
    price: float,
    quote_updated_at: Any,
    now_dt: Optional[datetime] = None,
) -> Dict[str, Any]:
    now = _aware_utc_dt(now_dt or datetime.now(timezone.utc)) or datetime.now(timezone.utc)
    stale, age_seconds, stale_threshold = _overnight_quote_stale(
        quote_updated_at,
        timeframe_key=timeframe_key,
        now_dt=now,
    )
    meta: Dict[str, Any] = {
        "recorded": False,
        "quote_stale": bool(stale),
        "quote_age_seconds": age_seconds,
        "quote_stale_threshold_seconds": stale_threshold,
        "synthetic_candle_count": 0,
        "synthetic_latest_ts": None,
        "skip_reason": "",
    }
    if stale:
        meta["skip_reason"] = "quote stale"
        return meta
    try:
        px = float(price)
    except Exception:
        meta["skip_reason"] = "invalid price"
        return meta
    if (not math.isfinite(px)) or px <= 0.0:
        meta["skip_reason"] = "invalid price"
        return meta

    rows = _overnight_symbol_rows(state, symbol, timeframe_key)
    bucket_dt = _bucket_start_dt(now, timeframe_key)
    bucket_key = _dt_iso_z(bucket_dt)
    rows = _normalize_overnight_rows(rows)
    updated = False
    for row in rows:
        if str(row.get("begins_at") or "") != bucket_key:
            continue
        row["high_price"] = max(float(row.get("high_price")), px)
        row["low_price"] = min(float(row.get("low_price")), px)
        row["close_price"] = px
        row["last_seen_at"] = _dt_iso_z(now)
        row["sample_count"] = int(_to_int_opt(row.get("sample_count")) or 1) + 1
        updated = True
        break
    if not updated:
        rows.append(
            {
                "begins_at": bucket_key,
                "open_price": px,
                "high_price": px,
                "low_price": px,
                "close_price": px,
                "session": "overnight",
                "source": "live_loop_overnight",
                "sample_count": 1,
                "first_seen_at": _dt_iso_z(now),
                "last_seen_at": _dt_iso_z(now),
            }
        )
    rows = _normalize_overnight_rows(rows)
    _overnight_symbol_rows(state, symbol, timeframe_key)[:] = rows
    meta["recorded"] = True
    meta["synthetic_candle_count"] = len(rows)
    meta["synthetic_latest_ts"] = str(rows[-1].get("begins_at")) if rows else None
    return meta


def _merge_overnight_synthetic_rows(
    broker_rows: List[Dict[str, Any]],
    state: Dict[str, Any],
    *,
    symbol: str,
    timeframe_key: str,
) -> Tuple[List[Dict[str, Any]], int, Optional[str]]:
    rows = _normalize_overnight_rows(_overnight_symbol_rows(state, symbol, timeframe_key))
    if not rows:
        return broker_rows, 0, None
    latest_broker_dt: Optional[datetime] = None
    for row in broker_rows:
        row_dt = _historical_row_dt(row)
        if row_dt is not None and (latest_broker_dt is None or row_dt > latest_broker_dt):
            latest_broker_dt = row_dt
    extra: List[Dict[str, Any]] = []
    for row in rows:
        row_dt = _historical_row_dt(row)
        if row_dt is None:
            continue
        if latest_broker_dt is not None and row_dt <= latest_broker_dt:
            continue
        extra.append(row)
    if not extra:
        return broker_rows, 0, None
    merged = _merge_historicals(broker_rows, extra)
    return merged, len(extra), str(extra[-1].get("begins_at") or "")


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
def get_account_snapshot() -> Dict[str, Any]:
    acct: Dict[str, Any] = {}
    portfolio: Dict[str, Any] = {}
    try:
        raw_acct = rh.profiles.load_account_profile()
        if isinstance(raw_acct, dict):
            acct = raw_acct
    except Exception:
        acct = {}
    try:
        raw_portfolio = rh.profiles.load_portfolio_profile()
        if isinstance(raw_portfolio, dict):
            portfolio = raw_portfolio
    except Exception:
        portfolio = {}
    return {"account": acct, "portfolio": portfolio}


def _pick_equity(snapshot: Dict[str, Any]) -> float:
    portfolio = snapshot.get("portfolio") if isinstance(snapshot, dict) else {}
    account = snapshot.get("account") if isinstance(snapshot, dict) else {}
    if not isinstance(portfolio, dict):
        portfolio = {}
    if not isinstance(account, dict):
        account = {}
    for source in (portfolio, account):
        for key in ("equity", "portfolio_equity", "market_value"):
            val = source.get(key)
            if val is not None:
                return float(val)
    return 0.0


def _pick_buying_power(snapshot: Dict[str, Any]) -> float:
    account = snapshot.get("account") if isinstance(snapshot, dict) else {}
    if not isinstance(account, dict):
        account = {}
    for key in ("buying_power", "cash_available_for_withdrawal", "cash"):
        val = account.get(key)
        if val is not None:
            return float(val)
    return 0.0


def _pick_available_cash(snapshot: Dict[str, Any]) -> float:
    account = snapshot.get("account") if isinstance(snapshot, dict) else {}
    if not isinstance(account, dict):
        account = {}
    for key in ("cash", "cash_available_for_withdrawal", "buying_power"):
        val = account.get(key)
        if val is not None:
            return float(val)
    return 0.0


def _day_trade_metrics(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    account = snapshot.get("account") if isinstance(snapshot, dict) else {}
    if not isinstance(account, dict):
        account = {}
    round_trips = None
    for key in ("day_trade_count", "day_trades", "round_trips", "roundTrips"):
        val = _to_int_opt(account.get(key))
        if val is not None:
            round_trips = int(val)
            break
    remaining = None if round_trips is None else max(0, NON_PDT_DAY_TRADE_LIMIT - int(round_trips))
    is_day_trader = _to_bool(account.get("is_pattern_day_trader") or account.get("pattern_day_trader"), False)
    return {
        "round_trips_used": round_trips,
        "remaining": remaining,
        "is_day_trader": is_day_trader,
    }


@reconnect_if_needed
def get_day_trade_metrics() -> Dict[str, Any]:
    try:
        trades = rh.get_day_trades()
    except Exception:
        trades = None
    if isinstance(trades, list):
        count = len([t for t in trades if isinstance(t, dict)])
        return {
            "round_trips_used": count,
            "remaining": max(0, NON_PDT_DAY_TRADE_LIMIT - count),
            "is_day_trader": False,
        }
    return {}


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


def _sar_series_with_trend(
    closes: List[float],
    *,
    highs: Optional[List[float]] = None,
    lows: Optional[List[float]] = None,
    step: float = 0.02,
    max_step: float = 0.2,
) -> Tuple[List[Optional[float]], List[Optional[bool]]]:
    n = len(closes)
    out: List[Optional[float]] = [None] * n
    trend: List[Optional[bool]] = [None] * n
    if n < 2:
        return out, trend
    close_vals: List[float] = []
    high_vals: List[float] = []
    low_vals: List[float] = []
    for i in range(n):
        c = _to_float_opt(closes[i] if i < len(closes) else None)
        if c is None or (not math.isfinite(float(c))):
            return out, trend
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
    trend[0] = bool(uptrend)

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
        trend[i] = bool(uptrend)
    return out, trend


def _sar_series(
    closes: List[float],
    *,
    highs: Optional[List[float]] = None,
    lows: Optional[List[float]] = None,
    step: float = 0.02,
    max_step: float = 0.2,
) -> List[Optional[float]]:
    values, _trend = _sar_series_with_trend(closes, highs=highs, lows=lows, step=step, max_step=max_step)
    return values


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
    trend_up: Optional[bool] = None,
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
        if trend_up is not None:
            return bool(trend_up)
        return prev_sar is not None and float(sar_now) > float(prev_sar)
    if c == "sar_falling":
        if trend_up is not None:
            return not bool(trend_up)
        return prev_sar is not None and float(sar_now) < float(prev_sar)
    if c == "trend_long":
        dir_ok = bool(trend_up) if trend_up is not None else (prev_sar is not None and float(sar_now) > float(prev_sar))
        return float(close_now) > float(sar_now) and bool(dir_ok)
    if c == "trend_short":
        dir_ok = (not bool(trend_up)) if trend_up is not None else (prev_sar is not None and float(sar_now) < float(prev_sar))
        return float(close_now) < float(sar_now) and bool(dir_ok)
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
        sar_vals, sar_trends = _sar_series_with_trend(closes, highs=highs, lows=lows, step=step, max_step=max_step)
        sar_now = sar_vals[-1] if sar_vals else None
        prev_sar = sar_vals[-2] if len(sar_vals) > 1 else None
        trend_up = sar_trends[-1] if sar_trends else None
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
            trend_up=trend_up,
        )
        sell_ok = True if sell_ignored else _sar_condition_hit(
            sell_cond,
            close_now=float(close_now),
            close_prev=close_prev,
            sar_now=float(sar_now),
            prev_sar=prev_sar,
            trend_up=trend_up,
        )
        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = bool(buy_ignored)
        out["sell_ignored"] = bool(sell_ignored)
        trend_txt = "up" if trend_up is True else ("down" if trend_up is False else "n/a")
        out["value"] = (
            f"SAR={_fmt(sar_now,4)} P={_fmt(close_now,4)} prevSAR={_fmt(prev_sar,4)} trend={trend_txt}"
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


def _round_to_cents(value: float) -> float:
    return max(0.01, round(float(value), 2))


def _round_up_to_cents(value: float) -> float:
    try:
        rounded = Decimal(str(float(value))).quantize(Decimal("0.01"), rounding=ROUND_CEILING)
        return max(0.01, float(rounded))
    except (InvalidOperation, ValueError, TypeError):
        return 0.01


def _round_down_to_cents(value: float) -> float:
    try:
        rounded = Decimal(str(float(value))).quantize(Decimal("0.01"), rounding=ROUND_FLOOR)
        return max(0.01, float(rounded))
    except (InvalidOperation, ValueError, TypeError):
        return 0.01


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
            price = _round_down_to_cents(float(raw_price))
            if price > float(current_price):
                return str(label), price
    if float(fallback_pct) > 0.0:
        price = _round_down_to_cents(float(current_price) * (1.0 + (float(fallback_pct) / 100.0)))
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
    price = _round_up_to_cents(basis * (1.0 + (float(profit_pct) / 100.0)))
    if price <= basis or price <= float(current_price):
        return None
    return f"profit +{float(profit_pct):g}%", price


def _preorder_target_allowed(target_price: Optional[float], current_price: float, avg_buy_price: float) -> bool:
    if target_price is None or target_price <= 0:
        return False
    basis = float(avg_buy_price) if avg_buy_price > 0 else float(current_price)
    return float(target_price) > float(current_price) and float(target_price) > basis


def _resolve_trail_amount(
    *,
    trailing_stop_mode: str,
    trailing_stop_amount: float,
    trailing_stop_atr_mult: float,
    atr: Optional[float],
) -> Optional[float]:
    trail_amount = max(MIN_TRAIL_AMOUNT_USD, float(trailing_stop_amount))
    if str(trailing_stop_mode or "").strip().lower() == "atr":
        if atr is None or atr <= 0:
            return None
        trail_amount = float(atr) * float(trailing_stop_atr_mult)
    return _round_to_cents(trail_amount)


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


def place_market_buy(symbol: str, qty: float, *, extended_hours: bool = False, market_session: str = "") -> Any:
    return place_stock_order(
        symbol=symbol,
        side="buy",
        order_type="market",
        quantity=float(qty),
        account_number=_resolve_account_number(),
        timeInForce="gfd",
        extendedHours=bool(extended_hours),
        market_session=market_session,
    )


def place_market_sell(symbol: str, qty: float, *, extended_hours: bool = False, market_session: str = "") -> Any:
    return place_stock_order(
        symbol=symbol,
        side="sell",
        order_type="market",
        quantity=float(qty),
        account_number=_resolve_account_number(),
        timeInForce="gfd",
        extendedHours=bool(extended_hours),
        market_session=market_session,
    )


def place_limit_buy(
    symbol: str,
    qty: float,
    price: float,
    *,
    extended_hours: bool = False,
    market_session: str = "",
    market_hours: Optional[str] = None,
) -> Any:
    return place_stock_order(
        symbol=symbol,
        side="buy",
        order_type="limit",
        quantity=float(qty),
        limitPrice=_round_up_to_cents(price),
        account_number=_resolve_account_number(),
        timeInForce="gfd",
        extendedHours=bool(extended_hours),
        market_hours=market_hours,
        market_session=market_session,
    )


def place_limit_sell(
    symbol: str,
    qty: float,
    price: float,
    *,
    extended_hours: bool = False,
    market_session: str = "",
    market_hours: Optional[str] = None,
    time_in_force: str = "gtc",
) -> Any:
    return place_stock_order(
        symbol=symbol,
        side="sell",
        order_type="limit",
        quantity=float(qty),
        limitPrice=_round_down_to_cents(price),
        account_number=_resolve_account_number(),
        timeInForce=str(time_in_force or "gtc").strip().lower() or "gtc",
        extendedHours=bool(extended_hours),
        market_hours=market_hours,
        market_session=market_session,
    )


def place_trailing_stop_order(
    symbol: str,
    side: str,
    qty: float,
    trail_amount: float,
    *,
    extended_hours: bool = False,
    market_session: str = "",
) -> Any:
    # Keep this in the same shape as the legacy working robin_stocks call:
    # rh.orders.order_trailing_stop(..., side="buy"|"sell", trailAmount=..., trailType="amount")
    return place_stock_order(
        symbol=symbol,
        side=side,
        order_type="trailing_stop",
        quantity=float(qty),
        trailAmount=float(trail_amount),
        trailType="amount",
        account_number=_resolve_account_number(),
        timeInForce="gtc",
        extendedHours=bool(extended_hours),
        market_session=market_session,
    )


def place_trailing_stop_buy(symbol: str, qty: float, trail_amount: float, *, extended_hours: bool = False, market_session: str = "") -> Any:
    return place_trailing_stop_order(
        symbol,
        "buy",
        qty,
        trail_amount,
        extended_hours=extended_hours,
        market_session=market_session,
    )


def place_trailing_stop_sell(symbol: str, qty: float, trail_amount: float, *, extended_hours: bool = False, market_session: str = "") -> Any:
    return place_trailing_stop_order(
        symbol,
        "sell",
        qty,
        trail_amount,
        extended_hours=extended_hours,
        market_session=market_session,
    )


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
    symbol: str,
    current_price: float,
    avg_buy_price: float,
    held_qty: float,
    target_gain_pct: float,
    stop_loss_pct: float,
    *,
    session_state: str,
    limit_mid: Optional[float],
    allow_extended_hours_orders: bool,
    trade_stats: Optional[Dict[str, Any]] = None,
) -> None:
    if avg_buy_price <= 0:
        return
    session_norm = str(session_state or "closed").strip().lower()
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
                print(f"[{symbol}] Stop-loss trigger hit while market closed; keeping stop-loss armed.")
                return
            if session_norm == "extended" and not bool(allow_extended_hours_orders):
                print(f"[{symbol}] Stop-loss trigger hit in extended hours, but extended-hours orders are disabled.")
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
                if session_norm in ("extended", "overnight"):
                    limit_price = float(limit_mid) if limit_mid and limit_mid > 0 else float(current_price)
                    if limit_price < float(avg_buy_price):
                        print(
                            f"[{symbol}] Stop-loss blocked by no-loss rule: midpoint "
                            f"({limit_price:.2f}) is below avg buy ({avg_buy_price:.2f})."
                        )
                        return
                    resp = place_limit_sell(
                        symbol,
                        float(sell_qty),
                        limit_price,
                        extended_hours=True,
                        market_session=session_norm,
                        market_hours=_order_market_hours_for_state(session_norm),
                    )
                else:
                    resp = place_market_sell(symbol, float(sell_qty), extended_hours=False, market_session=session_norm)
                if _order_success(resp):
                    print(f"Stop-loss SELL executed for {symbol}: {sell_qty} shares.")
                    if trade_stats is not None:
                        _record_trade(
                            trade_stats, side="sell", qty=float(sell_qty), price=current_price, avg_buy_price=avg_buy_price
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
        print(f"[{symbol}] Signal: {rule_signal} (execution: {execution_signal}; {execution_hold_reason})")
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
    allow_extended_hours_orders: bool,
    allow_seamless_overnight_orders: bool,
    portfolio_cap_rule_enabled: bool,
    portfolio_cap_mode: str,
    portfolio_cap_percent_by_symbol: Dict[str, float],
    portfolio_cap_percent: float,
    portfolio_cap_divisor: int,
    portfolio_cash_percent: float,
    portfolio_cash_source: str,
    timeframe: str,
    sleep_duration: float,
    include_extended_hours_data: bool,
    use_current_candle: bool,
    rules: List[Dict[str, Any]],
    overnight_history_path: Optional[Path] = None,
    trade_stats: Optional[Dict[str, Any]] = None,
    status_writer: Optional[Any] = None,
) -> None:
    if timeframe not in TIMEFRAMES:
        raise ValueError(f"Invalid timeframe '{timeframe}'. Choose from {list(TIMEFRAMES.keys())}.")
    interval = TIMEFRAMES[timeframe]["interval"]
    span = TIMEFRAMES[timeframe]["span"]
    print(f"Using timeframe: {timeframe} ({interval}, {span})")
    print(f"Symbols: {symbols}")
    print(f"Rules loaded: {len(rules)}")
    print(f"BUY order type: {buy_order_type} | SELL order type: {sell_order_type}")
    print(
        "Pre-sale order target: "
        + (
            f"pivot={'ON' if pivot_preorder_enabled else 'OFF'} "
            f"profit={'ON' if pivot_preorder_profit_enabled else 'OFF'} "
            f"profit_pct={float(pivot_preorder_profit_pct):g}% "
            f"pivot_offset={float(pivot_preorder_offset):g} "
            f"half_levels={'YES' if pivot_preorder_include_half_levels else 'NO'} "
            f"pivot_fallback={float(pivot_preorder_fallback_pct):g}%"
            if bool(pivot_preorder_enabled) or bool(pivot_preorder_profit_enabled)
            else "OFF"
        )
    )
    print(f"History include extended hours: {'YES' if include_extended_hours_data else 'NO'}")
    print(f"Allow extended hours orders: {'YES' if allow_extended_hours_orders else 'NO'}")
    print(f"Allow overnight/all-day orders: {'YES' if allow_seamless_overnight_orders else 'NO'}")
    rules = _rules_with_default_timeframe(rules, timeframe)
    rules_by_tf = _rules_by_timeframe(rules, timeframe)
    rule_min_candles = _rule_min_candles(rules)
    rule_min_candles_by_tf = {tf: _rule_min_candles(tf_rules) for tf, tf_rules in rules_by_tf.items()}
    print(f"Rule candle requirement: {rule_min_candles}")
    print(
        "Rule timeframes: "
        + ", ".join(f"{tf}({rule_min_candles_by_tf[tf]} candles)" for tf in rules_by_tf)
    )

    overnight_history_state: Dict[str, Any] = load_overnight_synthetic_history(overnight_history_path) if overnight_history_path else {
        "version": OVERNIGHT_HISTORY_STATE_VERSION,
        "symbols": {},
    }
    session_state = ""
    market_state = ""
    while True:
        overnight_history_dirty = False
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
                if allow_extended_hours_orders:
                    print("Session: extended hours (limit orders with extendedHours=True).")
                else:
                    print("Session: extended hours detected but disabled (orders skipped).")
            elif session_state == "overnight":
                print("Session: overnight enabled (all_day_hours midpoint limit orders).")
            else:
                print("Session: market closed (orders skipped).")

        tickers_status: List[Dict[str, Any]] = []
        try:
            positions = get_open_stock_positions(_db_path=db_path, _connection_id=connection_id)
            positions_map = build_positions_map(positions)
        except Exception:
            positions_map = {}
        try:
            account_snapshot = get_account_snapshot(_db_path=db_path, _connection_id=connection_id)
        except Exception:
            account_snapshot = {}
        day_trade_metrics = get_day_trade_metrics(_db_path=db_path, _connection_id=connection_id)
        if not day_trade_metrics:
            day_trade_metrics = _day_trade_metrics(account_snapshot)
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
        cash_position_source = _normalize_portfolio_cash_source(portfolio_cash_source)
        cap_pct: Optional[float] = None
        cash_target_value: Optional[float] = None
        cash_pct: Optional[float] = None
        try:
            buying_power = float(_pick_buying_power(account_snapshot))
        except Exception as e:
            print(f"[WARN] Buying power unavailable; buy power gate will not be enforced: {e}")
            buying_power = None
        if portfolio_cap_rule_enabled:
            try:
                portfolio_value = float(_pick_equity(account_snapshot))
                if cash_position_source == "cash":
                    available_cash = float(_pick_available_cash(account_snapshot))
                else:
                    available_cash = float(buying_power) if buying_power is not None else None
                cap_pct = None if portfolio_cap_mode == "percent" else 100.0 / float(divisor)
                if portfolio_value > 0:
                    cash_target_value = float(portfolio_value) * (float(cash_target_pct) / 100.0)
                    if available_cash is not None:
                        cash_pct = (float(available_cash) / float(portfolio_value)) * 100.0
            except Exception as e:
                print(f"[WARN] Portfolio cap enabled but portfolio/cash-source value unavailable: {e}")
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

        prefer_live_overnight_quotes = (
            session_state == "overnight" and bool(allow_seamless_overnight_orders)
        )
        quotes = get_quotes_map(symbols, prefer_live_overnight=prefer_live_overnight_quotes)

        for symbol in symbols:
            try:
                quote = _get_quote_for_symbol(quotes, symbol)
                prefer_session_price = bool(include_extended_hours_data) or bool(prefer_live_overnight_quotes)
                current_price_opt = _price_from_quote(quote, prefer_extended=prefer_session_price)
                if current_price_opt is None:
                    current_price_opt = get_stock_price(
                        symbol,
                        prefer_extended=prefer_session_price,
                        prefer_live_overnight=bool(prefer_live_overnight_quotes),
                        _db_path=db_path,
                        _connection_id=connection_id,
                    )
                    refreshed_quote = safe_stock_quote(symbol, retries=1, backoff=0.5)
                    if bool(prefer_live_overnight_quotes):
                        refreshed_quote = (
                            safe_live_overnight_quote(symbol, base_quote=refreshed_quote, retries=1, backoff=0.25)
                            or refreshed_quote
                        )
                    quote = refreshed_quote or quote
                current_price = float(current_price_opt)
                bid_price, ask_price = _quote_bid_ask(quote)
                mid_price = _round_up_to_cents(_mid_price(bid_price, ask_price, current_price))
                quote_source = str(quote.get("_quote_source") or "robinhood_marketdata")
                quote_updated_at = str(
                    quote.get("_quote_updated_at")
                    or quote.get("updated_at")
                    or quote.get("venue_last_non_reg_trade_time")
                    or quote.get("venue_last_trade_time")
                    or ""
                )
                now_dt = datetime.now(timezone.utc)
                overnight_history_meta: Dict[str, Any] = {
                    "recorded": False,
                    "quote_stale": False,
                    "quote_age_seconds": None,
                    "quote_stale_threshold_seconds": None,
                    "synthetic_candle_count": 0,
                    "synthetic_latest_ts": None,
                    "synthetic_rows_merged": 0,
                    "synthetic_merged_latest_ts": None,
                    "skip_reason": "",
                }
                fetch_timeframes = list(dict.fromkeys([timeframe] + list(rules_by_tf.keys())))
                hist_by_tf: Dict[str, List[Dict[str, Any]]] = {}
                ohlc_by_tf: Dict[str, Tuple[List[float], List[float], List[float], List[float], List[float], List[Any]]] = {}
                policy_by_tf: Dict[str, Any] = {}
                fetched_count_by_tf: Dict[str, int] = {}
                live_candle_appended_by_tf: Dict[str, bool] = {}
                missing_tf_reasons: List[str] = []

                for tf_key in fetch_timeframes:
                    tf_min = int(rule_min_candles_by_tf.get(tf_key, 30 if tf_key == timeframe else rule_min_candles))
                    tf_overnight_meta = dict(overnight_history_meta)
                    if session_state == "overnight":
                        tf_overnight_meta.update(
                            record_overnight_price_sample(
                                state=overnight_history_state,
                                symbol=symbol,
                                timeframe_key=tf_key,
                                price=float(current_price),
                                quote_updated_at=quote_updated_at,
                                now_dt=now_dt,
                            )
                        )
                        if bool(tf_overnight_meta.get("recorded")):
                            overnight_history_dirty = True
                        elif bool(tf_overnight_meta.get("quote_stale")) and tf_key == timeframe:
                            print(
                                f"[{symbol}] Overnight quote stale; skipping synthetic candle "
                                f"(quote_updated_at={quote_updated_at or '—'}, "
                                f"age={_fmt(tf_overnight_meta.get('quote_age_seconds'), 1)}s)."
                            )
                        if tf_key == timeframe:
                            overnight_history_meta.update(tf_overnight_meta)

                    hist_tf = _fetch_historicals_like_schwab(
                        symbol=symbol,
                        timeframe_key=tf_key,
                        include_extended_hours_data=bool(include_extended_hours_data),
                        min_candles=tf_min,
                        _db_path=db_path,
                        _connection_id=connection_id,
                    )
                    if session_state == "overnight":
                        hist_tf, merged_count, merged_latest_ts = _merge_overnight_synthetic_rows(
                            hist_tf,
                            overnight_history_state,
                            symbol=symbol,
                            timeframe_key=tf_key,
                        )
                        if tf_key == timeframe:
                            overnight_history_meta["synthetic_rows_merged"] = int(merged_count)
                            overnight_history_meta["synthetic_merged_latest_ts"] = merged_latest_ts

                    tf_opens, tf_highs, tf_lows, tf_closes, tf_volumes, tf_timestamps = _extract_ohlcv(hist_tf)
                    if len(tf_closes) < tf_min:
                        missing_tf_reasons.append(f"{tf_key}: got {len(tf_closes)}, need {tf_min}")
                        continue

                    fetched_count_by_tf[tf_key] = len(tf_closes)
                    live_appended = False
                    append_live_candle = True
                    if session_state == "overnight":
                        if bool(tf_overnight_meta.get("recorded")):
                            append_live_candle = False
                        elif bool(tf_overnight_meta.get("quote_stale")):
                            append_live_candle = False
                    if append_live_candle:
                        prev_close = float(tf_closes[-1])
                        cur = float(current_price)
                        tf_opens.append(prev_close)
                        tf_highs.append(max(prev_close, cur))
                        tf_lows.append(min(prev_close, cur))
                        tf_closes.append(cur)
                        tf_volumes.append(0.0)
                        tf_timestamps.append(iso_now())
                        live_appended = True

                    policy_tf = apply_final_candle_policy(
                        opens=tf_opens,
                        highs=tf_highs,
                        lows=tf_lows,
                        closes=tf_closes,
                        use_current_candle=True,
                    )
                    tf_opens, tf_highs, tf_lows, tf_closes = (
                        policy_tf.opens,
                        policy_tf.highs,
                        policy_tf.lows,
                        policy_tf.closes,
                    )
                    tf_volumes = tf_volumes[: len(tf_closes)]
                    tf_timestamps = tf_timestamps[: len(tf_closes)]
                    if live_appended and len(tf_closes) <= fetched_count_by_tf[tf_key]:
                        print(f"[{symbol}] CURRENT_CANDLE_UNAVAILABLE timeframe={tf_key}")
                    if len(tf_closes) < tf_min:
                        missing_tf_reasons.append(
                            f"{tf_key}: fetched {fetched_count_by_tf[tf_key]}, used {len(tf_closes)}, need {tf_min}"
                        )
                        continue
                    hist_by_tf[tf_key] = hist_tf
                    ohlc_by_tf[tf_key] = (tf_opens, tf_highs, tf_lows, tf_closes, tf_volumes, tf_timestamps)
                    policy_by_tf[tf_key] = policy_tf
                    live_candle_appended_by_tf[tf_key] = live_appended

                if missing_tf_reasons:
                    print(f"[{symbol}] Not enough historical candles by timeframe: {'; '.join(missing_tf_reasons)}.")
                    tickers_status.append({"symbol": symbol, "signal": "NO_DATA", "timeframes": list(rules_by_tf.keys())})
                    continue

                default_ohlc = ohlc_by_tf.get(timeframe) or next(iter(ohlc_by_tf.values()))
                opens, highs, lows, closes, volumes, timestamps = default_ohlc
                hist = hist_by_tf.get(timeframe) or next(iter(hist_by_tf.values()))
                policy = policy_by_tf.get(timeframe) or next(iter(policy_by_tf.values()))
                fetched_count = int(fetched_count_by_tf.get(timeframe, len(closes)))
                live_candle_appended = bool(live_candle_appended_by_tf.get(timeframe, False))
                indicator_price = float(closes[-1])

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
                    rule_tf = _rule_timeframe(r, timeframe)
                    rule_ohlc = ohlc_by_tf.get(rule_tf)
                    if rule_ohlc is None:
                        continue
                    rule_opens, rule_highs, rule_lows, rule_closes, rule_volumes, rule_timestamps = rule_ohlc
                    c = _eval_rule(
                        r,
                        rule_closes,
                        float(rule_closes[-1]),
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
                            child["_timeframe"] = rule_tf
                            if not str(child.get("_rule_id") or "").strip():
                                child["_rule_id"] = _ma_ribbon_level_rule_id(base_rule_id, child.get("_ribbon_slot"))
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
                            cash_source_label = "available cash" if cash_position_source == "cash" else "buying power"
                            print(
                                f"[{symbol}] BUY blocked by cash target: current {cash_source_label} "
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

                if session_state == "overnight" and bool(overnight_history_meta.get("quote_stale")):
                    execution_signal = "HOLD"
                    execution_hold_reason = "overnight quote stale"
                elif execution_signal != "HOLD":
                    if session_state == "closed":
                        execution_signal = "HOLD"
                        execution_hold_reason = "market closed"
                    elif session_state == "extended" and not bool(allow_extended_hours_orders):
                        execution_signal = "HOLD"
                        execution_hold_reason = "extended hours disabled"
                    elif session_state == "overnight" and not bool(allow_seamless_overnight_orders):
                        execution_signal = "HOLD"
                        execution_hold_reason = "overnight orders disabled"
                signal = execution_signal
                log_indicator_policy(
                    mode="LIVE",
                    symbol=symbol,
                    timeframe=timeframe,
                    session=market_state,
                    extended_hours=bool(include_extended_hours_data),
                    use_current_candle=True,
                    total_fetched=fetched_count + (1 if live_candle_appended else 0),
                    total_used=len(closes),
                    latest_ohlc=(opens[-1], highs[-1], lows[-1], closes[-1]) if closes else None,
                    latest_included=policy.latest_included,
                    latest_excluded=policy.latest_excluded,
                    final_signal=signal,
                )
                _print_rule_checks(symbol, checks, rule_consensus_signal, execution_signal, execution_hold_reason)

                pnl_pct: Optional[float] = None
                if avg_buy_price > 0:
                    pnl_pct = ((float(current_price) - avg_buy_price) / avg_buy_price) * 100.0

                ma20 = _ma_value(closes, 20)
                ma78 = _ma_value(closes, 78)
                ma190 = _ma_value(closes, 190)
                rsi = _rsi(closes, 14)
                drsi = _rsi_derivative(closes, 14)
                atr = _atr_from_historicals(hist, period=14)
                estimated_preorder_avg = _estimated_avg_after_buy(
                    float(pos_qty),
                    float(avg_buy_price),
                    float(shares_per_trade) if signal == "BUY" else 0.0,
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
                pivot_preorder_margin_per_share = None
                pivot_preorder_margin_total = None
                if pivot_preorder_target_price is not None and float(current_price) > 0.0:
                    pivot_preorder_margin_per_share = float(pivot_preorder_target_price) - float(current_price)
                    pivot_preorder_margin_pct = (pivot_preorder_margin_per_share / float(current_price)) * 100.0
                    pivot_preorder_margin_total = pivot_preorder_margin_per_share * float(shares_per_trade)

                tickers_status.append(
                    {
                        "symbol": symbol,
                        "signal": signal,
                        "rule_signal": rule_consensus_signal,
                        "execution_signal": execution_signal,
                        "execution_hold_reason": execution_hold_reason,
                        "price": float(current_price),
                        "quote_source": quote_source,
                        "quote_updated_at": quote_updated_at,
                        "quote_age_seconds": overnight_history_meta.get("quote_age_seconds"),
                        "quote_stale": bool(overnight_history_meta.get("quote_stale")),
                        "overnight_history_candles": overnight_history_meta.get("synthetic_candle_count"),
                        "overnight_history_latest_ts": overnight_history_meta.get("synthetic_latest_ts"),
                        "overnight_history_rows_merged": overnight_history_meta.get("synthetic_rows_merged"),
                        "overnight_history_merged_latest_ts": overnight_history_meta.get("synthetic_merged_latest_ts"),
                        "overnight_history_skip_reason": overnight_history_meta.get("skip_reason"),
                        "live_candle_appended": bool(live_candle_appended),
                        "bid_price": bid_price,
                        "ask_price": ask_price,
                        "mid_price": mid_price,
                        "qty": pos_qty,
                        "avg_buy": avg_buy_price,
                        "pnl_pct": pnl_pct,
                        "cap_pct": row_cap_pct,
                        "cash_pct": cash_pct,
                        "cash_target_pct": cash_target_pct if portfolio_cap_rule_enabled else None,
                        "cash_target_value": cash_target_value,
                        "available_cash": available_cash,
                        "cash_position_source": cash_position_source if portfolio_cap_rule_enabled else None,
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
                        "market_state": market_state,
                        "session_state": session_state,
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
                        "pivot_preorder_margin_per_share": pivot_preorder_margin_per_share,
                        "pivot_preorder_margin_total": pivot_preorder_margin_total,
                        "pivot_preorder_shares": float(shares_per_trade) if (bool(pivot_preorder_enabled) or bool(pivot_preorder_profit_enabled)) else None,
                        "pivot_preorder_order_status": "preview" if pivot_preorder_target is not None else ("no target" if (bool(pivot_preorder_enabled) or bool(pivot_preorder_profit_enabled)) else None),
                        "allow_extended_hours_orders": bool(allow_extended_hours_orders),
                        "allow_seamless_overnight_orders": bool(allow_seamless_overnight_orders),
                        "seamless_supported": bool(allow_seamless_overnight_orders),
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
                                "timeframe": str(c.get("_timeframe") or timeframe),
                                "kind": str(c.get("_rule_kind") or ""),
                                "rule_id": str(c.get("_rule_id") or ""),
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
                    try:
                        resp = None
                        extended_order = _order_extended_hours_for_state(session_state)
                        route_market_hours = _order_market_hours_for_state(session_state)
                        route_market_session = session_state if session_state in ("extended", "overnight") else market_state
                        if session_state == "overnight":
                            buy_limit_price = float(buy_order_price)
                            print(
                                f"[{symbol}] BUY signal (overnight) -> placing current-price limit buy for "
                                f"{shares_per_trade} shares at ${buy_limit_price:.2f}..."
                            )
                            resp = place_limit_buy(
                                symbol,
                                float(shares_per_trade),
                                buy_limit_price,
                                extended_hours=True,
                                market_session=route_market_session,
                                market_hours=route_market_hours,
                            )
                        elif session_state == "extended":
                            buy_limit_price = float(mid_price)
                            print(
                                f"[{symbol}] BUY signal (extended) -> placing midpoint limit buy for "
                                f"{shares_per_trade} shares at ${buy_limit_price:.2f}..."
                            )
                            resp = place_limit_buy(
                                symbol,
                                float(shares_per_trade),
                                buy_limit_price,
                                extended_hours=True,
                                market_session=route_market_session,
                                market_hours=route_market_hours,
                            )
                        elif buy_order_type == "market":
                            print(f"[{symbol}] BUY signal -> placing market buy for {shares_per_trade} shares...")
                            resp = place_market_buy(symbol, float(shares_per_trade), extended_hours=extended_order, market_session=route_market_session)
                        elif buy_order_type == "limit_midpoint":
                            buy_limit_price = float(mid_price)
                            print(
                                f"[{symbol}] BUY signal -> placing midpoint limit buy for {shares_per_trade} shares "
                                f"at ${buy_limit_price:.2f}..."
                            )
                            resp = place_limit_buy(
                                symbol,
                                float(shares_per_trade),
                                buy_limit_price,
                                extended_hours=extended_order,
                                market_session=route_market_session,
                                market_hours=route_market_hours,
                            )
                        elif buy_order_type == "trailing_stop":
                            trail_amount = _resolve_trail_amount(
                                trailing_stop_mode=trailing_stop_mode,
                                trailing_stop_amount=trailing_stop_amount,
                                trailing_stop_atr_mult=trailing_stop_atr_mult,
                                atr=atr,
                            )
                            if trail_amount is None:
                                print(f"[{symbol}] ATR unavailable; skipping trailing stop buy.")
                            else:
                                print(
                                    f"[{symbol}] BUY signal -> placing trailing stop buy for {shares_per_trade} shares, "
                                    f"trail=${float(trail_amount):.2f} ({trailing_stop_mode})..."
                                )
                                resp = place_trailing_stop_buy(
                                    symbol,
                                    float(shares_per_trade),
                                    float(trail_amount),
                                    extended_hours=extended_order,
                                    market_session=route_market_session,
                                )
                        else:
                            print(f"[{symbol}] BUY skipped: unsupported order type '{buy_order_type}'.")
                        if resp is not None:
                            if _order_success(resp):
                                print(f"[{symbol}] BUY order accepted: resp={resp}")
                                if trade_stats is not None:
                                    _record_trade(
                                        trade_stats,
                                        side="buy",
                                        qty=float(shares_per_trade),
                                        price=float(current_price),
                                        avg_buy_price=0.0,
                                    )
                                if bool(pivot_preorder_enabled) or bool(pivot_preorder_profit_enabled):
                                    if pivot_preorder_target is None:
                                        print(f"[{symbol}] Pre-sale order skipped: no profitable target above held average.")
                                        tickers_status[-1]["pivot_preorder_order_status"] = "no target"
                                    else:
                                        target_label, target_price = pivot_preorder_target
                                        print(
                                            f"[{symbol}] Pre-sale order -> placing limit SELL for {shares_per_trade} shares "
                                            f"at ${float(target_price):.2f} ({target_label})."
                                        )
                                        sell_resp = place_limit_sell(
                                            symbol,
                                            float(shares_per_trade),
                                            float(target_price),
                                            extended_hours=extended_order,
                                            market_session=route_market_session,
                                            market_hours=route_market_hours,
                                            time_in_force="gtc",
                                        )
                                        if _order_success(sell_resp):
                                            print(f"[{symbol}] Pre-sale limit SELL accepted: resp={sell_resp}")
                                            tickers_status[-1]["pivot_preorder_order_status"] = "accepted"
                                        else:
                                            reason = _order_failure_reason(sell_resp)
                                            print(
                                                f"[{symbol}] Pre-sale limit SELL rejected: "
                                                f"{reason} | resp={sell_resp}"
                                            )
                                            tickers_status[-1]["pivot_preorder_order_status"] = "rejected"
                                            tickers_status[-1]["pivot_preorder_order_reason"] = reason
                            else:
                                print(f"[{symbol}] BUY order rejected: {_order_failure_reason(resp)} | resp={resp}")
                    except Exception as e:
                        print(f"[{symbol}] Buy failed: {e}")

                if signal == "SELL" and pos_qty > 0:
                    if not _can_sell_without_loss(float(current_price), avg_buy_price):
                        print(
                            f"[{symbol}] SELL blocked by no-loss rule "
                            f"({float(current_price):.2f} < {avg_buy_price:.2f})."
                        )
                        continue
                    try:
                        resp = None
                        extended_order = _order_extended_hours_for_state(session_state)
                        route_market_hours = _order_market_hours_for_state(session_state)
                        route_market_session = session_state if session_state in ("extended", "overnight") else market_state
                        if session_state in ("extended", "overnight"):
                            if float(mid_price) < float(avg_buy_price):
                                print(
                                    f"[{symbol}] SELL blocked by no-loss rule: {session_state} midpoint "
                                    f"({float(mid_price):.2f}) is below avg buy ({avg_buy_price:.2f})."
                                )
                                continue
                            print(
                                f"[{symbol}] SELL signal ({session_state}) -> placing midpoint limit sell for "
                                f"{shares_per_trade} shares at ${float(mid_price):.2f}..."
                            )
                            resp = place_limit_sell(
                                symbol,
                                float(shares_per_trade),
                                float(mid_price),
                                extended_hours=True,
                                market_session=route_market_session,
                                market_hours=route_market_hours,
                            )
                        elif sell_order_type == "market":
                            print(f"[{symbol}] SELL signal -> placing market sell for {shares_per_trade} shares...")
                            resp = place_market_sell(symbol, float(shares_per_trade), extended_hours=extended_order, market_session=route_market_session)
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
                            resp = place_limit_sell(
                                symbol,
                                float(shares_per_trade),
                                float(mid_price),
                                extended_hours=extended_order,
                                market_session=route_market_session,
                                market_hours=route_market_hours,
                            )
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
                                float(trail_amount),
                                extended_hours=extended_order,
                                market_session=route_market_session,
                            )
                        else:
                            print(f"[{symbol}] SELL skipped: unsupported order type '{sell_order_type}'.")
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

        if overnight_history_dirty and overnight_history_path is not None:
            save_overnight_synthetic_history(overnight_history_path, overnight_history_state)

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
                        "seamless_supported": bool(allow_seamless_overnight_orders),
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


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--params-json", required=True)
    ap.add_argument("--db-path", required=True)
    ap.add_argument("--connection-id", required=True, type=int)
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    status_path = run_dir / "status.json"
    overnight_history_path = run_dir / "overnight_synthetic_history.json"
    last_status: Dict[str, Any] = {}
    trade_stats: Dict[str, Any] = {"pnl": 0.0, "trades": 0}

    def write_status(payload: Dict[str, Any]) -> None:
        p = dict(payload)
        p["ts"] = iso_now()
        p["script"] = "IndicatorForge.Robinhood"
        p["pnl"] = round(float(trade_stats.get("pnl", 0.0)), 2)
        p["trades"] = int(trade_stats.get("trades", 0))
        try:
            status_path.write_text(json.dumps(p, indent=2), encoding="utf-8")
            last_status.clear()
            last_status.update(p)
        except Exception:
            pass

    params = load_params(args.params_json)
    symbols = params.get("symbols", [])
    if isinstance(symbols, str):
        symbols = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    if not isinstance(symbols, list) or not symbols:
        raise ValueError("params.symbols must be a non-empty list.")
    symbols = [str(s).strip().upper() for s in symbols if str(s).strip()]

    sleep_duration = float(params.get("sleep_duration", 30))
    shares_per_trade = int(params.get("shares_per_trade", 1))
    trailing_stop_amount = float(params.get("trailing_stop_amount", 0.10))
    trailing_stop_mode = str(params.get("trailing_stop_mode", "fixed")).strip().lower()
    if trailing_stop_mode not in ("fixed", "atr"):
        trailing_stop_mode = "fixed"
    trailing_stop_atr_mult = float(params.get("trailing_stop_atr_mult", 3.0))
    if trailing_stop_atr_mult <= 0:
        trailing_stop_atr_mult = 3.0
    buy_order_type = _normalize_order_type(params.get("buy_order_type"), "market")
    sell_order_type = _normalize_order_type(params.get("sell_order_type"), "trailing_stop")
    pivot_preorder_enabled = _to_bool(params.get("pivot_preorder_enabled", False), False)
    pivot_preorder_profit_enabled = _to_bool(params.get("pivot_preorder_profit_enabled", False), False)
    pivot_preorder_profit_pct = max(0.0, float(params.get("pivot_preorder_profit_pct", 0) or 0))
    pivot_preorder_offset = max(0.5, float(params.get("pivot_preorder_offset", 1) or 1))
    pivot_preorder_include_half_levels = _to_bool(params.get("pivot_preorder_include_half_levels", False), False)
    pivot_preorder_fallback_pct = max(0.0, float(params.get("pivot_preorder_fallback_pct", 0) or 0))
    target_gain_pct = float(params.get("target_gain_pct", 0.5))
    stop_loss_pct = float(params.get("stop_loss_pct", -0.5))
    stoploss_enabled = bool(params.get("stoploss_enabled", False))
    allow_extended_hours_orders = _to_bool(params.get("allow_extended_hours_orders", False), False)
    allow_seamless_overnight_orders = _to_bool(params.get("allow_seamless_overnight_orders", False), False)
    include_extended_hours_data = _to_bool(params.get("include_extended_hours_data", False), False)
    # Current-candle inclusion is part of IndicatorForge indicator logic and is
    # intentionally not user-toggleable. Ignore stale saved params.
    use_current_candle = True
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
    portfolio_cash_source = _normalize_portfolio_cash_source(params.get("portfolio_cash_source", "buying_power"))
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
        allow_extended_hours_orders=allow_extended_hours_orders,
        allow_seamless_overnight_orders=allow_seamless_overnight_orders,
        portfolio_cap_rule_enabled=portfolio_cap_rule_enabled,
        portfolio_cap_mode=portfolio_cap_mode,
        portfolio_cap_percent_by_symbol=portfolio_cap_percent_by_symbol,
        portfolio_cap_percent=portfolio_cap_percent,
        portfolio_cap_divisor=portfolio_cap_divisor,
        portfolio_cash_percent=portfolio_cash_percent,
        portfolio_cash_source=portfolio_cash_source,
        timeframe=timeframe,
        sleep_duration=sleep_duration,
        include_extended_hours_data=include_extended_hours_data,
        use_current_candle=use_current_candle,
        rules=rules,
        overnight_history_path=overnight_history_path,
        trade_stats=trade_stats,
        status_writer=write_status,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
