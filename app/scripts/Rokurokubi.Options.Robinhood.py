#!/usr/bin/env python3
"""
Rokurokubi.Options.Robinhood.py (Web-App Compatible)

Strategy overview:
- Uses MA20/78/190 + RSI-derivative signals for stock buy/sell intent.
- On sell intent, attempts a covered call harvest (sell-to-open calls) instead of selling shares.
- Optional stop-loss can sell whole shares and disarms after a successful liquidation.
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

import numpy as np
import requests

try:
    import robin_stocks.robinhood as rh  # type: ignore
except Exception as e:
    raise RuntimeError(
        "robin_stocks is required for this script. Install with: pip install robin_stocks"
    ) from e

# -------------------------
# Ensure project imports work when executed as a script file
# -------------------------
THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.brokers.robinhood_connector import (  # noqa: E402
    _pickle_debug_info,
    _resolve_pickle_config,
    _restore_session_from_pickle,
)
from app.brokers.robin_stocks_adapter import place_option_order, place_stock_order  # noqa: E402
from app.db import get_broker_connection, read_connection_metadata, read_connection_secrets, set_broker_status  # noqa: E402


stoploss_state: Dict[str, Dict[str, Any]] = {}
_ACCOUNT_NUMBER: Optional[str] = None
CHART_POINTS = 90

timeframes = {
    "5m": {"interval": "5minute", "span": "week"},
    "10m": {"interval": "10minute", "span": "week"},
    "1h": {"interval": "hour", "span": "3month"},
    "1d": {"interval": "day", "span": "year"},
}

HISTORICAL_BOUNDS = "extended"
CC_SHORTLIST_MAX = 8
ATR_PERIOD = 14
MARKET_CODE = "XNYS"
MARKET_STATE_TTL = 60
_MARKET_STATE_CACHE: Dict[str, Any] = {"ts": 0, "state": "regular"}


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


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

    _, _, pickle_file = _resolve_pickle_config(
        db_path=db_path, connection_id=int(connection_id), secrets=secrets
    )
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


def _status_code(exc: BaseException) -> Optional[int]:
    return getattr(getattr(exc, "response", None), "status_code", None)


def safe_sleep(seconds: float) -> None:
    time.sleep(max(0.0, float(seconds)))


def safe_stock_quote(ticker: str, retries: int = 3, backoff: float = 0.8) -> dict:
    for attempt in range(retries):
        try:
            quote = rh.stocks.get_stock_quote_by_symbol(ticker)
            if not quote or quote.get("last_trade_price") in (None, "None"):
                raise ValueError(f"Quote missing last_trade_price for {ticker}: {quote}")
            return quote
        except requests.HTTPError as e:
            if _status_code(e) == 429 and attempt < retries - 1:
                safe_sleep(backoff * (2 ** attempt))
                continue
            raise
        except Exception:
            if attempt < retries - 1:
                safe_sleep(backoff * (2 ** attempt))
                continue
            raise
    raise RuntimeError(f"Failed to fetch quote for {ticker}")


def safe_stock_historicals(
    ticker: str, interval: str, span: str, *, bounds: str = "regular", retries: int = 3, backoff: float = 0.8
) -> list:
    for attempt in range(retries):
        try:
            h = rh.stocks.get_stock_historicals(ticker, interval=interval, span=span, bounds=bounds)
            if not h:
                raise ValueError(f"Empty historicals for {ticker} ({interval}, {span})")
            return h
        except requests.HTTPError as e:
            if _status_code(e) == 429 and attempt < retries - 1:
                safe_sleep(backoff * (2 ** attempt))
                continue
            raise
        except Exception:
            if attempt < retries - 1:
                safe_sleep(backoff * (2 ** attempt))
                continue
            raise
    raise RuntimeError(f"Failed to fetch historicals for {ticker}")


def safe_option_market_data(
    symbol: str, exp: str, strike: float, option_type: str = "call", retries: int = 3, backoff: float = 0.8
) -> dict:
    for attempt in range(retries):
        try:
            md_list = rh.options.get_option_market_data(symbol, exp, str(strike), option_type)
            if not md_list or md_list[0] is None:
                raise ValueError(f"Empty option market data for {symbol} {exp} {strike} {option_type}")
            return md_list[0]
        except requests.HTTPError as e:
            if _status_code(e) == 429 and attempt < retries - 1:
                safe_sleep(backoff * (2 ** attempt))
                continue
            raise
        except Exception:
            if attempt < retries - 1:
                safe_sleep(backoff * (2 ** attempt))
                continue
            raise
    raise RuntimeError(f"Failed to fetch option market data for {symbol} {exp} {strike} {option_type}")


def safe_option_market_data_by_id(option_id: str, retries: int = 3, backoff: float = 0.8) -> dict:
    for attempt in range(retries):
        try:
            md_list = rh.options.get_option_market_data_by_id(option_id)
            if not md_list or md_list[0] is None:
                raise ValueError(f"Empty option market data for id={option_id}")
            return md_list[0]
        except requests.HTTPError as e:
            if _status_code(e) == 429 and attempt < retries - 1:
                safe_sleep(backoff * (2 ** attempt))
                continue
            raise
        except Exception:
            if attempt < retries - 1:
                safe_sleep(backoff * (2 ** attempt))
                continue
            raise
    raise RuntimeError(f"Failed to fetch option market data for id={option_id}")


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        if val in (None, "None"):
            return default
        return float(val)
    except Exception:
        return default


def _parse_iso_ts(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text.replace("Z", "+00:00")
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _market_session_state() -> str:
    now = time.time()
    if now - float(_MARKET_STATE_CACHE.get("ts", 0)) < MARKET_STATE_TTL:
        return str(_MARKET_STATE_CACHE.get("state", "regular"))
    state = "regular"
    try:
        hours = rh.markets.get_market_today_hours(MARKET_CODE)
        if isinstance(hours, dict):
            now_dt = datetime.now(timezone.utc)
            reg_open = _parse_iso_ts(hours.get("opens_at"))
            reg_close = _parse_iso_ts(hours.get("closes_at"))
            ext_open = _parse_iso_ts(hours.get("extended_opens_at"))
            ext_close = _parse_iso_ts(hours.get("extended_closes_at"))
            if reg_open and reg_close and reg_open <= now_dt <= reg_close:
                state = "regular"
            elif ext_open and ext_close and ext_open <= now_dt <= ext_close:
                state = "extended"
            else:
                state = "closed"
    except Exception:
        state = "regular"
    _MARKET_STATE_CACHE["ts"] = now
    _MARKET_STATE_CACHE["state"] = state
    return state


def _price_from_quote(quote: dict, *, prefer_extended: bool) -> Optional[float]:
    if not isinstance(quote, dict):
        return None
    last = _safe_float(quote.get("last_trade_price"), default=0.0)
    last_ext = _safe_float(
        quote.get("last_extended_hours_trade_price")
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


def _mid_price(bid: float, ask: float, fallback: float) -> float:
    if bid > 0 and ask > 0:
        return (bid + ask) / 2.0
    if bid > 0:
        return bid
    if ask > 0:
        return ask
    return fallback


def _merge_historicals(base: List[Dict[str, Any]], extra: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not base:
        return extra
    if not extra:
        return base

    def _has_close(row: Dict[str, Any]) -> bool:
        val = row.get("close_price")
        return val not in (None, "None", "")

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
        if key:
            key = str(key)
            if key in merged:
                if _has_close(row) or not _has_close(merged[key]):
                    merged[key] = row
            else:
                merged[key] = row
    if not merged:
        return base
    return [merged[k] for k in sorted(merged.keys())]


def _order_success(resp: Any) -> bool:
    if hasattr(resp, "accepted") and hasattr(resp, "submitted"):
        return bool(resp.accepted and resp.submitted and not getattr(resp, "blocked", False))
    if isinstance(resp, dict):
        state = str(resp.get("state") or "").lower()
        if state:
            return state in ("queued", "confirmed", "filled")
        return bool(resp.get("id"))
    return bool(resp)


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
    ma190_full = _ma_series(prices, 190)
    if max_points > 0 and len(prices) > max_points:
        offset = len(prices) - max_points
        return {
            "price": [float(p) for p in prices[-max_points:]],
            "ma20": ma30_full[offset:],
            "ma78": ma78_full[offset:],
            "ma150": ma190_full[offset:],
        }
    return {
        "price": [float(p) for p in prices],
        "ma20": ma30_full,
        "ma78": ma78_full,
        "ma150": ma190_full,
    }


def reconnect_if_needed(func: Callable[..., Any]) -> Callable[..., Any]:
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        db_path = kwargs.pop("_db_path", None)
        connection_id = kwargs.pop("_connection_id", None)

        try:
            return func(*args, **kwargs)
        except (requests.RequestException, ConnectionError) as e:
            if db_path is not None and connection_id is not None:
                print(f"[WARN] Network error: {e}. Attempting Robinhood session restore...")
                ensure_robinhood_session(db_path, int(connection_id))
            safe_sleep(2.0)
            return func(*args, **kwargs)
        except Exception:
            raise

    return wrapper


@reconnect_if_needed
def get_stock_quote(symbol: str) -> dict:
    quote = safe_stock_quote(symbol)
    if not isinstance(quote, dict):
        raise RuntimeError(f"Failed to get quote for {symbol}")
    return quote


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
            symbol,
            interval=interval,
            span="day",
            bounds=HISTORICAL_BOUNDS,
            retries=1,
            backoff=0.5,
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


def build_positions_map(positions: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    pos_map: Dict[str, Dict[str, float]] = {}
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
        pos_map[sym] = {
            "quantity": _safe_float(pos.get("quantity")),
            "average_buy_price": _safe_float(pos.get("average_buy_price")),
        }
    return pos_map


def calculate_moving_average(prices: List[float], window_size: int) -> Optional[float]:
    if len(prices) < window_size:
        return None
    return float(np.mean(prices[-window_size:]))


def calculate_rsi(prices: List[float], period: int = 14) -> Optional[float]:
    if len(prices) < period + 1:
        return None

    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0.0)
    losses = np.where(deltas < 0, -deltas, 0.0)

    average_gain = float(np.sum(gains[:period]) / period)
    average_loss = float(np.sum(losses[:period]) / period)

    for i in range(period, len(deltas)):
        average_gain = ((average_gain * (period - 1)) + float(gains[i])) / period
        average_loss = ((average_loss * (period - 1)) + float(losses[i])) / period

    if average_loss == 0:
        return 100.0

    rs = average_gain / average_loss
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
    qty: float,
) -> bool:
    if rsi is None or rsi_derivative is None or ma30_derivative is None:
        return False
    if None in (ma30, ma78, ma190):
        return False
    sell_condition_a = (
        qty > 0
        and current_price > ma30
        and current_price > ma78
        and current_price > ma190
        and rsi > 70
        and rsi_derivative < 0
    )
    sell_condition_b = (
        qty > 0
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


def _to_date(s: str) -> datetime.date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def round_down_to_tick(price: float, tick: float) -> float:
    if tick <= 0:
        raise ValueError("Tick must be > 0")
    return math.floor(price / tick) * tick


def list_candidate_calls(symbol: str, spot: float, max_dte: int, strike_below: int, strike_above: int):
    chain = rh.options.get_chains(symbol)
    exps = chain.get("expiration_dates", []) if isinstance(chain, dict) else []
    today = datetime.utcnow().date()

    min_strike = math.floor(spot) - int(strike_below)
    max_strike = math.floor(spot) + int(strike_above)

    for exp in exps:
        dte = (_to_date(exp) - today).days
        if dte < 0 or dte > max_dte:
            continue

        # Robin_stocks helper enriches each option with market data by instrument id.
        opts = rh.options.find_options_by_expiration(symbol, exp, optionType="call")
        if not isinstance(opts, list):
            continue
        for o in opts:
            if not isinstance(o, dict):
                continue
            strike_str = str(o.get("strike_price") or "").strip()
            if not strike_str:
                continue
            strike = float(strike_str)
            if strike < min_strike or strike > max_strike:
                continue
            yield {
                "expiration": exp,
                "strike": strike,
                "strike_str": strike_str,
                "option_id": o.get("id"),
                "bid_price": o.get("bid_price"),
                "ask_price": o.get("ask_price"),
            }


def score_call(symbol: str, candidate: Dict[str, Any], avg_cost: float, min_bid: float):
    exp = str(candidate.get("expiration") or "")
    strike = float(candidate.get("strike") or 0.0)
    strike_str = str(candidate.get("strike_str") or strike)
    option_id = candidate.get("option_id")
    result = {
        "expiration": exp,
        "strike": float(strike),
        "strike_str": strike_str,
        "option_id": option_id,
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
    }
    bid = candidate.get("bid_price")
    ask = candidate.get("ask_price")
    if bid in (None, "None") or ask in (None, "None"):
        try:
            if option_id:
                md = safe_option_market_data_by_id(str(option_id))
            else:
                md = safe_option_market_data(symbol, exp, strike_str, "call")
            bid = md.get("bid_price")
            ask = md.get("ask_price")
        except Exception as e:
            result["reject_reason"] = f"market_data_error:{type(e).__name__}"
            return result

    if bid in (None, "None") or ask in (None, "None"):
        result["reject_reason"] = "missing_bid_or_ask"
        return result

    bid = float(bid)
    ask = float(ask)
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

    # Priority: highest breakeven; tie-break by highest capped gain % then highest bid.
    result["score"] = (breakeven, capped_gain_pct, bid)
    result["qualifies"] = True
    return result


def choose_best_call(
    symbol: str,
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

    for candidate in list_candidate_calls(symbol, spot, max_dte, strike_below, strike_above):
        checked += 1
        scored = score_call(symbol, candidate, avg_cost, min_bid)
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
        "market_data_error:RuntimeError": "Market data error",
        "missing_bid_or_ask": "Missing bid/ask",
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


def is_option_order_open(order_id: str) -> bool:
    try:
        open_orders = rh.orders.get_all_open_option_orders()
        if not open_orders:
            return False
        return any(o.get("id") == order_id for o in open_orders)
    except Exception as e:
        print(f"[WARN] Could not fetch open option orders: {e}")
        return True


def cancel_option_order(order_id: str) -> None:
    try:
        rh.orders.cancel_option_order(order_id)
        print(f"[ORDER] Canceled option order {order_id}")
    except Exception as e:
        print(f"[WARN] Cancel failed for {order_id}: {e}")


def place_sell_to_open_limit(
    symbol: str,
    exp: str,
    strike: Any,
    limit_price: float,
    qty_contracts: int,
    tif: str,
) -> Any:
    return place_option_order(
        side="sell",
        order_type="limit",
        positionEffect="open",
        creditOrDebit="credit",
        price=limit_price,
        symbol=symbol,
        quantity=qty_contracts,
        expirationDate=exp,
        strike=str(strike),
        optionType="call",
        timeInForce=tif,
    )


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

    best, meta = choose_best_call(
        ticker,
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
    print(
        f"[CC] {ticker}: Cap gain at exp = ${capped_gain_per_share:.2f}/sh "
        f"({capped_gain_pct:.2f}%) = ${capped_gain_per_contract:.2f}/contract"
    )

    order_id = None
    working_limit = None
    mode = None
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

        try:
            if best.get("option_id"):
                md = safe_option_market_data_by_id(str(best["option_id"]))
            else:
                md = safe_option_market_data(ticker, best["expiration"], best.get("strike_str", best["strike"]), "call")
        except Exception as e:
            print(f"[CC] {ticker}: Market data refresh failed for selected contract: {e}")
            time.sleep(poll_seconds)
            continue
        bid = float(md.get("bid_price") or 0.0)
        ask = float(md.get("ask_price") or 0.0)
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

            resp = place_sell_to_open_limit(
                symbol=ticker,
                exp=best["expiration"],
                strike=best.get("strike_str", best["strike"]),
                limit_price=new_limit,
                qty_contracts=lots,
                tif=tif,
            )
            order_id = resp.get("id") if isinstance(resp, dict) else None
            working_limit = new_limit
            mode = new_mode
            reprices += 1
            if trade_stats is not None and _order_success(resp):
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
    best, meta = choose_best_call(
        ticker,
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
                if session_state == "extended":
                    limit_price = limit_mid if limit_mid and limit_mid > 0 else current_price
                    response = place_stock_order(
                        symbol=ticker,
                        side="sell",
                        order_type="limit",
                        quantity=sell_qty,
                        limitPrice=float(limit_price),
                        timeInForce="gfd",
                        extendedHours=True,
                    )
                elif session_state == "regular":
                    response = place_stock_order(
                        symbol=ticker,
                        side="sell",
                        order_type="market",
                        quantity=sell_qty,
                        timeInForce="gfd",
                    )
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
                print(f"Order state: {response.get('state') if isinstance(response, dict) else 'unknown'}.")
            except Exception as e:
                print(f"Error placing sell order for {ticker}: {e}")

    return None


def load_params(params_path: str) -> dict[str, Any]:
    p = Path(params_path)
    obj = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError("params-json must be a JSON object.")
    return obj


def main_trading_loop(
    *,
    db_path: str,
    connection_id: int,
    symbols: List[str],
    shares_per_trade: int,
    enable_stock_buys: bool,
    target_gain_pct: float,
    stop_loss_pct: float,
    stoploss_enabled: bool,
    timeframe: str,
    sleep_duration: float,
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
    if timeframe not in timeframes:
        raise ValueError(f"Invalid timeframe '{timeframe}'. Choose from {list(timeframes.keys())}.")

    interval = timeframes[timeframe]["interval"]
    span = timeframes[timeframe]["span"]

    print(f"Using timeframe: {timeframe} ({interval}, {span})")
    print(f"Symbols: {symbols}")

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

        tickers_status: List[Dict[str, Any]] = []
        positions_map: Dict[str, Dict[str, float]] = {}
        try:
            positions = get_open_stock_positions(_db_path=db_path, _connection_id=connection_id)
            positions_map = build_positions_map(positions)
        except Exception:
            positions_map = {}

        for symbol in symbols:
            try:
                quote = get_stock_quote(symbol, _db_path=db_path, _connection_id=connection_id)
                current_price = _price_from_quote(quote, prefer_extended=session_state == "extended")
                if current_price is None:
                    raise RuntimeError(f"Missing price for {symbol}")
                bid_price = _safe_float(quote.get("bid_price"), default=0.0)
                ask_price = _safe_float(quote.get("ask_price"), default=0.0)
                mid_price = _mid_price(bid_price, ask_price, current_price)

                def _load_hlc(hist_interval: str, hist_span: str) -> Tuple[List[float], List[float], List[float]]:
                    h = get_stock_historicals(
                        symbol,
                        hist_interval,
                        hist_span,
                        _db_path=db_path,
                        _connection_id=connection_id,
                    )
                    highs: List[float] = []
                    lows: List[float] = []
                    closes_out: List[float] = []
                    for row in h:
                        try:
                            highs.append(float(row.get("high_price")))
                            lows.append(float(row.get("low_price")))
                            closes_out.append(float(row.get("close_price")))
                        except Exception:
                            continue
                    return highs, lows, closes_out

                hist_interval = interval
                hist_span = span
                if session_state == "extended" and timeframe in ("1h", "1d"):
                    hist_interval = "10minute"
                    hist_span = "week"

                highs, lows, closes = _load_hlc(hist_interval, hist_span)
                if session_state == "extended" and timeframe in ("1h", "1d"):
                    if (len(closes) + 1) < 190:
                        hist_interval = "5minute"
                        hist_span = "week"
                        highs, lows, closes = _load_hlc(hist_interval, hist_span)

                if len(closes) < 10:
                    print(f"[{symbol}] Not enough historicals to compute indicators.")
                    tickers_status.append(
                        {
                            "symbol": symbol,
                            "signal": "NO_DATA",
                        }
                    )
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
                            if session_state == "extended":
                                limit_price = mid_price
                                resp = place_stock_order(
                                    symbol=symbol,
                                    side="buy",
                                    order_type="limit",
                                    quantity=int(shares_per_trade),
                                    limitPrice=float(limit_price),
                                    timeInForce="gfd",
                                    extendedHours=True,
                                )
                            elif session_state == "regular":
                                resp = place_stock_order(
                                    symbol=symbol,
                                    side="buy",
                                    order_type="market",
                                    quantity=int(shares_per_trade),
                                    timeInForce="gfd",
                                )
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
                tickers_status.append(
                    {
                        "symbol": symbol,
                        "signal": "ERROR",
                    }
                )

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

        print(r"""  
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
""")
        print(f"Sleeping {sleep_duration} seconds...\n")
        time.sleep(float(sleep_duration))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True, help="Run directory allocated by the web app.")
    ap.add_argument("--params-json", required=True, help="Path to params.json provided by the web app.")
    ap.add_argument("--db-path", required=True, help="Path to Cryptid Exchange sqlite DB.")
    ap.add_argument("--connection-id", required=True, type=int, help="broker_connections.id for Robinhood.")
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    status_path = run_dir / "status.json"
    last_status: Dict[str, Any] = {}
    trade_stats: Dict[str, Any] = {"pnl": 0.0, "trades": 0}

    def write_status(payload: Dict[str, Any]) -> None:
        payload = dict(payload)
        payload["ts"] = iso_now()
        payload["script"] = "Rokurokubi.Options.Robinhood"
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
    timeframe = str(params.get("timeframe", "10m"))

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

    ensure_robinhood_session(args.db_path, int(args.connection_id))

    main_trading_loop(
        db_path=args.db_path,
        connection_id=int(args.connection_id),
        symbols=symbols,
        shares_per_trade=shares_per_trade,
        enable_stock_buys=enable_stock_buys,
        target_gain_pct=target_gain_pct,
        stop_loss_pct=stop_loss_pct,
        stoploss_enabled=stoploss_enabled,
        timeframe=timeframe,
        sleep_duration=sleep_duration,
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
