#!/usr/bin/env python3
"""
Dreadfox.Crypto.Robinhood.py (Web-App Compatible)

Refactor goals:
- NO interactive input() prompts
- Uses Cryptid Exchange broker_connections to restore Robinhood session (pickle-based)
- Reads runtime parameters from --params-json
- Preserves original crypto indicator logic, stop-loss arming/trigger behavior, and rate-limit helpers
- Optional sound playback (disabled by default; safe on headless systems)

Required CLI args (expected by the web app launcher):
  --run-dir <path>
  --params-json <path>
  --db-path <path_to_sqlite>
  --connection-id <int>   (Robinhood broker_connections.id)
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
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
    raise RuntimeError("robin_stocks is required. Install with: pip install robin_stocks") from e

# -------------------------
# Ensure project imports work when executed as a script file
# -------------------------
THIS_FILE = Path(__file__).resolve()
PROJECT_ROOT = THIS_FILE.parents[2]  # .../<root>/app/scripts/this.py -> parents[2] is <root>
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.brokers.robinhood_connector import (  # noqa: E402
    _pickle_debug_info,
    _resolve_pickle_config,
    _restore_session_from_pickle,
)
from app.brokers.robin_stocks_adapter import place_crypto_order  # noqa: E402
from app.db import (  # noqa: E402
    get_broker_connection,
    read_connection_metadata,
    read_connection_secrets,
    set_broker_status,
)


# -------------------------
# Optional sound support (safe on headless)
# -------------------------
_ENABLE_SOUNDS = False
sounds: Dict[str, Any] = {}


@contextlib.contextmanager
def suppress_pygame_output():
    with open(os.devnull, "w") as fnull:
        original_stdout = sys.stdout
        sys.stdout = fnull
        try:
            yield
        finally:
            sys.stdout = original_stdout


def init_sounds(enable: bool) -> None:
    global _ENABLE_SOUNDS, sounds
    _ENABLE_SOUNDS = bool(enable)
    if not _ENABLE_SOUNDS:
        sounds = {}
        return

    try:
        import pygame  # type: ignore
    except Exception as e:
        print(f"[WARN] enable_sounds=true but pygame not available: {e}")
        _ENABLE_SOUNDS = False
        sounds = {}
        return

    with suppress_pygame_output():
        try:
            pygame.init()
            pygame.mixer.init()
        except Exception as e:
            print(f"[WARN] Failed to initialize pygame mixer: {e}")
            _ENABLE_SOUNDS = False
            sounds = {}
            return

    sound_dir = os.path.join(os.path.dirname(__file__), "sounds")

    def load_sound(file_name: str):
        try:
            path = os.path.join(sound_dir, file_name)
            if not os.path.exists(path):
                print(f"[WARN] Sound file not found: {path}")
                return None
            return pygame.mixer.Sound(path)
        except Exception as e:
            print(f"[WARN] Failed to load sound {file_name}: {e}")
            return None

    sounds = {
        "Buy": load_sound("rpg_fire.wav"),
        "Sell": load_sound("chaching_sell.wav"),
        "Hold": load_sound("storm_hold.wav"),
    }


def play_sound(action: str) -> None:
    if not _ENABLE_SOUNDS:
        return
    sound = sounds.get(action)
    if sound:
        try:
            sound.play()
        except Exception as e:
            print(f"[WARN] Failed to play sound for '{action}': {e}")


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# -------------------------
# Robinhood session restore via DB + pickle path
# -------------------------
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


# -------------------------
# Original script globals
# -------------------------
CHART_POINTS = 90
stoploss_state: Dict[str, Dict[str, Any]] = {}

timeframes = {
    "5m": {"interval": "5minute", "span": "day"},
    "10m": {"interval": "10minute", "span": "week"},
    "1h": {"interval": "hour", "span": "month"},
    "1d": {"interval": "day", "span": "year"},
    "15s": {"interval": "15second", "span": "hour"},
}


# ========= Rate-limit-safe helpers =========
def _status_code(exc: BaseException) -> Optional[int]:
    return getattr(getattr(exc, "response", None), "status_code", None)


def safe_sleep(seconds: float) -> None:
    print(f"[RATE LIMIT] Cooling down for {seconds:.2f}s...")
    time.sleep(max(0.0, float(seconds)))


def safe_crypto_quote(ticker: str, retries: int = 3, backoff: float = 0.8) -> dict:
    """
    Returns a crypto quote dict, retrying on 429 and transient errors.
    """
    for attempt in range(retries):
        try:
            quote = rh.crypto.get_crypto_quote(ticker)
            if not quote or quote.get("mark_price") in (None, "None"):
                raise ValueError(f"Quote missing mark_price for {ticker}: {quote}")
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


def safe_crypto_historicals(
    ticker: str, interval: str, span: str, retries: int = 3, backoff: float = 0.8
) -> list:
    """
    Returns crypto historicals, retrying on 429 and transient errors.
    """
    for attempt in range(retries):
        try:
            h = rh.crypto.get_crypto_historicals(
                ticker, interval=interval, span=span, bounds="24_7"
            )
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


def _to_int_opt(value: Any) -> Optional[int]:
    try:
        if value in (None, "None", ""):
            return None
        return int(float(value))
    except Exception:
        return None


def _to_float_opt(value: Any) -> Optional[float]:
    try:
        if value in (None, "None", ""):
            return None
        return float(value)
    except Exception:
        return None


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


def _order_success(resp: Any) -> bool:
    if hasattr(resp, "accepted") and hasattr(resp, "submitted"):
        return bool(resp.accepted and resp.submitted and not getattr(resp, "blocked", False))
    if isinstance(resp, dict):
        http_status = _to_int_opt(resp.get("_http_status"))
        if http_status is not None and http_status >= 400 and not resp.get("id"):
            return False
        state = str(resp.get("state") or "").lower()
        if state:
            return state in ("queued", "confirmed", "filled")
        return bool(resp.get("id"))
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


# Decorator: reconnect once on transient failure by restoring session from pickle
def reconnect_if_needed(func: Callable[..., Any]) -> Callable[..., Any]:
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        db_path = kwargs.pop("_db_path", None)
        connection_id = kwargs.pop("_connection_id", None)

        try:
            return func(*args, **kwargs)
        except (requests.ConnectionError, requests.exceptions.RequestException) as e:
            print(f"[WARN] Connection error: {e}. Attempting session restore then retry...")
            if db_path is not None and connection_id is not None:
                ensure_robinhood_session(str(db_path), int(connection_id))
            time.sleep(2)
            return func(*args, **kwargs)
        except Exception:
            # Do not infinite-loop on auth errors; bubble up so UI can show failure.
            raise

    return wrapper


@reconnect_if_needed
def get_crypto_price(ticker: str) -> float:
    quote = safe_crypto_quote(ticker)
    return float(quote["mark_price"])


@reconnect_if_needed
def get_crypto_historicals(ticker: str, interval: str, span: str) -> List[float]:
    historicals = safe_crypto_historicals(ticker, interval, span)
    return [float(data["close_price"]) for data in historicals]


@reconnect_if_needed
def get_crypto_position(ticker: str) -> Tuple[float, float]:
    positions = rh.crypto.get_crypto_positions()
    for pos in positions:
        if pos["currency"]["code"] == ticker:
            quantity_available = float(pos["quantity_available"])
            if quantity_available == 0:
                return 0.0, 0.0
            avg_buy_price = float(pos["cost_bases"][0]["direct_cost_basis"]) / quantity_available
            return quantity_available, avg_buy_price
    return 0.0, 0.0


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
    ma190_full = _ma_series(prices, 190)
    if max_points > 0 and len(prices) > max_points:
        offset = len(prices) - max_points
        return {
            "price": [float(p) for p in prices[-max_points:]],
            "ma20": ma20_full[offset:],
            "ma78": ma78_full[offset:],
            "ma150": ma190_full[offset:],
        }
    return {
        "price": [float(p) for p in prices],
        "ma20": ma20_full,
        "ma78": ma78_full,
        "ma150": ma190_full,
    }


def calculate_rsi(prices: List[float], period: int = 14) -> Optional[float]:
    if len(prices) < period + 1:
        return None

    deltas = np.diff(prices)
    gains = np.where(deltas > 0, deltas, 0)
    losses = np.where(deltas < 0, -deltas, 0)

    average_gain = np.sum(gains[:period]) / period
    average_loss = np.sum(losses[:period]) / period

    for i in range(period, len(deltas)):
        average_gain = ((average_gain * (period - 1)) + gains[i]) / period
        average_loss = ((average_loss * (period - 1)) + losses[i]) / period

    if average_loss == 0:
        return 100.0

    rs = average_gain / average_loss
    rsi = 100 - (100 / (1 + rs))
    return float(rsi)


def calculate_rsi_and_derivative(prices: List[float], period: int = 14) -> Tuple[Optional[float], Optional[float]]:
    """
    Computes RSI and a 3-candle smoothed derivative:
      dRSI = (RSI_now - RSI_2_candles_back) / 2
    """
    if len(prices) < period + 3:
        return None, None

    rsi_t2 = calculate_rsi(prices[:-2], period)
    rsi_t1 = calculate_rsi(prices[:-1], period)
    rsi_t0 = calculate_rsi(prices, period)

    if rsi_t0 is None or rsi_t1 is None or rsi_t2 is None:
        return rsi_t0, None

    d_rsi = (rsi_t0 - rsi_t2) / 2.0
    return rsi_t0, float(d_rsi)


def should_buy(ticker: str, prices: List[float], *, db_path: str, connection_id: int) -> bool:
    moving_average_20 = calculate_moving_average(prices, 20)
    moving_average_78 = calculate_moving_average(prices, 78)
    moving_average_190 = calculate_moving_average(prices, 190)
    current_price = get_crypto_price(ticker, _db_path=db_path, _connection_id=connection_id)

    rsi, rsi_derivative = calculate_rsi_and_derivative(prices, period=14)
    if rsi is None or rsi_derivative is None:
        return False
    if None in (moving_average_20, moving_average_78, moving_average_190):
        return False

    return (
        current_price > moving_average_20
        and current_price < moving_average_78
        and current_price < moving_average_190
        and rsi < 55
        and rsi_derivative > 0.50
    )


def should_sell(ticker: str, prices: List[float], *, db_path: str, connection_id: int) -> bool:
    moving_average_20 = calculate_moving_average(prices, 20)
    moving_average_78 = calculate_moving_average(prices, 78)
    moving_average_190 = calculate_moving_average(prices, 190)
    current_price = get_crypto_price(ticker, _db_path=db_path, _connection_id=connection_id)

    bid_q = safe_crypto_quote(ticker)
    bid_price = float(bid_q.get("bid_price") or bid_q["mark_price"])

    rsi, rsi_derivative = calculate_rsi_and_derivative(prices, period=14)
    if rsi is None or rsi_derivative is None:
        return False

    quantity, avg_buy_price = get_crypto_position(ticker, _db_path=db_path, _connection_id=connection_id)
    if None in (moving_average_20, moving_average_78, moving_average_190):
        return False

    return (
        quantity > 0
        and bid_price > avg_buy_price
        and current_price > moving_average_20
        and current_price > moving_average_78
        and current_price > moving_average_190
        and rsi_derivative < 0.25
        and rsi > 69
    )


def print_indicator_signals(ticker: str, prices: List[float], *, db_path: str, connection_id: int) -> None:
    moving_average_20 = calculate_moving_average(prices, 20)
    moving_average_78 = calculate_moving_average(prices, 78)
    moving_average_190 = calculate_moving_average(prices, 190)
    current_price = get_crypto_price(ticker, _db_path=db_path, _connection_id=connection_id)

    rsi, rsi_derivative = calculate_rsi_and_derivative(prices, period=14)
    quantity, avg_buy_price = get_crypto_position(ticker, _db_path=db_path, _connection_id=connection_id)

    action = "Hold"
    percentage_gain_loss: Optional[float] = None
    if avg_buy_price > 0:
        percentage_gain_loss = ((current_price - avg_buy_price) / avg_buy_price) * 100

    if None not in (moving_average_20, moving_average_78, moving_average_190) and rsi is not None and rsi_derivative is not None:
        if current_price > moving_average_20:
            if (
                current_price > moving_average_20
                and current_price < moving_average_78
                and current_price < moving_average_190
                and rsi < 55
                and rsi_derivative > 0.50
            ):
                action = "Buy"
            elif (
                quantity > 0
                and current_price > moving_average_20
                and current_price > moving_average_78
                and current_price > moving_average_190
                and rsi_derivative < 0.25
                and rsi > 69
                and current_price > avg_buy_price
            ):
                action = "Sell"

    if action in ("Buy", "Sell"):
        play_sound(action)

    print(
        f"Ticker: {ticker}\n"
        f"Current Price: {current_price}\n"
        f"20-Period Moving Average: {moving_average_20}\n"
        f"78-Period Moving Average: {moving_average_78}\n"
        f"190-Period Moving Average: {moving_average_190}\n"
        f"RSI: {rsi}\n"
        f"RSI Derivative (3-candle): {rsi_derivative}\n"
        f"Quantity Held: {quantity}\n"
        f"Average Cost Basis: ${avg_buy_price:.2f} per unit\n"
        f"Percentage Gain/Loss: {'N/A' if percentage_gain_loss is None else f'{percentage_gain_loss:.2f}%'}\n"
        f"Determined Action: {action}\n"
        f"------------------------------"
    )


def check_stoploss_and_sell(
    avg_buy_price: float,
    ticker: str,
    target_gain_for_stoploss: float,
    stoploss_percentage: float,
    *,
    db_path: str,
    connection_id: int,
    trade_stats: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Preserved behavior:
    - Arm stop-loss after target gain is hit
    - If armed and price falls to trigger, sell all whole units
    - Disarm after a successful sell (or if no whole units remain)
    """
    current_price = get_crypto_price(ticker, _db_path=db_path, _connection_id=connection_id)
    percentage_gain = ((current_price - avg_buy_price) / avg_buy_price) * 100 if avg_buy_price else 0.0

    if ticker not in stoploss_state:
        stoploss_state[ticker] = {"armed": False}

    if not stoploss_state[ticker]["armed"] and percentage_gain >= target_gain_for_stoploss:
        stoploss_state[ticker]["armed"] = True
        print(f"Stop-loss armed for {ticker} with a percentage gain of {percentage_gain:.2f}%.")

    if stoploss_state[ticker]["armed"]:
        stoploss_trigger_price = avg_buy_price * (1 + stoploss_percentage / 100)
        if current_price <= stoploss_trigger_price:
            try:
                quantity_held, _ = get_crypto_position(ticker, _db_path=db_path, _connection_id=connection_id)
                if quantity_held <= 0:
                    print(f"[{ticker}] Stop-loss triggered but no units to sell (held={quantity_held}).")
                    stoploss_state[ticker]["armed"] = False
                    return

                position_value = float(quantity_held) * float(current_price)
                if position_value < 1.0:
                    print(f"[{ticker}] Stop-loss triggered but position value < $1.00; disarming.")
                    stoploss_state[ticker]["armed"] = False
                    return

                amount_dollars = int(round(position_value))
                if amount_dollars > position_value:
                    amount_dollars = int(position_value)
                if amount_dollars < 1:
                    print(f"[{ticker}] Stop-loss triggered but amount rounds below $1.00; disarming.")
                    stoploss_state[ticker]["armed"] = False
                    return

                resp = None
                order_ok = False
                raw_resp = place_crypto_order(
                    symbol=ticker,
                    side="sell",
                    order_type="market",
                    amountInDollars=float(amount_dollars),
                    jsonify=False,
                )
                resp = _normalize_order_response(raw_resp)
                order_ok = _order_success(resp)
                if (not order_ok) and _is_reprice_rejection(resp):
                    time.sleep(0.35)
                    raw_resp = place_crypto_order(
                        symbol=ticker,
                        side="sell",
                        order_type="market",
                        amountInDollars=float(amount_dollars),
                        jsonify=False,
                    )
                    resp = _normalize_order_response(raw_resp)
                    order_ok = _order_success(resp)
                if (not order_ok) and _is_reprice_rejection(resp):
                    limit_price = _compute_reprice_limit(ticker, "sell", float(current_price))
                    if limit_price is not None:
                        raw_resp = place_crypto_order(
                            symbol=ticker,
                            side="sell",
                            order_type="limit",
                            amountInDollars=float(amount_dollars),
                            limitPrice=float(limit_price),
                            jsonify=False,
                        )
                        resp = _normalize_order_response(raw_resp)
                        order_ok = _order_success(resp)
                else:
                    order_ok = False

                if order_ok:
                    print(
                        f"Stop-loss triggered for {ticker} at {current_price}. "
                        f"Submitted sell ~${amount_dollars}."
                    )
                    stoploss_state[ticker]["armed"] = False
                else:
                    reason = _order_failure_reason(resp)
                    print(f"[{ticker}] Stop-loss sell rejected: {reason} | resp={resp}")
                if trade_stats is not None and order_ok:
                    sold_qty = float(amount_dollars) / float(current_price) if current_price > 0 else 0.0
                    _record_trade(
                        trade_stats,
                        side="sell",
                        qty=float(sold_qty),
                        price=current_price,
                        avg_buy_price=avg_buy_price,
                    )
            except Exception as e:
                print(f"[{ticker}] Stop-loss sell failed: {e}")


def main_trading_loop(
    *,
    db_path: str,
    connection_id: int,
    trade_amount: float,
    tickers: List[str],
    target_gain_for_stoploss: float,
    stoploss_percentage: float,
    interval: str,
    span: str,
    trade_stats: Optional[Dict[str, Any]] = None,
    status_writer: Optional[Any] = None,
) -> None:
    tickers_status: List[Dict[str, Any]] = []
    for ticker in tickers:
        current_price = get_crypto_price(ticker, _db_path=db_path, _connection_id=connection_id)
        prices = get_crypto_historicals(ticker, interval, span, _db_path=db_path, _connection_id=connection_id)
        prices.append(current_price)  # inject current
        print_indicator_signals(ticker, prices, db_path=db_path, connection_id=connection_id)

        quantity, avg_buy_price = get_crypto_position(ticker, _db_path=db_path, _connection_id=connection_id)

        ma20 = calculate_moving_average(prices, 20)
        ma78 = calculate_moving_average(prices, 78)
        ma190 = calculate_moving_average(prices, 190)
        rsi, rsi_derivative = calculate_rsi_and_derivative(prices, period=14)

        buy_signal = should_buy(ticker, prices, db_path=db_path, connection_id=connection_id)
        sell_signal = should_sell(ticker, prices, db_path=db_path, connection_id=connection_id)
        signal = "SELL" if sell_signal else ("BUY" if buy_signal else "HOLD")
        pnl_pct: Optional[float] = None
        if avg_buy_price > 0:
            pnl_pct = ((current_price - avg_buy_price) / avg_buy_price) * 100.0

        tickers_status.append(
            {
                "symbol": ticker,
                "signal": signal,
                "price": current_price,
                "qty": quantity,
                "pnl_pct": pnl_pct,
                "ma20": ma20,
                "ma78": ma78,
                "ma150": ma190,
                "rsi": rsi,
                "rsi_d": rsi_derivative,
                "chart": _build_chart_series(prices) if status_writer is not None else {},
            }
        )

        if buy_signal:
            buy_price = get_crypto_price(ticker, _db_path=db_path, _connection_id=connection_id)
            try:
                raw_resp = place_crypto_order(
                    symbol=ticker,
                    side="buy",
                    order_type="market",
                    amountInDollars=float(trade_amount),
                    jsonify=False,
                )
                resp = _normalize_order_response(raw_resp)
                order_ok = _order_success(resp)
                if (not order_ok) and _is_reprice_rejection(resp):
                    time.sleep(0.35)
                    raw_resp = place_crypto_order(
                        symbol=ticker,
                        side="buy",
                        order_type="market",
                        amountInDollars=float(trade_amount),
                        jsonify=False,
                    )
                    resp = _normalize_order_response(raw_resp)
                    order_ok = _order_success(resp)
                if (not order_ok) and _is_reprice_rejection(resp):
                    limit_price = _compute_reprice_limit(ticker, "buy", float(buy_price))
                    if limit_price is not None:
                        raw_resp = place_crypto_order(
                            symbol=ticker,
                            side="buy",
                            order_type="limit",
                            amountInDollars=float(trade_amount),
                            limitPrice=float(limit_price),
                            jsonify=False,
                        )
                        resp = _normalize_order_response(raw_resp)
                        order_ok = _order_success(resp)
                if order_ok:
                    print(f"Bought ${trade_amount} of {ticker} at {buy_price}")
                    stoploss_state.setdefault(ticker, {})
                    stoploss_state[ticker]["armed"] = False
                    play_sound("Buy")
                else:
                    reason = _order_failure_reason(resp)
                    print(f"[{ticker}] Buy rejected: {reason} | resp={resp}")
                if trade_stats is not None and order_ok:
                    _record_trade(
                        trade_stats,
                        side="buy",
                        qty=0.0,
                        price=buy_price,
                        avg_buy_price=0.0,
                    )
            except Exception as e:
                print(f"[{ticker}] Buy failed: {e}")

        elif sell_signal and quantity > 0:
            try:
                raw_resp = place_crypto_order(
                    symbol=ticker,
                    side="sell",
                    order_type="market",
                    amountInDollars=float(trade_amount),
                    jsonify=False,
                )
                resp = _normalize_order_response(raw_resp)
                order_ok = _order_success(resp)
                if (not order_ok) and _is_reprice_rejection(resp):
                    time.sleep(0.35)
                    raw_resp = place_crypto_order(
                        symbol=ticker,
                        side="sell",
                        order_type="market",
                        amountInDollars=float(trade_amount),
                        jsonify=False,
                    )
                    resp = _normalize_order_response(raw_resp)
                    order_ok = _order_success(resp)
                if (not order_ok) and _is_reprice_rejection(resp):
                    limit_price = _compute_reprice_limit(ticker, "sell", float(current_price))
                    if limit_price is not None:
                        raw_resp = place_crypto_order(
                            symbol=ticker,
                            side="sell",
                            order_type="limit",
                            amountInDollars=float(trade_amount),
                            limitPrice=float(limit_price),
                            jsonify=False,
                        )
                        resp = _normalize_order_response(raw_resp)
                        order_ok = _order_success(resp)
                if order_ok:
                    print(f"Sold ${trade_amount} of {ticker}")
                    play_sound("Sell")
                else:
                    reason = _order_failure_reason(resp)
                    print(f"[{ticker}] Sell rejected: {reason} | resp={resp}")
                if trade_stats is not None and order_ok:
                    sell_qty = 0.0
                    if current_price > 0:
                        sell_qty = min(float(quantity), float(trade_amount) / current_price)
                    _record_trade(
                        trade_stats,
                        side="sell",
                        qty=sell_qty,
                        price=current_price,
                        avg_buy_price=avg_buy_price,
                    )
            except Exception as e:
                print(f"[{ticker}] Sell failed: {e}")

        if avg_buy_price > 0:
            check_stoploss_and_sell(
                avg_buy_price,
                ticker,
                target_gain_for_stoploss,
                stoploss_percentage,
                db_path=db_path,
                connection_id=connection_id,
                trade_stats=trade_stats,
            )

    if status_writer is not None:
        try:
            status_writer(
                {
                    "phase": "loop",
                    "tickers": tickers_status,
                }
            )
        except Exception:
            pass

    armed_tickers = [t for t, state in stoploss_state.items() if state.get("armed")]
    if armed_tickers:
        print("\nCurrently armed stop-losses:")
        for t in armed_tickers:
            print(f" - {t}: Stop-loss is armed.")
    else:
        print("\nNo tickers have an armed stop-loss.")


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
    ap.add_argument("--connection-id", required=True, type=int, help="broker_connections.id for Robinhood.")
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    status_path = run_dir / "status.json"
    last_status: Dict[str, Any] = {}
    trade_stats: Dict[str, Any] = {"pnl": 0.0, "trades": 0}

    def write_status(payload: Dict[str, Any]) -> None:
        payload = dict(payload)
        payload["ts"] = iso_now()
        payload["script"] = "Dreadfox.Crypto.Robinhood"
        payload["pnl"] = round(float(trade_stats.get("pnl", 0.0)), 2)
        payload["trades"] = int(trade_stats.get("trades", 0))
        try:
            status_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            last_status.clear()
            last_status.update(payload)
        except Exception:
            pass

    params = load_params(args.params_json)

    tickers = params.get("tickers", [])
    if isinstance(tickers, str):
        tickers = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    if not isinstance(tickers, list) or not tickers:
        raise ValueError("params.tickers must be a non-empty list (e.g., ['BTC','ETH']).")
    tickers = [str(t).strip().upper() for t in tickers if str(t).strip()]

    trade_amount = float(params.get("trade_amount", 10.0))
    target_gain_for_stoploss = float(params.get("target_gain_for_stoploss", 0.5))
    stoploss_percentage = float(params.get("stoploss_percentage", -0.5))
    sleep_duration = float(params.get("sleep_duration", 30))
    timeframe = str(params.get("timeframe", "10m")).strip()
    enable_sounds = bool(params.get("enable_sounds", False))

    if timeframe not in timeframes:
        print(f"[WARN] Invalid timeframe '{timeframe}'. Defaulting to '1h'.")
        timeframe = "1h"

    interval = timeframes[timeframe]["interval"]
    span = timeframes[timeframe]["span"]

    init_sounds(enable_sounds)

    # Ensure broker session is live before starting loop
    ensure_robinhood_session(args.db_path, int(args.connection_id))

    while True:
        main_trading_loop(
            db_path=args.db_path,
            connection_id=int(args.connection_id),
            trade_amount=trade_amount,
            tickers=tickers,
            target_gain_for_stoploss=target_gain_for_stoploss,
            stoploss_percentage=stoploss_percentage,
            interval=interval,
            span=span,
            trade_stats=trade_stats,
            status_writer=write_status,
        )

        print(
            r"""
                                                                   ,-, 
                                                             _.-=;~ /_\
                                                          _-~   '     ;.
                                                      _.-~     '   .-~-~`-._
                                                _.--~~:.             --.____88
                              ____.........--~~~. .' .  .        _..-------~~
                     _..--~~~~               .' .'             ,'
                 _.-~                        .       .     ` ,'
               .'                                    :.    ./
             .:     ,/          `                   ::.   ,'
           .:'     ,(            ;.                ::. ,-'
          .'     ./'..`.     . . /:::._______.... _/:.o/
         /     ./'.. . .)  . _.,'               `88;?88|
       ,'  . .,/'.__,-~ /_.o8P'                  88P ?8b
    _,' '. ,/' ,-'    d888P'                    88'  88|
 _.'~  . .,:oP'        ?88b              _..--- 88.--'8b.--..__
:     ...' 88o __,------.88o ...__..._.=~- .    `~~   `~~      ~-._ DreadFox.Crypto _.
`.;;;:='    ~~            ~~~                ~-    -       -   -
"""
        )
        print(f"Using timeframe: {timeframe} ({interval}, {span})")
        play_sound("Hold")
        time.sleep(max(0.0, sleep_duration))

    # unreachable
    # return 0


if __name__ == "__main__":
    raise SystemExit(main())
