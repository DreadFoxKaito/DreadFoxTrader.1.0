#!/usr/bin/env python3
"""
FoxScry.py (Web-App Compatible)

Original behaviors preserved:
- Superhexagon order placement flow and stop-loss mechanics
- "Hexagram" buy constraint: do not buy if ticker holdings exceed 1/6 of portfolio
- Trailing-stop sell for indicator-driven sells (3x ATR)
- Stop-loss arming/trigger logic (with disarm fixes)
- Prints a portfolio summary table at the start of each loop
- Infinite loop with user-defined sleep duration (now via params.json)

Refactor goals:
- NO interactive input() prompts
- Uses Cryptid Exchange broker_connections to restore Robinhood session (pickle-based)
- Reads runtime parameters from --params-json
- Compatible with the web app "scripts/" discovery + config builder

Required CLI args (expected by the web app launcher):
  --run-dir <path>
  --params-json <path>
  --db-path <path_to_sqlite>
  --connection-id <int>   (Robinhood broker_connections.id)
"""

from __future__ import annotations

import argparse
import json
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
PROJECT_ROOT = THIS_FILE.parents[2]  # .../<root>/app/scripts/this.py -> parents[2] is <root>
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.brokers.robinhood_connector import (  # noqa: E402
    _pickle_debug_info,
    _resolve_pickle_config,
    _restore_session_from_pickle,
)
from app.brokers.robin_stocks_adapter import place_stock_order  # noqa: E402
from app.db import (  # noqa: E402
    get_broker_connection,
    read_connection_metadata,
    read_connection_secrets,
    set_broker_status,
)


# Dictionary to track stop-loss activation per ticker
stoploss_state: Dict[str, Dict[str, Any]] = {}
_ACCOUNT_NUMBER: Optional[str] = None
CHART_POINTS = 90

# Define available timeframes
timeframes = {
    "5m": {"interval": "5minute", "span": "week"},
    "10m": {"interval": "10minute", "span": "week"},
    "1h": {"interval": "hour", "span": "3month"},
    "1d": {"interval": "day", "span": "year"},
}

ATR_PERIOD = 14
ATR_MULTIPLIER = 3.0
MIN_TRAIL_AMOUNT_USD = 0.01
HISTORICAL_BOUNDS = "extended"


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


# ========= Rate-limit-safe helpers (stocks) =========
def _status_code(exc: BaseException) -> Optional[int]:
    return getattr(getattr(exc, "response", None), "status_code", None)


def safe_sleep(seconds: float) -> None:
    print(f"[RATE LIMIT] Cooling down for {seconds:.2f}s...")
    time.sleep(max(0.0, float(seconds)))


def safe_stock_quote(ticker: str, retries: int = 3, backoff: float = 0.8) -> dict:
    """
    Returns a stock quote dict, retrying on 429 and transient errors.
    Raises after final attempt.
    """
    for attempt in range(retries):
        try:
            quote = rh.stocks.get_stock_quote_by_symbol(ticker)
            if not quote:
                raise ValueError(f"Empty quote for {ticker}")
            return quote
        except requests.HTTPError as e:
            if _status_code(e) == 429 and attempt < retries - 1:
                safe_sleep(backoff * (2**attempt))
                continue
            raise
        except Exception:
            if attempt < retries - 1:
                safe_sleep(backoff * (2**attempt))
                continue
            raise
    raise RuntimeError(f"Failed to fetch quote for {ticker}")


def safe_stock_historicals(
    ticker: str,
    interval: str,
    span: str,
    *,
    bounds: str = "regular",
    retries: int = 3,
    backoff: float = 0.8,
) -> list:
    """
    Returns stock historicals list, retrying on 429 and transient errors.
    """
    for attempt in range(retries):
        try:
            if bounds:
                h = rh.stocks.get_stock_historicals(ticker, interval=interval, span=span, bounds=bounds)
            else:
                h = rh.stocks.get_stock_historicals(ticker, interval=interval, span=span)
            if not h:
                raise ValueError(f"Empty historicals for {ticker} ({interval}, {span})")
            cleaned = [row for row in h if isinstance(row, dict)]
            if not cleaned:
                raise ValueError(f"Historicals missing rows for {ticker} ({interval}, {span})")
            return cleaned
        except requests.HTTPError as e:
            if _status_code(e) == 429 and attempt < retries - 1:
                safe_sleep(backoff * (2**attempt))
                continue
            raise
        except Exception:
            if attempt < retries - 1:
                safe_sleep(backoff * (2**attempt))
                continue
            raise
    raise RuntimeError(f"Failed to fetch historicals for {ticker}")


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        if val in (None, "None"):
            return default
        return float(val)
    except Exception:
        return default


def _whole_share_qty(value: Any, *, label: str) -> int:
    try:
        qty_float = float(value)
    except Exception as exc:
        raise ValueError(f"{label} must be a number, got {value!r}.") from exc
    qty_int = int(qty_float)
    if qty_float != float(qty_int):
        print(f"[WARN] {label} {qty_float} is not a whole share; using {qty_int}.")
    return qty_int


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

    return wrapper


@reconnect_if_needed
def get_stock_quote(ticker: str) -> dict:
    return safe_stock_quote(ticker)


@reconnect_if_needed
def get_stock_price(ticker: str, *, prefer_extended: Optional[bool] = None) -> float:
    quote = safe_stock_quote(ticker)
    if prefer_extended is None:
        prefer_extended = False
    price = _price_from_quote(quote, prefer_extended=bool(prefer_extended))
    if price is None:
        raise RuntimeError(f"Missing price for {ticker}")
    return float(price)


@reconnect_if_needed
def get_stock_historicals(ticker: str, interval: str, span: str) -> List[Dict[str, Any]]:
    data = safe_stock_historicals(ticker, interval, span, bounds="regular")
    if not isinstance(data, list):
        raise RuntimeError(f"Failed to get historicals for {ticker}")
    try:
        extra = safe_stock_historicals(
            ticker,
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


def _get_open_stock_positions() -> List[Dict[str, Any]]:
    account_number = _resolve_account_number()
    if not account_number:
        raise RuntimeError("Robinhood account_number missing; cannot fetch positions.")
    positions = rh.account.get_open_stock_positions(account_number=account_number)
    return positions if isinstance(positions, list) else []


@reconnect_if_needed
def get_stock_position(ticker: str) -> Tuple[float, float]:
    """
    Returns (quantity, avg_buy_price) for open stock positions.
    """
    positions = _get_open_stock_positions()
    for pos in positions:
        instrument_data = rh.account.get_instrument_by_url(pos["instrument"])
        if instrument_data.get("symbol") == ticker:
            quantity_available = float(pos["quantity"])
            avg_buy_price = float(pos["average_buy_price"])
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
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return float(rsi)


def _extract_hlc(rows: List[Dict[str, Any]]) -> Tuple[List[float], List[float], List[float]]:
    highs: List[float] = []
    lows: List[float] = []
    closes: List[float] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        hp = row.get("high_price")
        lp = row.get("low_price")
        cp = row.get("close_price")
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
    period: int = ATR_PERIOD,
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


@reconnect_if_needed
def get_portfolio_value() -> float:
    """
    Fetches the total portfolio value from Robinhood.
    """
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
def get_ticker_holding_value(
    ticker: str,
    *,
    db_path: str,
    connection_id: int,
    current_price: Optional[float] = None,
) -> float:
    """
    Calculates the total value of holdings for a specific ticker.
    """
    positions = _get_open_stock_positions()
    for pos in positions:
        instrument_data = rh.account.get_instrument_by_url(pos["instrument"])
        if instrument_data.get("symbol") == ticker:
            quantity = float(pos["quantity"])
            price = current_price
            if price is None:
                price = get_stock_price(ticker, _db_path=db_path, _connection_id=connection_id)
            return quantity * float(price)
    return 0.0


def _rsi_derivative(prices: List[float]) -> Optional[float]:
    """
    Original used: np.gradient([calculate_rsi(prices[:-1]), rsi])[-1]
    We'll preserve the same approach, but guard against None values.
    """
    rsi_now = calculate_rsi(prices)
    rsi_prev = calculate_rsi(prices[:-1]) if len(prices) > 1 else None
    if rsi_now is None or rsi_prev is None:
        return None
    return float(np.gradient([rsi_prev, rsi_now])[-1])


def _ma_derivative(prices: List[float], window_size: int) -> Optional[float]:
    if len(prices) < window_size + 1:
        return None
    ma_now = calculate_moving_average(prices, window_size)
    ma_prev = calculate_moving_average(prices[:-1], window_size)
    if ma_now is None or ma_prev is None:
        return None
    return float(ma_now - ma_prev)


def should_buy(
    ticker: str,
    prices: List[float],
    *,
    db_path: str,
    connection_id: int,
    portfolio_cap_mode: str,
    portfolio_cap_percent_by_symbol: Dict[str, float],
    portfolio_cap_percent: float,
    portfolio_cap_divisor: int,
    num_shares: int,
) -> bool:
    """
    FoxScry buy condition:
    - price < MA190
    - price < MA78
    - price > MA30
    - RSI derivative > 0
    - MA30 derivative > 0
    Plus the same portfolio cap limit as Superhexagon.
    """
    ma30 = calculate_moving_average(prices, 30)
    ma78 = calculate_moving_average(prices, 78)
    ma150 = calculate_moving_average(prices, 190)

    current_price = prices[-1] if prices else 0.0
    rsi_derivative = _rsi_derivative(prices)
    ma30_derivative = _ma_derivative(prices, 30)

    if None in (ma30, ma78, ma150, rsi_derivative, ma30_derivative):
        return False

    # Hexagram portfolio cap (1/divisor)
    portfolio_value = get_portfolio_value(_db_path=db_path, _connection_id=connection_id)
    buying_power = get_buying_power(_db_path=db_path, _connection_id=connection_id)
    available_cash = get_available_cash(_db_path=db_path, _connection_id=connection_id)
    ticker_value = get_ticker_holding_value(
        ticker,
        db_path=db_path,
        connection_id=connection_id,
        current_price=current_price,
    )
    divisor = max(2, int(portfolio_cap_divisor))
    buy_order_cost = float(num_shares) * float(current_price)

    if portfolio_cap_mode == "percent":
        ticker_cap_pct = max(
            0.01,
            float(portfolio_cap_percent_by_symbol.get(str(ticker).strip().upper(), portfolio_cap_percent)),
        )
        ticker_cap_value = portfolio_value * (ticker_cap_pct / 100.0)
        if ticker_value > ticker_cap_value:
            print(
                f"Cannot buy {ticker}. Current holdings ${ticker_value:.2f} exceed "
                f"{ticker_cap_pct:.2f}% of portfolio (${ticker_cap_value:.2f})."
            )
            return False
    elif ticker_value > (portfolio_value / divisor):
        print(
            f"Cannot buy {ticker}. Current holdings ${ticker_value:.2f} exceed "
            f"1/{divisor} of portfolio (${portfolio_value:.2f})."
        )
        return False
    if buying_power < buy_order_cost:
        print(f"Cannot buy {ticker}. Need ${buy_order_cost:.2f}, buying power ${buying_power:.2f}.")
        return False
    cash_target_value = portfolio_value / divisor if portfolio_value > 0 else 0.0
    if portfolio_cap_mode != "percent" and available_cash < cash_target_value:
        print(
            f"Cannot buy {ticker}. Current available cash ${available_cash:.2f} "
            f"is below 1/{divisor} cash target (${cash_target_value:.2f})."
        )
        return False

    return (
        current_price > ma30
        and current_price < ma78
        and current_price < ma150
        and rsi_derivative > 0
        and ma30_derivative > 0
    )


def should_sell(ticker: str, prices: List[float], *, db_path: str, connection_id: int) -> bool:
    ma30 = calculate_moving_average(prices, 30)
    ma78 = calculate_moving_average(prices, 78)
    ma150 = calculate_moving_average(prices, 190)

    current_price = prices[-1] if prices else 0.0
    rsi = calculate_rsi(prices)
    rsi_derivative = _rsi_derivative(prices)
    ma30_derivative = _ma_derivative(prices, 30)
    quantity, _ = get_stock_position(ticker, _db_path=db_path, _connection_id=connection_id)

    if None in (ma30, ma78, ma150, rsi, rsi_derivative, ma30_derivative):
        return False

    sell_condition_a = (
        current_price > ma150
        and current_price > ma78
        and current_price > ma30
        and rsi > 70
        and rsi_derivative < 0
    )
    sell_condition_b = (
        current_price > ma150
        and current_price > ma78
        and ma30_derivative < 0
    )

    return (
        quantity > 0
        and (sell_condition_a or sell_condition_b)
    )


def print_indicator_signals(
    ticker: str,
    prices: List[float],
    *,
    db_path: str,
    connection_id: int,
    buy_signal: Optional[bool] = None,
    sell_signal: Optional[bool] = None,
) -> None:
    """
    Prints diagnostics for trading indicators and percentage gain/loss.
    Preserves original printing fields + action decision.
    """
    ma30 = calculate_moving_average(prices, 30)
    ma78 = calculate_moving_average(prices, 78)
    ma150 = calculate_moving_average(prices, 190)

    current_price = prices[-1] if prices else 0.0
    rsi = calculate_rsi(prices)
    rsi_derivative = _rsi_derivative(prices)
    ma30_derivative = _ma_derivative(prices, 30)

    quantity, avg_buy_price = get_stock_position(ticker, _db_path=db_path, _connection_id=connection_id)
    action = "Hold"
    percentage_gain_loss: Optional[float] = None

    if avg_buy_price > 0:
        percentage_gain_loss = ((current_price - avg_buy_price) / avg_buy_price) * 100

    if sell_signal is True:
        action = "Sell"
    elif buy_signal is True:
        action = "Buy"
    elif None not in (ma30, ma78, ma150, rsi, rsi_derivative, ma30_derivative):
        if (
            current_price < ma150
            and current_price < ma78
            and current_price > ma30
            and rsi_derivative > 0
            and ma30_derivative > 0
        ):
            action = "Buy"
        elif (
            quantity > 0
            and (
                (
                    current_price > ma150
                    and current_price > ma78
                    and current_price > ma30
                    and rsi > 70
                    and rsi_derivative < 0
                )
                or (
                    current_price > ma150
                    and current_price > ma78
                    and ma30_derivative < 0
                )
            )
        ):
            action = "Sell"

    stop_state = stoploss_state.get(ticker) or {}
    stop_armed = bool(stop_state.get("armed"))
    stop_peak = stop_state.get("peak_price")
    stop_trigger = stop_state.get("trigger_price")
    if stop_armed and avg_buy_price > 0 and stop_peak:
        stop_peak_pct = ((float(stop_peak) - avg_buy_price) / avg_buy_price) * 100.0
    else:
        stop_peak_pct = None

    print(
        f"Ticker: {ticker}\n"
        f"Current Price: {current_price}\n"
        f"30-Period Moving Average: {ma30}\n"
        f"78-Period Moving Average: {ma78}\n"
        f"190-Period Moving Average: {ma150}\n"
        f"RSI: {rsi}\n"
        f"RSI Derivative: {rsi_derivative}\n"
        f"MA30 Derivative: {ma30_derivative}\n"
        f"Quantity Held: {quantity}\n"
        f"Average Buy Price: {avg_buy_price}\n"
        f"Percentage Gain/Loss: {'N/A' if percentage_gain_loss is None else f'{percentage_gain_loss:.2f}%'}\n"
        f"Stop-loss Armed: {'Yes' if stop_armed else 'No'}\n"
        f"Stop-loss Peak Gain: {'N/A' if stop_peak_pct is None else f'{stop_peak_pct:.2f}%'}\n"
        f"Stop-loss Trigger Price: {'N/A' if stop_trigger is None else f'${float(stop_trigger):.2f}'}\n"
        f"Determined Action: {action}\n"
        f"------------------------------"
    )


def check_stoploss_and_sell(
    avg_buy_price: float,
    ticker: str,
    target_gain_for_stoploss: float,
    current_price: float,
    *,
    db_path: str,
    connection_id: int,
    trade_stats: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """
    Midpoint trailing stop-loss:
    - Arm after target gain.
    - Trigger price is the midpoint between avg entry and the highest price since arming.
    - Only raises the trigger on new highs.
    - Sells all whole shares; disarms after sell or if no whole shares remain.
    """
    if avg_buy_price is None or avg_buy_price <= 0:
        return None

    percentage_gain = ((current_price - avg_buy_price) / avg_buy_price) * 100

    if ticker not in stoploss_state:
        stoploss_state[ticker] = {"armed": False, "trigger_price": None, "peak_price": None}

    if (not stoploss_state[ticker]["armed"]) and percentage_gain >= target_gain_for_stoploss:
        stoploss_state[ticker]["armed"] = True
        stoploss_state[ticker]["peak_price"] = float(current_price)
        stoploss_state[ticker]["trigger_price"] = (avg_buy_price + float(current_price)) / 2.0
        print(f"Stop-loss armed for {ticker} with a percentage gain of {percentage_gain:.2f}%.")

    if stoploss_state[ticker]["armed"]:
        peak_price = float(stoploss_state[ticker].get("peak_price") or current_price)
        if current_price > peak_price:
            peak_price = float(current_price)
            stoploss_state[ticker]["peak_price"] = peak_price
            midpoint = (avg_buy_price + peak_price) / 2.0
            prev_trigger = float(stoploss_state[ticker].get("trigger_price") or 0.0)
            if midpoint > prev_trigger:
                stoploss_state[ticker]["trigger_price"] = midpoint
        stoploss_trigger_price = float(stoploss_state[ticker].get("trigger_price") or 0.0)

        if current_price <= stoploss_trigger_price:
            quantity, _ = get_stock_position(ticker, _db_path=db_path, _connection_id=connection_id)
            sell_qty = int(quantity)

            if sell_qty <= 0:
                print(f"Cannot sell {ticker}. No whole shares available.")
                stoploss_state[ticker]["armed"] = False
                stoploss_state[ticker]["trigger_price"] = None
                stoploss_state[ticker]["peak_price"] = None
                print(f"Stop-loss DISARMED for {ticker} (quantity < 1).")
                return None

            try:
                response = place_stock_order(
                    symbol=ticker,
                    side="sell",
                    order_type="market",
                    quantity=sell_qty,
                    timeInForce="gfd",
                )
                state = (response or {}).get("state")
                if state in ("queued", "confirmed", "filled"):
                    print(f"Sold {sell_qty} shares of {ticker}.")
                    stoploss_state[ticker]["armed"] = False
                    stoploss_state[ticker]["trigger_price"] = None
                    stoploss_state[ticker]["peak_price"] = None
                    print(f"Stop-loss DISARMED for {ticker}.")
                    if trade_stats is not None:
                        _record_trade(
                            trade_stats,
                            side="sell",
                            qty=float(sell_qty),
                            price=current_price,
                            avg_buy_price=avg_buy_price,
                        )
                    return f"Sold {sell_qty} shares of {ticker} due to stop-loss."
                else:
                    print(f"Order state: {state}. Sell might not have executed.")
            except Exception as e:
                print(f"Error placing sell order for {ticker}: {e}")

    return None


@reconnect_if_needed
def print_portfolio_summary(*, db_path: str, connection_id: int) -> None:
    """
    Prints a summary of all positions held in the portfolio.
    Preserves original format.
    """
    portfolio_value = get_portfolio_value(_db_path=db_path, _connection_id=connection_id)
    positions = _get_open_stock_positions()

    print("\nPortfolio Summary:")
    print(f"{'Ticker':<10}{'Quantity':<10}{'% of Portfolio':<15}{'Avg Buy Price':<15}{'% Gain/Loss':<15}")
    print("-" * 65)

    for pos in positions:
        instrument_data = rh.account.get_instrument_by_url(pos["instrument"])
        ticker = instrument_data["symbol"]
        quantity = float(pos["quantity"])
        avg_buy_price = float(pos["average_buy_price"])
        current_price = get_stock_price(
            ticker,
            _db_path=db_path,
            _connection_id=connection_id,
        )

        position_value = quantity * current_price
        portfolio_percentage = (position_value / portfolio_value) * 100 if portfolio_value > 0 else 0
        percentage_gain_loss = ((current_price - avg_buy_price) / avg_buy_price) * 100 if avg_buy_price > 0 else 0

        print(f"{ticker:<10}{quantity:<10.2f}{portfolio_percentage:<15.2f}{avg_buy_price:<15.2f}{percentage_gain_loss:<15.2f}")

    print("-" * 65)


def print_portfolio_weight_divisor(*, tickers: List[str], portfolio_cap_divisor: int) -> None:
    """
    Prints the portfolio weight divisor calculation used for cap logic.
    Shows the divisor % based on n(+1 for cash).
    """
    clean = [str(t).strip().upper() for t in tickers if str(t).strip()]
    n = len(clean)
    n_plus_cash = max(2, n + 1)
    divisor = max(2, int(portfolio_cap_divisor))

    n_plus_cash_pct = 100.0 / n_plus_cash
    divisor_pct = 100.0 / divisor

    print("\nPortfolio Weight Divisor:")
    print(f"Tickers (n): {n}")
    print("Cash slice: +1")
    print(f"n+1: {n_plus_cash}")
    print(f"n+1 divisor pct: 1/{n_plus_cash} = {n_plus_cash_pct:.2f}%")
    print(f"Configured divisor: {divisor} -> 1/{divisor} = {divisor_pct:.2f}%")
    if divisor != n_plus_cash:
        print(f"Note: configured divisor ({divisor}) differs from n+1 ({n_plus_cash}).")


def main_trading_loop(
    *,
    db_path: str,
    connection_id: int,
    tickers: List[str],
    num_shares: int,
    target_gain_for_stoploss: float,
    timeframe: str,
    portfolio_cap_mode: str,
    portfolio_cap_percent_by_symbol: Dict[str, float],
    portfolio_cap_percent: float,
    portfolio_cap_divisor: int,
    trade_stats: Optional[Dict[str, Any]] = None,
    status_writer: Optional[Any] = None,
) -> None:
    trades_made: List[str] = []
    tickers_status: List[Dict[str, Any]] = []
    portfolio_value = 0.0
    try:
        portfolio_value = float(get_portfolio_value(_db_path=db_path, _connection_id=connection_id))
    except Exception:
        portfolio_value = 0.0
    divisor = max(2, int(portfolio_cap_divisor))
    cap_pct = None if portfolio_cap_mode == "percent" else (100.0 / divisor if divisor > 0 else 0.0)

    for ticker in tickers:
        try:
            current_price = get_stock_price(
                ticker,
                _db_path=db_path,
                _connection_id=connection_id,
            )

            def _load_hlc(hist_interval: str, hist_span: str) -> Tuple[List[float], List[float], List[float]]:
                historicals = get_stock_historicals(
                    ticker,
                    hist_interval,
                    hist_span,
                    _db_path=db_path,
                    _connection_id=connection_id,
                )
                return _extract_hlc(historicals)

            hist_interval = timeframes[timeframe]["interval"]
            hist_span = timeframes[timeframe]["span"]

            highs, lows, closes = _load_hlc(hist_interval, hist_span)

            if len(closes) < 10:
                print(f"[{ticker}] Not enough historicals to compute indicators.")
                tickers_status.append({"symbol": ticker, "signal": "NO_DATA"})
                continue

            prices = list(closes)
            if not prices or prices[-1] != current_price:
                prices.append(current_price)

            quantity, avg_buy_price = get_stock_position(ticker, _db_path=db_path, _connection_id=connection_id)

            ma30 = calculate_moving_average(prices, 30)
            ma78 = calculate_moving_average(prices, 78)
            ma150 = calculate_moving_average(prices, 190)
            rsi = calculate_rsi(prices)
            rsi_derivative = _rsi_derivative(prices)
            ma30_derivative = _ma_derivative(prices, 30)
            atr = calculate_atr_wilder(highs, lows, closes, period=ATR_PERIOD)

            held_pct: Optional[float] = None
            cap_delta_pct: Optional[float] = None
            row_cap_pct = (
                max(
                    0.01,
                    float(portfolio_cap_percent_by_symbol.get(str(ticker).strip().upper(), portfolio_cap_percent)),
                )
                if portfolio_cap_mode == "percent"
                else cap_pct
            )
            if portfolio_value > 0:
                held_value = float(quantity) * float(current_price)
                held_pct = (held_value / portfolio_value) * 100.0
                if row_cap_pct is not None:
                    cap_delta_pct = held_pct - row_cap_pct

            stop_state = stoploss_state.get(ticker) or {}
            stop_armed = bool(stop_state.get("armed"))
            stop_peak_price = stop_state.get("peak_price")
            stop_trigger_price = stop_state.get("trigger_price")
            stop_peak_pct: Optional[float] = None
            if stop_armed and avg_buy_price > 0 and stop_peak_price:
                stop_peak_pct = ((float(stop_peak_price) - avg_buy_price) / avg_buy_price) * 100.0

            buy_signal = should_buy(
                ticker,
                prices,
                db_path=db_path,
                connection_id=connection_id,
                portfolio_cap_mode=portfolio_cap_mode,
                portfolio_cap_percent_by_symbol=portfolio_cap_percent_by_symbol,
                portfolio_cap_percent=portfolio_cap_percent,
                portfolio_cap_divisor=portfolio_cap_divisor,
                num_shares=num_shares,
            )
            sell_signal = should_sell(ticker, prices, db_path=db_path, connection_id=connection_id)

            # Print diagnostic signals using the same trading signals
            print_indicator_signals(
                ticker,
                prices,
                db_path=db_path,
                connection_id=connection_id,
                buy_signal=buy_signal,
                sell_signal=sell_signal,
            )
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
                    "held_pct": held_pct,
                    "cap_pct": row_cap_pct,
                    "cap_delta_pct": cap_delta_pct,
                    "alloc_pct": held_pct,
                    "delta_pct": cap_delta_pct,
                    "ma20": ma30,
                    "ma78": ma78,
                    "ma150": ma150,
                    "rsi": rsi,
                    "rsi_d": rsi_derivative,
                    "ma30_d": ma30_derivative,
                    "atr": atr,
                    "stoploss_armed": stop_armed,
                    "stoploss_peak_pct": stop_peak_pct,
                    "stoploss_trigger": stop_trigger_price,
                    "chart": _build_chart_series(prices) if status_writer is not None else {},
                }
            )
        except Exception as e:
            print(f"[{ticker}] ERROR: {e}")
            tickers_status.append({"symbol": ticker, "signal": "ERROR", "error": str(e)})
            continue

        # Buy logic (with portfolio cap)
        if buy_signal:
            if num_shares < 1:
                print(f"[{ticker}] Buy skipped; num_shares must be >= 1 (got {num_shares}).")
            else:
                try:
                    resp = place_stock_order(
                        symbol=ticker,
                        side="buy",
                        order_type="market",
                        quantity=int(num_shares),
                        timeInForce="gfd",
                    )

                    if resp is not None:
                        print(f"Bought {num_shares} shares of {ticker}.")
                        trades_made.append(f"Bought {num_shares} shares of {ticker}.")
                        if trade_stats is not None and _order_success(resp):
                            _record_trade(
                                trade_stats,
                                side="buy",
                                qty=float(num_shares),
                                price=current_price,
                                avg_buy_price=0.0,
                            )

                        stoploss_state.setdefault(ticker, {})
                        stoploss_state[ticker]["armed"] = False
                        stoploss_state[ticker]["trigger_price"] = None
                        stoploss_state[ticker]["peak_price"] = None
                except Exception as e:
                    print(f"[{ticker}] Buy failed: {e}")

        # Sell logic (indicator-driven trailing stop)
        elif sell_signal and quantity > 0:
            if num_shares < 1:
                print(f"[{ticker}] Sell skipped; num_shares must be >= 1 (got {num_shares}).")
            else:
                try:
                    if atr is None:
                        print(f"[{ticker}] ATR unavailable; skipping trailing stop sell.")
                        resp = None
                    else:
                        trail_amount = round(max(MIN_TRAIL_AMOUNT_USD, ATR_MULTIPLIER * atr), 2)
                        resp = place_stock_order(
                            symbol=ticker,
                            side="sell",
                            order_type="trailing_stop",
                            quantity=int(num_shares),
                            trailAmount=float(trail_amount),
                            trailType="amount",
                            timeInForce="gtc",
                        )
                        print(
                            f"Sold {num_shares} shares of {ticker} with trailing stop "
                            f"(trail=${trail_amount:.2f})."
                        )
                        trades_made.append(
                            f"Sold {num_shares} shares of {ticker} with trailing stop (trail=${trail_amount:.2f})."
                        )

                    if resp is not None and trade_stats is not None and _order_success(resp):
                        _record_trade(
                            trade_stats,
                            side="sell",
                            qty=float(num_shares),
                            price=current_price,
                            avg_buy_price=avg_buy_price,
                        )
                except Exception as e:
                    print(f"[{ticker}] Trailing stop sell failed: {e}")

        # Stop-loss check
        if buy_signal and int(quantity) <= 0:
            stoploss_state.setdefault(ticker, {})
            stoploss_state[ticker]["armed"] = False
            stoploss_state[ticker]["trigger_price"] = None
            stoploss_state[ticker]["peak_price"] = None
        if avg_buy_price > 0:
            stoploss_trade = check_stoploss_and_sell(
                avg_buy_price,
                ticker,
                target_gain_for_stoploss,
                current_price,
                db_path=db_path,
                connection_id=connection_id,
                trade_stats=trade_stats,
            )
            if stoploss_trade:
                trades_made.append(stoploss_trade)

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

    # Summary of trades
    if trades_made:
        print("\nTrades made in this loop:")
        for trade in trades_made:
            print(f" - {trade}")
    else:
        print("\nNo trades made in this loop.")

    # Armed stop-losses
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
        payload["script"] = "FoxScry.Robinhood"
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
        raise ValueError("params.tickers must be a non-empty list (e.g., ['AAPL','MSFT']).")
    tickers = [str(t).strip().upper() for t in tickers if str(t).strip()]

    raw_num_shares = params.get("num_shares", 1)
    num_shares = _whole_share_qty(raw_num_shares, label="num_shares")
    target_gain_for_stoploss = float(params.get("target_gain_for_stoploss", 0.5))

    timeframe = str(params.get("timeframe", "1h")).strip()
    if timeframe not in timeframes:
        print(f"[WARN] Invalid timeframe '{timeframe}'. Defaulting to '1h'.")
        timeframe = "1h"
    sleep_duration = float(params.get("sleep_duration", 30))
    portfolio_cap_mode = str(params.get("portfolio_cap_mode", "divisor_cash_slice")).strip().lower()
    if portfolio_cap_mode not in ("divisor_cash_slice", "percent"):
        portfolio_cap_mode = "divisor_cash_slice"
    portfolio_cap_percent = float(params.get("portfolio_cap_percent", 20.0))
    if portfolio_cap_percent <= 0:
        portfolio_cap_percent = 20.0
    portfolio_cap_percent_by_symbol = _parse_symbol_cap_map(params.get("portfolio_cap_percent_by_symbol"))
    portfolio_cap_divisor = int(params.get("portfolio_cap_divisor", 6))

    # Ensure broker session is live before loop
    ensure_robinhood_session(args.db_path, int(args.connection_id))
    print("Rules: BUY price<MA190 & <MA78 & >MA30 with RSI_d>0 and MA30_d>0; SELL by RSI reversal or MA30_d<0 above MA190/MA78.")

    while True:
        interval = timeframes[timeframe]["interval"]
        span = timeframes[timeframe]["span"]

        print_portfolio_weight_divisor(
            tickers=tickers,
            portfolio_cap_divisor=portfolio_cap_divisor,
        )
        print_portfolio_summary(
            db_path=args.db_path,
            connection_id=int(args.connection_id),
        )
        main_trading_loop(
            db_path=args.db_path,
            connection_id=int(args.connection_id),
            tickers=tickers,
            num_shares=num_shares,
            target_gain_for_stoploss=target_gain_for_stoploss,
            timeframe=timeframe,
            portfolio_cap_mode=portfolio_cap_mode,
            portfolio_cap_percent_by_symbol=portfolio_cap_percent_by_symbol,
            portfolio_cap_percent=portfolio_cap_percent,
            portfolio_cap_divisor=portfolio_cap_divisor,
            trade_stats=trade_stats,
            status_writer=write_status,
        )

        print(
            r"""
       /\   /\   DreadFox.SuperHexagon  ____
      //\\_//\\     ____            /    \
      \_     _/    /   /       ____/      \____
       / * * \    /^^^]       /    \      /    \
       \_\O/_/    [   ]      /      \____/      \
        /   \_    [   /      \      /    \      /
        \     \_  /  /        \____/      \____/
         [ [ /  \/ _/         /    \      /    \
        _[ [ \  /_/          /      \____/      \
                             \      /    \      /
                              \____/      \____/
                                   \      /
                                    \____/
"""
        )
        print(f"Using timeframe: {timeframe} ({interval}, {span})")
        time.sleep(max(0.0, sleep_duration))

    # unreachable
    # return 0


if __name__ == "__main__":
    raise SystemExit(main())
