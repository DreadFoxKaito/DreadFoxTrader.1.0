#!/usr/bin/env python3
"""
DreadFox.Stock.Robinhood.py (Web-App Compatible)

Refactor goals:
- NO interactive input() prompts
- Uses the Cryptid Exchange broker_connections table to restore a Robinhood session
  (pickle-based session file) rather than asking for username/password.
- Reads runtime parameters from --params-json (created by the web app).
- Preserves the original indicator + trading logic as closely as possible.

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
from typing import Any, Callable, Dict, List, Optional

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
from app.brokers.robin_stocks_adapter import (  # noqa: E402
    get_10m_stock_historicals as adapter_get_10m_stock_historicals,
    get_stock_historicals as adapter_get_stock_historicals,
    place_stock_order,
)
from app.db import get_broker_connection, read_connection_metadata, read_connection_secrets, set_broker_status  # noqa: E402


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

HISTORICAL_BOUNDS = "extended"


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
def _status_code(resp: Any) -> int:
    try:
        return int(getattr(resp, "status_code", 0) or 0)
    except Exception:
        return 0


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


def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        if val in (None, "None"):
            return default
        return float(val)
    except Exception:
        return default


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


def reconnect_if_needed(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Original script attempted to re-login with stored username/password/MFA.
    In the web app flow, we only restore session from pickle. We retry once.
    """
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        db_path = kwargs.pop("_db_path", None)
        connection_id = kwargs.pop("_connection_id", None)

        try:
            return func(*args, **kwargs)
        except (requests.RequestException, ConnectionError) as e:
            if db_path is not None and connection_id is not None:
                try:
                    print(f"[WARN] Network error: {e}. Attempting Robinhood session restore...")
                    ensure_robinhood_session(db_path, int(connection_id))
                except Exception as e2:
                    raise RuntimeError(f"Reconnect failed: {e2}") from e
            safe_sleep(2.0)
            return func(*args, **kwargs)
        except Exception:
            # For unexpected exceptions, do not loop endlessly.
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
    if interval == "10minute":
        try:
            data = adapter_get_10m_stock_historicals(symbol, span=span, bounds="regular", min_candles=150)
        except RuntimeError as exc:
            if str(exc) == "INSUFFICIENT_CANDLES_FOR_10M_CALCULATION":
                print(f"[{symbol}] INSUFFICIENT_CANDLES_FOR_10M_CALCULATION")
                return []
            raise
    else:
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


def calculate_rsi_and_derivative(prices: List[float], period: int = 14) -> tuple[Optional[float], Optional[float]]:
    if len(prices) < period + 3:
        return None, None

    rsi_t2 = calculate_rsi(prices[:-2], period)
    rsi_t1 = calculate_rsi(prices[:-1], period)
    rsi_t0 = calculate_rsi(prices, period)

    if rsi_t0 is None or rsi_t1 is None or rsi_t2 is None:
        return rsi_t0, None

    d_rsi = (rsi_t0 - rsi_t2) / 2.0
    return rsi_t0, float(d_rsi)


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
    """
    Preserve original stop-loss behavior with whole-share sell:
      - arm stop-loss once gain threshold reached (based on avg_buy_price)
      - if price falls to stop-loss threshold after activation, sell all whole shares
      - disarm after a successful sell or when no shares remain
    """
    if avg_buy_price <= 0:
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
                resp = place_stock_order(
                    symbol=symbol,
                    side="sell",
                    order_type="market",
                    quantity=sell_qty,
                    timeInForce="gfd",
                )
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
                    print(f"Order state: {resp.get('state') if isinstance(resp, dict) else 'unknown'}. Sell might not have executed.")
            except Exception as e:
                print(f"Error placing sell order for {symbol}: {e}")


def main_trading_loop(
    *,
    db_path: str,
    connection_id: int,
    symbols: List[str],
    shares_per_trade: int,
    trailing_stop_amount: float,
    target_gain_pct: float,
    stop_loss_pct: float,
    timeframe: str,
    sleep_duration: float,
    trade_stats: Optional[Dict[str, Any]] = None,
    status_writer: Optional[Any] = None,
) -> None:
    if timeframe not in timeframes:
        raise ValueError(f"Invalid timeframe '{timeframe}'. Choose from {list(timeframes.keys())}.")

    interval = timeframes[timeframe]["interval"]
    span = timeframes[timeframe]["span"]

    print(f"Using timeframe: {timeframe} ({interval}, {span})")
    print(f"Symbols: {symbols}")

    while True:
        tickers_status: List[Dict[str, Any]] = []
        positions_map: Dict[str, Dict[str, float]] = {}
        try:
            positions = get_open_stock_positions(_db_path=db_path, _connection_id=connection_id)
            positions_map = build_positions_map(positions)
        except Exception:
            positions_map = {}

        for symbol in symbols:
            try:
                current_price = get_stock_price(symbol, _db_path=db_path, _connection_id=connection_id)

                def _load_closes(hist_interval: str, hist_span: str) -> List[float]:
                    h = get_stock_historicals(
                        symbol,
                        hist_interval,
                        hist_span,
                        _db_path=db_path,
                        _connection_id=connection_id,
                    )
                    out: List[float] = []
                    for row in h:
                        try:
                            out.append(float(row.get("close_price")))
                        except Exception:
                            continue
                    return out

                closes = _load_closes(interval, span)

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

                # Stop-loss logic check (preserved)
                check_stoploss_and_sell(
                    symbol=symbol,
                    current_price=current_price,
                    avg_buy_price=avg_buy_price,
                    held_qty=pos_qty,
                    target_gain_pct=target_gain_pct,
                    stop_loss_pct=stop_loss_pct,
                    trade_stats=trade_stats,
                )

                # Trade logic (preserved style)
                if buy_signal:
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
                                trade_stats,
                                side="buy",
                                qty=float(shares_per_trade),
                                price=current_price,
                                avg_buy_price=0.0,
                            )
                    except Exception as e:
                        print(f"[{symbol}] Buy failed: {e}")

                if pos_qty > 0 and sell_signal:
                    print(
                        f"[{symbol}] SELL signal -> placing trailing stop sell for {shares_per_trade} shares, trail=${trailing_stop_amount}..."
                    )
                    try:
                        resp = place_stock_order(
                            symbol=symbol,
                            side="sell",
                            order_type="trailing_stop",
                            quantity=int(shares_per_trade),
                            trailAmount=float(trailing_stop_amount),
                            trailType="amount",
                            timeInForce="gtc",
                        )
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

        # ASCII signature (kept)
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
        :     ...' 88o __,------.88o ...__..._.=~- .    `~~   `~~      ~-._ DreadFox.Stock _.
        `.;;;:='    ~~            ~~~                ~-    -       -   -
        """)

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
    ap.add_argument("--connection-id", required=True, type=int, help="broker_connections.id for Robinhood.")
    args = ap.parse_args()

    run_dir = Path(args.run_dir)
    status_path = run_dir / "status.json"
    last_status: Dict[str, Any] = {}
    trade_stats: Dict[str, Any] = {"pnl": 0.0, "trades": 0}

    def write_status(payload: Dict[str, Any]) -> None:
        payload = dict(payload)
        payload["ts"] = iso_now()
        payload["script"] = "DreadFox.Stock.Robinhood"
        payload["pnl"] = round(float(trade_stats.get("pnl", 0.0)), 2)
        payload["trades"] = int(trade_stats.get("trades", 0))
        try:
            status_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            last_status.clear()
            last_status.update(payload)
        except Exception:
            pass

    params = load_params(args.params_json)

    # Required/expected params with defaults
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
    timeframe = str(params.get("timeframe", "10m"))

    # Ensure broker session is live before starting loop
    ensure_robinhood_session(args.db_path, int(args.connection_id))

    main_trading_loop(
        db_path=args.db_path,
        connection_id=int(args.connection_id),
        symbols=symbols,
        shares_per_trade=shares_per_trade,
        trailing_stop_amount=trailing_stop_amount,
        target_gain_pct=target_gain_pct,
        stop_loss_pct=stop_loss_pct,
        timeframe=timeframe,
        sleep_duration=sleep_duration,
        trade_stats=trade_stats,
        status_writer=write_status,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
