from __future__ import annotations

import time
from typing import Any, Optional

import requests

try:
    import robin_stocks.robinhood as rh  # type: ignore
except Exception as e:  # pragma: no cover
    rh = None  # type: ignore
    _IMPORT_ERR = e
else:
    _IMPORT_ERR = None

from .brokers.robinhood_connector import _pickle_debug_info, _resolve_pickle_config, _restore_session_from_pickle
from .db import get_broker_connection, read_connection_metadata, read_connection_secrets, set_broker_status


TIMEFRAMES: dict[str, dict[str, Any]] = {
    "5m": {"interval": "5minute", "span": "week", "cache_ttl": 60},
    "10m": {"interval": "10minute", "span": "week", "cache_ttl": 120},
    "1h": {"interval": "hour", "span": "3month", "cache_ttl": 300},
    "1d": {"interval": "day", "span": "year", "cache_ttl": 1800},
}

_INDICATOR_CACHE: dict[tuple[int, str, str], dict[str, Any]] = {}


def _utc_ts() -> int:
    return int(time.time())


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _status_code(exc: BaseException) -> Optional[int]:
    resp = getattr(exc, "response", None)
    code = getattr(resp, "status_code", None)
    try:
        return int(code) if code is not None else None
    except Exception:
        return None


def _safe_sleep(seconds: float) -> None:
    time.sleep(max(0.0, float(seconds)))


def _safe_stock_quote(symbol: str, retries: int = 3, backoff: float = 1.2) -> Optional[dict[str, Any]]:
    if rh is None:
        raise RuntimeError(f"robin_stocks unavailable: {_IMPORT_ERR}")
    for attempt in range(retries):
        try:
            quote = rh.stocks.get_stock_quote_by_symbol(symbol)
            if isinstance(quote, dict) and quote:
                return quote
        except requests.HTTPError as exc:
            if _status_code(exc) == 429 and attempt < retries - 1:
                _safe_sleep(backoff * (attempt + 1))
                continue
            raise
        except Exception:
            if attempt < retries - 1:
                _safe_sleep(backoff * (attempt + 1))
                continue
            raise
    return None


def _safe_stock_historicals(
    symbol: str,
    interval: str,
    span: str,
    *,
    bounds: str = "regular",
    retries: int = 3,
    backoff: float = 1.2,
) -> list[dict[str, Any]]:
    if rh is None:
        raise RuntimeError(f"robin_stocks unavailable: {_IMPORT_ERR}")
    for attempt in range(retries):
        try:
            data = rh.stocks.get_stock_historicals(symbol, interval=interval, span=span, bounds=bounds)
            if isinstance(data, list) and data:
                return data
            raise RuntimeError("empty historicals")
        except requests.HTTPError as exc:
            if _status_code(exc) == 429 and attempt < retries - 1:
                _safe_sleep(backoff * (attempt + 1))
                continue
            raise
        except Exception:
            if attempt < retries - 1:
                _safe_sleep(backoff * (attempt + 1))
                continue
            raise
    return []


def _price_from_quote(quote: Optional[dict[str, Any]]) -> tuple[Optional[float], str]:
    if not quote:
        return None, ""
    for key in (
        "last_trade_price",
        "mark_price",
        "last_extended_hours_trade_price",
        "ask_price",
        "bid_price",
        "previous_close",
    ):
        val = _safe_float(quote.get(key))
        if val is not None:
            return val, key
    return None, ""


def _calculate_moving_average(prices: list[float], window: int) -> Optional[float]:
    if len(prices) < window:
        return None
    return sum(prices[-window:]) / float(window)


def _calculate_rsi(prices: list[float], period: int = 14) -> Optional[float]:
    if len(prices) < period + 1:
        return None
    gains = []
    losses = []
    for i in range(1, len(prices)):
        delta = prices[i] - prices[i - 1]
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))
    avg_gain = sum(gains[:period]) / float(period)
    avg_loss = sum(losses[:period]) / float(period)
    for i in range(period, len(gains)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / float(period)
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / float(period)
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _calculate_rsi_and_derivative(prices: list[float], period: int = 14) -> tuple[Optional[float], Optional[float]]:
    if len(prices) < period + 3:
        return None, None
    rsi_t2 = _calculate_rsi(prices[:-2], period)
    rsi_t1 = _calculate_rsi(prices[:-1], period)
    rsi_t0 = _calculate_rsi(prices, period)
    if rsi_t0 is None or rsi_t1 is None or rsi_t2 is None:
        return rsi_t0, None
    return rsi_t0, (rsi_t0 - rsi_t2) / 2.0


def _ensure_robinhood_session(db_path: str, connection_id: int) -> None:
    row = get_broker_connection(db_path, connection_id)
    if not row:
        raise RuntimeError(f"Robinhood connection_id {connection_id} not found.")

    status = str(row["status"] or "")
    if status != "connected":
        raise RuntimeError(f"Robinhood connection_id {connection_id} status='{status}'.")

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
            metadata={**(meta or {}), "error": "Missing Robinhood session pickle.", "debug": debug},
        )
        raise RuntimeError("Robinhood session pickle missing; relink required.")
    restored = _restore_session_from_pickle(pickle_file, expires_in=86400, scope="internal", validate=True)
    if not restored:
        debug = _pickle_debug_info(pickle_file)
        set_broker_status(
            db_path=db_path,
            connection_id=connection_id,
            status="needs_auth",
            metadata={**(meta or {}), "error": "Failed to restore Robinhood session.", "debug": debug},
        )
        raise RuntimeError("Robinhood session restore failed; relink required.")


def _indicator_payload(
    *,
    symbol: str,
    quote: Optional[dict[str, Any]],
    interval: str,
    span: str,
    bounds: str,
) -> dict[str, Any]:
    price, price_source = _price_from_quote(quote)
    historicals = _safe_stock_historicals(symbol, interval, span, bounds=bounds)
    closes: list[float] = []
    for row in historicals:
        try:
            closes.append(float(row.get("close_price")))
        except Exception:
            continue
    points = len(closes)
    used_current = False
    if price is not None:
        closes.append(price)
        used_current = True
    if len(closes) < 10:
        return {
            "interval": interval,
            "span": span,
            "bounds": bounds,
            "points": points,
            "price": price,
            "price_source": price_source,
            "used_current_price": used_current,
            "error": "not enough historicals",
            "as_of": _utc_ts(),
        }
    ma20 = _calculate_moving_average(closes, 20)
    ma78 = _calculate_moving_average(closes, 78)
    ma190 = _calculate_moving_average(closes, 190)
    rsi, rsi_d = _calculate_rsi_and_derivative(closes, 14)
    return {
        "interval": interval,
        "span": span,
        "bounds": bounds,
        "points": points,
        "price": price,
        "price_source": price_source,
        "used_current_price": used_current,
        "ma20": ma20,
        "ma78": ma78,
        "ma190": ma190,
        "rsi": rsi,
        "rsi_d": rsi_d,
        "as_of": _utc_ts(),
    }


def _get_cached_indicator(
    connection_id: int, symbol: str, timeframe: str
) -> Optional[dict[str, Any]]:
    cached = _INDICATOR_CACHE.get((connection_id, symbol, timeframe))
    if not cached:
        return None
    ttl = int(TIMEFRAMES.get(timeframe, {}).get("cache_ttl", 60))
    if (_utc_ts() - int(cached.get("ts", 0))) > ttl:
        return None
    return cached.get("data")


def _set_cached_indicator(
    connection_id: int, symbol: str, timeframe: str, data: dict[str, Any]
) -> None:
    _INDICATOR_CACHE[(connection_id, symbol, timeframe)] = {"ts": _utc_ts(), "data": data}


def fetch_robinhood_indicators(
    *,
    db_path: str,
    connection_id: int,
    symbols: list[str],
    bounds: str = "regular",
) -> dict[str, Any]:
    _ensure_robinhood_session(db_path, connection_id)
    out: dict[str, Any] = {}
    for symbol in symbols:
        symbol = str(symbol).strip().upper()
        if not symbol:
            continue
        ticker_payload: dict[str, Any] = {}
        quote: Optional[dict[str, Any]] = None
        try:
            quote = _safe_stock_quote(symbol)
        except Exception as exc:
            quote = None
            ticker_payload["quote_error"] = str(exc)
        for tf, cfg in TIMEFRAMES.items():
            cached = _get_cached_indicator(connection_id, symbol, tf)
            if cached is not None:
                ticker_payload[tf] = cached
                continue
            try:
                payload = _indicator_payload(
                    symbol=symbol,
                    quote=quote,
                    interval=str(cfg["interval"]),
                    span=str(cfg["span"]),
                    bounds=bounds,
                )
            except Exception as exc:
                payload = {
                    "interval": str(cfg["interval"]),
                    "span": str(cfg["span"]),
                    "bounds": bounds,
                    "error": str(exc),
                    "as_of": _utc_ts(),
                }
            _set_cached_indicator(connection_id, symbol, tf, payload)
            ticker_payload[tf] = payload
        out[symbol] = ticker_payload
    return out


def build_robinhood_indicator_context(
    *,
    db_path: str,
    portfolio_data: list[dict[str, Any]],
    max_tickers: int = 25,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    if not portfolio_data:
        return results
    for snap in portfolio_data:
        if str(snap.get("broker")) != "robinhood":
            continue
        connection_id = int(snap.get("connection_id") or 0)
        tickers: list[str] = []
        for acc in snap.get("accounts", []):
            for pos in acc.get("positions", []):
                sym = str(pos.get("symbol") or "").strip().upper()
                if sym and sym not in tickers:
                    tickers.append(sym)
        tickers_truncated = False
        if max_tickers and len(tickers) > max_tickers:
            tickers = tickers[:max_tickers]
            tickers_truncated = True
        if not tickers:
            results.append(
                {
                    "broker": "robinhood",
                    "connection_id": connection_id,
                    "label": snap.get("label", ""),
                    "tickers_truncated": False,
                    "tickers": {},
                    "error": "no robinhood positions",
                }
            )
            continue
        try:
            indicators = fetch_robinhood_indicators(
                db_path=db_path, connection_id=connection_id, symbols=tickers
            )
            results.append(
                {
                    "broker": "robinhood",
                    "connection_id": connection_id,
                    "label": snap.get("label", ""),
                    "tickers_truncated": tickers_truncated,
                    "tickers": indicators,
                }
            )
        except Exception as exc:
            results.append(
                {
                    "broker": "robinhood",
                    "connection_id": connection_id,
                    "label": snap.get("label", ""),
                    "tickers_truncated": tickers_truncated,
                    "tickers": {},
                    "error": str(exc),
                }
            )
    return results
