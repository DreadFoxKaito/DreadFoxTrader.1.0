from __future__ import annotations

import math
import time
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional

_DAY_MS = 24 * 60 * 60 * 1000
_MARKET_MINUTES_PER_DAY = 390  # Regular-hours candles; scripts request needExtendedHoursData=false.


FetchPriceHistoryFn = Callable[..., List[Dict[str, Any]]]


def required_candles_for_lookbacks(
    lookbacks: Iterable[int],
    *,
    baseline: int = 220,
    extra_candles: int = 12,
) -> int:
    max_lookback = 0
    for raw in lookbacks:
        try:
            value = int(raw)
        except Exception:
            continue
        if value > max_lookback:
            max_lookback = value
    return max(int(baseline), max_lookback + max(0, int(extra_candles)))


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _candle_ts_ms(row: Dict[str, Any]) -> Optional[int]:
    if not isinstance(row, dict):
        return None
    raw = row.get("datetime")
    if raw is None:
        return None
    try:
        return int(raw)
    except Exception:
        return None


def _merge_unique_candles(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_ts: Dict[int, Dict[str, Any]] = {}
    passthrough: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        ts = _candle_ts_ms(row)
        if ts is None:
            passthrough.append(row)
            continue
        by_ts[ts] = row
    merged = [by_ts[k] for k in sorted(by_ts.keys())]
    if passthrough:
        merged.extend(passthrough)
    return merged


def _oldest_ts_ms(rows: List[Dict[str, Any]]) -> Optional[int]:
    oldest: Optional[int] = None
    for row in rows:
        ts = _candle_ts_ms(row)
        if ts is None:
            continue
        if oldest is None or ts < oldest:
            oldest = ts
    return oldest


def _estimate_candles_per_batch(
    *,
    period_type: str,
    period: int,
    frequency_type: str,
    frequency: int,
) -> int:
    pt = str(period_type or "").strip().lower()
    ft = str(frequency_type or "").strip().lower()
    per = max(1, _to_int(period, 1))
    freq = max(1, _to_int(frequency, 1))

    if ft == "minute":
        bars_per_day = max(1, _MARKET_MINUTES_PER_DAY // freq)
        if pt == "day":
            return bars_per_day * per
        return bars_per_day
    if ft == "daily":
        if pt == "year":
            return 252 * per
        if pt == "month":
            return 21 * per
        if pt == "ytd":
            return 252
        return 21
    if ft == "weekly":
        if pt == "year":
            return 52 * per
        if pt == "month":
            return 4 * per
        if pt == "ytd":
            return 52
        return 4
    if ft == "monthly":
        if pt == "year":
            return 12 * per
        return 12
    return per


def _window_days_for_batch(period_type: str, period: int) -> int:
    pt = str(period_type or "").strip().lower()
    per = max(1, _to_int(period, 1))
    if pt == "day":
        # Convert trading-day window to calendar days for startDate/endDate paging.
        return max(7, int(math.ceil(per * 1.6)))
    if pt == "month":
        return max(30, 31 * per)
    if pt == "year":
        return max(365, 366 * per)
    if pt == "ytd":
        return 366
    return 14


def fetch_price_history_with_min_candles(
    *,
    fetch_fn: FetchPriceHistoryFn,
    symbol: str,
    period_type: str,
    period: int,
    frequency_type: str,
    frequency: int,
    need_extended: bool,
    min_candles: int,
    max_batches: int = 12,
) -> List[Dict[str, Any]]:
    """
    Fetch price history with minimum number of candles, including current day data.

    CRITICAL FIX: Always specify endDate=NOW to include current day's candles.
    Per Schwab API docs, if endDate is omitted, it defaults to previous business day close.
    """
    target = max(1, int(min_candles))
    current_time_ms = int(time.time() * 1000)

    # FIRST CALL: Fetch with explicit endDate=NOW to get current day candles
    candles = fetch_fn(
        symbol=symbol,
        period_type=period_type,
        period=period,
        frequency_type=frequency_type,
        frequency=frequency,
        need_extended=need_extended,
        end_date_ms=current_time_ms,  # CRITICAL: Include current day
    )
    merged = _merge_unique_candles(candles if isinstance(candles, list) else [])

    # Diagnostic logging: Show date range of fetched candles
    if merged:
        oldest_ts = _oldest_ts_ms(merged)
        newest_ts = max((_candle_ts_ms(c) for c in merged if _candle_ts_ms(c)), default=None)
        if oldest_ts and newest_ts:
            oldest_dt = datetime.fromtimestamp(oldest_ts / 1000).strftime('%Y-%m-%d %H:%M')
            newest_dt = datetime.fromtimestamp(newest_ts / 1000).strftime('%Y-%m-%d %H:%M')
            print(f"[{symbol}] Fetched {len(merged)} candles: {oldest_dt} to {newest_dt} ({frequency_type}/{frequency})")
    if len(merged) >= target:
        return merged

    # If we need more candles, fetch historical data going backwards
    est_per_batch = _estimate_candles_per_batch(
        period_type=period_type,
        period=period,
        frequency_type=frequency_type,
        frequency=frequency,
    )
    deficit = max(0, target - len(merged))
    estimated_batches = int(math.ceil(deficit / max(1, est_per_batch)))
    batch_limit = min(32, max(int(max_batches), estimated_batches + 2))

    cursor_end = (_oldest_ts_ms(merged) - 1) if merged else (current_time_ms - _DAY_MS)
    window_days = _window_days_for_batch(period_type, period)

    prev_count = len(merged)
    prev_oldest = _oldest_ts_ms(merged)

    for _ in range(batch_limit):
        if len(merged) >= target:
            break
        start_ms = int(cursor_end - (window_days * _DAY_MS))
        batch = fetch_fn(
            symbol=symbol,
            period_type=period_type,
            period=period,
            frequency_type=frequency_type,
            frequency=frequency,
            need_extended=need_extended,
            start_date_ms=start_ms,
            end_date_ms=cursor_end,
        )
        if isinstance(batch, list) and batch:
            merged = _merge_unique_candles(merged + batch)
        oldest = _oldest_ts_ms(merged)
        if oldest is None:
            cursor_end = start_ms - 1
        else:
            cursor_end = oldest - 1

        made_progress = len(merged) > prev_count or (oldest is not None and prev_oldest is not None and oldest < prev_oldest)
        if not made_progress:
            window_days = min(400, int(math.ceil(window_days * 1.5)))
            cursor_end = start_ms - 1

        prev_count = len(merged)
        prev_oldest = oldest

    return merged
