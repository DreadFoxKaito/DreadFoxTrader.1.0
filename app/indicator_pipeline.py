from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

LOG = logging.getLogger(__name__)
ET_TZ = ZoneInfo("America/New_York")


@dataclass
class CandlePolicyResult:
    opens: list[float]
    highs: list[float]
    lows: list[float]
    closes: list[float]
    latest_included: bool
    latest_excluded: bool


def apply_final_candle_policy(
    *,
    opens: list[float],
    highs: list[float],
    lows: list[float],
    closes: list[float],
    use_current_candle: bool,
) -> CandlePolicyResult:
    if bool(use_current_candle):
        return CandlePolicyResult(opens, highs, lows, closes, bool(closes), False)
    if not closes:
        return CandlePolicyResult(opens, highs, lows, closes, False, False)
    return CandlePolicyResult(opens[:-1], highs[:-1], lows[:-1], closes[:-1], False, True)


def heikin_ashi_series(
    opens: list[float],
    highs: list[float],
    lows: list[float],
    closes: list[float],
) -> tuple[list[float], list[float], list[float], list[float]]:
    n = min(len(opens), len(highs), len(lows), len(closes))
    ha_open: list[float] = []
    ha_high: list[float] = []
    ha_low: list[float] = []
    ha_close: list[float] = []
    for i in range(n):
        o = float(opens[i])
        h = float(highs[i])
        l = float(lows[i])
        c = float(closes[i])
        close = (o + h + l + c) / 4.0
        if i == 0:
            open_ = (o + c) / 2.0
        else:
            open_ = (ha_open[-1] + ha_close[-1]) / 2.0
        high = max(h, open_, close)
        low = min(l, open_, close)
        ha_open.append(open_)
        ha_high.append(high)
        ha_low.append(low)
        ha_close.append(close)
    return ha_open, ha_high, ha_low, ha_close


def log_indicator_policy(
    *,
    mode: str,
    symbol: str,
    timeframe: str,
    session: str,
    extended_hours: bool,
    use_current_candle: bool,
    total_fetched: int,
    total_used: int,
    latest_fetched_ts: Optional[str] = None,
    latest_used_ts: Optional[str] = None,
    latest_ohlc: Optional[tuple[float, float, float, float]] = None,
    latest_included: bool = False,
    latest_excluded: bool = False,
    final_signal: str = "HOLD",
) -> None:
    now_utc = datetime.now(timezone.utc)
    LOG.info(
        "Indicator policy mode=%s symbol=%s timeframe=%s session=%s extended_hours=%s "
        "use_current_candle=%s use_closed_candle_only=%s total_candles_fetched=%s total_candles_used=%s "
        "latest_fetched_ts=%s latest_used_ts=%s current_utc=%s current_et=%s latest_used_ohlc=%s "
        "latest_included=%s latest_excluded=%s final_signal_candle_ts=%s final_signal=%s",
        mode,
        symbol,
        timeframe,
        session,
        bool(extended_hours),
        bool(use_current_candle),
        not bool(use_current_candle),
        int(total_fetched),
        int(total_used),
        latest_fetched_ts,
        latest_used_ts,
        now_utc.isoformat(),
        now_utc.astimezone(ET_TZ).isoformat(),
        latest_ohlc,
        bool(latest_included),
        bool(latest_excluded),
        latest_used_ts,
        final_signal,
    )
