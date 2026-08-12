from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class SupertrendPoint:
    atr: Optional[float]
    basic_upper: Optional[float]
    basic_lower: Optional[float]
    final_upper: Optional[float]
    final_lower: Optional[float]
    direction: Optional[float]
    trend: Optional[float]
    flip_up: bool = False
    flip_down: bool = False


def calculate_true_range(
    highs: list[float],
    lows: list[float],
    closes: list[float],
) -> list[float]:
    n = min(len(highs), len(lows), len(closes))
    out: list[float] = []
    for i in range(n):
        high = float(highs[i])
        low = float(lows[i])
        if i == 0:
            out.append(high - low)
            continue
        prev_close = float(closes[i - 1])
        out.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    return out


def calculate_wilder_atr(true_ranges: list[float], period: int) -> list[Optional[float]]:
    n = len(true_ranges)
    period = max(1, int(period))
    out: list[Optional[float]] = [None] * n
    if n < period:
        return out
    seed = sum(float(v) for v in true_ranges[:period]) / float(period)
    out[period - 1] = seed
    prev = seed
    for i in range(period, n):
        prev = ((prev * float(period - 1)) + float(true_ranges[i])) / float(period)
        out[i] = prev
    return out


def initialize_supertrend_direction(
    *,
    close: float,
    final_upper: float,
    final_lower: float,
) -> float:
    midpoint = (float(final_upper) + float(final_lower)) / 2.0
    return 1.0 if float(close) >= midpoint else -1.0


def calculate_supertrend(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    period: int,
    factor: float,
) -> list[SupertrendPoint]:
    n = min(len(highs), len(lows), len(closes))
    period = max(1, int(period))
    factor = max(0.1, float(factor))
    empty = SupertrendPoint(None, None, None, None, None, None, None)
    out: list[SupertrendPoint] = [empty] * n
    if n <= 0:
        return out

    high_vals = [float(v) for v in highs[:n]]
    low_vals = [float(v) for v in lows[:n]]
    close_vals = [float(v) for v in closes[:n]]
    atr_values = calculate_wilder_atr(calculate_true_range(high_vals, low_vals, close_vals), period)

    prev_final_upper: Optional[float] = None
    prev_final_lower: Optional[float] = None
    prev_direction: Optional[float] = None

    for i in range(n):
        atr_now = atr_values[i]
        if atr_now is None:
            continue

        hl2 = (high_vals[i] + low_vals[i]) / 2.0
        basic_upper = hl2 + (factor * float(atr_now))
        basic_lower = hl2 - (factor * float(atr_now))

        if prev_final_upper is None or prev_final_lower is None:
            final_upper = basic_upper
            final_lower = basic_lower
            direction = initialize_supertrend_direction(
                close=close_vals[i],
                final_upper=final_upper,
                final_lower=final_lower,
            )
            flip_up = False
            flip_down = False
        else:
            prev_close = close_vals[i - 1]
            final_upper = (
                basic_upper
                if basic_upper < prev_final_upper or prev_close > prev_final_upper
                else prev_final_upper
            )
            final_lower = (
                basic_lower
                if basic_lower > prev_final_lower or prev_close < prev_final_lower
                else prev_final_lower
            )
            if close_vals[i] > prev_final_upper:
                direction = 1.0
            elif close_vals[i] < prev_final_lower:
                direction = -1.0
            else:
                direction = prev_direction if prev_direction is not None else 1.0
            flip_up = prev_direction is not None and prev_direction <= 0.0 and direction > 0.0
            flip_down = prev_direction is not None and prev_direction >= 0.0 and direction < 0.0

        trend = final_lower if direction >= 0.0 else final_upper
        out[i] = SupertrendPoint(
            atr=float(atr_now),
            basic_upper=basic_upper,
            basic_lower=basic_lower,
            final_upper=final_upper,
            final_lower=final_lower,
            direction=direction,
            trend=trend,
            flip_up=flip_up,
            flip_down=flip_down,
        )
        prev_final_upper = final_upper
        prev_final_lower = final_lower
        prev_direction = direction

    return out


def segment_supertrend_runs(points: list[SupertrendPoint]) -> list[tuple[float, int, int]]:
    segments: list[tuple[float, int, int]] = []
    start: Optional[int] = None
    direction: Optional[float] = None
    last_index: Optional[int] = None

    for i, point in enumerate(points):
        if point.trend is None or point.direction is None:
            if start is not None and direction is not None and last_index is not None:
                segments.append((direction, start, last_index))
            start = None
            direction = None
            last_index = None
            continue

        point_direction = 1.0 if float(point.direction) >= 0.0 else -1.0
        if start is None:
            start = i
            direction = point_direction
        elif direction != point_direction:
            if last_index is not None:
                segments.append((float(direction), start, last_index))
            start = i
            direction = point_direction
        last_index = i

    if start is not None and direction is not None and last_index is not None:
        segments.append((float(direction), start, last_index))
    return segments
