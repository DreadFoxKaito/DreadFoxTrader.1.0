from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


PIVOT_LEVEL_ORDER = ("S3", "S2", "S1", "P", "R1", "R2", "R3")


@dataclass(frozen=True)
class PivotPointLevels:
    source_index: int
    high: float
    low: float
    close: float
    s3: float
    s2: float
    s1: float
    p: float
    r1: float
    r2: float
    r3: float

    def as_dict(self) -> dict[str, float]:
        return {
            "S3": self.s3,
            "S2": self.s2,
            "S1": self.s1,
            "P": self.p,
            "R1": self.r1,
            "R2": self.r2,
            "R3": self.r3,
        }


def calculate_pivot_points(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    *,
    source_index: int = -2,
) -> Optional[PivotPointLevels]:
    n = min(len(highs), len(lows), len(closes))
    if n <= 0:
        return None
    idx = int(source_index)
    if idx < 0:
        idx = n + idx
    if idx < 0 or idx >= n:
        return None

    high = float(highs[idx])
    low = float(lows[idx])
    close = float(closes[idx])
    if high <= 0.0 or low <= 0.0 or close <= 0.0 or high < low:
        return None

    p = (high + low + close) / 3.0
    r1 = (2.0 * p) - low
    s1 = (2.0 * p) - high
    r2 = p + (high - low)
    s2 = p - (high - low)
    r3 = high + (2.0 * (p - low))
    s3 = low - (2.0 * (high - p))
    return PivotPointLevels(
        source_index=idx,
        high=high,
        low=low,
        close=close,
        s3=s3,
        s2=s2,
        s1=s1,
        p=p,
        r1=r1,
        r2=r2,
        r3=r3,
    )


def pivot_level_sequence(
    levels: PivotPointLevels,
    *,
    include_half_levels: bool = False,
) -> list[tuple[str, float]]:
    base = [(name, float(levels.as_dict()[name])) for name in PIVOT_LEVEL_ORDER]
    base.sort(key=lambda item: item[1])
    if not include_half_levels:
        return base

    out: list[tuple[str, float]] = []
    for idx, item in enumerate(base):
        out.append(item)
        if idx >= len(base) - 1:
            continue
        next_item = base[idx + 1]
        out.append((f"{item[0]}/{next_item[0]}", (item[1] + next_item[1]) / 2.0))
    return out


def pivot_target_above_price(
    levels: PivotPointLevels,
    price: float,
    *,
    offset: float = 1.0,
    include_half_levels: bool = False,
) -> Optional[tuple[str, float]]:
    step_count = max(1, int(round(float(offset) * (2.0 if include_half_levels else 1.0))))
    candidates = [
        (name, value)
        for name, value in pivot_level_sequence(levels, include_half_levels=include_half_levels)
        if float(value) > float(price)
    ]
    if not candidates:
        return None
    idx = min(len(candidates) - 1, step_count - 1)
    return candidates[idx]


def pivot_target_below_price(
    levels: PivotPointLevels,
    price: float,
    *,
    offset: float = 1.0,
    include_half_levels: bool = False,
) -> Optional[tuple[str, float]]:
    step_count = max(1, int(round(float(offset) * (2.0 if include_half_levels else 1.0))))
    candidates = [
        (name, value)
        for name, value in reversed(pivot_level_sequence(levels, include_half_levels=include_half_levels))
        if float(value) < float(price)
    ]
    if not candidates:
        return None
    idx = min(len(candidates) - 1, step_count - 1)
    return candidates[idx]
