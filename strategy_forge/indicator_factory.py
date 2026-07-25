from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import time
from typing import Any

from .data_loader import OHLCVData, parse_timestamp

NAN = float("nan")


def is_valid(value: float | None) -> bool:
    return value is not None and not math.isnan(float(value))


def _nan_list(n: int) -> list[float]:
    return [NAN] * int(n)


def sma(values: list[float], length: int) -> list[float]:
    n = len(values)
    length = max(1, int(length))
    out = _nan_list(n)
    running = 0.0
    for i, value in enumerate(values):
        running += float(value)
        if i >= length:
            running -= float(values[i - length])
        if i >= length - 1:
            out[i] = running / float(length)
    return out


def ema(values: list[float], length: int) -> list[float]:
    n = len(values)
    length = max(1, int(length))
    out = _nan_list(n)
    if not values:
        return out
    alpha = 2.0 / float(length + 1)
    seed = float(values[0])
    out[0] = seed
    for i in range(1, n):
        seed = (float(values[i]) * alpha) + (seed * (1.0 - alpha))
        out[i] = seed
    return out


def moving_average(values: list[float], length: int, ma_type: str = "ema") -> list[float]:
    return sma(values, length) if str(ma_type).lower() == "sma" else ema(values, length)


def rsi(closes: list[float], length: int) -> list[float]:
    n = len(closes)
    length = max(1, int(length))
    out = _nan_list(n)
    if n <= length:
        return out
    gains: list[float] = [0.0] * n
    losses: list[float] = [0.0] * n
    for i in range(1, n):
        delta = float(closes[i]) - float(closes[i - 1])
        gains[i] = max(delta, 0.0)
        losses[i] = max(-delta, 0.0)
    avg_gain = sum(gains[1 : length + 1]) / float(length)
    avg_loss = sum(losses[1 : length + 1]) / float(length)
    out[length] = 100.0 if avg_loss == 0.0 else 100.0 - (100.0 / (1.0 + (avg_gain / avg_loss)))
    for i in range(length + 1, n):
        avg_gain = ((avg_gain * (length - 1)) + gains[i]) / float(length)
        avg_loss = ((avg_loss * (length - 1)) + losses[i]) / float(length)
        out[i] = 100.0 if avg_loss == 0.0 else 100.0 - (100.0 / (1.0 + (avg_gain / avg_loss)))
    return out


def true_range(highs: list[float], lows: list[float], closes: list[float]) -> list[float]:
    out: list[float] = []
    for i in range(len(closes)):
        h = float(highs[i])
        l = float(lows[i])
        if i == 0:
            out.append(h - l)
        else:
            prev_close = float(closes[i - 1])
            out.append(max(h - l, abs(h - prev_close), abs(l - prev_close)))
    return out


def atr(highs: list[float], lows: list[float], closes: list[float], length: int) -> list[float]:
    return sma(true_range(highs, lows, closes), length)


def macd(closes: list[float], fast: int, slow: int, signal: int) -> tuple[list[float], list[float], list[float]]:
    fast_ema = ema(closes, fast)
    slow_ema = ema(closes, slow)
    line = [f - s if is_valid(f) and is_valid(s) else NAN for f, s in zip(fast_ema, slow_ema)]
    clean_line = [0.0 if not is_valid(v) else float(v) for v in line]
    sig = ema(clean_line, signal)
    hist = [m - s if is_valid(m) and is_valid(s) else NAN for m, s in zip(line, sig)]
    return line, sig, hist


def bollinger_bands(closes: list[float], length: int, std_mult: float) -> tuple[list[float], list[float], list[float], list[float]]:
    n = len(closes)
    length = max(1, int(length))
    mid = sma(closes, length)
    upper = _nan_list(n)
    lower = _nan_list(n)
    percent_b = _nan_list(n)
    for i in range(length - 1, n):
        window = [float(v) for v in closes[i - length + 1 : i + 1]]
        mean = mid[i]
        variance = sum((v - mean) ** 2 for v in window) / float(length)
        sd = math.sqrt(max(0.0, variance))
        upper[i] = mean + (float(std_mult) * sd)
        lower[i] = mean - (float(std_mult) * sd)
        width = upper[i] - lower[i]
        percent_b[i] = (float(closes[i]) - lower[i]) / width if width > 0 else NAN
    return mid, upper, lower, percent_b


def relative_volume(volumes: list[float], length: int = 20) -> list[float]:
    avg = sma(volumes, length)
    out = _nan_list(len(volumes))
    for i, vol in enumerate(volumes):
        if is_valid(avg[i]) and avg[i] > 0:
            out[i] = float(vol) / float(avg[i])
    return out


def vwap(data: OHLCVData) -> list[float]:
    out = _nan_list(len(data))
    cum_pv = 0.0
    cum_vol = 0.0
    current_day = ""
    for i in range(len(data)):
        parsed = parse_timestamp(data.timestamps[i])
        day = parsed.date().isoformat() if parsed else "all"
        if day != current_day:
            current_day = day
            cum_pv = 0.0
            cum_vol = 0.0
        typical = (float(data.highs[i]) + float(data.lows[i]) + float(data.closes[i])) / 3.0
        vol = max(0.0, float(data.volumes[i]))
        if vol <= 0:
            vol = 1.0
        cum_pv += typical * vol
        cum_vol += vol
        out[i] = cum_pv / cum_vol if cum_vol > 0 else NAN
    return out


def donchian_channels(highs: list[float], lows: list[float], lookback: int) -> tuple[list[float], list[float]]:
    """Prior-bar Donchian channels to avoid current-bar lookahead."""

    n = len(highs)
    lookback = max(1, int(lookback))
    upper = _nan_list(n)
    lower = _nan_list(n)
    for i in range(n):
        start = max(0, i - lookback)
        if i - start < 1:
            continue
        upper[i] = max(float(v) for v in highs[start:i])
        lower[i] = min(float(v) for v in lows[start:i])
    return upper, lower


def ichimoku(
    highs: list[float],
    lows: list[float],
    tenkan: int,
    kijun: int,
    senkou_b: int,
) -> dict[str, list[float]]:
    n = len(highs)

    def midpoint(length: int) -> list[float]:
        length = max(1, int(length))
        out = _nan_list(n)
        for i in range(length - 1, n):
            hh = max(float(v) for v in highs[i - length + 1 : i + 1])
            ll = min(float(v) for v in lows[i - length + 1 : i + 1])
            out[i] = (hh + ll) / 2.0
        return out

    ten = midpoint(tenkan)
    kij = midpoint(kijun)
    span_a = [(a + b) / 2.0 if is_valid(a) and is_valid(b) else NAN for a, b in zip(ten, kij)]
    span_b = midpoint(senkou_b)
    top = [max(a, b) if is_valid(a) and is_valid(b) else NAN for a, b in zip(span_a, span_b)]
    bottom = [min(a, b) if is_valid(a) and is_valid(b) else NAN for a, b in zip(span_a, span_b)]
    return {
        "ichimoku_tenkan": ten,
        "ichimoku_kijun": kij,
        "ichimoku_span_a": span_a,
        "ichimoku_span_b": span_b,
        "ichimoku_cloud_top": top,
        "ichimoku_cloud_bottom": bottom,
    }


def supertrend(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    atr_length: int,
    multiplier: float,
) -> tuple[list[float], list[float]]:
    n = len(closes)
    atr_values = atr(highs, lows, closes, atr_length)
    trend = _nan_list(n)
    direction = _nan_list(n)
    final_upper = _nan_list(n)
    final_lower = _nan_list(n)
    for i in range(n):
        if not is_valid(atr_values[i]):
            continue
        hl2 = (float(highs[i]) + float(lows[i])) / 2.0
        basic_upper = hl2 + float(multiplier) * atr_values[i]
        basic_lower = hl2 - float(multiplier) * atr_values[i]
        if i == 0 or not is_valid(final_upper[i - 1]):
            final_upper[i] = basic_upper
            final_lower[i] = basic_lower
            direction[i] = 1.0
            trend[i] = final_lower[i]
            continue
        prev_close = float(closes[i - 1])
        final_upper[i] = basic_upper if basic_upper < final_upper[i - 1] or prev_close > final_upper[i - 1] else final_upper[i - 1]
        final_lower[i] = basic_lower if basic_lower > final_lower[i - 1] or prev_close < final_lower[i - 1] else final_lower[i - 1]
        if float(closes[i]) > final_upper[i - 1]:
            direction[i] = 1.0
        elif float(closes[i]) < final_lower[i - 1]:
            direction[i] = -1.0
        else:
            direction[i] = direction[i - 1]
        trend[i] = final_lower[i] if direction[i] >= 0 else final_upper[i]
    return trend, direction


def label_sessions(data: OHLCVData) -> list[str]:
    labels: list[str] = []
    for raw_ts, raw_session in zip(data.timestamps, data.sessions):
        session = str(raw_session or "").lower()
        if session in {"pre", "premarket", "pre_market"}:
            labels.append("premarket")
            continue
        if session in {"post", "afterhours", "after_hours", "postmarket"}:
            labels.append("after_hours")
            continue
        parsed = parse_timestamp(raw_ts)
        if parsed is None:
            labels.append("regular_session")
            continue
        local_t = parsed.timetz().replace(tzinfo=None)
        if time(13, 30) <= local_t <= time(20, 0):
            labels.append("regular_session")
        else:
            labels.append("extended_hours")
    return labels


def label_market_regimes(data: OHLCVData) -> list[set[str]]:
    close_ma = sma(data.closes, 50)
    atr_values = atr(data.highs, data.lows, data.closes, 14)
    atr_ma = sma([0.0 if not is_valid(v) else v for v in atr_values], 50)
    rv = relative_volume(data.volumes, 20)
    session_labels = label_sessions(data)
    out: list[set[str]] = []
    for i, close in enumerate(data.closes):
        labels = {session_labels[i]}
        if is_valid(close_ma[i]):
            if close > close_ma[i] * 1.01:
                labels.add("trend_up")
            elif close < close_ma[i] * 0.99:
                labels.add("trend_down")
            else:
                labels.add("range_chop")
        else:
            labels.add("range_chop")
        if is_valid(atr_values[i]) and is_valid(atr_ma[i]) and atr_ma[i] > 0:
            labels.add("high_volatility" if atr_values[i] > atr_ma[i] else "low_volatility")
        if is_valid(rv[i]):
            labels.add("high_volume" if rv[i] >= 1.2 else "low_volume")
        out.append(labels)
    return out


@dataclass
class IndicatorCache:
    data: OHLCVData
    values: dict[tuple[str, tuple[tuple[str, Any], ...]], Any] = field(default_factory=dict)

    def get(self, name: str, **params: Any) -> Any:
        key = (name, tuple(sorted(params.items())))
        if key in self.values:
            return self.values[key]
        if name == "sma":
            value = sma(self.data.closes, int(params["length"]))
        elif name == "ema":
            value = ema(self.data.closes, int(params["length"]))
        elif name == "ma":
            value = moving_average(self.data.closes, int(params["length"]), str(params.get("ma_type") or "ema"))
        elif name == "rsi":
            value = rsi(self.data.closes, int(params["length"]))
        elif name == "atr":
            value = atr(self.data.highs, self.data.lows, self.data.closes, int(params["length"]))
        elif name == "macd":
            value = macd(self.data.closes, int(params["fast"]), int(params["slow"]), int(params["signal"]))
        elif name == "bollinger":
            value = bollinger_bands(self.data.closes, int(params["length"]), float(params["std_mult"]))
        elif name == "vwap":
            value = vwap(self.data)
        elif name == "relative_volume":
            value = relative_volume(self.data.volumes, int(params.get("length") or 20))
        elif name == "donchian":
            value = donchian_channels(self.data.highs, self.data.lows, int(params["lookback"]))
        elif name == "ichimoku":
            value = ichimoku(
                self.data.highs,
                self.data.lows,
                int(params["tenkan"]),
                int(params["kijun"]),
                int(params["senkou_b"]),
            )
        elif name == "supertrend":
            value = supertrend(
                self.data.highs,
                self.data.lows,
                self.data.closes,
                int(params["atr_length"]),
                float(params["multiplier"]),
            )
        elif name == "regimes":
            value = label_market_regimes(self.data)
        else:
            raise KeyError(f"unknown indicator: {name}")
        self.values[key] = value
        return value
