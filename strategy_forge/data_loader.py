from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from . import DEFAULT_DATA_DIR


@dataclass
class OHLCVData:
    """Normalized OHLCV arrays used by Strategy Forge backtests."""

    symbol: str
    timeframe: str
    timestamps: list[str]
    opens: list[float]
    highs: list[float]
    lows: list[float]
    closes: list[float]
    volumes: list[float]
    sessions: list[str]
    source: str = ""

    def __len__(self) -> int:
        return len(self.closes)

    def subset(self, start: int, end: int) -> "OHLCVData":
        start_i = max(0, int(start))
        end_i = max(start_i, min(int(end), len(self)))
        return OHLCVData(
            symbol=self.symbol,
            timeframe=self.timeframe,
            timestamps=self.timestamps[start_i:end_i],
            opens=self.opens[start_i:end_i],
            highs=self.highs[start_i:end_i],
            lows=self.lows[start_i:end_i],
            closes=self.closes[start_i:end_i],
            volumes=self.volumes[start_i:end_i],
            sessions=self.sessions[start_i:end_i],
            source=self.source,
        )

    def parsed_times(self) -> list[Optional[datetime]]:
        return [parse_timestamp(ts) for ts in self.timestamps]


def parse_timestamp(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    for candidate in (text, text.replace(" ", "T")):
        try:
            parsed = datetime.fromisoformat(candidate)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y %H:%M", "%m/%d/%Y"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


def _first(row: dict[str, Any], names: Iterable[str], default: Any = None) -> Any:
    lowered = {str(k).lower(): v for k, v in row.items()}
    for name in names:
        if name in row:
            return row[name]
        low = name.lower()
        if low in lowered:
            return lowered[low]
    return default


def _float_or_none(value: Any) -> Optional[float]:
    try:
        if value in (None, "", "None", "nan"):
            return None
        out = float(value)
        return out if out == out else None
    except Exception:
        return None


def normalize_rows(
    rows: Iterable[dict[str, Any]],
    *,
    symbol: str,
    timeframe: str,
    source: str = "",
) -> OHLCVData:
    timestamps: list[str] = []
    opens: list[float] = []
    highs: list[float] = []
    lows: list[float] = []
    closes: list[float] = []
    volumes: list[float] = []
    sessions: list[str] = []

    for row in rows:
        if not isinstance(row, dict):
            continue
        o = _float_or_none(_first(row, ("open", "open_price", "Open")))
        h = _float_or_none(_first(row, ("high", "high_price", "High")))
        l = _float_or_none(_first(row, ("low", "low_price", "Low")))
        c = _float_or_none(_first(row, ("close", "close_price", "Close", "adj_close")))
        if o is None or h is None or l is None or c is None:
            continue
        if min(o, h, l, c) <= 0 or h < l or o < l or o > h or c < l or c > h:
            continue
        ts = _first(row, ("timestamp", "datetime", "date", "time", "begins_at", "beginsAt"), "")
        vol = _float_or_none(_first(row, ("volume", "Volume"), 0.0)) or 0.0
        session = str(_first(row, ("session", "market_session"), "") or "").strip().lower()
        timestamps.append(str(ts))
        opens.append(float(o))
        highs.append(float(h))
        lows.append(float(l))
        closes.append(float(c))
        volumes.append(float(vol))
        sessions.append(session)

    return OHLCVData(
        symbol=str(symbol).strip().upper(),
        timeframe=str(timeframe).strip().lower(),
        timestamps=timestamps,
        opens=opens,
        highs=highs,
        lows=lows,
        closes=closes,
        volumes=volumes,
        sessions=sessions,
        source=source,
    )


def _read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def _read_json(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        payload = json.load(fh)
    if isinstance(payload, dict):
        for key in ("rows", "candles", "data", "historicals"):
            value = payload.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
        return []
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    return []


def _candidate_paths(symbol: str, timeframe: str, data_dir: Optional[Path]) -> list[Path]:
    sym = symbol.upper()
    tf = timeframe.lower()
    roots = []
    if data_dir is not None:
        roots.append(Path(data_dir))
    roots.extend(
        [
            DEFAULT_DATA_DIR / "history",
            Path("app") / "data" / "history",
            Path("data"),
        ]
    )
    names = [
        f"{sym}_{tf}.csv",
        f"{sym}-{tf}.csv",
        f"{sym}_{tf}.json",
        f"{sym}-{tf}.json",
        f"{sym}.csv",
        f"{sym}.json",
        str(Path(tf) / f"{sym}.csv"),
        str(Path(tf) / f"{sym}.json"),
    ]
    out: list[Path] = []
    for root in roots:
        for name in names:
            out.append(root / name)
    return out


def find_history_file(symbol: str, timeframe: str, data_dir: Optional[str | Path] = None) -> Optional[Path]:
    root = Path(data_dir) if data_dir else None
    for path in _candidate_paths(symbol, timeframe, root):
        if path.exists() and path.is_file():
            return path
    return None


def load_ohlcv(
    symbol: str,
    timeframe: str,
    *,
    data_dir: Optional[str | Path] = None,
    path: Optional[str | Path] = None,
    min_candles: int = 0,
    broker_hint: str = "robinhood",
    include_extended: bool = False,
    allow_project_fetch: bool = True,
) -> OHLCVData:
    """Load OHLCV data from CSV/JSON, falling back to project market fetchers.

    File data is preferred because optimization runs must be reproducible. The
    project fetcher fallback never appends a live quote.
    """

    resolved = Path(path) if path else find_history_file(symbol, timeframe, data_dir)
    if resolved is not None:
        suffix = resolved.suffix.lower()
        rows = _read_json(resolved) if suffix == ".json" else _read_csv(resolved)
        data = normalize_rows(rows, symbol=symbol, timeframe=timeframe, source=str(resolved))
        if min_candles and len(data) < int(min_candles):
            raise ValueError(f"{resolved} has {len(data)} candles; need at least {min_candles}")
        return data

    if allow_project_fetch:
        try:
            from app.main import _market_fetch_ohlc  # type: ignore

            opens, highs, lows, closes, rows, requested_bounds = _market_fetch_ohlc(
                symbol,
                timeframe,
                broker_hint=broker_hint,
                min_candles=min_candles,
                include_extended=include_extended,
            )
            if rows:
                data = normalize_rows(
                    rows,
                    symbol=symbol,
                    timeframe=timeframe,
                    source=f"app.main._market_fetch_ohlc:{requested_bounds}",
                )
            else:
                synthetic = [
                    {"open": opens[i], "high": highs[i], "low": lows[i], "close": closes[i], "volume": 0.0}
                    for i in range(min(len(opens), len(highs), len(lows), len(closes)))
                ]
                data = normalize_rows(synthetic, symbol=symbol, timeframe=timeframe, source="synthetic_project_fetch")
            if data:
                return data
        except Exception:
            pass

    raise FileNotFoundError(
        f"No OHLCV history found for {symbol} {timeframe}. Provide --data-dir or --data-file."
    )


def load_many(
    symbols: Iterable[str],
    timeframes: Iterable[str],
    *,
    data_dir: Optional[str | Path] = None,
    min_candles: int = 0,
    include_extended: bool = False,
) -> dict[tuple[str, str], OHLCVData]:
    out: dict[tuple[str, str], OHLCVData] = {}
    for symbol in symbols:
        for timeframe in timeframes:
            data = load_ohlcv(
                symbol,
                timeframe,
                data_dir=data_dir,
                min_candles=min_candles,
                include_extended=include_extended,
            )
            out[(data.symbol, data.timeframe)] = data
    return out
