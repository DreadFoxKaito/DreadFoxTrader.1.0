"""
Signal change monitor
Detects significant changes in cryptid signals
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from ..events.event import Event, EventPriority, EventSeverity
from .base_monitor import BaseMonitor


class SignalMonitor(BaseMonitor):
    """
    Monitors cryptid signals for significant changes
    - Signal flips on held positions
    - Consensus signals (multiple cryptids agree)
    - Divergence (cryptids disagree)
    """

    def __init__(self, db_path: Path, config: Dict[str, Any]):
        interval = config.get("interval_sec", 120)  # 2 min default
        super().__init__("signal", interval, db_path, config)

        # Thresholds
        self.consensus_threshold = config.get("triggers", {}).get("consensus_threshold", 4)
        self.divergence_threshold_pct = config.get("triggers", {}).get("divergence_threshold_pct", 50)
        self.high_conviction_min_cryptids = config.get("triggers", {}).get("high_conviction_min_cryptids", 3)

        # State tracking
        self.last_signals: Dict[int, Dict[str, List[str]]] = {}  # run_id -> {BUY: [...], SELL: [...], HOLD: [...]}
        self.held_positions: Set[str] = set()

    def check(self) -> List[Event]:
        """Check signals for significant changes"""
        events = []

        try:
            # Update held positions
            self._update_held_positions()

            # Get current signals from running cryptids
            current_signals = self._get_current_signals()

            # Check for signal flips on held positions
            flip_events = self._check_signal_flips(current_signals)
            events.extend(flip_events)

            # Check for consensus
            consensus_events = self._check_consensus(current_signals)
            events.extend(consensus_events)

            # Check for divergence on held positions
            divergence_events = self._check_divergence(current_signals)
            events.extend(divergence_events)

            # Update state
            self.last_signals = current_signals

        except Exception as e:
            print(f"[SignalMonitor] Error: {e}")

        return events

    def _update_held_positions(self) -> None:
        """Update list of held positions from portfolio"""
        try:
            from ...brokers.registry import get_portfolio_context_data

            portfolio = get_portfolio_context_data(db_path=str(self.db_path), max_positions=100)

            tickers = set()
            for broker_snap in portfolio:
                for account in broker_snap.get("accounts", []):
                    for pos in account.get("positions", []):
                        ticker = pos.get("symbol")
                        if ticker:
                            tickers.add(ticker)

            self.held_positions = tickers

        except Exception:
            pass

    def _get_current_signals(self) -> Dict[int, Dict[str, List[str]]]:
        """Get current signals from all running cryptids"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cur.execute("""
            SELECT r.id, r.run_dir, r.status
            FROM runs r
            WHERE r.status = 'running'
            ORDER BY r.id DESC
            LIMIT 20
        """)

        rows = cur.fetchall()
        conn.close()

        signals = {}

        for row in rows:
            run_id = int(row["id"])
            run_dir = Path(row["run_dir"])
            status_file = run_dir / "status.json"

            if not status_file.exists():
                continue

            try:
                status_data = json.loads(status_file.read_text())
                run_signals = {"BUY": [], "SELL": [], "HOLD": []}

                for ticker_data in status_data.get("tickers", []):
                    signal = str(ticker_data.get("signal", "")).upper()
                    ticker = str(ticker_data.get("symbol") or ticker_data.get("ticker") or "").strip().upper()

                    if signal in run_signals and ticker:
                        run_signals[signal].append(ticker)

                signals[run_id] = run_signals

            except Exception:
                continue

        return signals

    def _check_signal_flips(self, current_signals: Dict[int, Dict[str, List[str]]]) -> List[Event]:
        """Check for signal flips on held positions"""
        events = []

        for run_id, curr_sig in current_signals.items():
            if run_id not in self.last_signals:
                continue

            prev_sig = self.last_signals[run_id]

            # Check each held position
            for ticker in self.held_positions:
                curr_signal = None
                prev_signal = None

                # Find current signal
                for sig_type, tickers in curr_sig.items():
                    if ticker in tickers:
                        curr_signal = sig_type
                        break

                # Find previous signal
                for sig_type, tickers in prev_sig.items():
                    if ticker in tickers:
                        prev_signal = sig_type
                        break

                # Detect flip
                if curr_signal and prev_signal and curr_signal != prev_signal:
                    # Ignore HOLD changes (less significant)
                    if prev_signal != "HOLD" and curr_signal != "HOLD":
                        events.append(Event(
                            event_type="signal_flip_held",
                            severity=EventSeverity.WARNING,
                            priority=EventPriority.HIGH,
                            description=f"Signal flip on held position {ticker}: {prev_signal} → {curr_signal} (run {run_id})",
                            tickers=[ticker],
                            data={
                                "ticker": ticker,
                                "previous_signal": prev_signal,
                                "current_signal": curr_signal,
                                "run_id": run_id
                            }
                        ))

        return events

    def _check_consensus(self, current_signals: Dict[int, Dict[str, List[str]]]) -> List[Event]:
        """Check for strong consensus across cryptids"""
        events = []

        # Count votes per ticker
        buy_votes: Dict[str, int] = {}
        sell_votes: Dict[str, int] = {}

        for run_id, signals in current_signals.items():
            for ticker in signals.get("BUY", []):
                buy_votes[ticker] = buy_votes.get(ticker, 0) + 1

            for ticker in signals.get("SELL", []):
                sell_votes[ticker] = sell_votes.get(ticker, 0) + 1

        # Find strong consensus
        for ticker, votes in buy_votes.items():
            if votes >= self.consensus_threshold:
                events.append(Event(
                    event_type="signal_consensus",
                    severity=EventSeverity.WARNING,
                    priority=EventPriority.HIGH,
                    description=f"Strong BUY consensus on {ticker}: {votes} cryptids agree",
                    tickers=[ticker],
                    data={
                        "ticker": ticker,
                        "signal": "BUY",
                        "vote_count": votes,
                        "is_held": ticker in self.held_positions
                    }
                ))

        for ticker, votes in sell_votes.items():
            if votes >= self.consensus_threshold:
                # Higher priority if we hold the position
                severity = EventSeverity.WARNING if ticker in self.held_positions else EventSeverity.INFO

                events.append(Event(
                    event_type="signal_consensus",
                    severity=severity,
                    priority=EventPriority.HIGH if ticker in self.held_positions else EventPriority.MEDIUM,
                    description=f"Strong SELL consensus on {ticker}: {votes} cryptids agree",
                    tickers=[ticker],
                    data={
                        "ticker": ticker,
                        "signal": "SELL",
                        "vote_count": votes,
                        "is_held": ticker in self.held_positions
                    }
                ))

        return events

    def _check_divergence(self, current_signals: Dict[int, Dict[str, List[str]]]) -> List[Event]:
        """Check for divergent signals on held positions"""
        events = []

        # Count opinions per held ticker
        for ticker in self.held_positions:
            buy_count = 0
            sell_count = 0
            total_count = 0

            for run_id, signals in current_signals.items():
                if ticker in signals.get("BUY", []):
                    buy_count += 1
                    total_count += 1
                elif ticker in signals.get("SELL", []):
                    sell_count += 1
                    total_count += 1

            # Divergence: both buy and sell signals present
            if buy_count > 0 and sell_count > 0 and total_count >= 3:
                divergence_pct = (min(buy_count, sell_count) / total_count) * 100.0

                if divergence_pct >= self.divergence_threshold_pct:
                    events.append(Event(
                        event_type="signal_divergence",
                        severity=EventSeverity.INFO,
                        priority=EventPriority.MEDIUM,
                        description=f"Divergent signals on held position {ticker}: {buy_count} BUY, {sell_count} SELL",
                        tickers=[ticker],
                        data={
                            "ticker": ticker,
                            "buy_votes": buy_count,
                            "sell_votes": sell_count,
                            "total_votes": total_count
                        }
                    ))

        return events
