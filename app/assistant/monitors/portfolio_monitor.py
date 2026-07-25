"""
Portfolio state monitor
Detects significant portfolio changes
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..events.event import Event, EventPriority, EventSeverity
from .base_monitor import BaseMonitor


def _safe_float(x: Any) -> Optional[float]:
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


class PortfolioMonitor(BaseMonitor):
    """
    Monitors portfolio for significant changes
    - Large value swings
    - Position concentration risks
    - New/closed positions
    """

    def __init__(self, db_path: Path, config: Dict[str, Any]):
        interval = config.get("interval_sec", 300)  # 5 min default
        super().__init__("portfolio", interval, db_path, config)

        # Thresholds
        self.portfolio_swing_pct = config.get("triggers", {}).get("portfolio_swing_pct", 5.0)
        self.position_swing_pct = config.get("triggers", {}).get("position_swing_pct", 10.0)
        self.concentration_threshold_pct = config.get("triggers", {}).get("concentration_threshold_pct", 25.0)
        self.buying_power_critical_pct = config.get("triggers", {}).get("buying_power_critical_pct", 5.0)

        # State tracking
        self.last_snapshot: Optional[Dict[str, Any]] = None

    def check(self) -> List[Event]:
        """Check portfolio for significant changes"""
        events = []

        try:
            # Get portfolio data from broker registry
            snapshot = self._get_portfolio_snapshot()

            if snapshot and self.last_snapshot:
                # Check for portfolio-level changes
                portfolio_events = self._check_portfolio_changes(snapshot, self.last_snapshot)
                events.extend(portfolio_events)

                # Check for position-level changes
                position_events = self._check_position_changes(snapshot, self.last_snapshot)
                events.extend(position_events)

            self.last_snapshot = snapshot

        except Exception as e:
            print(f"[PortfolioMonitor] Error getting snapshot: {e}")

        return events

    def _get_portfolio_snapshot(self) -> Optional[Dict[str, Any]]:
        """Get current portfolio snapshot"""
        try:
            # Import here to avoid circular dependency
            from ...brokers.registry import get_portfolio_context_data, get_portfolio_performance_context

            portfolio = get_portfolio_context_data(db_path=str(self.db_path), max_positions=100)
            performance = get_portfolio_performance_context(db_path=str(self.db_path))

            return {
                "portfolio": portfolio,
                "performance": performance
            }
        except Exception:
            return None

    def _check_portfolio_changes(
        self,
        current: Dict[str, Any],
        previous: Dict[str, Any]
    ) -> List[Event]:
        """Check for portfolio-level changes"""
        events = []

        curr_perf = current.get("performance", {})
        prev_perf = previous.get("performance", {})

        curr_equity = _safe_float(curr_perf.get("current_equity"))
        prev_equity = _safe_float(prev_perf.get("current_equity"))

        if curr_equity and prev_equity and prev_equity > 0:
            pct_change = ((curr_equity - prev_equity) / prev_equity) * 100.0

            if abs(pct_change) >= self.portfolio_swing_pct:
                severity = EventSeverity.CRITICAL if abs(pct_change) >= 10.0 else EventSeverity.WARNING

                events.append(Event(
                    event_type="portfolio_swing_major",
                    severity=severity,
                    priority=EventPriority.CRITICAL if severity == EventSeverity.CRITICAL else EventPriority.HIGH,
                    description=f"Portfolio {'gained' if pct_change > 0 else 'lost'} {abs(pct_change):.2f}% ({pct_change:+.2f}%)",
                    data={
                        "current_equity": curr_equity,
                        "previous_equity": prev_equity,
                        "change_pct": pct_change,
                        "change_dollars": curr_equity - prev_equity
                    }
                ))

        # Check concentration risk
        concentration_events = self._check_concentration(current)
        events.extend(concentration_events)

        return events

    def _check_concentration(self, snapshot: Dict[str, Any]) -> List[Event]:
        """Check for position concentration risks"""
        events = []

        portfolio = snapshot.get("portfolio", [])
        total_value = 0.0
        positions = []

        for broker_snap in portfolio:
            for account in broker_snap.get("accounts", []):
                for pos in account.get("positions", []):
                    mv = _safe_float(pos.get("market_value"))
                    if mv:
                        total_value += mv
                        positions.append({
                            "symbol": pos.get("symbol"),
                            "value": mv
                        })

        if total_value > 0:
            for pos in positions:
                weight = (pos["value"] / total_value) * 100.0
                if weight >= self.concentration_threshold_pct:
                    events.append(Event(
                        event_type="concentration_risk",
                        severity=EventSeverity.WARNING,
                        priority=EventPriority.MEDIUM,
                        description=f"{pos['symbol']} represents {weight:.1f}% of portfolio (threshold: {self.concentration_threshold_pct}%)",
                        tickers=[pos["symbol"]],
                        data={
                            "ticker": pos["symbol"],
                            "weight_pct": weight,
                            "value": pos["value"],
                            "total_portfolio": total_value
                        }
                    ))

        return events

    def _check_position_changes(
        self,
        current: Dict[str, Any],
        previous: Dict[str, Any]
    ) -> List[Event]:
        """Check for individual position changes"""
        events = []

        # Build position maps
        curr_positions = self._build_position_map(current)
        prev_positions = self._build_position_map(previous)

        # Check for new positions
        for ticker, pos in curr_positions.items():
            if ticker not in prev_positions:
                mv = _safe_float(pos.get("market_value"))
                if mv and mv > 1000:  # Only report significant new positions
                    events.append(Event(
                        event_type="position_opened",
                        severity=EventSeverity.INFO,
                        priority=EventPriority.LOW,
                        description=f"New position opened: {ticker} (${mv:,.2f})",
                        tickers=[ticker],
                        data=pos
                    ))

        # Check for closed positions
        for ticker, pos in prev_positions.items():
            if ticker not in curr_positions:
                mv = _safe_float(pos.get("market_value"))
                events.append(Event(
                    event_type="position_closed",
                    severity=EventSeverity.INFO,
                    priority=EventPriority.LOW,
                    description=f"Position closed: {ticker}",
                    tickers=[ticker],
                    data=pos
                ))

        # Check for large position swings
        for ticker in set(curr_positions.keys()) & set(prev_positions.keys()):
            curr_mv = _safe_float(curr_positions[ticker].get("market_value"))
            prev_mv = _safe_float(prev_positions[ticker].get("market_value"))

            if curr_mv and prev_mv and prev_mv > 0:
                pct_change = ((curr_mv - prev_mv) / prev_mv) * 100.0

                if abs(pct_change) >= self.position_swing_pct:
                    events.append(Event(
                        event_type="position_swing",
                        severity=EventSeverity.WARNING,
                        priority=EventPriority.MEDIUM,
                        description=f"{ticker} {'gained' if pct_change > 0 else 'lost'} {abs(pct_change):.1f}%",
                        tickers=[ticker],
                        data={
                            "ticker": ticker,
                            "current_value": curr_mv,
                            "previous_value": prev_mv,
                            "change_pct": pct_change,
                            "change_dollars": curr_mv - prev_mv
                        }
                    ))

        return events

    def _build_position_map(self, snapshot: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Build map of ticker -> position data"""
        positions = {}

        for broker_snap in snapshot.get("portfolio", []):
            for account in broker_snap.get("accounts", []):
                for pos in account.get("positions", []):
                    ticker = pos.get("symbol")
                    if ticker:
                        # Aggregate if same ticker across accounts
                        if ticker in positions:
                            curr_mv = _safe_float(positions[ticker].get("market_value", 0))
                            new_mv = _safe_float(pos.get("market_value", 0))
                            positions[ticker]["market_value"] = (curr_mv or 0) + (new_mv or 0)
                        else:
                            positions[ticker] = pos.copy()

        return positions
