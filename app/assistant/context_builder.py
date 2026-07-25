"""
Enhanced context builder for AI assistant
Aggregates portfolio, cryptid signals, indicators, market data, and memory
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from ..brokers.registry import get_portfolio_context_data, get_portfolio_performance_context
except Exception:
    get_portfolio_context_data = None
    get_portfolio_performance_context = None

try:
    from ..assistant_indicators import build_robinhood_indicator_context
except Exception:
    build_robinhood_indicator_context = None


def _utc_ts() -> int:
    return int(time.time())


def _safe_float(x: Any) -> Optional[float]:
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


class ContextBuilder:
    """
    Builds comprehensive context for AI assistant
    """

    def __init__(self, db_path: Path, runs_dir: Path, memory_manager: Optional[Any] = None):
        self.db_path = db_path
        self.runs_dir = runs_dir
        self.memory = memory_manager

    def build_full_context(
        self,
        include_portfolio: bool = True,
        include_cryptids: bool = True,
        include_indicators: bool = False,
        include_memory: bool = True,
        memory_query: Optional[str] = None,
        max_memory_items: int = 5
    ) -> Dict[str, Any]:
        """
        Build comprehensive context for AI

        Args:
            include_portfolio: Include portfolio positions and P/L
            include_cryptids: Include running cryptid status
            include_indicators: Include technical indicators (expensive)
            include_memory: Include relevant memory context
            memory_query: Semantic search query for memory
            max_memory_items: Max memory items to retrieve

        Returns:
            Complete context dict
        """
        context: Dict[str, Any] = {
            "generated_at": _utc_ts(),
            "timestamp_readable": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
        }

        # Portfolio data
        if include_portfolio:
            context["portfolio"] = self._get_portfolio_context()
            context["portfolio_performance"] = self._get_performance_context()
            context["risk_metrics"] = self._calculate_risk_metrics(context.get("portfolio", []))

        # Running cryptids
        if include_cryptids:
            context["cryptids"] = self._get_cryptid_context()
            context["signal_summary"] = self._summarize_signals(context.get("cryptids", []))

        # Technical indicators
        if include_indicators:
            context["indicators"] = self._get_indicator_context(context.get("portfolio", []))

        # Relevant memory
        if include_memory and self.memory:
            if memory_query:
                context["relevant_memory"] = self._search_memory(memory_query, max_memory_items)
            else:
                context["recent_memory"] = self._get_recent_memory(max_memory_items)

        return context

    def _get_portfolio_context(self) -> List[Dict[str, Any]]:
        """Get current portfolio holdings"""
        if get_portfolio_context_data is None:
            return []

        try:
            return get_portfolio_context_data(db_path=str(self.db_path), max_positions=50)
        except Exception:
            return []

    def _get_performance_context(self) -> Dict[str, Any]:
        """Get portfolio performance metrics"""
        if get_portfolio_performance_context is None:
            return {}

        try:
            return get_portfolio_performance_context(db_path=str(self.db_path))
        except Exception:
            return {}

    def _calculate_risk_metrics(self, portfolio: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Calculate portfolio risk metrics

        Returns:
            Risk assessment dict
        """
        if not portfolio:
            return {}

        total_value = 0.0
        position_values = []
        tickers = []

        for broker_snap in portfolio:
            for account in broker_snap.get("accounts", []):
                for pos in account.get("positions", []):
                    mv = _safe_float(pos.get("market_value"))
                    if mv:
                        total_value += mv
                        position_values.append(mv)
                        tickers.append(pos.get("symbol"))

        if total_value == 0:
            return {"total_value": 0.0}

        # Position weights
        weights = [v / total_value for v in position_values]

        # Concentration (max weight)
        max_weight = max(weights) if weights else 0.0

        # Count positions
        num_positions = len(position_values)

        # HHI (Herfindahl-Hirschman Index) for concentration
        hhi = sum(w * w for w in weights) if weights else 0.0

        return {
            "total_value": total_value,
            "num_positions": num_positions,
            "max_position_weight": max_weight,
            "concentration_index": hhi,
            "diversification_score": 1.0 - hhi if hhi > 0 else 0.0,
            "top_holdings": [
                {"ticker": tickers[i], "weight": weights[i]}
                for i in sorted(range(len(weights)), key=lambda x: weights[x], reverse=True)[:5]
            ] if tickers else []
        }

    def _get_cryptid_context(self) -> List[Dict[str, Any]]:
        """Get running cryptid status and signals"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cur.execute("""
            SELECT r.id, r.algorithm_name, r.status, r.run_dir, r.pid, r.start_ts,
                   b.name as base_script_name
            FROM runs r
            LEFT JOIN algorithms a ON a.id = r.algorithm_id
            LEFT JOIN base_scripts b ON b.id = a.base_script_id
            WHERE r.status IN ('running', 'crashed')
            ORDER BY r.id DESC
            LIMIT 20
        """)

        rows = cur.fetchall()
        conn.close()

        cryptids = []
        for row in rows:
            run_dir = Path(row["run_dir"])
            status_file = run_dir / "status.json"

            status_data = {}
            if status_file.exists():
                try:
                    status_data = json.loads(status_file.read_text())
                except Exception:
                    pass

            # Extract signal counts
            signal_counts = {"BUY": 0, "SELL": 0, "HOLD": 0}
            tickers_by_signal = {"BUY": [], "SELL": [], "HOLD": []}

            for ticker_data in status_data.get("tickers", []):
                signal = str(ticker_data.get("signal", "")).upper()
                ticker = str(ticker_data.get("symbol") or ticker_data.get("ticker") or "").strip().upper()
                if signal in signal_counts:
                    signal_counts[signal] += 1
                    if ticker:
                        tickers_by_signal[signal].append(ticker)

            runtime = _utc_ts() - int(row["start_ts"]) if row["start_ts"] else 0

            cryptids.append({
                "id": int(row["id"]),
                "name": row["algorithm_name"],
                "base_script": row["base_script_name"] or "",
                "status": row["status"],
                "pid": row["pid"],
                "runtime_seconds": runtime,
                "last_heartbeat": status_data.get("ts") or status_data.get("heartbeat"),
                "pnl": status_data.get("pnl"),
                "trades": status_data.get("trades"),
                "signal_counts": signal_counts,
                "signals": tickers_by_signal
            })

        return cryptids

    def _summarize_signals(self, cryptids: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Aggregate signals across all cryptids

        Returns:
            Signal consensus summary
        """
        if not cryptids:
            return {}

        all_buy = set()
        all_sell = set()
        all_hold = set()

        buy_votes: Dict[str, int] = {}
        sell_votes: Dict[str, int] = {}

        for cryptid in cryptids:
            signals = cryptid.get("signals", {})
            for ticker in signals.get("BUY", []):
                all_buy.add(ticker)
                buy_votes[ticker] = buy_votes.get(ticker, 0) + 1

            for ticker in signals.get("SELL", []):
                all_sell.add(ticker)
                sell_votes[ticker] = sell_votes.get(ticker, 0) + 1

            for ticker in signals.get("HOLD", []):
                all_hold.add(ticker)

        # Find consensus (3+ cryptids agree)
        strong_buys = [t for t, v in buy_votes.items() if v >= 3]
        strong_sells = [t for t, v in sell_votes.items() if v >= 3]

        # Find divergence (split opinions)
        all_tickers = all_buy | all_sell | all_hold
        divergent = []
        for ticker in all_tickers:
            if ticker in all_buy and ticker in all_sell:
                divergent.append({
                    "ticker": ticker,
                    "buy_votes": buy_votes.get(ticker, 0),
                    "sell_votes": sell_votes.get(ticker, 0)
                })

        return {
            "total_cryptids": len(cryptids),
            "unique_tickers": len(all_tickers),
            "strong_consensus_buy": strong_buys,
            "strong_consensus_sell": strong_sells,
            "divergent_signals": divergent,
            "overall_sentiment": self._calculate_sentiment(buy_votes, sell_votes)
        }

    def _calculate_sentiment(self, buy_votes: Dict[str, int], sell_votes: Dict[str, int]) -> str:
        """Calculate overall market sentiment from signals"""
        total_buy = sum(buy_votes.values())
        total_sell = sum(sell_votes.values())
        total = total_buy + total_sell

        if total == 0:
            return "neutral"

        buy_pct = total_buy / total

        if buy_pct > 0.65:
            return "bullish"
        elif buy_pct < 0.35:
            return "bearish"
        else:
            return "mixed"

    def _get_indicator_context(self, portfolio: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Get technical indicators for held positions"""
        if build_robinhood_indicator_context is None:
            return []

        try:
            return build_robinhood_indicator_context(
                db_path=str(self.db_path),
                portfolio_data=portfolio,
                max_tickers=20
            )
        except Exception:
            return []

    def _search_memory(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Search memory with semantic similarity"""
        if not self.memory:
            return []

        try:
            results = self.memory.search(query, limit=limit, max_age_days=30)
            return [
                {
                    "text": r[1],
                    "metadata": r[2],
                    "relevance": r[3]
                }
                for r in results
            ]
        except Exception:
            return []

    def _get_recent_memory(self, limit: int = 5) -> List[Dict[str, Any]]:
        """Get recent memory items"""
        if not self.memory:
            return []

        try:
            return self.memory.get_recent_context(hours=24, limit=limit)
        except Exception:
            return []
