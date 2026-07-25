"""
Self-learning memory system
Allows AI to reflect on past experiences and update its own memory
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np


class MemoryLearning:
    """
    Enables AI to learn from past experiences and update memory
    """

    def __init__(self, db_path: Path, memory_manager: Any, ollama_client: Any):
        self.db_path = db_path
        self.memory = memory_manager
        self.ollama = ollama_client

    def reflect_on_events(
        self,
        lookback_days: int = 7,
        min_events: int = 5
    ) -> Dict[str, Any]:
        """
        Analyze recent events to extract patterns and learnings

        Args:
            lookback_days: How many days to review
            min_events: Minimum events needed for reflection

        Returns:
            Reflection summary with insights
        """
        # Get recent events
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cutoff = int(time.time()) - (lookback_days * 86400)

        cur.execute("""
            SELECT e.*, a.analysis_text, a.model_used
            FROM assistant_events e
            LEFT JOIN assistant_analyses a ON a.event_id = e.id
            WHERE e.ts >= ?
            ORDER BY e.ts DESC
        """, (cutoff,))

        events = cur.fetchall()
        conn.close()

        if len(events) < min_events:
            return {
                "status": "insufficient_data",
                "events_found": len(events),
                "min_required": min_events
            }

        # Build event summary for reflection
        event_summaries = []
        for event in events:
            event_data = {
                "timestamp": event["ts"],
                "type": event["event_type"],
                "severity": event["severity"],
                "tickers": json.loads(event["tickers"] or "[]"),
                "description": self._get_event_description(event),
                "had_analysis": bool(event["analysis_text"])
            }
            event_summaries.append(event_data)

        # Generate reflection prompt
        prompt = self._build_reflection_prompt(event_summaries, lookback_days)

        # Get AI reflection
        messages = [
            {"role": "system", "content": self._reflection_system_prompt()},
            {"role": "user", "content": prompt}
        ]

        try:
            reflection_model = os.getenv("OLLAMA_MODEL_STRATEGIC") or os.getenv("OLLAMA_MODEL") or getattr(
                self.ollama, "default_model", "qwen2.5:14b"
            )
            response = self.ollama.chat(messages, model=reflection_model)
            reflection_text = response.get("message", {}).get("content", "")

            # Store reflection in memory
            if reflection_text:
                self.memory.store_analysis(
                    analysis_text=reflection_text,
                    model_used=reflection_model,
                    recommendations=[]
                )

                # Extract and store insights
                insights = self._extract_insights(reflection_text)
                for insight in insights:
                    self.memory.store_event(
                        event_type="learning_insight",
                        description=insight["text"],
                        data={"confidence": insight.get("confidence", 0.5), "source": "self_reflection"}
                    )

            return {
                "status": "success",
                "events_analyzed": len(events),
                "reflection": reflection_text,
                "insights_extracted": len(insights) if reflection_text else 0,
                "timestamp": int(time.time())
            }

        except Exception as e:
            return {
                "status": "error",
                "error": str(e)
            }

    def extract_patterns(
        self,
        event_type: Optional[str] = None,
        lookback_days: int = 30
    ) -> List[Dict[str, Any]]:
        """
        Find recurring patterns in historical events

        Args:
            event_type: Filter by specific event type
            lookback_days: Analysis window

        Returns:
            List of detected patterns
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cutoff = int(time.time()) - (lookback_days * 86400)

        query = "SELECT * FROM assistant_events WHERE ts >= ?"
        params = [cutoff]

        if event_type:
            query += " AND event_type = ?"
            params.append(event_type)

        query += " ORDER BY ts ASC"

        cur.execute(query, params)
        events = cur.fetchall()
        conn.close()

        if len(events) < 3:
            return []

        patterns = []

        # Pattern 1: Ticker correlation
        ticker_patterns = self._find_ticker_correlations(events)
        patterns.extend(ticker_patterns)

        # Pattern 2: Time-of-day patterns
        time_patterns = self._find_temporal_patterns(events)
        patterns.extend(time_patterns)

        # Pattern 3: Event cascades (one event leads to another)
        cascade_patterns = self._find_event_cascades(events)
        patterns.extend(cascade_patterns)

        # Store significant patterns in memory
        for pattern in patterns:
            if pattern.get("confidence", 0) > 0.7:
                description = self._format_pattern_description(pattern)
                self.memory.store_event(
                    event_type="pattern_discovered",
                    description=description,
                    data=pattern
                )

        return patterns

    def evaluate_predictions(self) -> Dict[str, Any]:
        """
        Review past predictions/analyses and check accuracy

        Returns:
            Evaluation summary
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Get analyses with recommendations
        cutoff = int(time.time()) - (14 * 86400)  # Last 2 weeks

        cur.execute("""
            SELECT a.*, e.event_type, e.tickers, e.data_json
            FROM assistant_analyses a
            LEFT JOIN assistant_events e ON e.id = a.event_id
            WHERE a.ts >= ? AND a.recommendations_json IS NOT NULL
            ORDER BY a.ts ASC
        """, (cutoff,))

        analyses = cur.fetchall()
        conn.close()

        if not analyses:
            return {
                "status": "no_predictions",
                "message": "No analyses with recommendations found"
            }

        evaluations = []

        for analysis in analyses:
            eval_result = self._evaluate_single_prediction(analysis)
            if eval_result:
                evaluations.append(eval_result)

        # Generate learning from evaluations
        if evaluations:
            learning_text = self._generate_learning_from_evaluations(evaluations)
            if learning_text:
                self.memory.store_event(
                    event_type="self_correction",
                    description=learning_text,
                    data={"evaluations": len(evaluations), "timestamp": int(time.time())}
                )

        return {
            "status": "success",
            "evaluations_performed": len(evaluations),
            "correct_predictions": sum(1 for e in evaluations if e.get("correct")),
            "learning_generated": bool(learning_text) if evaluations else False
        }

    def _get_event_description(self, event: sqlite3.Row) -> str:
        """Build human-readable event description"""
        try:
            stored = str(event["description"] or "").strip()
            if stored:
                return stored
        except Exception:
            pass

        data = json.loads(event["data_json"] or "{}")

        # Use stored description or build from data
        desc = []
        if event["event_type"] == "portfolio_swing_major":
            pct = data.get("change_pct", 0)
            desc.append(f"Portfolio {'gained' if pct > 0 else 'lost'} {abs(pct):.1f}%")

        elif event["event_type"] == "signal_consensus":
            ticker = data.get("ticker", "?")
            signal = data.get("signal", "?")
            votes = data.get("vote_count", 0)
            desc.append(f"{votes} cryptids consensus {signal} on {ticker}")

        elif event["event_type"] == "cryptid_crashed":
            name = data.get("name", "unknown")
            desc.append(f"Cryptid {name} crashed")

        else:
            # Generic description
            desc.append(event["event_type"].replace("_", " ").title())

        return " ".join(desc)

    def _build_reflection_prompt(self, events: List[Dict[str, Any]], days: int) -> str:
        """Build prompt for reflection"""
        events_json = json.dumps(events, indent=2)

        return f"""Review the following {len(events)} events from the past {days} days and provide insights:

EVENTS:
{events_json}

Analyze:
1. What patterns do you notice?
2. Which predictions/analyses were correct? Which were wrong?
3. What did you learn about market behavior?
4. What did you learn about the cryptids' performance?
5. What should you remember for next time?

Provide 3-5 specific, actionable insights based on this data.
Format each insight as: "INSIGHT: [specific learning]"
"""

    def _reflection_system_prompt(self) -> str:
        """System prompt for reflection"""
        return """You are ZENKO PRIME in reflection mode.

Review your past events and analyses to extract learnings.
Be honest about mistakes and clear about successful predictions.
Focus on actionable patterns that will improve future analysis.

Your insights should be:
- Specific (reference actual events/tickers/timeframes)
- Evidence-based (cite what happened)
- Actionable (inform future decisions)
- Humble (acknowledge uncertainty and mistakes)

Format insights clearly so they can be stored in memory."""

    def _extract_insights(self, reflection_text: str) -> List[Dict[str, Any]]:
        """Extract structured insights from reflection text"""
        insights = []

        # Look for lines starting with "INSIGHT:"
        for line in reflection_text.split("\n"):
            line = line.strip()
            if line.upper().startswith("INSIGHT:"):
                insight_text = line[8:].strip()
                if insight_text:
                    # Estimate confidence based on language
                    confidence = 0.5
                    if any(word in insight_text.lower() for word in ["consistently", "always", "never", "strong"]):
                        confidence = 0.8
                    elif any(word in insight_text.lower() for word in ["may", "might", "possibly", "sometimes"]):
                        confidence = 0.4

                    insights.append({
                        "text": insight_text,
                        "confidence": confidence,
                        "extracted_at": int(time.time())
                    })

        return insights

    def _find_ticker_correlations(self, events: List[sqlite3.Row]) -> List[Dict[str, Any]]:
        """Find tickers that frequently appear together in events"""
        patterns = []

        # Build co-occurrence matrix
        ticker_pairs: Dict[Tuple[str, str], int] = {}

        for event in events:
            tickers = json.loads(event["tickers"] or "[]")
            if len(tickers) >= 2:
                for i, t1 in enumerate(tickers):
                    for t2 in tickers[i+1:]:
                        pair = tuple(sorted([t1, t2]))
                        ticker_pairs[pair] = ticker_pairs.get(pair, 0) + 1

        # Find significant correlations
        for (t1, t2), count in ticker_pairs.items():
            if count >= 3:  # Appeared together 3+ times
                patterns.append({
                    "type": "ticker_correlation",
                    "ticker1": t1,
                    "ticker2": t2,
                    "occurrences": count,
                    "confidence": min(0.9, count / len(events))
                })

        return patterns

    def _find_temporal_patterns(self, events: List[sqlite3.Row]) -> List[Dict[str, Any]]:
        """Find time-of-day or day-of-week patterns"""
        patterns = []

        # Group by hour of day
        hour_counts: Dict[int, int] = {}
        for event in events:
            hour = time.localtime(event["ts"]).tm_hour
            hour_counts[hour] = hour_counts.get(hour, 0) + 1

        # Find peak hours
        if hour_counts:
            max_count = max(hour_counts.values())
            avg_count = sum(hour_counts.values()) / len(hour_counts)

            for hour, count in hour_counts.items():
                if count > avg_count * 1.5:  # 50% above average
                    patterns.append({
                        "type": "temporal_pattern",
                        "hour": hour,
                        "occurrences": count,
                        "description": f"Events often occur around {hour}:00",
                        "confidence": min(0.8, count / max_count)
                    })

        return patterns

    def _find_event_cascades(self, events: List[sqlite3.Row]) -> List[Dict[str, Any]]:
        """Find events that tend to follow other events"""
        patterns = []

        # Look for event A followed by event B within 1 hour
        for i in range(len(events) - 1):
            e1 = events[i]
            e2 = events[i + 1]

            time_diff = e2["ts"] - e1["ts"]

            if time_diff <= 3600:  # Within 1 hour
                cascade_key = (e1["event_type"], e2["event_type"])

                # Check if this cascade happens frequently
                cascade_count = sum(
                    1 for j in range(len(events) - 1)
                    if events[j]["event_type"] == e1["event_type"]
                    and events[j + 1]["event_type"] == e2["event_type"]
                    and (events[j + 1]["ts"] - events[j]["ts"]) <= 3600
                )

                if cascade_count >= 2:  # Happened 2+ times
                    patterns.append({
                        "type": "event_cascade",
                        "trigger_event": e1["event_type"],
                        "following_event": e2["event_type"],
                        "occurrences": cascade_count,
                        "typical_delay_minutes": int(time_diff / 60),
                        "confidence": min(0.7, cascade_count / 5)
                    })

        return patterns

    def _format_pattern_description(self, pattern: Dict[str, Any]) -> str:
        """Format pattern as human-readable text"""
        ptype = pattern.get("type")

        if ptype == "ticker_correlation":
            return f"Pattern: {pattern['ticker1']} and {pattern['ticker2']} often move together (observed {pattern['occurrences']} times)"

        elif ptype == "temporal_pattern":
            return f"Pattern: Events frequently occur around {pattern['hour']}:00 ({pattern['occurrences']} times)"

        elif ptype == "event_cascade":
            delay = pattern.get("typical_delay_minutes", 0)
            return f"Pattern: {pattern['trigger_event']} often followed by {pattern['following_event']} within {delay} minutes"

        return f"Pattern: {ptype}"

    def _evaluate_single_prediction(self, analysis: sqlite3.Row) -> Optional[Dict[str, Any]]:
        """Evaluate if a prediction/recommendation was correct"""
        # This is a simplified evaluation - real implementation would check
        # actual portfolio changes, ticker movements, etc.

        recommendations = json.loads(analysis["recommendations_json"] or "[]")
        if not recommendations:
            return None

        # Get tickers mentioned
        tickers = json.loads(analysis["tickers"] or "[]") if "tickers" in analysis.keys() else []

        # Check what happened after the analysis
        # (In real implementation, fetch actual price/position data)

        return {
            "analysis_id": analysis["id"],
            "timestamp": analysis["ts"],
            "tickers": tickers,
            "recommendations": recommendations,
            "correct": None,  # Would be determined by checking actual outcomes
            "notes": "Evaluation pending actual outcome data"
        }

    def _generate_learning_from_evaluations(self, evaluations: List[Dict[str, Any]]) -> str:
        """Generate learning text from prediction evaluations"""
        # Simplified - would use AI to generate insights
        correct = sum(1 for e in evaluations if e.get("correct") is True)
        total = len([e for e in evaluations if e.get("correct") is not None])

        if total == 0:
            return ""

        accuracy = (correct / total) * 100

        return f"Self-evaluation: {accuracy:.0f}% accuracy on {total} predictions. Continue refining analysis approach."
