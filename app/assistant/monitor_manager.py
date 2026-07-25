"""
Monitor manager - coordinates all autonomous monitors
"""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from .events.event import Event
from .events.event_processor import EventProcessor
from .monitors.base_monitor import BaseMonitor
from .monitors.cryptid_health_monitor import CryptidHealthMonitor
from .monitors.portfolio_monitor import PortfolioMonitor
from .monitors.signal_monitor import SignalMonitor
from .strategic_agent import StrategicAgent
from .learning_worker import LearningWorker


class MonitorManager:
    """
    Manages all autonomous monitors and event processing
    """

    def __init__(
        self,
        db_path: Path,
        runs_dir: Path,
        data_dir: Path,
        config_path: Optional[Path] = None
    ):
        self.db_path = db_path
        self.runs_dir = runs_dir
        self.data_dir = data_dir

        # Load configuration
        if config_path and config_path.exists():
            self.config = json.loads(config_path.read_text())
        else:
            self.config = self._default_config()

        # Initialize strategic agent
        memory_dir = data_dir / "assistant_memory"
        self.agent = StrategicAgent(
            db_path=db_path,
            runs_dir=runs_dir,
            memory_dir=memory_dir
        )
        # Ensure assistant tables exist before monitors start emitting events.
        try:
            _ = self.agent.memory.vector_store
        except Exception as e:
            print(f"[MonitorManager] Warning: failed to initialize assistant storage schema: {e}")

        # Initialize event processor
        global_config = self.config.get("global", {})
        cooldown_config = self._build_cooldown_config()

        self.event_processor = EventProcessor(
            db_path=db_path,
            cooldown_config=cooldown_config,
            max_analyses_per_hour=global_config.get("ai_analysis_max_per_hour", 10)
        )

        # Register AI trigger callback
        self.event_processor.set_ai_trigger_callback(self._handle_ai_trigger)

        # Initialize monitors
        self.monitors: List[BaseMonitor] = []
        self._init_monitors()

        # Initialize learning worker
        self.learning_worker: Optional[LearningWorker] = None
        learning_enabled = global_config.get("learning_enabled", True)
        if learning_enabled:
            reflection_hours = global_config.get("learning_reflection_hours", 24)
            pattern_hours = global_config.get("learning_pattern_hours", 6)

            self.learning_worker = LearningWorker(
                db_path=db_path,
                memory_manager=self.agent.memory,
                ollama_client=self.agent.ollama,
                reflection_interval_hours=reflection_hours,
                pattern_check_interval_hours=pattern_hours
            )
            print(f"[MonitorManager] Initialized LearningWorker")

    def _default_config(self) -> Dict[str, Any]:
        """Default configuration if file not found"""
        return {
            "global": {
                "ai_analysis_max_per_hour": 10,
                "ai_analysis_max_per_day": 50,
                "critical_events_bypass_limits": True
            },
            "portfolio_monitor": {
                "enabled": True,
                "interval_sec": 300,
                "triggers": {"portfolio_swing_pct": 5.0}
            },
            "signal_monitor": {
                "enabled": True,
                "interval_sec": 120,
                "triggers": {"consensus_threshold": 4}
            },
            "cryptid_health_monitor": {
                "enabled": True,
                "interval_sec": 120,
                "triggers": {"heartbeat_timeout_sec": 300}
            }
        }

    def _build_cooldown_config(self) -> Dict[str, int]:
        """Extract cooldown configuration for event processor"""
        cooldowns = {}

        for monitor_name, monitor_config in self.config.items():
            if monitor_name == "global":
                continue
            if isinstance(monitor_config, dict):
                cooldown = monitor_config.get("cooldown_sec", 300)
                # Map monitor name to event types it generates
                # This is a simplified mapping
                cooldowns[monitor_name.replace("_monitor", "")] = cooldown

        return cooldowns

    def _init_monitors(self) -> None:
        """Initialize all enabled monitors"""
        # Portfolio monitor
        portfolio_config = self.config.get("portfolio_monitor", {})
        if portfolio_config.get("enabled", False):
            monitor = PortfolioMonitor(self.db_path, portfolio_config)
            monitor.set_event_callback(self._handle_event)
            self.monitors.append(monitor)
            print(f"[MonitorManager] Initialized PortfolioMonitor")

        # Signal monitor
        signal_config = self.config.get("signal_monitor", {})
        if signal_config.get("enabled", False):
            monitor = SignalMonitor(self.db_path, signal_config)
            monitor.set_event_callback(self._handle_event)
            self.monitors.append(monitor)
            print(f"[MonitorManager] Initialized SignalMonitor")

        # Cryptid health monitor
        health_config = self.config.get("cryptid_health_monitor", {})
        if health_config.get("enabled", False):
            monitor = CryptidHealthMonitor(self.db_path, health_config)
            monitor.set_event_callback(self._handle_event)
            self.monitors.append(monitor)
            print(f"[MonitorManager] Initialized CryptidHealthMonitor")

        # TODO: Add indicator and market regime monitors when implemented

    def _handle_event(self, event: Event) -> None:
        """Callback for monitors to submit events"""
        accepted = self.event_processor.submit_event(event)
        if accepted:
            print(f"[MonitorManager] Event: {event.event_type} - {event.description}")
        else:
            # Event was deduplicated
            pass

    def _handle_ai_trigger(self, event: Event) -> None:
        """Callback for AI analysis triggers"""
        print(f"[MonitorManager] AI Analysis triggered by: {event.event_type}")

        try:
            event_db_id: Optional[int] = None
            if isinstance(event.context, dict):
                raw_event_db_id = event.context.get("_event_db_id")
                if isinstance(raw_event_db_id, (int, float)):
                    event_db_id = int(raw_event_db_id)
                elif isinstance(raw_event_db_id, str) and raw_event_db_id.strip().isdigit():
                    event_db_id = int(raw_event_db_id.strip())

            # Generate analysis
            analysis = self.agent.analyze_event(event.to_dict())

            # Store analysis
            analysis_text = analysis.get("analysis", "")
            if analysis_text:
                conn = sqlite3.connect(self.db_path)
                cur = conn.cursor()

                cur.execute("""
                    INSERT INTO assistant_analyses
                    (ts, event_id, model_used, prompt_type, analysis_text, reasoning_text, recommendations_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    analysis.get("timestamp", 0),
                    event_db_id,
                    analysis.get("model_used"),
                    "event_analysis",
                    analysis_text,
                    "",
                    "[]"
                ))

                analysis_id = cur.lastrowid
                conn.commit()
                conn.close()

                # Link analysis to event
                if event_db_id is not None and event_db_id > 0:
                    self.event_processor.storage.update_event_analysis(event_db_id, analysis_id)

                print(f"[MonitorManager] AI Analysis completed (model: {analysis.get('model_used')})")

        except Exception as e:
            print(f"[MonitorManager] AI Analysis failed: {e}")

    def start(self) -> None:
        """Start all monitors and learning worker"""
        for monitor in self.monitors:
            monitor.start()

        if self.learning_worker:
            self.learning_worker.start()
            print(f"[MonitorManager] Started {len(self.monitors)} monitors + learning worker")
        else:
            print(f"[MonitorManager] Started {len(self.monitors)} monitors")

    def stop(self) -> None:
        """Stop all monitors and learning worker"""
        for monitor in self.monitors:
            monitor.stop()

        if self.learning_worker:
            self.learning_worker.stop()

        # Wait for threads to finish
        for monitor in self.monitors:
            monitor.join(timeout=5.0)

        if self.learning_worker:
            self.learning_worker.join(timeout=5.0)

        print(f"[MonitorManager] Stopped all monitors and learning worker")

    def get_health_stats(self) -> Dict[str, Any]:
        """Get health statistics for all monitors"""
        monitors = [m.get_health_stats() for m in self.monitors]
        analyses_triggered = 0
        total_events = 0
        try:
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()
                cur.execute("SELECT COUNT(*) FROM assistant_events")
                row = cur.fetchone()
                total_events = int(row[0]) if row and row[0] is not None else 0
                cur.execute("SELECT COUNT(*) FROM assistant_analyses")
                row = cur.fetchone()
                analyses_triggered = int(row[0]) if row and row[0] is not None else 0
        except Exception:
            total_events = 0
            analyses_triggered = 0

        budget_remaining = self.event_processor.get_analysis_budget_remaining()
        stats = {
            "status": "running",
            "monitors": monitors,
            "monitors_by_name": {str(m.get("name") or ""): m for m in monitors},
            "event_processor": {
                "queue_size": self.event_processor.queue.size(),
                "analysis_budget_remaining": budget_remaining,
                "budget_remaining": budget_remaining,
                "total_events": total_events,
                "analyses_triggered": analyses_triggered,
            },
            "agent": {
                "memory_stats": self.agent.get_memory_stats()
            }
        }

        if self.learning_worker:
            stats["learning"] = self.learning_worker.get_stats()

        return stats

    def get_agent(self) -> StrategicAgent:
        """Get the strategic agent for direct use"""
        return self.agent
