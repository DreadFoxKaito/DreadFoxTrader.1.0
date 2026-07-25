"""
Background worker for autonomous learning and memory updates
"""

from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any, Optional

from .learning import MemoryLearning


class LearningWorker(threading.Thread):
    """
    Background thread that periodically reflects on experiences and updates memory
    """

    def __init__(
        self,
        db_path: Path,
        memory_manager: Any,
        ollama_client: Any,
        reflection_interval_hours: int = 24,
        pattern_check_interval_hours: int = 6
    ):
        super().__init__(name="learning-worker", daemon=True)

        self.db_path = db_path
        self.memory = memory_manager
        self.ollama = ollama_client

        self.reflection_interval = reflection_interval_hours * 3600
        self.pattern_interval = pattern_check_interval_hours * 3600

        self.stop_event = threading.Event()
        self.learner = MemoryLearning(db_path, memory_manager, ollama_client)

        # Track last execution times
        self.last_reflection = 0
        self.last_pattern_check = 0

        # Statistics
        self.reflections_performed = 0
        self.patterns_discovered = 0
        self.insights_generated = 0

    def run(self) -> None:
        """Main learning loop"""
        print(f"[LearningWorker] Started (reflection every {self.reflection_interval // 3600}h)")

        # Wait a bit before first learning cycle (let system collect some data)
        initial_delay = 3600  # 1 hour
        self.stop_event.wait(initial_delay)

        while not self.stop_event.is_set():
            now = int(time.time())

            try:
                # Periodic reflection on recent events
                if (now - self.last_reflection) >= self.reflection_interval:
                    self._perform_reflection()
                    self.last_reflection = now

                # Pattern discovery
                if (now - self.last_pattern_check) >= self.pattern_interval:
                    self._discover_patterns()
                    self.last_pattern_check = now

                # Prediction evaluation (less frequent)
                if (now - self.last_reflection) >= (self.reflection_interval * 7):  # Weekly
                    self._evaluate_predictions()

            except Exception as e:
                print(f"[LearningWorker] Error: {e}")

            # Check every hour
            self.stop_event.wait(3600)

        print("[LearningWorker] Stopped")

    def _perform_reflection(self) -> None:
        """Reflect on recent events and extract insights"""
        print("[LearningWorker] Performing reflection on recent events...")

        try:
            result = self.learner.reflect_on_events(
                lookback_days=7,
                min_events=5
            )

            status = result.get("status")

            if status == "success":
                insights = result.get("insights_extracted", 0)
                self.reflections_performed += 1
                self.insights_generated += insights
                print(f"[LearningWorker] Reflection complete: {insights} insights extracted")

            elif status == "insufficient_data":
                print(f"[LearningWorker] Insufficient data for reflection ({result.get('events_found', 0)} events)")

            else:
                print(f"[LearningWorker] Reflection failed: {result.get('error', 'unknown')}")

        except Exception as e:
            print(f"[LearningWorker] Reflection error: {e}")

    def _discover_patterns(self) -> None:
        """Find patterns in historical data"""
        print("[LearningWorker] Discovering patterns...")

        try:
            patterns = self.learner.extract_patterns(
                event_type=None,  # All event types
                lookback_days=30
            )

            if patterns:
                significant = [p for p in patterns if p.get("confidence", 0) > 0.7]
                self.patterns_discovered += len(significant)
                print(f"[LearningWorker] Found {len(significant)} significant patterns")

                # Log top patterns
                for pattern in significant[:3]:
                    ptype = pattern.get("type", "unknown")
                    print(f"[LearningWorker]   - {ptype}: confidence {pattern.get('confidence', 0):.2f}")

            else:
                print("[LearningWorker] No significant patterns found")

        except Exception as e:
            print(f"[LearningWorker] Pattern discovery error: {e}")

    def _evaluate_predictions(self) -> None:
        """Evaluate past predictions for accuracy"""
        print("[LearningWorker] Evaluating past predictions...")

        try:
            result = self.learner.evaluate_predictions()

            if result.get("status") == "success":
                evals = result.get("evaluations_performed", 0)
                correct = result.get("correct_predictions", 0)

                if evals > 0:
                    accuracy = (correct / evals) * 100
                    print(f"[LearningWorker] Evaluated {evals} predictions: {accuracy:.0f}% accuracy")
                else:
                    print("[LearningWorker] No predictions to evaluate yet")

        except Exception as e:
            print(f"[LearningWorker] Evaluation error: {e}")

    def stop(self) -> None:
        """Stop the worker"""
        self.stop_event.set()

    def get_stats(self) -> dict[str, Any]:
        """Get learning statistics"""
        if self.stop_event.is_set():
            status = "stopped"
        elif self.is_alive():
            status = "running"
        else:
            status = "idle"
        return {
            "status": status,
            "reflections_performed": self.reflections_performed,
            "patterns_discovered": self.patterns_discovered,
            "insights_generated": self.insights_generated,
            "last_reflection": self.last_reflection,
            "last_pattern_check": self.last_pattern_check,
            "next_reflection_in_hours": max(0, (self.last_reflection + self.reflection_interval - int(time.time())) // 3600)
        }
