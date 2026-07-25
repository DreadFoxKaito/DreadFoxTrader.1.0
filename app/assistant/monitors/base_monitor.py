"""
Base class for all monitors
"""

from __future__ import annotations

import threading
import time
import traceback
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from ..events.event import Event


class BaseMonitor(threading.Thread):
    """
    Base class for autonomous monitors
    All monitors run as background threads
    """

    def __init__(
        self,
        name: str,
        interval_sec: int,
        db_path: Path,
        config: Dict[str, Any]
    ):
        super().__init__(name=f"monitor-{name}", daemon=True)

        self.monitor_name = name
        self.interval = interval_sec
        self.db_path = db_path
        self.config = config

        self.stop_event = threading.Event()
        self.enabled = config.get("enabled", True)

        # Health tracking
        self.last_check_time = 0
        self.last_success_time = 0
        self.error_count = 0
        self.total_checks = 0
        self.events_generated = 0

        # Event callback
        self.event_callback: Optional[Callable[[Event], None]] = None

    def set_event_callback(self, callback: Callable[[Event], None]) -> None:
        """Register callback for submitting events"""
        self.event_callback = callback

    def run(self) -> None:
        """Main thread loop"""
        print(f"[{self.monitor_name}] Started (interval={self.interval}s)")

        while not self.stop_event.is_set():
            if not self.enabled:
                self.stop_event.wait(60)
                continue

            self.last_check_time = int(time.time())
            self.total_checks += 1

            try:
                events = self.check()
                self.error_count = 0
                self.last_success_time = int(time.time())

                # Submit events via callback
                if events and self.event_callback:
                    for event in events:
                        event.source = self.monitor_name
                        self.event_callback(event)
                        self.events_generated += 1

            except Exception as e:
                self.error_count += 1
                print(f"[{self.monitor_name}] Error: {e}")
                if self.error_count <= 2:  # Only print traceback for first few errors
                    traceback.print_exc()

                # Back off if too many errors
                if self.error_count > 5:
                    backoff = min(300, self.interval * 2)
                    print(f"[{self.monitor_name}] Too many errors, backing off {backoff}s")
                    self.stop_event.wait(backoff)
                    continue

            # Wait for next interval
            self.stop_event.wait(self.interval)

        print(f"[{self.monitor_name}] Stopped")

    @abstractmethod
    def check(self) -> List[Event]:
        """
        Perform monitoring check

        Returns:
            List of events detected (empty if none)
        """
        pass

    def stop(self) -> None:
        """Stop the monitor"""
        self.stop_event.set()

    def get_health_stats(self) -> Dict[str, Any]:
        """Get monitor health statistics"""
        uptime = int(time.time()) - self.last_check_time if self.last_check_time else 0
        if not self.enabled:
            status = "disabled"
        elif self.is_alive():
            status = "running"
        else:
            status = "stopped"
        return {
            "name": self.monitor_name,
            "status": status,
            "enabled": self.enabled,
            "interval_sec": self.interval,
            "last_check": self.last_check_time,
            "last_success": self.last_success_time,
            "uptime_sec": uptime,
            "total_checks": self.total_checks,
            "error_count": self.error_count,
            "events_generated": self.events_generated,
            "health": "healthy" if self.error_count < 3 else "degraded" if self.error_count < 10 else "unhealthy"
        }
