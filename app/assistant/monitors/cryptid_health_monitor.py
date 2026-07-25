"""
Cryptid health monitor
Detects operational issues with running cryptids
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List

from ..events.event import Event, EventPriority, EventSeverity
from .base_monitor import BaseMonitor


class CryptidHealthMonitor(BaseMonitor):
    """
    Monitors running cryptids for operational issues
    - Crashed processes
    - Heartbeat timeouts
    - Error spikes
    """

    def __init__(self, db_path: Path, config: Dict[str, Any]):
        interval = config.get("interval_sec", 120)  # 2 min default
        super().__init__("cryptid_health", interval, db_path, config)

        # Thresholds
        self.heartbeat_timeout_sec = config.get("triggers", {}).get("heartbeat_timeout_sec", 300)
        self.error_threshold = config.get("triggers", {}).get("error_threshold", 10)
        self.error_window_sec = config.get("triggers", {}).get("error_window_sec", 600)

        # State tracking (run_id -> error_count)
        self.error_history: Dict[int, List[int]] = {}

    def check(self) -> List[Event]:
        """Check cryptid health"""
        events = []

        try:
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

            now = int(time.time())

            for row in rows:
                run_id = int(row["id"])
                name = row["algorithm_name"]
                status = row["status"]
                run_dir = Path(row["run_dir"])

                # Check if crashed
                if status == "crashed":
                    events.append(Event(
                        event_type="cryptid_crashed",
                        severity=EventSeverity.CRITICAL,
                        priority=EventPriority.CRITICAL,
                        description=f"Cryptid crashed: {name} (run {run_id})",
                        data={
                            "run_id": run_id,
                            "name": name,
                            "base_script": row["base_script_name"]
                        }
                    ))
                    continue

                # Check heartbeat
                status_file = run_dir / "status.json"
                if status_file.exists():
                    try:
                        status_data = json.loads(status_file.read_text())
                        heartbeat = status_data.get("ts") or status_data.get("heartbeat") or 0

                        if heartbeat and (now - int(heartbeat)) > self.heartbeat_timeout_sec:
                            events.append(Event(
                                event_type="cryptid_heartbeat_timeout",
                                severity=EventSeverity.CRITICAL,
                                priority=EventPriority.CRITICAL,
                                description=f"Cryptid heartbeat timeout: {name} (last seen {(now - int(heartbeat)) // 60} min ago)",
                                data={
                                    "run_id": run_id,
                                    "name": name,
                                    "last_heartbeat": heartbeat,
                                    "timeout_sec": now - int(heartbeat)
                                }
                            ))

                    except Exception:
                        pass

                # Check error rate
                log_file = run_dir / "algo.log"
                if log_file.exists():
                    error_count = self._count_recent_errors(log_file, self.error_window_sec)

                    # Track error history
                    if run_id not in self.error_history:
                        self.error_history[run_id] = []

                    self.error_history[run_id].append(error_count)

                    # Keep only last 10 checks
                    if len(self.error_history[run_id]) > 10:
                        self.error_history[run_id] = self.error_history[run_id][-10:]

                    if error_count >= self.error_threshold:
                        events.append(Event(
                            event_type="cryptid_error_spike",
                            severity=EventSeverity.WARNING,
                            priority=EventPriority.HIGH,
                            description=f"Cryptid error spike: {name} ({error_count} errors in {self.error_window_sec // 60} min)",
                            data={
                                "run_id": run_id,
                                "name": name,
                                "error_count": error_count,
                                "window_sec": self.error_window_sec
                            }
                        ))

        except Exception as e:
            print(f"[CryptidHealthMonitor] Error: {e}")

        return events

    def _count_recent_errors(self, log_file: Path, window_sec: int) -> int:
        """Count ERROR/CRITICAL lines in recent window"""
        try:
            now = time.time()
            cutoff = now - window_sec

            error_count = 0

            with open(log_file, 'r', errors='ignore') as f:
                # Read last 1000 lines (more efficient than reading entire file)
                lines = f.readlines()[-1000:]

                for line in lines:
                    # Check if line has error/critical
                    if "ERROR" in line or "CRITICAL" in line or "Exception" in line:
                        error_count += 1

            return error_count

        except Exception:
            return 0
