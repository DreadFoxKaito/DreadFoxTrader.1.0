"""
Event processing pipeline with deduplication and rate limiting
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from .event import Event, EventPriority, EventSeverity


class EventDeduplicator:
    """
    Prevents duplicate event processing
    """

    def __init__(self, cooldown_config: Dict[str, int]):
        """
        Args:
            cooldown_config: Dict mapping event_type to cooldown seconds
        """
        self.cooldowns = cooldown_config
        self.last_seen: Dict[str, int] = {}
        self.lock = threading.Lock()

    def should_process(self, event: Event) -> bool:
        """Check if event should be processed based on cooldown"""
        with self.lock:
            # Critical events always process
            if event.severity == EventSeverity.CRITICAL:
                return True

            # Build dedup key
            key = f"{event.event_type}"
            if event.tickers:
                key += f":{','.join(sorted(event.tickers))}"

            last_ts = self.last_seen.get(key, 0)
            cooldown = self.cooldowns.get(event.event_type, 300)  # Default 5 min

            now = int(time.time())

            if (now - last_ts) >= cooldown:
                self.last_seen[key] = now
                return True

            return False

    def mark_processed(self, event: Event) -> None:
        """Mark event as processed"""
        with self.lock:
            key = f"{event.event_type}"
            if event.tickers:
                key += f":{','.join(sorted(event.tickers))}"
            self.last_seen[key] = int(time.time())


class EventStorage:
    """
    Stores events in database
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path

    def store_event(self, event: Event) -> int:
        """Store event and return database ID"""
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO assistant_events
            (ts, event_uid, event_type, severity, description, tickers, data_json, context_json, ai_analysis_id, acknowledged)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event.timestamp,
            event.event_id,
            event.event_type,
            event.severity.value,
            event.description,
            json.dumps(event.tickers),
            json.dumps(event.data),
            json.dumps(event.context),
            event.ai_analysis_id,
            int(event.acknowledged)
        ))

        event_db_id = cur.lastrowid
        conn.commit()
        conn.close()

        return event_db_id

    def update_event_analysis(self, event_db_id: int, analysis_id: int) -> None:
        """Link event to AI analysis"""
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        cur.execute(
            "UPDATE assistant_events SET ai_analysis_id = ? WHERE id = ?",
            (analysis_id, event_db_id)
        )

        conn.commit()
        conn.close()

    def get_recent_events(self, hours: int = 24, limit: int = 50) -> List[Dict[str, Any]]:
        """Retrieve recent events"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cutoff = int(time.time()) - (hours * 3600)

        cur.execute("""
            SELECT * FROM assistant_events
            WHERE ts >= ?
            ORDER BY ts DESC
            LIMIT ?
        """, (cutoff, limit))

        rows = cur.fetchall()
        conn.close()

        out: List[Dict[str, Any]] = []
        for row in rows:
            raw = dict(row)
            tickers_val: Any = []
            data_val: Any = {}
            context_val: Any = {}

            try:
                tickers_val = json.loads(raw.get("tickers") or "[]")
                if not isinstance(tickers_val, list):
                    tickers_val = []
            except Exception:
                tickers_val = []

            try:
                data_val = json.loads(raw.get("data_json") or "{}")
                if not isinstance(data_val, dict):
                    data_val = {}
            except Exception:
                data_val = {}

            try:
                context_val = json.loads(raw.get("context_json") or "{}")
                if not isinstance(context_val, dict):
                    context_val = {}
            except Exception:
                context_val = {}

            desc = str(raw.get("description") or "").strip()
            if not desc:
                desc = str(data_val.get("description") or "").strip()
            if not desc:
                desc = str(raw.get("event_type") or "").replace("_", " ").strip().title() or "Event"

            out.append(
                {
                    "id": int(raw.get("id") or 0),
                    "event_id": str(raw.get("event_uid") or ""),
                    "timestamp": int(raw.get("ts") or 0),
                    "event_type": str(raw.get("event_type") or ""),
                    "severity": str(raw.get("severity") or "info"),
                    "description": desc,
                    "tickers": tickers_val,
                    "data": data_val,
                    "context": context_val,
                    "ai_analysis_id": raw.get("ai_analysis_id"),
                    "acknowledged": bool(raw.get("acknowledged")),
                    # Legacy aliases used by older UI code.
                    "ts": int(raw.get("ts") or 0),
                }
            )

        return out


class EventQueue:
    """
    Priority queue for event processing
    """

    def __init__(self, max_size: int = 1000):
        self.queues: Dict[EventPriority, deque] = {
            EventPriority.CRITICAL: deque(maxlen=max_size),
            EventPriority.HIGH: deque(maxlen=max_size),
            EventPriority.MEDIUM: deque(maxlen=max_size),
            EventPriority.LOW: deque(maxlen=max_size)
        }
        self.lock = threading.Lock()

    def push(self, event: Event) -> None:
        """Add event to queue"""
        with self.lock:
            self.queues[event.priority].append(event)

    def pop(self, priority: Optional[EventPriority] = None) -> Optional[Event]:
        """Get next event, respecting priority"""
        with self.lock:
            if priority:
                # Get from specific priority
                if self.queues[priority]:
                    return self.queues[priority].popleft()
                return None

            # Get highest priority available
            for p in [EventPriority.CRITICAL, EventPriority.HIGH, EventPriority.MEDIUM, EventPriority.LOW]:
                if self.queues[p]:
                    return self.queues[p].popleft()

            return None

    def size(self, priority: Optional[EventPriority] = None) -> int:
        """Get queue size"""
        with self.lock:
            if priority:
                return len(self.queues[priority])
            return sum(len(q) for q in self.queues.values())

    def peek(self, priority: Optional[EventPriority] = None) -> Optional[Event]:
        """Look at next event without removing"""
        with self.lock:
            if priority:
                return self.queues[priority][0] if self.queues[priority] else None

            for p in [EventPriority.CRITICAL, EventPriority.HIGH, EventPriority.MEDIUM, EventPriority.LOW]:
                if self.queues[p]:
                    return self.queues[p][0]

            return None


class EventProcessor:
    """
    Main event processing pipeline
    """

    def __init__(
        self,
        db_path: Path,
        cooldown_config: Dict[str, int],
        max_analyses_per_hour: int = 10
    ):
        self.db_path = db_path
        self.deduplicator = EventDeduplicator(cooldown_config)
        self.storage = EventStorage(db_path)
        self.queue = EventQueue()

        # Rate limiting
        self.max_analyses_per_hour = max_analyses_per_hour
        self.analysis_timestamps: deque = deque(maxlen=max_analyses_per_hour)
        self.analysis_lock = threading.Lock()

        # Callbacks
        self.ai_trigger_callback: Optional[Callable[[Event], None]] = None

    def submit_event(self, event: Event) -> bool:
        """
        Submit event for processing

        Returns:
            True if event was accepted, False if deduplicated
        """
        # Check deduplication
        if not self.deduplicator.should_process(event):
            return False

        # Store in database
        event_db_id = self.storage.store_event(event)
        # Keep DB identifier on the in-memory event so callbacks can reliably
        # link analyses to the stored event row.
        if not isinstance(event.context, dict):
            event.context = {}
        event.context["_event_db_id"] = int(event_db_id)

        # Add to processing queue
        self.queue.push(event)

        # If should trigger AI and callback registered, trigger immediately
        if event.should_trigger_ai() and self.ai_trigger_callback:
            if self._can_trigger_analysis():
                try:
                    self.ai_trigger_callback(event)
                    event.ai_analysis_triggered = True
                    self._record_analysis()
                except Exception as e:
                    print(f"[EventProcessor] AI trigger failed: {e}")

        event.processed = True
        return True

    def set_ai_trigger_callback(self, callback: Callable[[Event], None]) -> None:
        """Register callback for AI analysis triggers"""
        self.ai_trigger_callback = callback

    def _can_trigger_analysis(self) -> bool:
        """Check if we're within rate limits"""
        with self.analysis_lock:
            now = time.time()
            # Remove timestamps older than 1 hour
            while self.analysis_timestamps and (now - self.analysis_timestamps[0]) > 3600:
                self.analysis_timestamps.popleft()

            return len(self.analysis_timestamps) < self.max_analyses_per_hour

    def _record_analysis(self) -> None:
        """Record that an analysis was triggered"""
        with self.analysis_lock:
            self.analysis_timestamps.append(time.time())

    def get_analysis_budget_remaining(self) -> int:
        """Get remaining analysis budget for this hour"""
        with self.analysis_lock:
            now = time.time()
            while self.analysis_timestamps and (now - self.analysis_timestamps[0]) > 3600:
                self.analysis_timestamps.popleft()

            return self.max_analyses_per_hour - len(self.analysis_timestamps)

    def process_queue(self, max_items: int = 10) -> int:
        """
        Process items from queue

        Returns:
            Number of items processed
        """
        processed = 0

        for _ in range(max_items):
            event = self.queue.pop()
            if not event:
                break

            # Already processed in submit_event, but could add additional processing here
            processed += 1

        return processed
