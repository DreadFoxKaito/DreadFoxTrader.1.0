"""
Event data structures
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class EventSeverity(Enum):
    """Event severity levels"""
    CRITICAL = "critical"   # Requires immediate attention
    WARNING = "warning"     # Should be reviewed
    INFO = "info"           # Informational only


class EventPriority(Enum):
    """Processing priority for events"""
    CRITICAL = 0    # Process immediately
    HIGH = 1        # Process within 5 min
    MEDIUM = 2      # Process within 15 min
    LOW = 3         # Process when idle


@dataclass
class Event:
    """
    Represents a detected event from monitors
    """
    event_type: str
    severity: EventSeverity
    description: str
    tickers: List[str] = field(default_factory=list)
    data: Dict[str, Any] = field(default_factory=dict)
    context: Dict[str, Any] = field(default_factory=dict)

    # Auto-generated fields
    event_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    timestamp: int = field(default_factory=lambda: int(time.time()))
    source: str = "unknown"
    priority: EventPriority = EventPriority.MEDIUM

    # Processing state
    processed: bool = False
    ai_analysis_triggered: bool = False
    ai_analysis_id: Optional[int] = None
    acknowledged: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for JSON serialization"""
        return {
            "event_id": self.event_id,
            "timestamp": self.timestamp,
            "event_type": self.event_type,
            "severity": self.severity.value,
            "priority": self.priority.value,
            "description": self.description,
            "tickers": self.tickers,
            "data": self.data,
            "context": self.context,
            "source": self.source,
            "processed": self.processed,
            "ai_analysis_triggered": self.ai_analysis_triggered,
            "ai_analysis_id": self.ai_analysis_id,
            "acknowledged": self.acknowledged
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Event:
        """Create Event from dict"""
        severity = EventSeverity(data.get("severity", "info"))
        priority = EventPriority(data.get("priority", 2))

        event = cls(
            event_type=data["event_type"],
            severity=severity,
            description=data["description"],
            tickers=data.get("tickers", []),
            data=data.get("data", {}),
            context=data.get("context", {})
        )

        event.event_id = data.get("event_id", event.event_id)
        event.timestamp = data.get("timestamp", event.timestamp)
        event.source = data.get("source", "unknown")
        event.priority = priority
        event.processed = data.get("processed", False)
        event.ai_analysis_triggered = data.get("ai_analysis_triggered", False)
        event.ai_analysis_id = data.get("ai_analysis_id")
        event.acknowledged = data.get("acknowledged", False)

        return event

    def should_trigger_ai(self) -> bool:
        """Determine if this event warrants AI analysis"""
        # Critical events always trigger
        if self.severity == EventSeverity.CRITICAL:
            return True

        # High priority warnings trigger
        if self.severity == EventSeverity.WARNING and self.priority in (EventPriority.CRITICAL, EventPriority.HIGH):
            return True

        # Specific event types that always trigger
        trigger_types = [
            "portfolio_swing_major",
            "signal_consensus",
            "cryptid_crashed",
            "indicator_confluence",
            "market_regime_change",
            "signal_flip_held"
        ]

        if self.event_type in trigger_types:
            return True

        return False
