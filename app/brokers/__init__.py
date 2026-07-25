"""
Broker connector package for Cryptid Exchange.

Purpose:
- Provide a unified abstraction layer for multiple brokerages (Schwab, Robinhood, etc.)
- Normalize portfolio/account/positions data so the UI can render broker-agnostically
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


# -------------------------
# Exceptions
# -------------------------
class BrokerError(Exception):
    """Base exception for broker connector failures."""


class BrokerAuthError(BrokerError):
    """Authentication/authorization failure (token expired, MFA required, etc.)."""

    def __init__(self, message: str, status_code: int = 401):
        super().__init__(message)
        self.status_code = status_code


class BrokerConnectorError(BrokerError):
    """Connector error (API down, parsing issue, upstream change, etc.)."""

    def __init__(self, message: str, status_code: int = 500):
        super().__init__(message)
        self.status_code = status_code


# -------------------------
# Normalized DTOs
# -------------------------
@dataclass
class NormalizedPosition:
    symbol: str
    quantity: float | int | None = None
    average_price: float | None = None
    market_price: float | None = None
    market_value: float | None = None
    unrealized_pl: float | None = None
    asset_type: str = ""  # equity/option/crypto/etc.
    raw: dict[str, Any] | None = None


@dataclass
class NormalizedAccount:
    account_id: str  # broker-specific ID or masked account number
    account_type: str = ""
    balances: dict[str, Any] | None = None
    positions: list[NormalizedPosition] | None = None
    raw: dict[str, Any] | None = None


@dataclass
class NormalizedPortfolioSnapshot:
    broker: str
    label: str
    connection_id: int
    accounts: list[NormalizedAccount]
    raw: dict[str, Any] | None = None


# -------------------------
# Connector interface contract
# -------------------------
class BrokerConnector:
    """
    Broker connector interface.

    Implementations should:
    - store/retrieve any secrets via the broker_connections table, using encrypted-at-rest
      blobs once Step 10 is complete.
    - return normalized snapshot data for UI.
    """

    broker_id: str  # e.g. "schwab", "robinhood"
    broker_name: str  # e.g. "Schwab", "Robinhood"

    def link(self, *, db_path: str, label: str, **kwargs: Any) -> int:
        """
        Create (or update) a broker connection and persist secrets/metadata.
        Returns connection_id.
        """
        raise NotImplementedError

    def unlink(self, *, db_path: str, connection_id: int) -> None:
        """Remove the broker connection and any stored secret references."""
        raise NotImplementedError

    def portfolio_snapshot(
        self, *, db_path: str, connection_id: int
    ) -> NormalizedPortfolioSnapshot:
        """
        Fetch and normalize portfolio data for a given connection.
        Should raise BrokerAuthError for auth issues, BrokerConnectorError otherwise.
        """
        raise NotImplementedError


# -------------------------
# Small shared helpers
# -------------------------
def safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(float(x))
    except Exception:
        return None