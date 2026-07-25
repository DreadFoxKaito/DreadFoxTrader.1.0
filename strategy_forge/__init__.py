"""DreadFox Strategy Forge optimization engine.

Strategy Forge is intentionally separate from the live trading scripts. It is a
research and paper-trading validation workflow; exports require explicit user
action before they can be used by any trading runner.
"""

from __future__ import annotations

from pathlib import Path

__all__ = ["DEFAULT_DB_PATH", "__version__"]

__version__ = "0.1.0"

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_DIR = PROJECT_ROOT / "app" / "data" / "strategy_forge"
DEFAULT_DB_PATH = DEFAULT_DATA_DIR / "strategy_forge.sqlite3"
