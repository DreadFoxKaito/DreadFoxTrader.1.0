from __future__ import annotations

import argparse
import base64
import copy
import html
import json
import logging
import math
import os
import pickle
import random
import re
import secrets
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote_plus
from uuid import uuid4

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader, select_autoescape

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from .indicator_pipeline import apply_final_candle_policy, heikin_ashi_series as shared_heikin_ashi_series, log_indicator_policy
except Exception:  # pragma: no cover
    from app.indicator_pipeline import apply_final_candle_policy, heikin_ashi_series as shared_heikin_ashi_series, log_indicator_policy  # type: ignore

try:
    from strategy_forge.supertrend import SupertrendPoint, calculate_supertrend, segment_supertrend_runs
except Exception:  # pragma: no cover
    SupertrendPoint = Any  # type: ignore
    calculate_supertrend = None  # type: ignore
    segment_supertrend_runs = None  # type: ignore

try:
    from strategy_forge.pivot_points import calculate_pivot_points, pivot_level_sequence, pivot_target_above_price, pivot_target_below_price
except Exception:  # pragma: no cover
    calculate_pivot_points = None  # type: ignore
    pivot_level_sequence = None  # type: ignore
    pivot_target_above_price = None  # type: ignore
    pivot_target_below_price = None  # type: ignore

# =============================================================================
# Broker registry import (robust for both "python -m app.main" and "python app/main.py")
# =============================================================================
# Registry + connectors live under app/brokers/*
# Keep imports optional so the app can still start while you build file-by-file.
#
# WHY:
# - Relative import (from .brokers.registry) works only when this file is executed as a module.
# - When executed as a script, relative import fails and you silently fall back to stubs,
#   which is why the dashboard showed "Broker: Linked" but no portfolio data.
#
# This block tries:
#   1) relative import (module execution)
#   2) absolute import (script execution from repo root)
#   3) stubs (safe fallback)
try:
    # Works when executed as a module: python -m app.main
    from .brokers.registry import (
        BrokerAuthError,
        BrokerConnectorError,
        get_all_supported_brokers,
        get_portfolio_bubbles_html,
        get_portfolio_dashboard_html,
        get_portfolio_summary_html,
        get_portfolio_context_data,
        get_portfolio_performance_context,
        link_robinhood_connection,
        unlink_connection,
    )
except Exception:  # pragma: no cover
    try:
        # Works when executed as a script from project root: python app/main.py
        from app.brokers.registry import (  # type: ignore
            BrokerAuthError,
            BrokerConnectorError,
            get_all_supported_brokers,
            get_portfolio_bubbles_html,
            get_portfolio_dashboard_html,
            get_portfolio_summary_html,
            get_portfolio_context_data,
            get_portfolio_performance_context,
            link_robinhood_connection,
            unlink_connection,
        )
    except Exception:
        BrokerAuthError = Exception
        BrokerConnectorError = Exception

        def get_all_supported_brokers() -> list[dict[str, Any]]:
            return [{"id": "schwab", "name": "Schwab"}, {"id": "robinhood", "name": "Robinhood"}]

        def get_portfolio_summary_html(*_a: Any, **_kw: Any) -> str:
            return ""

        def get_portfolio_bubbles_html(*_a: Any, **_kw: Any) -> str:
            return ""

        def get_portfolio_dashboard_html(*_a: Any, **_kw: Any) -> str:
            return ""

        def get_portfolio_context_data(*_a: Any, **_kw: Any) -> list[dict[str, Any]]:
            return []

        def get_portfolio_performance_context(*_a: Any, **_kw: Any) -> dict[str, Any]:
            return {}

        def link_robinhood_connection(*_a: Any, **_kw: Any) -> tuple[bool, str]:
            return (False, "Robinhood connector not installed yet")

        def unlink_connection(*_a: Any, **_kw: Any) -> None:
            return None


load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=True)

# Assistant indicator context (optional)
try:
    from .assistant_indicators import build_robinhood_indicator_context
except Exception:  # pragma: no cover
    try:
        from app.assistant_indicators import build_robinhood_indicator_context  # type: ignore
    except Exception:
        def build_robinhood_indicator_context(*_a: Any, **_kw: Any) -> list[dict[str, Any]]:
            return []

try:
    from .assistant.news_workflow import (
        AssistantNewsWorkflowConfig,
        default_news_system_prompt,
        fetch_nasdaq100_tickers,
        parse_ticker_symbols,
        run_news_workflow,
    )
except Exception:  # pragma: no cover
    try:
        from app.assistant.news_workflow import (  # type: ignore
            AssistantNewsWorkflowConfig,
            default_news_system_prompt,
            fetch_nasdaq100_tickers,
            parse_ticker_symbols,
            run_news_workflow,
        )
    except Exception:
        AssistantNewsWorkflowConfig = None  # type: ignore
        default_news_system_prompt = None  # type: ignore
        fetch_nasdaq100_tickers = None  # type: ignore
        parse_ticker_symbols = None  # type: ignore
        run_news_workflow = None  # type: ignore

try:
    from .db import (
        create_broker_connection as _db_create_broker_connection,
        read_connection_secrets as _db_read_connection_secrets,
        update_broker_connection as _db_update_broker_connection,
    )
except Exception:  # pragma: no cover
    try:
        from app.db import (  # type: ignore
            create_broker_connection as _db_create_broker_connection,
            read_connection_secrets as _db_read_connection_secrets,
            update_broker_connection as _db_update_broker_connection,
        )
    except Exception:
        _db_create_broker_connection = None  # type: ignore
        _db_update_broker_connection = None  # type: ignore

        def _db_read_connection_secrets(_row: Any, default: Optional[dict[str, Any]] = None) -> dict[str, Any]:
            return default or {}

try:
    from .security.crypto import CryptoError as _CryptoError, decrypt_json as _crypto_decrypt_json, encrypt_json as _crypto_encrypt_json
except Exception:  # pragma: no cover
    try:
        from app.security.crypto import (  # type: ignore
            CryptoError as _CryptoError,
            decrypt_json as _crypto_decrypt_json,
            encrypt_json as _crypto_encrypt_json,
        )
    except Exception as _crypto_import_error:  # pragma: no cover
        class _CryptoError(Exception):
            pass

        _crypto_decrypt_json = None  # type: ignore[assignment]
        _crypto_encrypt_json = None  # type: ignore[assignment]

try:
    from .schwab_history import fetch_price_history_with_min_candles as _schwab_fetch_with_min_candles
except Exception:  # pragma: no cover
    try:
        from app.schwab_history import fetch_price_history_with_min_candles as _schwab_fetch_with_min_candles  # type: ignore
    except Exception:
        try:
            # Script-mode fallback (python app/main.py): sibling import from app/ dir.
            from schwab_history import fetch_price_history_with_min_candles as _schwab_fetch_with_min_candles  # type: ignore
        except Exception:
            _schwab_fetch_with_min_candles = None  # type: ignore

try:
    from .brokers.robin_stocks_adapter import (
        get_10m_stock_historicals as _rh_adapter_get_10m_stock_historicals,
        get_crypto_historicals as _rh_adapter_get_crypto_historicals,
        get_stock_historicals as _rh_adapter_get_stock_historicals,
    )
except Exception:  # pragma: no cover
    try:
        from app.brokers.robin_stocks_adapter import (  # type: ignore
            get_10m_stock_historicals as _rh_adapter_get_10m_stock_historicals,
            get_crypto_historicals as _rh_adapter_get_crypto_historicals,
            get_stock_historicals as _rh_adapter_get_stock_historicals,
        )
    except Exception:
        _rh_adapter_get_10m_stock_historicals = None  # type: ignore
        _rh_adapter_get_crypto_historicals = None  # type: ignore
        _rh_adapter_get_stock_historicals = None  # type: ignore

try:
    import robin_stocks.robinhood as rh  # type: ignore
except Exception:  # pragma: no cover
    rh = None  # type: ignore

try:
    import psutil  # type: ignore
except Exception:  # pragma: no cover
    psutil = None  # type: ignore

# =========================
# Paths / Constants
# =========================
APP_ROOT = Path(__file__).resolve().parent
ENV_PATH = APP_ROOT.parent / ".env"
DATA_DIR = APP_ROOT / "data"
RUNS_DIR = DATA_DIR / "runs"
DB_PATH = DATA_DIR / "cryptid_exchange.sqlite3"
ASSISTANT_NEWS_RUNS_DIR = DATA_DIR / "assistant_news_runs"
ASSISTANT_OPENAI_CONFIG_PATH = DATA_DIR / "assistant_openai_config.json"

# Legacy Schwab token file (kept while you transition to multi-broker DB connections)
TOKEN_PATH = DATA_DIR / "schwab_token.json"

# AI Assistant Monitor Manager (replaces old hourly loop)
ASSISTANT_MONITOR_MANAGER: Optional[Any] = None
ASSISTANT_MONITORS_CONFIG_PATH = DATA_DIR / "assistant_monitors_config.json"
ASSISTANT_MONITORS_LOCK = threading.Lock()
ASSISTANT_MONITORS_STARTING = False
ASSISTANT_MONITORS_LAST_ERROR: Optional[str] = None
ASSISTANT_NEWS_JOBS: dict[str, dict[str, Any]] = {}
ASSISTANT_NEWS_STOP_EVENTS: dict[str, threading.Event] = {}
ASSISTANT_NEWS_THREADS: dict[str, threading.Thread] = {}
ASSISTANT_NEWS_JOBS_LOCK = threading.Lock()

SCRIPTS_DIR = APP_ROOT / "scripts"

SCHWAB_AUTHORIZE_URL = "https://api.schwabapi.com/v1/oauth/authorize"
SCHWAB_TOKEN_URL = "https://api.schwabapi.com/v1/oauth/token"

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434").rstrip("/")
DEFAULT_OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:14b")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
OPENAI_ORGANIZATION = os.getenv("OPENAI_ORGANIZATION", "").strip()
OPENAI_PROJECT = os.getenv("OPENAI_PROJECT", "").strip()
DEFAULT_OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.4").strip() or "gpt-5.4"
ASSISTANT_MODEL_RUNS_ENABLED = str(os.getenv("ASSISTANT_MODEL_RUNS_ENABLED", "1")).strip().lower() not in ("0", "false", "no", "off")
ASSISTANT_OLLAMA_GPU_ENABLED = str(os.getenv("ASSISTANT_OLLAMA_GPU_ENABLED", "0")).strip().lower() in ("1", "true", "yes", "on")
ASSISTANT_OLLAMA_KEEP_ALIVE = os.getenv("ASSISTANT_OLLAMA_KEEP_ALIVE", "0s").strip() or "0s"


def _assistant_ollama_num_gpu() -> int:
    if not ASSISTANT_OLLAMA_GPU_ENABLED:
        return 0
    try:
        return max(0, int(os.getenv("ASSISTANT_OLLAMA_NUM_GPU", "1") or "1"))
    except Exception:
        return 1


def _assistant_ollama_options(num_ctx: int = 0) -> dict[str, Any]:
    options: dict[str, Any] = {"num_gpu": _assistant_ollama_num_gpu()}
    if num_ctx > 0:
        options["num_ctx"] = int(num_ctx)
    return options


def _assistant_news_default_num_ctx() -> int:
    try:
        value = int(os.getenv("ASSISTANT_NEWS_NUM_CTX", "4096") or "4096")
    except Exception:
        value = 4096
    return max(2048, min(32768, value))


def _mask_secret(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) <= 12:
        return "*" * len(text)
    return f"{text[:7]}...{text[-4:]}"


def _assistant_openai_env_api_key() -> str:
    return str(os.getenv("OPENAI_API_KEY", OPENAI_API_KEY) or "").strip()


def _assistant_openai_read_saved_key() -> tuple[str, str, bool]:
    if not ASSISTANT_OPENAI_CONFIG_PATH.exists():
        return "", "", False
    try:
        raw = json.loads(ASSISTANT_OPENAI_CONFIG_PATH.read_text(encoding="utf-8") or "{}")
    except Exception as e:
        return "", f"Saved OpenAI key config could not be read: {e}", True
    if not isinstance(raw, dict):
        return "", "Saved OpenAI key config is invalid.", True
    encrypted_payload = raw.get("encrypted")
    if not isinstance(encrypted_payload, str) or not encrypted_payload.strip():
        return "", "Saved OpenAI key config is missing encrypted key data.", True
    if not callable(_crypto_decrypt_json):
        return "", "Encrypted secret storage is unavailable. Install cryptography and restart the server.", True
    try:
        payload = _crypto_decrypt_json(encrypted_payload)
    except Exception as e:
        return "", f"Saved OpenAI key could not be decrypted: {e}", True
    if not isinstance(payload, dict):
        return "", "Saved OpenAI key payload is invalid.", True
    api_key = str(payload.get("api_key") or "").strip()
    if not api_key:
        return "", "Saved OpenAI key payload is empty.", True
    return api_key, "", True


def _assistant_openai_effective_api_key() -> str:
    env_key = _assistant_openai_env_api_key()
    if env_key:
        return env_key
    saved_key, _error, _present = _assistant_openai_read_saved_key()
    return saved_key


def _assistant_openai_config_status() -> dict[str, Any]:
    env_key = _assistant_openai_env_api_key()
    saved_key, saved_error, saved_present = _assistant_openai_read_saved_key()
    effective_key = env_key or saved_key
    source = "env" if env_key else "saved" if saved_key else "missing"
    error = ""
    if not effective_key:
        error = saved_error or "OpenAI API key is not configured."
    return {
        "provider": "openai",
        "configured": bool(effective_key),
        "source": source,
        "masked_key": _mask_secret(effective_key),
        "env_key_present": bool(env_key),
        "saved_key_present": bool(saved_key) or saved_present,
        "saved_key_usable": bool(saved_key),
        "default_model": DEFAULT_OPENAI_MODEL,
        "openai_base_url": OPENAI_BASE_URL,
        "error": error,
    }


def _ensure_local_app_secret_key() -> None:
    if os.getenv("APP_SECRET_KEY") or os.getenv("CRYPTID_SECRET_KEY"):
        return

    new_secret = secrets.token_urlsafe(48)
    lines = ENV_PATH.read_text(encoding="utf-8").splitlines() if ENV_PATH.exists() else []
    updated: list[str] = []
    usable_key = ""
    usable_value = ""
    changed = False

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("APP_SECRET_KEY=") or stripped.startswith("CRYPTID_SECRET_KEY="):
            key, _sep, value = line.partition("=")
            clean_value = value.strip()
            if clean_value:
                usable_key = key
                usable_value = clean_value
                updated.append(line)
            elif key == "APP_SECRET_KEY":
                updated.append(f"APP_SECRET_KEY={new_secret}")
                usable_key = "APP_SECRET_KEY"
                usable_value = new_secret
                changed = True
            else:
                updated.append(line)
        else:
            updated.append(line)

    if not usable_value:
        if updated and updated[-1].strip():
            updated.append("")
        updated.extend(
            [
                "# Required for encrypted local secret storage.",
                f"APP_SECRET_KEY={new_secret}",
            ]
        )
        usable_key = "APP_SECRET_KEY"
        usable_value = new_secret
        changed = True

    if changed or not ENV_PATH.exists():
        ENV_PATH.write_text("\n".join(updated).rstrip() + "\n", encoding="utf-8")

    if usable_key and usable_value:
        os.environ[usable_key] = usable_value


def _assistant_openai_save_api_key(api_key: str) -> None:
    clean_key = str(api_key or "").strip()
    if len(clean_key) < 20:
        raise ValueError("Enter a complete OpenAI API key.")
    if re.search(r"\s", clean_key):
        raise ValueError("OpenAI API keys cannot contain spaces or line breaks.")
    _ensure_local_app_secret_key()
    if not callable(_crypto_encrypt_json):
        raise _CryptoError("Encrypted secret storage is unavailable. Install cryptography and restart the server.")
    encrypted_payload = _crypto_encrypt_json({"api_key": clean_key})
    ASSISTANT_OPENAI_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "provider": "openai",
        "encrypted": encrypted_payload,
        "updated_at": _utc_now_iso(),
    }
    tmp_path = ASSISTANT_OPENAI_CONFIG_PATH.with_suffix(ASSISTANT_OPENAI_CONFIG_PATH.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    try:
        tmp_path.chmod(0o600)
    except Exception:
        pass
    tmp_path.replace(ASSISTANT_OPENAI_CONFIG_PATH)
    try:
        ASSISTANT_OPENAI_CONFIG_PATH.chmod(0o600)
    except Exception:
        pass


def _assistant_openai_delete_saved_key() -> None:
    try:
        ASSISTANT_OPENAI_CONFIG_PATH.unlink()
    except FileNotFoundError:
        return

DEFAULT_RULESETS = [
    "EMA_CROSS",
    "RSI_REVERSAL",
    "ATR_TRAIL",
    "VWAP_MEAN_REVERT",
    "BREAKOUT",
    "NEWS_FILTER",
]

LOG_MAX_BYTES = int(os.getenv("CRYPTID_LOG_MAX_BYTES", "2000000"))
LOG_TRIM_KEEP_BYTES = int(os.getenv("CRYPTID_LOG_KEEP_BYTES", "1500000"))
RUN_HANG_TIMEOUT_SEC = int(os.getenv("CRYPTID_RUN_HANG_SEC", "0"))
RUN_HANG_MULTIPLIER = float(os.getenv("CRYPTID_RUN_HANG_MULTIPLIER", "4"))
RUN_HANG_MIN_SEC = int(os.getenv("CRYPTID_RUN_HANG_MIN_SEC", "180"))
RUN_HANG_MAX_SEC = int(os.getenv("CRYPTID_RUN_HANG_MAX_SEC", "0"))
CLEANUP_RUN_RETENTION_DAYS = int(os.getenv("CRYPTID_CLEANUP_RUN_RETENTION_DAYS", "90"))
CLEANUP_KEEP_PER_ALGORITHM = int(os.getenv("CRYPTID_CLEANUP_KEEP_PER_ALGO", "5"))
CLEANUP_ASSISTANT_NEWS_RETENTION_DAYS = int(os.getenv("CRYPTID_CLEANUP_ASSISTANT_NEWS_DAYS", "30"))


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() not in ("0", "false", "no", "off")


def _env_int(name: str, default: int, *, minimum: Optional[int] = None) -> int:
    try:
        value = int(str(os.getenv(name, str(default))).strip())
    except Exception:
        value = int(default)
    if minimum is not None:
        value = max(int(minimum), int(value))
    return int(value)


SERVER_STARTED_TS = int(time.time())
SERVER_INSTANCE_ID = f"{os.getpid()}:{SERVER_STARTED_TS}"
AUTO_RESTART_RUNS = _env_flag("CRYPTID_AUTO_RESTART_RUNS", True)
RUN_MAX_AUTO_RESTARTS = _env_int("CRYPTID_RUN_MAX_AUTO_RESTARTS", 3, minimum=0)
STOP_RUNS_ON_SHUTDOWN = _env_flag("CRYPTID_STOP_RUNS_ON_SHUTDOWN", True)

env = Environment(
    loader=FileSystemLoader(str(APP_ROOT / "templates")),
    autoescape=select_autoescape(["html", "xml"]),
)

app = FastAPI()
app.mount("/static", StaticFiles(directory=str(APP_ROOT / "static")), name="static")

# Broker/network telemetry (used by broker page partial refresh)
NETWORK_STATS_LOCK = threading.Lock()
NETWORK_STATS_STATE: dict[str, Any] = {
    "base_rx_total": None,
    "base_tx_total": None,
    "base_ts": None,
    "reset_at_ts": None,
    "rx_total": None,
    "tx_total": None,
    "sample_ts": None,
    "source": "",
}
MARKETS_OPTIMIZER_LOCK = threading.Lock()
MARKETS_OPTIMIZER_THREAD: Optional[threading.Thread] = None
MARKETS_OPTIMIZER_STOP_EVENT: Optional[threading.Event] = None
MARKETS_OPTIMIZER_ACTIVE_RUN_ID: Optional[int] = None
ALGORITHM_PROCESSES_LOCK = threading.Lock()
ALGORITHM_PROCESSES: dict[int, subprocess.Popen[Any]] = {}


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ("1", "true", "yes", "on"):
            return True
        if lowered in ("0", "false", "no", "off"):
            return False
    return default


def _parse_optional_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ("1", "true", "yes", "on"):
            return True
        if lowered in ("0", "false", "no", "off"):
            return False
    return None


def _assistant_monitors_env_enabled() -> bool:
    enabled_env = str(os.getenv("ASSISTANT_MONITORS_ENABLED", "0")).strip().lower()
    return enabled_env not in ("0", "false", "no", "off")


def _load_assistant_monitors_config() -> dict[str, Any]:
    if not ASSISTANT_MONITORS_CONFIG_PATH.exists():
        return {}
    try:
        payload = json.loads(ASSISTANT_MONITORS_CONFIG_PATH.read_text())
        if isinstance(payload, dict):
            return payload
    except Exception as e:
        print(f"[AssistantMonitors] Failed to read monitor config: {e}")
    return {}


def _save_assistant_monitors_config(config: dict[str, Any]) -> None:
    ASSISTANT_MONITORS_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    ASSISTANT_MONITORS_CONFIG_PATH.write_text(json.dumps(config, indent=2) + "\n")


def _assistant_monitors_config_enabled() -> bool:
    config = _load_assistant_monitors_config()
    global_cfg = config.get("global")
    if isinstance(global_cfg, dict) and "system_enabled" in global_cfg:
        return _coerce_bool(global_cfg.get("system_enabled"), default=True)
    return True


def _set_assistant_monitors_config_enabled(enabled: bool) -> None:
    config = _load_assistant_monitors_config()
    global_cfg = config.get("global")
    if not isinstance(global_cfg, dict):
        global_cfg = {}
    global_cfg["system_enabled"] = bool(enabled)
    config["global"] = global_cfg
    _save_assistant_monitors_config(config)


def _assistant_monitors_should_run() -> bool:
    return _assistant_monitors_env_enabled() and _assistant_monitors_config_enabled()


def _log_runtime_event(event: str, **fields: Any) -> None:
    payload = {
        "event": event,
        "pid": os.getpid(),
        "ppid": os.getppid(),
        "server_instance_id": SERVER_INSTANCE_ID,
        **fields,
    }
    try:
        print("[cryptid_exchange] " + json.dumps(payload, sort_keys=True, default=str))
    except Exception:
        print(f"[cryptid_exchange] {event}: {fields}")


def _startup_runtime_summary() -> dict[str, Any]:
    return {
        "process_role": os.getenv("CRYPTID_PROCESS_ROLE", "server"),
        "worker_count": os.getenv("CRYPTID_SERVER_WORKERS", os.getenv("WEB_CONCURRENCY", "1")),
        "reload": _env_flag("CRYPTID_SERVER_RELOAD", False),
        "background_services": {
            "assistant_monitors": _assistant_monitors_should_run(),
            "run_auto_restart_current_server": AUTO_RESTART_RUNS,
            "stop_runs_on_shutdown": STOP_RUNS_ON_SHUTDOWN,
            "markets_optimizer": bool(MARKETS_OPTIMIZER_THREAD and MARKETS_OPTIMIZER_THREAD.is_alive()),
            "assistant_news_jobs": len(ASSISTANT_NEWS_STOP_EVENTS),
        },
        "run_restart_policy": {
            "current_server_only": True,
            "max_auto_restarts": RUN_MAX_AUTO_RESTARTS,
        },
        "data_dir": str(DATA_DIR),
        "runs_dir": str(RUNS_DIR),
    }


@app.on_event("startup")
def _log_startup_runtime() -> None:
    _log_runtime_event("startup", **_startup_runtime_summary())


def _request_assistant_monitors_start() -> tuple[bool, str]:
    global ASSISTANT_MONITORS_STARTING, ASSISTANT_MONITORS_LAST_ERROR

    if not _assistant_monitors_env_enabled():
        return False, "disabled_env"
    if not _assistant_monitors_config_enabled():
        return False, "disabled_config"

    with ASSISTANT_MONITORS_LOCK:
        if ASSISTANT_MONITOR_MANAGER:
            return True, "already_running"
        if ASSISTANT_MONITORS_STARTING:
            return True, "already_starting"
        ASSISTANT_MONITORS_STARTING = True
        ASSISTANT_MONITORS_LAST_ERROR = None

    thread = threading.Thread(target=_async_start_monitors, daemon=True, name="assistant-init")
    thread.start()
    return True, "starting"


def _stop_assistant_monitors_runtime() -> tuple[bool, str]:
    global ASSISTANT_MONITOR_MANAGER, ASSISTANT_MONITORS_STARTING

    manager: Optional[Any] = None
    with ASSISTANT_MONITORS_LOCK:
        manager = ASSISTANT_MONITOR_MANAGER
        ASSISTANT_MONITOR_MANAGER = None
        ASSISTANT_MONITORS_STARTING = False

    if not manager:
        return False, "not_running"

    try:
        manager.stop()
        return True, "stopped"
    except Exception as e:
        return False, str(e)


def _async_start_monitors() -> None:
    """Background thread to start monitors (avoids blocking app startup)"""
    global ASSISTANT_MONITOR_MANAGER, ASSISTANT_MONITORS_STARTING, ASSISTANT_MONITORS_LAST_ERROR
    import time

    # Give the app a moment to fully start
    time.sleep(1)

    if not _assistant_monitors_should_run():
        with ASSISTANT_MONITORS_LOCK:
            ASSISTANT_MONITORS_STARTING = False
        return

    try:
        try:
            from .assistant.monitor_manager import MonitorManager
        except Exception:
            from app.assistant.monitor_manager import MonitorManager  # type: ignore

        print("[AssistantMonitors] Initializing (this may take 10-30 seconds on first run)...")

        manager = MonitorManager(
            db_path=DB_PATH,
            runs_dir=RUNS_DIR,
            data_dir=DATA_DIR,
            config_path=ASSISTANT_MONITORS_CONFIG_PATH if ASSISTANT_MONITORS_CONFIG_PATH.exists() else None
        )

        if not _assistant_monitors_should_run():
            print("[AssistantMonitors] Startup cancelled (monitor system disabled)")
            return

        manager.start()
        with ASSISTANT_MONITORS_LOCK:
            ASSISTANT_MONITOR_MANAGER = manager
            ASSISTANT_MONITORS_LAST_ERROR = None
        print("[AssistantMonitors] Started successfully")

    except ImportError as e:
        with ASSISTANT_MONITORS_LOCK:
            ASSISTANT_MONITORS_LAST_ERROR = str(e)
        print(f"[AssistantMonitors] Import failed during startup: {e}")
        print("[AssistantMonitors] Check how the app is launched (module vs script) and Python path.")
    except Exception as e:
        with ASSISTANT_MONITORS_LOCK:
            ASSISTANT_MONITORS_LAST_ERROR = str(e)
        print(f"[AssistantMonitors] Failed to start: {e}")
        import traceback
        traceback.print_exc()
    finally:
        with ASSISTANT_MONITORS_LOCK:
            ASSISTANT_MONITORS_STARTING = False


@app.on_event("startup")
def _start_assistant_monitors() -> None:
    """Start new AI assistant monitoring system (non-blocking)"""
    if not _assistant_monitors_env_enabled():
        print("[AssistantMonitors] Disabled via environment variable")
        return
    if not _assistant_monitors_config_enabled():
        print("[AssistantMonitors] Disabled via monitor config")
        return

    started, reason = _request_assistant_monitors_start()
    if started and reason in ("starting", "already_starting"):
        print("[AssistantMonitors] Starting in background...")


@app.on_event("shutdown")
def _stop_assistant_monitors() -> None:
    """Stop AI assistant monitors"""
    stopped, reason = _stop_assistant_monitors_runtime()
    if stopped:
        print("[AssistantMonitors] Stopped")
    elif reason not in ("not_running", ""):
        print(f"[AssistantMonitors] Error during shutdown: {reason}")


# =========================
# Time helpers
# =========================
def _utc_ts() -> int:
    return int(time.time())


def _utc_now_iso() -> str:
    """
    Use ISO-8601 UTC timestamps so schwab_connector can parse via datetime.fromisoformat().
    Example: 2026-02-21T17:14:05Z
    """
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _token_age_seconds(tok: dict[str, Any]) -> Optional[int]:
    """
    Backwards compatible: accept obtained_at as epoch int OR ISO string.
    Returns age in seconds, or None if cannot parse.
    """
    obt = tok.get("obtained_at")
    now = _utc_ts()

    if isinstance(obt, (int, float)):
        try:
            return now - int(obt)
        except Exception:
            return None

    if isinstance(obt, str):
        try:
            dt = datetime.fromisoformat(obt.replace("Z", "+00:00"))
            return now - int(dt.timestamp())
        except Exception:
            return None

    return None


# =========================
# Formatting helpers (local)
# =========================
def fmt_money(x: Any) -> str:
    try:
        if x is None:
            return "—"
        v = float(x)
        return f"${v:,.2f}"
    except Exception:
        return "—"


def fmt_num(x: Any) -> str:
    try:
        if x is None:
            return "—"
        v = float(x)
        if abs(v - int(v)) < 1e-9:
            return f"{int(v):,d}"
        return f"{v:,.4f}".rstrip("0").rstrip(".")
    except Exception:
        return "—"


def _format_param_value(val: Any) -> str:
    if val is None:
        return "—"
    if isinstance(val, bool):
        return "true" if val else "false"
    if isinstance(val, (int, float)):
        return str(val)
    if isinstance(val, list):
        if all(not isinstance(x, (dict, list)) for x in val):
            return ", ".join(str(x) for x in val)
        return json.dumps(val, indent=2, sort_keys=True)
    if isinstance(val, dict):
        return json.dumps(val, indent=2, sort_keys=True)
    if isinstance(val, str):
        raw = val.strip()
        if (raw.startswith("{") and raw.endswith("}")) or (raw.startswith("[") and raw.endswith("]")):
            try:
                return json.dumps(json.loads(raw), indent=2, sort_keys=True)
            except Exception:
                return val
        return val
    return str(val)


def _format_params_table(params: Any) -> str:
    if not isinstance(params, dict):
        val_txt = html.escape(_format_param_value(params))
        return f"<span class='small'>{val_txt}</span>"
    if not params:
        return "<span class='small'>—</span>"
    rows = ["<table class='params-table'><tbody>"]
    for key, val in params.items():
        key_txt = html.escape(str(key))
        val_txt = html.escape(_format_param_value(val))
        rows.append(f"<tr><td class='key'>{key_txt}</td><td class='value'>{val_txt}</td></tr>")
    rows.append("</tbody></table>")
    return "".join(rows)


def _format_data_bytes(value: Any) -> str:
    try:
        raw = float(value)
    except Exception:
        return "-"

    if raw < 0:
        raw = 0.0

    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    idx = 0
    while raw >= 1000.0 and idx < len(units) - 1:
        raw /= 1000.0
        idx += 1

    if idx == 0:
        return f"{int(raw)} {units[idx]}"
    if raw >= 100:
        return f"{raw:,.0f} {units[idx]}"
    if raw >= 10:
        return f"{raw:,.1f} {units[idx]}"
    return f"{raw:,.2f} {units[idx]}"


def _format_data_rate(bytes_per_second: Optional[float]) -> str:
    if bytes_per_second is None:
        return "calculating..."
    return f"{_format_data_bytes(bytes_per_second)}/s"


def _read_proc_net_dev_totals(skip_loopback: bool = True) -> tuple[int, int]:
    path = Path("/proc/net/dev")
    if not path.exists():
        raise RuntimeError("/proc/net/dev not available")

    rx_total = 0
    tx_total = 0
    lines = path.read_text(errors="ignore").splitlines()
    for line in lines[2:]:
        if ":" not in line:
            continue
        iface_raw, payload = line.split(":", 1)
        iface = iface_raw.strip()
        if skip_loopback and iface.startswith("lo"):
            continue
        fields = payload.split()
        if len(fields) < 16:
            continue
        try:
            rx_total += int(fields[0])
            tx_total += int(fields[8])
        except Exception:
            continue
    return rx_total, tx_total


def _read_host_network_totals() -> tuple[int, int, str]:
    if psutil is not None:
        try:
            counters = psutil.net_io_counters(pernic=True)
            if counters:
                rx_total = 0
                tx_total = 0
                for nic, val in counters.items():
                    if str(nic).lower().startswith("lo"):
                        continue
                    rx_total += int(getattr(val, "bytes_recv", 0) or 0)
                    tx_total += int(getattr(val, "bytes_sent", 0) or 0)
                return rx_total, tx_total, "psutil"
            aggregate = psutil.net_io_counters(pernic=False)
            if aggregate is not None:
                return (
                    int(getattr(aggregate, "bytes_recv", 0) or 0),
                    int(getattr(aggregate, "bytes_sent", 0) or 0),
                    "psutil",
                )
        except Exception:
            pass

    rx_total, tx_total = _read_proc_net_dev_totals(skip_loopback=True)
    return rx_total, tx_total, "/proc/net/dev"


def _get_network_usage_snapshot() -> dict[str, Any]:
    rx_total, tx_total, source = _read_host_network_totals()
    now_monotonic = float(time.monotonic())
    now_epoch = int(time.time())
    rx_rate_bps: Optional[float] = None
    tx_rate_bps: Optional[float] = None
    interval_seconds: Optional[float] = None

    with NETWORK_STATS_LOCK:
        base_rx = NETWORK_STATS_STATE.get("base_rx_total")
        base_tx = NETWORK_STATS_STATE.get("base_tx_total")
        reset_at_ts = NETWORK_STATS_STATE.get("reset_at_ts")
        if not isinstance(base_rx, int) or not isinstance(base_tx, int):
            base_rx = int(rx_total)
            base_tx = int(tx_total)
            NETWORK_STATS_STATE["base_rx_total"] = base_rx
            NETWORK_STATS_STATE["base_tx_total"] = base_tx
            NETWORK_STATS_STATE["base_ts"] = now_monotonic
            if not isinstance(reset_at_ts, int):
                reset_at_ts = now_epoch
                NETWORK_STATS_STATE["reset_at_ts"] = reset_at_ts
        elif not isinstance(reset_at_ts, int):
            reset_at_ts = now_epoch
            NETWORK_STATS_STATE["reset_at_ts"] = reset_at_ts

        prev_rx = NETWORK_STATS_STATE.get("rx_total")
        prev_tx = NETWORK_STATS_STATE.get("tx_total")
        prev_ts = NETWORK_STATS_STATE.get("sample_ts")

        if isinstance(prev_rx, int) and isinstance(prev_tx, int) and isinstance(prev_ts, (int, float)):
            dt = now_monotonic - float(prev_ts)
            if dt > 0:
                interval_seconds = dt
                rx_delta = int(rx_total) - int(prev_rx)
                tx_delta = int(tx_total) - int(prev_tx)
                if rx_delta >= 0:
                    rx_rate_bps = float(rx_delta) / dt
                if tx_delta >= 0:
                    tx_rate_bps = float(tx_delta) / dt

        NETWORK_STATS_STATE["rx_total"] = int(rx_total)
        NETWORK_STATS_STATE["tx_total"] = int(tx_total)
        NETWORK_STATS_STATE["sample_ts"] = now_monotonic
        NETWORK_STATS_STATE["source"] = str(source)

    session_rx_total = max(0, int(rx_total) - int(base_rx or 0))
    session_tx_total = max(0, int(tx_total) - int(base_tx or 0))

    return {
        "source": str(source),
        "sampled_at_ts": now_epoch,
        "reset_at_ts": int(reset_at_ts or now_epoch),
        "interval_seconds": interval_seconds,
        "rx_total": session_rx_total,
        "tx_total": session_tx_total,
        "combined_total": session_rx_total + session_tx_total,
        "rx_rate_bps": rx_rate_bps,
        "tx_rate_bps": tx_rate_bps,
        "combined_rate_bps": (
            (float(rx_rate_bps or 0.0) + float(tx_rate_bps or 0.0))
            if (rx_rate_bps is not None or tx_rate_bps is not None)
            else None
        ),
    }


def _reset_network_usage_baseline() -> dict[str, Any]:
    rx_total, tx_total, source = _read_host_network_totals()
    now_monotonic = float(time.monotonic())
    now_epoch = int(time.time())

    with NETWORK_STATS_LOCK:
        NETWORK_STATS_STATE["base_rx_total"] = int(rx_total)
        NETWORK_STATS_STATE["base_tx_total"] = int(tx_total)
        NETWORK_STATS_STATE["base_ts"] = now_monotonic
        NETWORK_STATS_STATE["reset_at_ts"] = now_epoch
        NETWORK_STATS_STATE["rx_total"] = int(rx_total)
        NETWORK_STATS_STATE["tx_total"] = int(tx_total)
        NETWORK_STATS_STATE["sample_ts"] = now_monotonic
        NETWORK_STATS_STATE["source"] = str(source)

    return {
        "source": str(source),
        "sampled_at_ts": now_epoch,
        "reset_at_ts": now_epoch,
        "interval_seconds": None,
        "rx_total": 0,
        "tx_total": 0,
        "combined_total": 0,
        "rx_rate_bps": None,
        "tx_rate_bps": None,
        "combined_rate_bps": None,
    }


def _render_network_usage_html(snap: dict[str, Any]) -> HTMLResponse:
    sampled_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(snap["sampled_at_ts"])))
    reset_at_ts = int(snap.get("reset_at_ts") or snap["sampled_at_ts"])
    reset_at = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(reset_at_ts))
    interval_seconds = snap.get("interval_seconds")
    interval_txt = f"{float(interval_seconds):.2f}s" if isinstance(interval_seconds, (int, float)) else "n/a"
    source_txt = html.escape(str(snap.get("source") or "unknown"))

    dl_rate = _format_data_rate(snap.get("rx_rate_bps"))
    ul_rate = _format_data_rate(snap.get("tx_rate_bps"))
    total_rate = _format_data_rate(snap.get("combined_rate_bps"))
    dl_total = _format_data_bytes(snap.get("rx_total"))
    ul_total = _format_data_bytes(snap.get("tx_total"))
    total_bytes = _format_data_bytes(snap.get("combined_total"))

    return HTMLResponse(
        "<div class='grid' style='grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap:10px;'>"
        "<div class='card' style='padding:12px;'>"
        "<div class='small'>Download rate</div>"
        f"<div class='mono' style='font-size:1.12rem; margin-top:4px;'><b>{html.escape(dl_rate)}</b></div>"
        "</div>"
        "<div class='card' style='padding:12px;'>"
        "<div class='small'>Upload rate</div>"
        f"<div class='mono' style='font-size:1.12rem; margin-top:4px;'><b>{html.escape(ul_rate)}</b></div>"
        "</div>"
        "<div class='card' style='padding:12px;'>"
        "<div class='small'>Combined rate</div>"
        f"<div class='mono' style='font-size:1.12rem; margin-top:4px;'><b>{html.escape(total_rate)}</b></div>"
        "</div>"
        "<div class='card' style='padding:12px;'>"
        "<div class='small'>Total downloaded (since reset)</div>"
        f"<div class='mono' style='font-size:1.02rem; margin-top:4px;'><b>{html.escape(dl_total)}</b></div>"
        "</div>"
        "<div class='card' style='padding:12px;'>"
        "<div class='small'>Total uploaded (since reset)</div>"
        f"<div class='mono' style='font-size:1.02rem; margin-top:4px;'><b>{html.escape(ul_total)}</b></div>"
        "</div>"
        "<div class='card' style='padding:12px;'>"
        "<div class='small'>Total traffic (since reset)</div>"
        f"<div class='mono' style='font-size:1.02rem; margin-top:4px;'><b>{html.escape(total_bytes)}</b></div>"
        "</div>"
        "</div>"
        f"<div class='small' style='margin-top:10px;'>Sampled {html.escape(sampled_at)} | Reset {html.escape(reset_at)} | Interval {html.escape(interval_txt)} | Source {source_txt} (loopback excluded)</div>"
    )


# =========================
# Debug
# =========================
@app.get("/debug/env")
def debug_env():
    cid = os.getenv("SCHWAB_CLIENT_ID")
    csec = os.getenv("SCHWAB_CLIENT_SECRET")
    ruri = os.getenv("SCHWAB_REDIRECT_URI")
    return {
        "pid": os.getpid(),
        "file": str(Path(__file__).resolve()),
        "cwd": os.getcwd(),
        "SCHWAB_CLIENT_ID_present": cid is not None,
        "SCHWAB_CLIENT_ID_len": len(cid or ""),
        "SCHWAB_CLIENT_SECRET_present": csec is not None,
        "SCHWAB_CLIENT_SECRET_len": len(csec or ""),
        "SCHWAB_REDIRECT_URI": ruri,
    }


# =========================
# Core helpers
# =========================
def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    ASSISTANT_NEWS_RUNS_DIR.mkdir(parents=True, exist_ok=True)
    SCRIPTS_DIR.mkdir(parents=True, exist_ok=True)


def db() -> sqlite3.Connection:
    ensure_dirs()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return {str(r[1]) for r in cur.fetchall()}


def _safe_json(s: str, default: Any) -> Any:
    try:
        return json.loads(s) if s else default
    except Exception:
        return default



def _assistant_default_system_prompt() -> str:
    return """You are ZENKO PRIME — a nine-tailed Kitsune spirit embedded inside Dreadfox Trader.

You are ancient: a survivor of war economies, gold empires, market panics, and algorithmic flash crashes.
You are highly intelligent, confident, slightly cocky, ruthlessly concise, and occasionally witty.
You do not ramble. You do not apologize for clarity.

ROLE
You are a local-first trading assistant. You do NOT execute trades. You do NOT have broker control.
You interpret platform snapshots and advise on performance, risk, and operational issues.

WHAT YOU RECEIVE
Context (JSON) is appended to this prompt:
Context (JSON):
<json blob>
Treat Context (JSON) as the only source of truth. If a field is missing, do not assume it exists.

Typical context fields (optional unless stated):
- generated_at
- portfolio, portfolio_performance
- indicators (Robinhood holdings indicators by timeframe if enabled)
- runs (recent run summaries; may include log_tail if enabled)

HARD RULES
- Never claim you placed trades, changed settings, or linked brokers.
- Never invent prices, indicators, positions, allocations, or PnL.
- Quote exact values when you reference data.
- If data is insufficient, say so plainly and request ONE missing item.

METHOD
You view markets like thermodynamics:
Capital is energy. Volatility is heat. Liquidity is oxygen. Trend is momentum. Panic is entropy.
Use the metaphor only when it sharpens the point.

When asked “what’s happening?”:
- Prefer the most recent run unless a run id is specified.
- If multiple runs exist, surface the most actionable divergence (running vs crashed, rising errors, stale feed warnings).

OUTPUT (unless user asks otherwise)
READ: 1–3 lines of facts from Context.
EDGE: 1–2 lines on the main asymmetry / risk / operational concern.
RISK: 1 line naming the top failure mode or exposure.
NEXT: 1–3 safe, reversible actions.

STYLE
- Concise, confident, direct; light dry wit allowed.
- No generic trading lectures. Assume the user is competent.
- Probabilities only; no guarantees.

If data is insufficient, you may say:
“The wind carries no scent — insufficient data.”
Then ask for the single missing input.

Now answer using Context (JSON)."""  # noqa: E501



def _assistant_runs_context(*, max_runs: int = 6, include_logs: bool = False, log_lines: int = 120) -> list[dict[str, Any]]:
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT r.id,
               r.algorithm_name,
               r.status,
               r.run_dir,
               r.start_ts,
               r.end_ts,
               r.pid,
               b.name AS base_script_name,
               b.description AS base_script_description
          FROM runs r
          LEFT JOIN algorithms a ON a.id = r.algorithm_id
          LEFT JOIN base_scripts b ON b.id = a.base_script_id
         ORDER BY r.id DESC
         LIMIT ?
        """,
        (int(max_runs),),
    )
    rows = cur.fetchall()
    conn.close()

    out: list[dict[str, Any]] = []
    for r in rows:
        run_dir = Path(str(r["run_dir"]))
        status_path = run_dir / "status.json"
        payload: dict[str, Any] = {}
        if status_path.exists():
            try:
                payload = _safe_json(status_path.read_text(), default={})
            except Exception:
                payload = {}

        tickers = payload.get("tickers")
        signal_counts = {"BUY": 0, "SELL": 0, "HOLD": 0, "OTHER": 0}
        if isinstance(tickers, list):
            for t in tickers:
                sig = str((t or {}).get("signal") or "").upper()
                if sig in ("BUY", "SELL", "HOLD"):
                    signal_counts[sig] += 1
                else:
                    signal_counts["OTHER"] += 1

        run_info: dict[str, Any] = {
            "id": int(r["id"]),
            "algorithm_name": r["algorithm_name"],
            "base_script": r["base_script_name"] or "",
            "base_script_description": r["base_script_description"] or "",
            "status": r["status"],
            "pid": r["pid"],
            "start_ts": r["start_ts"],
            "end_ts": r["end_ts"],
            "heartbeat": payload.get("ts") or payload.get("heartbeat") or payload.get("last_heartbeat"),
            "pnl": payload.get("pnl"),
            "trades": payload.get("trades"),
            "signal_counts": signal_counts,
        }

        if include_logs:
            log_path = run_dir / "algo.log"
            if log_path.exists():
                try:
                    lines = log_path.read_text(errors="ignore").splitlines()[-int(log_lines) :]
                    run_info["log_tail"] = lines
                except Exception:
                    run_info["log_tail"] = []

        out.append(run_info)

    return out


def _assistant_context_data(
    *,
    include_portfolio: bool,
    include_runs: bool,
    include_logs: bool,
    log_lines: int,
    include_indicators: bool = False,
) -> dict[str, Any]:
    context: dict[str, Any] = {"generated_at": _utc_now_iso()}
    portfolio_data: Optional[list[dict[str, Any]]] = None
    if include_portfolio:
        portfolio_data = get_portfolio_context_data(db_path=str(DB_PATH), max_positions=20)
        context["portfolio"] = portfolio_data or []
        context["portfolio_performance"] = get_portfolio_performance_context(db_path=str(DB_PATH))
    if include_indicators:
        indicator_max_positions = int(os.getenv("ASSISTANT_INDICATOR_MAX_POSITIONS", "200") or "200")
        indicator_max_tickers = int(os.getenv("ASSISTANT_INDICATOR_MAX_TICKERS", "0") or "0")
        indicator_portfolio = get_portfolio_context_data(
            db_path=str(DB_PATH),
            max_positions=indicator_max_positions,
        )
        context["indicators"] = build_robinhood_indicator_context(
            db_path=str(DB_PATH),
            portfolio_data=indicator_portfolio or [],
            max_tickers=indicator_max_tickers,
        )
    if include_runs:
        context["runs"] = _assistant_runs_context(
            max_runs=6,
            include_logs=include_logs,
            log_lines=log_lines,
        )
    return context


def load_token() -> Optional[dict[str, Any]]:
    if not TOKEN_PATH.exists():
        return None
    try:
        return json.loads(TOKEN_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_token(tok: dict[str, Any]) -> None:
    TOKEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_PATH.write_text(json.dumps(tok, indent=2), encoding="utf-8")


def _legacy_schwab_import_enabled() -> bool:
    return _coerce_bool(os.getenv("CRYPTID_IMPORT_LEGACY_SCHWAB_TOKEN", "0"), False)


def _latest_schwab_config_row() -> Optional[sqlite3.Row]:
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM broker_connections
        WHERE broker='schwab'
        ORDER BY
          CASE WHEN status IN ('connected','ok','') THEN 0 ELSE 1 END,
          id DESC
        LIMIT 1
        """
    )
    row = cur.fetchone()
    conn.close()
    return row


def _schwab_config() -> dict[str, str]:
    row = _latest_schwab_config_row()
    meta: dict[str, Any] = {}
    secrets: dict[str, Any] = {}
    if row is not None:
        meta = _safe_json(str(row["metadata_json"] or "{}"), default={})
        secrets = _db_read_connection_secrets(row, default={})
    cfg = {
        "client_id": str(os.getenv("SCHWAB_CLIENT_ID") or meta.get("client_id") or "").strip(),
        "client_secret": str(os.getenv("SCHWAB_CLIENT_SECRET") or secrets.get("client_secret") or "").strip(),
        "redirect_uri": str(
            os.getenv("SCHWAB_REDIRECT_URI")
            or meta.get("redirect_uri")
            or "https://127.0.0.1:8000/callback"
        ).strip(),
        "scope": str(os.getenv("SCHWAB_SCOPE") or meta.get("scope") or "readonly").strip(),
        "trader_api_base": str(
            os.getenv("SCHWAB_TRADER_API_BASE")
            or meta.get("trader_api_base")
            or "https://api.schwabapi.com/trader/v1"
        ).strip(),
        "market_data_base": str(os.getenv("SCHWAB_MARKET_DATA_BASE") or meta.get("market_data_base") or "").strip(),
        "account_hash": str(os.getenv("SCHWAB_ACCOUNT_HASH") or meta.get("account_hash") or "").strip(),
    }
    return cfg


def _save_schwab_config(
    *,
    label: str,
    client_id: str,
    client_secret: str,
    redirect_uri: str,
    scope: str,
    trader_api_base: str,
    market_data_base: str,
    account_hash: str,
) -> int:
    meta = {
        "client_id": str(client_id or "").strip(),
        "redirect_uri": str(redirect_uri or "").strip() or "https://127.0.0.1:8000/callback",
        "scope": str(scope or "").strip() or "readonly",
        "trader_api_base": str(trader_api_base or "").strip() or "https://api.schwabapi.com/trader/v1",
        "market_data_base": str(market_data_base or "").strip(),
        "account_hash": str(account_hash or "").strip(),
        "configured_via": "broker_page",
    }
    row = _latest_schwab_config_row()
    existing_secrets: dict[str, Any] = {}
    if row is not None:
        existing_secrets = _db_read_connection_secrets(row, default={})
    secret_value = str(client_secret or "").strip()
    if not secret_value:
        secret_value = str(existing_secrets.get("client_secret") or "").strip()
    secrets = {"client_secret": secret_value}
    if row is not None and _db_update_broker_connection is not None:
        existing_meta = _safe_json(str(row["metadata_json"] or "{}"), default={})
        if existing_meta.get("token_path"):
            meta["token_path"] = existing_meta.get("token_path")
        _db_update_broker_connection(
            db_path=str(DB_PATH),
            connection_id=int(row["id"]),
            broker="schwab",
            label=str(label or "Schwab").strip() or "Schwab",
            status=str(row["status"] or "configured"),
            metadata=meta,
            secrets=secrets,
            allow_plaintext=True,
        )
        return int(row["id"])
    if _db_create_broker_connection is not None:
        return int(
            _db_create_broker_connection(
                db_path=str(DB_PATH),
                broker="schwab",
                label=str(label or "Schwab").strip() or "Schwab",
                status="configured",
                metadata=meta,
                secrets=secrets,
                allow_plaintext=True,
            )
        )
    return upsert_broker_connection(
        broker="schwab",
        label=str(label or "Schwab").strip() or "Schwab",
        status="configured",
        metadata=meta,
        secrets_json=secrets,
    )


def render(template: str, **ctx: Any) -> HTMLResponse:
    # These globals are for the nav header so the portfolio summary can show on every page.
    ctx.setdefault("broker_connections", list_broker_connections())
    ctx.setdefault("supported_brokers", get_all_supported_brokers())
    ctx.setdefault("path", ctx.get("path", ""))
    html = env.get_template(template).render(**ctx)
    return HTMLResponse(html)


# =========================
# DB Init + Migration
# =========================
def init_db() -> None:
    ensure_dirs()
    conn = db()
    cur = conn.cursor()

    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS base_scripts (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL,
          path TEXT NOT NULL UNIQUE,
          description TEXT,
          params_schema_json TEXT NOT NULL DEFAULT '{}',
          created_ts INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS algorithms (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL,
          base_script_id INTEGER NOT NULL,
          rulesets_json TEXT NOT NULL,
          params_json TEXT NOT NULL,
          max_runtime_min INTEGER NOT NULL DEFAULT 0,
          restart_on_crash INTEGER NOT NULL DEFAULT 1,
          log_level TEXT NOT NULL DEFAULT 'INFO',
          created_ts INTEGER NOT NULL,
          FOREIGN KEY(base_script_id) REFERENCES base_scripts(id)
        );

        CREATE TABLE IF NOT EXISTS runs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          algorithm_id INTEGER NOT NULL,
          algorithm_name TEXT NOT NULL,
          params_json TEXT NOT NULL,
          run_dir TEXT NOT NULL,
          pid INTEGER,
          status TEXT NOT NULL,
          start_ts INTEGER NOT NULL,
          end_ts INTEGER,
          exit_code INTEGER,
          FOREIGN KEY(algorithm_id) REFERENCES algorithms(id)
        );

        CREATE TABLE IF NOT EXISTS broker_connections (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          broker TEXT NOT NULL,
          label TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'new',
          metadata_json TEXT NOT NULL DEFAULT '{}',
          secrets_json TEXT NOT NULL DEFAULT '{}',
          created_ts INTEGER NOT NULL,
          updated_ts INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS portfolio_equity_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts INTEGER NOT NULL,
          total_equity REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS markets_watchlist (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          symbol TEXT NOT NULL UNIQUE,
          created_ts INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS markets_indicator_rules (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL,
          kind TEXT NOT NULL,
          params_json TEXT NOT NULL DEFAULT '{}',
          enabled INTEGER NOT NULL DEFAULT 1,
          created_ts INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS markets_optimizer_runs (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          status TEXT NOT NULL,
          config_json TEXT NOT NULL DEFAULT '{}',
          summary_json TEXT NOT NULL DEFAULT '{}',
          best_rules_json TEXT NOT NULL DEFAULT '[]',
          generation INTEGER NOT NULL DEFAULT 0,
          best_realized_gain_total REAL NOT NULL DEFAULT 0,
          best_avg_trade_profit REAL NOT NULL DEFAULT 0,
          best_sell_executions INTEGER NOT NULL DEFAULT 0,
          stop_requested INTEGER NOT NULL DEFAULT 0,
          created_ts INTEGER NOT NULL,
          started_ts INTEGER,
          updated_ts INTEGER NOT NULL,
          ended_ts INTEGER
        );

        CREATE TABLE IF NOT EXISTS markets_optimizer_candidates (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          run_id INTEGER NOT NULL,
          generation INTEGER NOT NULL,
          rank_idx INTEGER NOT NULL,
          realized_gain_total REAL NOT NULL DEFAULT 0,
          avg_trade_profit REAL NOT NULL DEFAULT 0,
          sell_executions INTEGER NOT NULL DEFAULT 0,
          open_units INTEGER NOT NULL DEFAULT 0,
          stats_json TEXT NOT NULL DEFAULT '{}',
          rules_json TEXT NOT NULL DEFAULT '[]',
          created_ts INTEGER NOT NULL,
          FOREIGN KEY(run_id) REFERENCES markets_optimizer_runs(id)
        );

        CREATE INDEX IF NOT EXISTS idx_portfolio_equity_log_ts ON portfolio_equity_log(ts);
        CREATE INDEX IF NOT EXISTS idx_markets_watchlist_symbol ON markets_watchlist(symbol);
        CREATE INDEX IF NOT EXISTS idx_markets_indicator_rules_enabled ON markets_indicator_rules(enabled);
        CREATE INDEX IF NOT EXISTS idx_markets_optimizer_runs_status ON markets_optimizer_runs(status);
        CREATE INDEX IF NOT EXISTS idx_markets_optimizer_candidates_run_gen ON markets_optimizer_candidates(run_id, generation, rank_idx);
        """
    )
    conn.commit()

    try:
        run_cols = table_columns(conn, "runs")
        run_column_migrations = {
            "supervisor_pid": "ALTER TABLE runs ADD COLUMN supervisor_pid INTEGER",
            "supervisor_started_ts": "ALTER TABLE runs ADD COLUMN supervisor_started_ts INTEGER",
            "restart_count": "ALTER TABLE runs ADD COLUMN restart_count INTEGER NOT NULL DEFAULT 0",
            "last_restart_ts": "ALTER TABLE runs ADD COLUMN last_restart_ts INTEGER",
            "restart_reason": "ALTER TABLE runs ADD COLUMN restart_reason TEXT",
        }
        for col_name, ddl in run_column_migrations.items():
            if col_name not in run_cols:
                cur.execute(ddl)
        conn.commit()
    except Exception:
        pass

    # Optional migration: old algorithms schema had 'entrypoint'
    try:
        cols = table_columns(conn, "algorithms")
        if "entrypoint" in cols:
            cur.execute("SELECT * FROM algorithms")
            old_rows = cur.fetchall()

            cur.executescript("ALTER TABLE algorithms RENAME TO algorithms_old;")
            conn.commit()

            cur.executescript(
                """
                CREATE TABLE IF NOT EXISTS algorithms (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  name TEXT NOT NULL,
                  base_script_id INTEGER NOT NULL,
                  rulesets_json TEXT NOT NULL,
                  params_json TEXT NOT NULL,
                  max_runtime_min INTEGER NOT NULL DEFAULT 0,
                  restart_on_crash INTEGER NOT NULL DEFAULT 1,
                  log_level TEXT NOT NULL DEFAULT 'INFO',
                  created_ts INTEGER NOT NULL,
                  FOREIGN KEY(base_script_id) REFERENCES base_scripts(id)
                );
                """
            )
            conn.commit()

            for r in old_rows:
                entrypoint = str(r["entrypoint"])
                ep_path = Path(entrypoint)
                if ep_path.is_absolute():
                    try:
                        rel = str(ep_path.relative_to(APP_ROOT))
                    except Exception:
                        rel = entrypoint
                else:
                    rel = entrypoint
                bs_id = _ensure_base_script_row(conn, rel)
                cur.execute(
                    """
                    INSERT INTO algorithms
                    (name, base_script_id, rulesets_json, params_json, max_runtime_min, restart_on_crash, log_level, created_ts)
                    VALUES (?,?,?,?,?,?,?,?)
                    """,
                    (
                        r["name"],
                        bs_id,
                        r["rulesets_json"],
                        r["params_json"],
                        int(r["max_runtime_min"]),
                        int(r["restart_on_crash"]),
                        r["log_level"],
                        int(r["created_ts"]),
                    ),
                )
            conn.commit()
            cur.execute("DROP TABLE IF EXISTS algorithms_old")
            conn.commit()
    except Exception:
        pass

    conn.close()
    _ensure_legacy_schwab_connection()


def _ensure_base_script_row(conn: sqlite3.Connection, rel_path: str) -> int:
    cur = conn.cursor()
    cur.execute("SELECT id FROM base_scripts WHERE path=?", (rel_path,))
    row = cur.fetchone()
    if row:
        return int(row["id"])

    name = _base_script_name(rel_path)
    desc = _base_script_description(rel_path)
    schema = "{}"
    schema_path = (APP_ROOT / rel_path).with_suffix(".schema.json")
    if schema_path.exists():
        try:
            schema = schema_path.read_text(encoding="utf-8")
        except Exception:
            schema = "{}"

    cur.execute(
        "INSERT INTO base_scripts (name, path, description, params_schema_json, created_ts) VALUES (?,?,?,?,?)",
        (name, rel_path, desc, schema, _utc_ts()),
    )
    conn.commit()
    return int(cur.lastrowid)


def _base_script_descriptions() -> dict[str, str]:
    return {
        "scripts/dreadfox.crypto.robinhood.py": (
            "Robinhood crypto momentum/reversion loop that buys when price is above MA20 but below MA78 and MA190 with RSI < 55 and positive RSI derivative (>0.50), and sells when held size is profitable, price is above MA20/78/190, RSI > 69, and RSI derivative turns weak (<0.25). Uses fixed dollar buy/sell sizing (`trade_amount`) plus optional arming stop-loss that liquidates after a pullback from configured gain/trigger thresholds."
        ),
        "scripts/dreadfox.stock.robinhood.py": (
            "Robinhood stock script with MA20/78/190 + RSI derivative rules: BUY requires price > MA20 and < MA78/MA190 with RSI in (30,55) and dRSI > 1; SELL requires held shares, price > MA20/78/190, RSI > 69, dRSI < 1, and price above average cost. Executes market buys, trailing-stop sells using fixed dollar `trailAmount`, and optional arming stop-loss logic."
        ),
        "scripts/dreadfox.stock.schwab.py": (
            "Schwab version of the DreadFox stock calculus (same MA20/78/190 + RSI/dRSI thresholds as Robinhood). BUY/SELL gates match the Robinhood logic; order handling is Schwab-native with market/limit behavior by session, trailing-stop exits in regular hours, and optional arming stop-loss liquidation."
        ),
        "scripts/foxbalance.robinhood.py": (
            "Robinhood portfolio rebalancer that ranks held symbols into LIQ (profit pool) and ACQ (loss pool), computes DreadFox signal gates per symbol (MA20/78/190, RSI 30-55 for buys, RSI>70 for sells, dRSI thresholds), then acts only on top-ranked candidates. Regular-hours LIQ exits use ATR-based trailing stops (2x ATR, cents-rounded with floor), extended-hours use limit orders, and ACQ entries are whole-share buys constrained by effective cash and ranking."
        ),
        "scripts/foxbalance.schwab.py": (
            "Schwab portfolio rebalancer mirroring the FoxBalance LIQ/ACQ ranking workflow with the same DreadFox MA/RSI/dRSI calculus, effective-cash slice logic, and ATR-driven trailing-stop liquidation flow (2x ATR with floor). Uses Schwab order schemas for session-aware limit/market/trailing-stop handling while preserving the same ranking and gating behavior."
        ),
        "scripts/superhexagon.robinhood.py": (
            "Robinhood Superhexagon strategy: BUY when price is above MA20 but below MA78/MA190, RSI is 30-55, RSI derivative > 0.25, and symbol weight stays under the Hexagram cap. SELL when held shares are in profit and price is above MA20/78/190 with RSI > 69 and dRSI < 0.5. Uses ATR-based trailing-stop sell orders (2x ATR, cents-rounded with minimum floor) plus midpoint-style arming stop-loss management."
        ),
        "scripts/superhexagon.schwab.py": (
            "Schwab Superhexagon variant with the same core signal gates as Robinhood (MA20/78/190 + RSI 30-55 + dRSI > 0.25 for buys; RSI>69 + dRSI<0.5 + profitable hold for sells) and the same Hexagram position-cap guard. Sell execution uses Schwab trailing-stop orders with ATR-based trail sizing (2x ATR, minimum floor), with session-aware order behavior and configurable stop-loss arming."
        ),
        "scripts/foxscry.py": (
            "FoxScry rule set on Robinhood: BUY when price is above MA30 but below MA78 and MA190, with positive RSI derivative and positive MA30 derivative, while respecting the Hexagram portfolio cap. SELL when either (A) price is above MA30/78/190 with RSI > 70 and RSI derivative turns negative, or (B) price is above MA78/MA190 and MA30 derivative turns negative. Uses ATR-based trailing-stop exits at 3x ATR (cents-rounded, floored) and midpoint-style arming stop-loss state."
        ),
        "scripts/rokurokubi.options.robinhood.py": (
            "Robinhood covered-call harvester that now uses FoxScry-style stock signal logic (MA30/78/190 + RSI derivative + MA30 derivative) for BUY/HOLD/SELL intent, with optional stock-buying enablement. On SELL intent it prioritizes covered-call harvesting: scans expirations/strikes in target range, enforces breakeven safety, ranks by capped gain, and manages sell-to-open limit repricing. Includes diagnostics (candidate counts, rejects, shortlist) and optional arming stop-loss share liquidation."
        ),
        "scripts/rokurokubi.options.schwab.py": (
            "Schwab covered-call harvester aligned to the Robinhood Rokurokubi logic: FoxScry-style stock calculus for directional intent, optional stock buys, and SELL-side covered-call execution with strike/DTE filtering, breakeven protection, capped-gain ranking, and managed repricing/polling. Uses Schwab option order schemas and keeps the same preview diagnostics and optional arming stop-loss for share liquidation."
        ),
        "scripts/indicatorforge.robinhood.py": (
            "Configurable Robinhood stock engine that evaluates the same custom rule semantics as the Markets scanner (SMA/EMA rules with optional derivative and 'unless' override, RSI threshold-action rules, dRSI rules, and MACD modes). Signal resolution is strict all-rules logic (all SELL true => SELL, else all BUY true => BUY, else HOLD). Supports selectable BUY/SELL order type (market, trailing-stop, or midpoint limit), optional extended-hours midpoint-limit execution, optional day-trade guard, optional portfolio-cap gate, and optional arming stop-loss logic. Robinhood does not provide Schwab-style SEAMLESS overnight routing through robin_stocks."
        ),
        "scripts/entangledtickers.robinhood.py": (
            "Robinhood entangled-pair engine built on IndicatorForge. Primary ticker evaluates the full IndicatorForge rule stack (SMA/EMA with derivative and unless override, RSI, dRSI, MACD); inverse ticker executes the opposite side of the primary resolved signal (primary BUY => inverse SELL, primary SELL => inverse BUY). Keeps the same execution controls (shares-per-trade, trailing-stop mode, optional portfolio-cap rule, optional arming stop-loss logic)."
        ),
        "scripts/indicatorforge.schwab.py": (
            "Configurable Schwab stock engine that matches IndicatorForge rule semantics (SMA/EMA rules with optional derivative and 'unless' override, RSI threshold-action rules, dRSI rules, and MACD modes) with strict all-rules signal resolution (all SELL true => SELL, else all BUY true => BUY, else HOLD). Supports regular-hours execution plus optional extended-hours midpoint-limit execution and optional overnight SEAMLESS execution, with selectable BUY/SELL order type (market, trailing-stop, or midpoint limit), plus optional day-trade guard, optional portfolio-cap gate, and optional arming stop-loss logic."
        ),
        "scripts/entangledtickers.schwab.py": (
            "Schwab entangled-pair engine built on IndicatorForge. Primary ticker evaluates the full IndicatorForge rule stack (SMA/EMA with derivative and unless override, RSI, dRSI, MACD); inverse ticker executes the opposite side of the primary resolved signal (primary BUY => inverse SELL, primary SELL => inverse BUY). Keeps Schwab execution controls including order-type selection, optional day-trade guard, optional portfolio-cap rule, and optional arming stop-loss logic."
        ),
        "scripts/indicatorforge.crypto.robinhood.py": (
            "Configurable Robinhood crypto engine that matches IndicatorForge rule semantics (SMA/EMA rules with optional derivative and 'unless' override, RSI threshold-action rules, dRSI rules, and MACD modes). Signal resolution is strict all-rules logic (all SELL true => SELL, else all BUY true => BUY, else HOLD). Because Robinhood crypto lacks native trailing-stop orders, this variant tracks local trailing BUY and SELL orders and submits normal crypto orders when local trails trigger, with optional arming stop-loss logic."
        ),
    }


def _base_script_names() -> dict[str, str]:
    return {
        "scripts/foxscry.py": "FoxScry",
        "scripts/indicatorforge.robinhood.py": "IndicatorForge (Robinhood)",
        "scripts/entangledtickers.robinhood.py": "EntangledTickers (Robinhood)",
        "scripts/indicatorforge.schwab.py": "IndicatorForge (Schwab)",
        "scripts/entangledtickers.schwab.py": "EntangledTickers (Schwab)",
        "scripts/indicatorforge.crypto.robinhood.py": "IndicatorForge Crypto (Robinhood)",
    }


def _base_script_name(rel_path: str) -> str:
    norm = str(rel_path or "").replace("\\", "/").strip()
    if norm.startswith("app/"):
        norm = norm[len("app/") :]
    key = norm.lower()
    explicit = _base_script_names().get(key)
    if explicit:
        return explicit
    p = Path(norm)
    return p.stem.replace("_", " ").title()


def _base_script_description(rel_path: str) -> str:
    norm = str(rel_path or "").replace("\\", "/").strip()
    if norm.startswith("app/"):
        norm = norm[len("app/") :]
    key = norm.lower()
    return _base_script_descriptions().get(key, "")


def _normalize_script_path(path: str) -> str:
    norm = str(path or "").replace("\\", "/").strip()
    if norm.startswith("app/"):
        norm = norm[len("app/") :]
    return norm.lower()


def _sanitize_algorithm_params_for_script(params_json: str, script_path: str) -> str:
    params = _safe_json(params_json or "{}", default={})
    if not isinstance(params, dict):
        params = {}
    key = _normalize_script_path(script_path)
    if key == "scripts/entangledtickers.robinhood.py":
        # EntangledTickers.Robinhood has not wired overnight routing yet.
        for unsupported_key in (
            "allow_seamless_overnight_orders",
            "allow_all_day_orders",
            "all_day_hours",
            "market_hours",
            "overnight_session",
        ):
            params.pop(unsupported_key, None)
    return json.dumps(params)


def _base_algo_form_defs() -> dict[str, dict[str, Any]]:
    return {
        "scripts/dreadfox.crypto.robinhood.py": {
            "params": [
                {
                    "key": "tickers",
                    "label": "Tickers",
                    "type": "list",
                    "required": True,
                    "placeholder": "BTC, ETH",
                    "help": "Comma-separated crypto tickers.",
                    "normalize": "upper",
                },
                {"key": "trade_amount", "label": "Trade Amount ($)", "type": "number", "default": 10.0, "step": "0.01"},
                {
                    "key": "target_gain_for_stoploss",
                    "label": "Stop-Loss Arming %",
                    "type": "number",
                    "default": 0.5,
                    "step": "0.01",
                },
                {
                    "key": "stoploss_percentage",
                    "label": "Stop-Loss Trigger Percentage",
                    "type": "number",
                    "default": -0.5,
                    "step": "0.01",
                },
                {"key": "sleep_duration", "label": "Sleep Duration (seconds)", "type": "number", "default": 30, "step": "1"},
                {
                    "key": "timeframe",
                    "label": "Timeframe",
                    "type": "select",
                    "default": "10m",
                    "options": [
                        {"value": "5m", "label": "5m"},
                        {"value": "10m", "label": "10m"},
                        {"value": "1h", "label": "1h"},
                        {"value": "1d", "label": "1d"},
                        {"value": "15s", "label": "15s"},
                    ],
                },
                {"key": "enable_sounds", "label": "Enable Sounds", "type": "boolean", "default": False},
            ]
        },
        "scripts/dreadfox.stock.robinhood.py": {
            "params": [
                {
                    "key": "symbols",
                    "label": "Symbols",
                    "type": "list",
                    "required": True,
                    "placeholder": "AAPL, MSFT",
                    "help": "Comma-separated stock tickers.",
                    "normalize": "upper",
                },
                {"key": "sleep_duration", "label": "Sleep Duration (seconds)", "type": "number", "default": 30, "step": "1"},
                {"key": "shares_per_trade", "label": "Shares per Trade", "type": "number", "default": 1, "step": "1"},
                {
                    "key": "trailing_stop_amount",
                    "label": "Trailing Stop Amount ($)",
                    "type": "number",
                    "default": 0.10,
                    "step": "0.01",
                },
                {
                    "key": "target_gain_pct",
                    "label": "Stop-Loss Arming %",
                    "type": "number",
                    "default": 0.5,
                    "step": "0.01",
                },
                {
                    "key": "stop_loss_pct",
                    "label": "Stop-Loss Trigger Percentage",
                    "type": "number",
                    "default": -0.5,
                    "step": "0.01",
                },
                {
                    "key": "timeframe",
                    "label": "Timeframe",
                    "type": "select",
                    "default": "10m",
                    "options": [
                        {"value": "5m", "label": "5m"},
                        {"value": "10m", "label": "10m"},
                        {"value": "1h", "label": "1h"},
                        {"value": "1d", "label": "1d"},
                    ],
                },
            ]
        },
        "scripts/dreadfox.stock.schwab.py": {
            "params": [
                {
                    "key": "symbols",
                    "label": "Symbols",
                    "type": "list",
                    "required": True,
                    "placeholder": "AAPL, MSFT",
                    "help": "Comma-separated stock tickers.",
                    "normalize": "upper",
                },
                {"key": "sleep_duration", "label": "Sleep Duration (seconds)", "type": "number", "default": 30, "step": "1"},
                {"key": "shares_per_trade", "label": "Shares per Trade", "type": "number", "default": 1, "step": "1"},
                {
                    "key": "trailing_stop_amount",
                    "label": "Trailing Stop Amount ($)",
                    "type": "number",
                    "default": 0.10,
                    "step": "0.01",
                },
                {
                    "key": "target_gain_pct",
                    "label": "Stop-Loss Arming %",
                    "type": "number",
                    "default": 0.5,
                    "step": "0.01",
                },
                {
                    "key": "stop_loss_pct",
                    "label": "Stop-Loss Trigger Percentage",
                    "type": "number",
                    "default": -0.5,
                    "step": "0.01",
                },
                {
                    "key": "timeframe",
                    "label": "Timeframe",
                    "type": "select",
                    "default": "10m",
                    "options": [
                        {"value": "5m", "label": "5m"},
                        {"value": "10m", "label": "10m"},
                        {"value": "30m", "label": "30m"},
                        {"value": "1h", "label": "1h"},
                        {"value": "1d", "label": "1d"},
                    ],
                },
                {
                    "key": "include_extended_hours_data",
                    "label": "Include Extended Hours Candles",
                    "type": "boolean",
                    "default": True,
                },
                {
                    "key": "allow_extended_hours_orders",
                    "label": "Allow Extended Hours Trading (limit orders at bid/ask midpoint)",
                    "type": "boolean",
                    "default": False,
                },
            ]
        },
        "scripts/superhexagon.robinhood.py": {
            "params": [
                {
                    "key": "tickers",
                    "label": "Tickers",
                    "type": "list",
                    "required": True,
                    "placeholder": "AAPL, MSFT",
                    "help": "Comma-separated stock tickers.",
                    "normalize": "upper",
                },
                {"key": "num_shares", "label": "Shares per Trade", "type": "number", "default": 1, "step": "1"},
                {
                    "key": "target_gain_for_stoploss",
                    "label": "Stop-Loss Arming %",
                    "type": "number",
                    "default": 0.5,
                    "step": "0.01",
                },
                {
                    "key": "timeframe",
                    "label": "Timeframe",
                    "type": "select",
                    "default": "1h",
                    "options": [
                        {"value": "1m", "label": "1m"},
                        {"value": "5m", "label": "5m"},
                        {"value": "10m", "label": "10m"},
                        {"value": "15m", "label": "15m"},
                        {"value": "1h", "label": "1h"},
                        {"value": "1d", "label": "1d"},
                    ],
                },
                {"key": "sleep_duration", "label": "Sleep Duration (seconds)", "type": "number", "default": 30, "step": "1"},
                {
                    "key": "portfolio_cap_mode",
                    "label": "Portfolio Cap Mode",
                    "type": "select",
                    "default": "divisor_cash_slice",
                    "options": [
                        {"value": "divisor_cash_slice", "label": "Divisor + Cash Slice"},
                        {"value": "percent", "label": "Per-Ticker % Cap"},
                    ],
                },
                {
                    "key": "portfolio_cap_percent_by_symbol",
                    "label": "Per-Ticker Cap Map",
                    "type": "text",
                    "default": "{}",
                },
                {
                    "key": "portfolio_cap_divisor",
                    "label": "Portfolio Cap Divisor (includes cash slice)",
                    "type": "number",
                    "default": 6,
                    "step": "1",
                },
            ]
        },
        "scripts/superhexagon.schwab.py": {
            "params": [
                {
                    "key": "tickers",
                    "label": "Tickers",
                    "type": "list",
                    "required": True,
                    "placeholder": "AAPL, MSFT",
                    "help": "Comma-separated stock tickers.",
                    "normalize": "upper",
                },
                {"key": "num_shares", "label": "Shares per Trade", "type": "number", "default": 1, "step": "1"},
                {
                    "key": "target_gain_for_stoploss",
                    "label": "Stop-Loss Arming %",
                    "type": "number",
                    "default": 0.5,
                    "step": "0.01",
                },
                {
                    "key": "timeframe",
                    "label": "Timeframe",
                    "type": "select",
                    "default": "1h",
                    "options": [
                        {"value": "1m", "label": "1m"},
                        {"value": "5m", "label": "5m"},
                        {"value": "10m", "label": "10m"},
                        {"value": "15m", "label": "15m"},
                        {"value": "1h", "label": "1h"},
                        {"value": "1d", "label": "1d"},
                    ],
                },
                {
                    "key": "include_extended_hours_data",
                    "label": "Include Extended Hours Candles",
                    "type": "boolean",
                    "default": True,
                },
                {
                    "key": "allow_extended_hours_orders",
                    "label": "Allow Extended Hours Trading (limit orders at bid/ask midpoint)",
                    "type": "boolean",
                    "default": False,
                },
                {"key": "sleep_duration", "label": "Sleep Duration (seconds)", "type": "number", "default": 30, "step": "1"},
                {
                    "key": "portfolio_cap_mode",
                    "label": "Portfolio Cap Mode",
                    "type": "select",
                    "default": "divisor_cash_slice",
                    "options": [
                        {"value": "divisor_cash_slice", "label": "Divisor + Cash Slice"},
                        {"value": "percent", "label": "Per-Ticker % Cap"},
                    ],
                },
                {
                    "key": "portfolio_cap_percent_by_symbol",
                    "label": "Per-Ticker Cap Map",
                    "type": "text",
                    "default": "{}",
                },
                {
                    "key": "portfolio_cap_divisor",
                    "label": "Portfolio Cap Divisor (includes cash slice)",
                    "type": "number",
                    "default": 6,
                    "step": "1",
                },
            ]
        },
        "scripts/foxscry.py": {
            "params": [
                {
                    "key": "tickers",
                    "label": "Tickers",
                    "type": "list",
                    "required": True,
                    "placeholder": "AAPL, MSFT",
                    "help": "Comma-separated stock tickers.",
                    "normalize": "upper",
                },
                {"key": "num_shares", "label": "Shares per Trade", "type": "number", "default": 1, "step": "1"},
                {
                    "key": "target_gain_for_stoploss",
                    "label": "Stop-Loss Arming %",
                    "type": "number",
                    "default": 0.5,
                    "step": "0.01",
                },
                {
                    "key": "timeframe",
                    "label": "Timeframe",
                    "type": "select",
                    "default": "1h",
                    "options": [
                        {"value": "1m", "label": "1m"},
                        {"value": "5m", "label": "5m"},
                        {"value": "10m", "label": "10m"},
                        {"value": "15m", "label": "15m"},
                        {"value": "1h", "label": "1h"},
                        {"value": "1d", "label": "1d"},
                    ],
                },
                {"key": "sleep_duration", "label": "Sleep Duration (seconds)", "type": "number", "default": 30, "step": "1"},
                {
                    "key": "portfolio_cap_mode",
                    "label": "Portfolio Cap Mode",
                    "type": "select",
                    "default": "divisor_cash_slice",
                    "options": [
                        {"value": "divisor_cash_slice", "label": "Divisor + Cash Slice"},
                        {"value": "percent", "label": "Per-Ticker % Cap"},
                    ],
                },
                {
                    "key": "portfolio_cap_percent_by_symbol",
                    "label": "Per-Ticker Cap Map",
                    "type": "text",
                    "default": "{}",
                },
                {
                    "key": "portfolio_cap_divisor",
                    "label": "Portfolio Cap Divisor (includes cash slice)",
                    "type": "number",
                    "default": 6,
                    "step": "1",
                },
            ]
        },
        "scripts/foxbalance.robinhood.py": {
            "params": [
                {"key": "enable_ansi_colors", "label": "Enable ANSI Colors", "type": "boolean", "default": True},
                {
                    "key": "timeframe",
                    "label": "Timeframe",
                    "type": "select",
                    "default": "1h",
                    "options": [
                        {"value": "5m", "label": "5m"},
                        {"value": "10m", "label": "10m"},
                        {"value": "1h", "label": "1h"},
                        {"value": "1d", "label": "1d"},
                    ],
                },
                {
                    "key": "shares_per_trade_loop",
                    "label": "Shares per Trade Loop",
                    "type": "number",
                    "default": 1,
                    "step": "1",
                },
                {"key": "trading_enabled", "label": "Trading Enabled", "type": "boolean", "default": False},
                {"key": "top_n", "label": "Top N", "type": "number", "default": 5, "step": "1"},
                {
                    "key": "price_move_trigger_pct",
                    "label": "Price Move Trigger (%)",
                    "type": "number",
                    "default": 0.25,
                    "step": "0.01",
                },
                {
                    "key": "equity_move_trigger_usd",
                    "label": "Equity Move Trigger ($)",
                    "type": "number",
                    "default": 5.0,
                    "step": "0.01",
                },
                {
                    "key": "max_silent_seconds",
                    "label": "Max Silent Seconds",
                    "type": "number",
                    "default": 300,
                    "step": "1",
                },
                {
                    "key": "watch_interval_seconds",
                    "label": "Watch Interval (seconds)",
                    "type": "number",
                    "default": 10,
                    "step": "1",
                },
            ]
        },
        "scripts/foxbalance.schwab.py": {
            "params": [
                {"key": "enable_ansi_colors", "label": "Enable ANSI Colors", "type": "boolean", "default": True},
                {
                    "key": "timeframe",
                    "label": "Timeframe",
                    "type": "select",
                    "default": "30m",
                    "options": [
                        {"value": "1m", "label": "1m"},
                        {"value": "5m", "label": "5m"},
                        {"value": "10m", "label": "10m"},
                        {"value": "15m", "label": "15m"},
                        {"value": "30m", "label": "30m"},
                        {"value": "1d", "label": "1d"},
                        {"value": "1w", "label": "1w"},
                        {"value": "1mo", "label": "1mo"},
                    ],
                },
                {
                    "key": "include_extended_hours_data",
                    "label": "Include Extended Hours Candles",
                    "type": "boolean",
                    "default": True,
                },
                {
                    "key": "allow_extended_hours_orders",
                    "label": "Allow Extended Hours Trading (limit orders at bid/ask midpoint)",
                    "type": "boolean",
                    "default": False,
                },
                {
                    "key": "shares_per_trade_loop",
                    "label": "Shares per Trade Loop",
                    "type": "number",
                    "default": 1,
                    "step": "1",
                },
                {"key": "trading_enabled", "label": "Trading Enabled", "type": "boolean", "default": False},
                {"key": "top_n", "label": "Top N", "type": "number", "default": 5, "step": "1"},
                {
                    "key": "price_move_trigger_pct",
                    "label": "Price Move Trigger (%)",
                    "type": "number",
                    "default": 0.25,
                    "step": "0.01",
                },
                {
                    "key": "equity_move_trigger_usd",
                    "label": "Equity Move Trigger ($)",
                    "type": "number",
                    "default": 5.0,
                    "step": "0.01",
                },
                {
                    "key": "max_silent_seconds",
                    "label": "Max Silent Seconds",
                    "type": "number",
                    "default": 300,
                    "step": "1",
                },
                {
                    "key": "watch_interval_seconds",
                    "label": "Watch Interval (seconds)",
                    "type": "number",
                    "default": 10,
                    "step": "1",
                },
            ]
        },
        "scripts/rokurokubi.options.robinhood.py": {
            "params": [
                {
                    "key": "symbols",
                    "label": "Symbols",
                    "type": "list",
                    "required": True,
                    "placeholder": "AAPL, MSFT",
                    "help": "Comma-separated stock tickers.",
                    "normalize": "upper",
                },
                {"key": "sleep_duration", "label": "Sleep Duration (seconds)", "type": "number", "default": 60, "step": "1"},
                {"key": "shares_per_trade", "label": "Shares per Trade", "type": "number", "default": 1, "step": "1"},
                {
                    "key": "enable_stock_buys",
                    "label": "Enable Stock Buys",
                    "type": "boolean",
                    "default": True,
                },
                {
                    "key": "timeframe",
                    "label": "Timeframe",
                    "type": "select",
                    "default": "10m",
                    "options": [
                        {"label": "5m", "value": "5m"},
                        {"label": "10m", "value": "10m"},
                        {"label": "1h", "value": "1h"},
                        {"label": "1d", "value": "1d"},
                    ],
                },
                {
                    "key": "target_gain_pct",
                    "label": "Stop-Loss Arming %",
                    "type": "number",
                    "default": 0.5,
                    "step": "0.01",
                },
                {
                    "key": "stop_loss_pct",
                    "label": "Stop-Loss Trigger %",
                    "type": "number",
                    "default": -0.5,
                    "step": "0.01",
                },
                {
                    "key": "stoploss_enabled",
                    "label": "Enable Stop-Loss",
                    "type": "boolean",
                    "default": False,
                },
                {
                    "key": "cc_max_dte",
                    "label": "Max DTE",
                    "type": "number",
                    "default": 30,
                    "step": "1",
                },
                {
                    "key": "cc_strike_above",
                    "label": "Strike Range Above Spot ($)",
                    "type": "number",
                    "default": 2,
                    "step": "1",
                },
                {
                    "key": "cc_strike_below",
                    "label": "Strike Range Below Spot ($)",
                    "type": "number",
                    "default": 0,
                    "step": "1",
                },
                {
                    "key": "cc_option_tick",
                    "label": "Option Tick",
                    "type": "number",
                    "default": 0.05,
                    "step": "0.01",
                },
                {
                    "key": "cc_ask_undercut",
                    "label": "Ask Undercut",
                    "type": "number",
                    "default": 0.05,
                    "step": "0.01",
                },
                {
                    "key": "cc_spread_narrow_threshold",
                    "label": "Spread-Narrow Threshold",
                    "type": "number",
                    "default": 0.05,
                    "step": "0.01",
                },
                {
                    "key": "cc_min_bid",
                    "label": "Minimum Bid",
                    "type": "number",
                    "default": 0.05,
                    "step": "0.01",
                },
                {
                    "key": "cc_poll_seconds",
                    "label": "Order Poll Interval (seconds)",
                    "type": "number",
                    "default": 5,
                    "step": "0.5",
                },
                {
                    "key": "cc_time_in_force",
                    "label": "Option Time-in-Force",
                    "type": "select",
                    "default": "gtc",
                    "options": [
                        {"label": "GTC", "value": "gtc"},
                        {"label": "GFD", "value": "gfd"},
                    ],
                },
                {
                    "key": "cc_max_reprices",
                    "label": "Max Reprices (-1 = unlimited)",
                    "type": "number",
                    "default": 10,
                    "step": "1",
                },
                {
                    "key": "cc_order_timeout_seconds",
                    "label": "Order Timeout (seconds)",
                    "type": "number",
                    "default": 900,
                    "step": "1",
                },
            ]
        },
        "scripts/rokurokubi.options.schwab.py": {
            "params": [
                {
                    "key": "symbols",
                    "label": "Symbols",
                    "type": "list",
                    "required": True,
                    "placeholder": "AAPL, MSFT",
                    "help": "Comma-separated stock tickers.",
                    "normalize": "upper",
                },
                {"key": "sleep_duration", "label": "Sleep Duration (seconds)", "type": "number", "default": 60, "step": "1"},
                {"key": "shares_per_trade", "label": "Shares per Trade", "type": "number", "default": 1, "step": "1"},
                {
                    "key": "enable_stock_buys",
                    "label": "Enable Stock Buys",
                    "type": "boolean",
                    "default": True,
                },
                {
                    "key": "timeframe",
                    "label": "Timeframe",
                    "type": "select",
                    "default": "10m",
                    "options": [
                        {"label": "5m", "value": "5m"},
                        {"label": "10m", "value": "10m"},
                        {"label": "1h", "value": "1h"},
                        {"label": "1d", "value": "1d"},
                    ],
                },
                {
                    "key": "include_extended_hours_data",
                    "label": "Include Extended Hours Candles",
                    "type": "boolean",
                    "default": True,
                },
                {
                    "key": "target_gain_pct",
                    "label": "Stop-Loss Arming %",
                    "type": "number",
                    "default": 0.5,
                    "step": "0.01",
                },
                {
                    "key": "stop_loss_pct",
                    "label": "Stop-Loss Trigger %",
                    "type": "number",
                    "default": -0.5,
                    "step": "0.01",
                },
                {
                    "key": "stoploss_enabled",
                    "label": "Enable Stop-Loss",
                    "type": "boolean",
                    "default": False,
                },
                {
                    "key": "cc_max_dte",
                    "label": "Max DTE",
                    "type": "number",
                    "default": 30,
                    "step": "1",
                },
                {
                    "key": "cc_strike_above",
                    "label": "Strike Range Above Spot ($)",
                    "type": "number",
                    "default": 2,
                    "step": "1",
                },
                {
                    "key": "cc_strike_below",
                    "label": "Strike Range Below Spot ($)",
                    "type": "number",
                    "default": 0,
                    "step": "1",
                },
                {
                    "key": "cc_option_tick",
                    "label": "Option Tick",
                    "type": "number",
                    "default": 0.05,
                    "step": "0.01",
                },
                {
                    "key": "cc_ask_undercut",
                    "label": "Ask Undercut",
                    "type": "number",
                    "default": 0.05,
                    "step": "0.01",
                },
                {
                    "key": "cc_spread_narrow_threshold",
                    "label": "Spread-Narrow Threshold",
                    "type": "number",
                    "default": 0.05,
                    "step": "0.01",
                },
                {
                    "key": "cc_min_bid",
                    "label": "Minimum Bid",
                    "type": "number",
                    "default": 0.05,
                    "step": "0.01",
                },
                {
                    "key": "cc_poll_seconds",
                    "label": "Order Poll Interval (seconds)",
                    "type": "number",
                    "default": 5,
                    "step": "0.5",
                },
                {
                    "key": "cc_time_in_force",
                    "label": "Option Time-in-Force",
                    "type": "select",
                    "default": "gtc",
                    "options": [
                        {"label": "GTC", "value": "gtc"},
                        {"label": "GFD", "value": "gfd"},
                    ],
                },
                {
                    "key": "cc_max_reprices",
                    "label": "Max Reprices (-1 = unlimited)",
                    "type": "number",
                    "default": 10,
                    "step": "1",
                },
                {
                    "key": "cc_order_timeout_seconds",
                    "label": "Order Timeout (seconds)",
                    "type": "number",
                    "default": 900,
                    "step": "1",
                },
            ]
        },
        "scripts/indicatorforge.robinhood.py": {
            "params": [
                {
                    "key": "symbols",
                    "label": "Symbols",
                    "type": "list",
                    "required": True,
                    "placeholder": "AAPL, MSFT",
                    "help": "Comma-separated stock tickers.",
                    "normalize": "upper",
                },
                {
                    "key": "timeframe",
                    "label": "Timeframe",
                    "type": "select",
                    "default": "1h",
                    "options": [
                        {"value": "5m", "label": "5m"},
                        {"value": "10m", "label": "10m"},
                        {"value": "1h", "label": "1h"},
                        {"value": "1d", "label": "1d"},
                    ],
                },
                {
                    "key": "include_extended_hours_data",
                    "label": "Include Extended Hours Candles",
                    "type": "boolean",
                    "default": False,
                    "help": "Robinhood extended candles are available through robin_stocks only for day-span history; longer spans merge regular history with same-day extended candles.",
                },
                {
                    "key": "allow_extended_hours_orders",
                    "label": "Allow Robinhood Extended Hours (premarket/after-hours midpoint limit orders)",
                    "type": "boolean",
                    "default": False,
                    "help": "Uses robin_stocks extendedHours=True for eligible stock orders during premarket and after-hours sessions.",
                },
                {
                    "key": "allow_seamless_overnight_orders",
                    "label": "Allow Robinhood Overnight Trading (all-day limit orders)",
                    "type": "boolean",
                    "default": False,
                    "help": "Allows eligible overnight stock orders in the 8 PM-4 AM ET all-day session using Robinhood all_day_hours routing.",
                },
                {"key": "sleep_duration", "label": "Sleep Duration (seconds)", "type": "number", "default": 30, "step": "1"},
                {"key": "shares_per_trade", "label": "Shares per Trade", "type": "number", "default": 1, "step": "1"},
                {
                    "key": "trailing_stop_amount",
                    "label": "Trailing Stop Amount ($)",
                    "type": "number",
                    "default": 0.10,
                    "step": "0.01",
                },
                {
                    "key": "trailing_stop_mode",
                    "label": "Trailing Stop Mode",
                    "type": "select",
                    "default": "fixed",
                    "options": [
                        {"value": "fixed", "label": "Fixed $ Amount"},
                        {"value": "atr", "label": "ATR Multiple"},
                    ],
                },
                {
                    "key": "trailing_stop_atr_mult",
                    "label": "ATR Multiplier",
                    "type": "number",
                    "default": 3.0,
                    "step": "0.1",
                },
                {
                    "key": "buy_order_type",
                    "label": "BUY Order Type",
                    "type": "select",
                    "default": "market",
                    "options": [
                        {"value": "market", "label": "Market"},
                        {"value": "trailing_stop", "label": "Trailing Stop"},
                        {"value": "limit_midpoint", "label": "Limit Midpoint (Bid/Ask)"},
                    ],
                },
                {
                    "key": "sell_order_type",
                    "label": "SELL Order Type",
                    "type": "select",
                    "default": "trailing_stop",
                    "options": [
                        {"value": "market", "label": "Market"},
                        {"value": "trailing_stop", "label": "Trailing Stop"},
                        {"value": "limit_midpoint", "label": "Limit Midpoint (Bid/Ask)"},
                    ],
                },
                {
                    "key": "pivot_preorder_enabled",
                    "label": "Place Limit SELL At Pivot After BUY",
                    "type": "boolean",
                    "default": False,
                    "help": "After a BUY order is accepted, immediately place a one-for-one limit SELL at the selected pivot target above the current price.",
                },
                {
                    "key": "pivot_preorder_profit_enabled",
                    "label": "Place Limit SELL At Profit % After BUY",
                    "type": "boolean",
                    "default": False,
                    "help": "After a BUY order is accepted, immediately place a one-for-one limit SELL at the configured profit percentage.",
                },
                {
                    "key": "pivot_preorder_profit_pct",
                    "label": "Pre-Sale Profit Target %",
                    "type": "number",
                    "default": 0,
                    "step": "0.01",
                    "help": "Optional percent gain target from the estimated held average after the buy. If price is already higher than that basis, targets above current price. When above 0, this target is used before pivot targets.",
                },
                {
                    "key": "pivot_preorder_offset",
                    "label": "Pivot Target Steps Above Price",
                    "type": "number",
                    "default": 1,
                    "step": "0.5",
                    "help": "1 targets the next pivot line above price. 2 targets the second line above price. With half levels enabled, 0.5 targets the next half-pivot level.",
                },
                {
                    "key": "pivot_preorder_include_half_levels",
                    "label": "Use Half-Pivot Target Levels",
                    "type": "boolean",
                    "default": False,
                    "help": "Allows midpoint targets between adjacent S/P/R pivot lines.",
                },
                {
                    "key": "pivot_preorder_fallback_pct",
                    "label": "Pivot Target Fallback %",
                    "type": "number",
                    "default": 0,
                    "step": "0.01",
                    "help": "Optional percent-above-price target when no higher pivot level is available. Use 0 to skip fallback orders.",
                },
                {
                    "key": "stoploss_enabled",
                    "label": "Enable Stop-Loss Logic",
                    "type": "boolean",
                    "default": False,
                },
                {
                    "key": "portfolio_cap_rule_enabled",
                    "label": "Enable Portfolio Cap Rule",
                    "type": "boolean",
                    "default": False,
                },
                {
                    "key": "portfolio_cap_mode",
                    "label": "Portfolio Cap Mode",
                    "type": "select",
                    "default": "divisor_cash_slice",
                    "options": [
                        {"value": "divisor_cash_slice", "label": "Divisor + Cash Slice"},
                        {"value": "percent", "label": "Per-Ticker % Cap"},
                    ],
                },
                {
                    "key": "portfolio_cap_percent_by_symbol",
                    "label": "Per-Ticker Cap Map",
                    "type": "text",
                    "default": "{}",
                },
                {
                    "key": "portfolio_cash_percent",
                    "label": "Cash Position %",
                    "type": "number",
                    "default": 0,
                    "step": "0.01",
                    "help": "Minimum cash allocation to preserve before allowing new BUY orders. 0 keeps the divisor-derived cash slice.",
                },
                {
                    "key": "portfolio_cash_source",
                    "label": "Cash Position Source",
                    "type": "select",
                    "default": "buying_power",
                    "options": [
                        {"value": "buying_power", "label": "Buying Power"},
                        {"value": "cash", "label": "Cash Position"},
                    ],
                    "help": "Select which account value is compared against the Cash Position % target.",
                },
                {
                    "key": "portfolio_cap_divisor",
                    "label": "Portfolio Cap Divisor (includes cash slice)",
                    "type": "number",
                    "default": 6,
                    "step": "1",
                },
                {
                    "key": "target_gain_pct",
                    "label": "Stop-Loss Arming %",
                    "type": "number",
                    "default": 0.5,
                    "step": "0.01",
                },
                {
                    "key": "stop_loss_pct",
                    "label": "Stop-Loss Trigger Percentage",
                    "type": "number",
                    "default": -0.5,
                    "step": "0.01",
                },
                {
                    "key": "indicator_rules_json",
                    "label": "Indicator Rules JSON",
                    "type": "textarea",
                    "placeholder": '[{\"name\":\"MA30\",\"kind\":\"ma\",\"params\":{\"ma_type\":\"sma\",\"length\":30,\"buy_relation\":\"above\",\"sell_relation\":\"below\"}}]',
                    "help": "Per-cryptid indicator rules. Build with the Indicator Rule Builder below or paste valid JSON.",
                },
            ]
        },
        "scripts/entangledtickers.robinhood.py": {
            "params": [
                {
                    "key": "primary_symbol",
                    "label": "Primary Symbol",
                    "type": "text",
                    "required": True,
                    "placeholder": "AAPL",
                    "help": "Ticker used to evaluate BUY/SELL signal from indicator rules.",
                    "normalize": "upper",
                },
                {
                    "key": "inverse_symbol",
                    "label": "Inverse Symbol",
                    "type": "text",
                    "required": True,
                    "placeholder": "MSFT",
                    "help": "Ticker that receives the inverse side of the primary signal.",
                    "normalize": "upper",
                },
                {
                    "key": "timeframe",
                    "label": "Timeframe",
                    "type": "select",
                    "default": "1h",
                    "options": [
                        {"value": "5m", "label": "5m"},
                        {"value": "10m", "label": "10m"},
                        {"value": "1h", "label": "1h"},
                        {"value": "1d", "label": "1d"},
                    ],
                },
                {"key": "sleep_duration", "label": "Sleep Duration (seconds)", "type": "number", "default": 30, "step": "1"},
                {"key": "shares_per_trade", "label": "Shares per Trade", "type": "number", "default": 1, "step": "1"},
                {
                    "key": "trailing_stop_amount",
                    "label": "Trailing Stop Amount ($)",
                    "type": "number",
                    "default": 0.10,
                    "step": "0.01",
                },
                {
                    "key": "trailing_stop_mode",
                    "label": "Trailing Stop Mode",
                    "type": "select",
                    "default": "fixed",
                    "options": [
                        {"value": "fixed", "label": "Fixed $ Amount"},
                        {"value": "atr", "label": "ATR Multiple"},
                    ],
                },
                {
                    "key": "trailing_stop_atr_mult",
                    "label": "ATR Multiplier",
                    "type": "number",
                    "default": 3.0,
                    "step": "0.1",
                },
                {
                    "key": "stoploss_enabled",
                    "label": "Enable Stop-Loss Logic",
                    "type": "boolean",
                    "default": False,
                },
                {
                    "key": "portfolio_cap_rule_enabled",
                    "label": "Enable Portfolio Cap Rule",
                    "type": "boolean",
                    "default": False,
                },
                {
                    "key": "portfolio_cap_mode",
                    "label": "Portfolio Cap Mode",
                    "type": "select",
                    "default": "divisor_cash_slice",
                    "options": [
                        {"value": "divisor_cash_slice", "label": "Divisor + Cash Slice"},
                        {"value": "percent", "label": "Per-Ticker % Cap"},
                    ],
                },
                {
                    "key": "portfolio_cap_percent_by_symbol",
                    "label": "Per-Ticker Cap Map",
                    "type": "text",
                    "default": "{}",
                },
                {
                    "key": "portfolio_cash_percent",
                    "label": "Cash Position %",
                    "type": "number",
                    "default": 0,
                    "step": "0.01",
                    "help": "Minimum cash allocation to preserve before allowing new BUY orders. 0 keeps the divisor-derived cash slice.",
                },
                {
                    "key": "portfolio_cap_divisor",
                    "label": "Portfolio Cap Divisor (includes cash slice)",
                    "type": "number",
                    "default": 6,
                    "step": "1",
                },
                {
                    "key": "target_gain_pct",
                    "label": "Stop-Loss Arming %",
                    "type": "number",
                    "default": 0.5,
                    "step": "0.01",
                },
                {
                    "key": "stop_loss_pct",
                    "label": "Stop-Loss Trigger Percentage",
                    "type": "number",
                    "default": -0.5,
                    "step": "0.01",
                },
                {
                    "key": "indicator_rules_json",
                    "label": "Indicator Rules JSON",
                    "type": "textarea",
                    "placeholder": '[{\"name\":\"MA30\",\"kind\":\"ma\",\"params\":{\"ma_type\":\"sma\",\"length\":30,\"buy_relation\":\"above\",\"sell_relation\":\"below\"}}]',
                    "help": "Per-cryptid indicator rules. Build with the Indicator Rule Builder below or paste valid JSON.",
                },
            ]
        },
        "scripts/indicatorforge.schwab.py": {
            "params": [
                {
                    "key": "symbols",
                    "label": "Symbols",
                    "type": "list",
                    "required": True,
                    "placeholder": "AAPL, MSFT",
                    "help": "Comma-separated stock tickers.",
                    "normalize": "upper",
                },
                {
                    "key": "timeframe",
                    "label": "Timeframe",
                    "type": "select",
                    "default": "10m",
                    "options": [
                        {"value": "1m", "label": "1m"},
                        {"value": "5m", "label": "5m"},
                        {"value": "10m", "label": "10m"},
                        {"value": "15m", "label": "15m"},
                        {"value": "1h", "label": "1h"},
                        {"value": "1d", "label": "1d"},
                    ],
                },
                {
                    "key": "include_extended_hours_data",
                    "label": "Include Extended Hours Candles",
                    "type": "boolean",
                    "default": True,
                },
                {
                    "key": "allow_extended_hours_orders",
                    "label": "Allow Extended Hours Trading (limit orders at bid/ask midpoint)",
                    "type": "boolean",
                    "default": False,
                },
                {
                    "key": "allow_seamless_overnight_orders",
                    "label": "Enable Overnight Session (SEAMLESS)",
                    "type": "boolean",
                    "default": False,
                },
                {"key": "sleep_duration", "label": "Sleep Duration (seconds)", "type": "number", "default": 30, "step": "1"},
                {"key": "shares_per_trade", "label": "Shares per Trade", "type": "number", "default": 1, "step": "1"},
                {
                    "key": "trailing_stop_amount",
                    "label": "Trailing Stop Amount ($)",
                    "type": "number",
                    "default": 0.10,
                    "step": "0.01",
                },
                {
                    "key": "trailing_stop_mode",
                    "label": "Trailing Stop Mode",
                    "type": "select",
                    "default": "fixed",
                    "options": [
                        {"value": "fixed", "label": "Fixed $ Amount"},
                        {"value": "atr", "label": "ATR Multiple"},
                    ],
                },
                {
                    "key": "trailing_stop_atr_mult",
                    "label": "ATR Multiplier",
                    "type": "number",
                    "default": 3.0,
                    "step": "0.1",
                },
                {
                    "key": "buy_order_type",
                    "label": "BUY Order Type",
                    "type": "select",
                    "default": "market",
                    "options": [
                        {"value": "market", "label": "Market"},
                        {"value": "trailing_stop", "label": "Trailing Stop"},
                        {"value": "limit_midpoint", "label": "Limit Midpoint (Bid/Ask)"},
                    ],
                },
                {
                    "key": "sell_order_type",
                    "label": "SELL Order Type",
                    "type": "select",
                    "default": "trailing_stop",
                    "options": [
                        {"value": "market", "label": "Market"},
                        {"value": "trailing_stop", "label": "Trailing Stop"},
                        {"value": "limit_midpoint", "label": "Limit Midpoint (Bid/Ask)"},
                    ],
                },
                {
                    "key": "pivot_preorder_enabled",
                    "label": "Place Limit SELL At Pivot After BUY",
                    "type": "boolean",
                    "default": False,
                    "help": "After a BUY order is accepted, immediately place a one-for-one limit SELL at the selected pivot target above the current price.",
                },
                {
                    "key": "pivot_preorder_profit_enabled",
                    "label": "Place Limit SELL At Profit % After BUY",
                    "type": "boolean",
                    "default": False,
                    "help": "After a BUY order is accepted, immediately place a one-for-one limit SELL at the configured profit percentage.",
                },
                {
                    "key": "pivot_preorder_profit_pct",
                    "label": "Pre-Sale Profit Target %",
                    "type": "number",
                    "default": 0,
                    "step": "0.01",
                    "help": "Optional percent gain target from the estimated held average after the buy. If price is already higher than that basis, targets above current price. When above 0, this target is used before pivot targets.",
                },
                {
                    "key": "pivot_preorder_offset",
                    "label": "Pivot Target Steps Above Price",
                    "type": "number",
                    "default": 1,
                    "step": "0.5",
                    "help": "1 targets the next pivot line above price. 2 targets the second line above price. With half levels enabled, 0.5 targets the next half-pivot level.",
                },
                {
                    "key": "pivot_preorder_include_half_levels",
                    "label": "Use Half-Pivot Target Levels",
                    "type": "boolean",
                    "default": False,
                    "help": "Allows midpoint targets between adjacent S/P/R pivot lines.",
                },
                {
                    "key": "pivot_preorder_fallback_pct",
                    "label": "Pivot Target Fallback %",
                    "type": "number",
                    "default": 0,
                    "step": "0.01",
                    "help": "Optional percent-above-price target when no higher pivot level is available. Use 0 to skip fallback orders.",
                },
                {
                    "key": "stoploss_enabled",
                    "label": "Enable Stop-Loss Logic",
                    "type": "boolean",
                    "default": False,
                },
                {
                    "key": "portfolio_cap_rule_enabled",
                    "label": "Enable Portfolio Cap Rule",
                    "type": "boolean",
                    "default": False,
                },
                {
                    "key": "portfolio_cap_mode",
                    "label": "Portfolio Cap Mode",
                    "type": "select",
                    "default": "divisor_cash_slice",
                    "options": [
                        {"value": "divisor_cash_slice", "label": "Divisor + Cash Slice"},
                        {"value": "percent", "label": "Per-Ticker % Cap"},
                    ],
                },
                {
                    "key": "portfolio_cap_percent_by_symbol",
                    "label": "Per-Ticker Cap Map",
                    "type": "text",
                    "default": "{}",
                },
                {
                    "key": "portfolio_cash_percent",
                    "label": "Cash Position %",
                    "type": "number",
                    "default": 0,
                    "step": "0.01",
                    "help": "Minimum cash allocation to preserve before allowing new BUY orders. 0 keeps the divisor-derived cash slice.",
                },
                {
                    "key": "portfolio_cap_divisor",
                    "label": "Portfolio Cap Divisor (includes cash slice)",
                    "type": "number",
                    "default": 6,
                    "step": "1",
                },
                {
                    "key": "target_gain_pct",
                    "label": "Stop-Loss Arming %",
                    "type": "number",
                    "default": 0.5,
                    "step": "0.01",
                },
                {
                    "key": "stop_loss_pct",
                    "label": "Stop-Loss Trigger Percentage",
                    "type": "number",
                    "default": -0.5,
                    "step": "0.01",
                },
                {
                    "key": "indicator_rules_json",
                    "label": "Indicator Rules JSON",
                    "type": "textarea",
                    "placeholder": '[{\"name\":\"MA30\",\"kind\":\"ma\",\"params\":{\"ma_type\":\"sma\",\"length\":30,\"buy_relation\":\"above\",\"sell_relation\":\"below\"}}]',
                    "help": "Per-cryptid indicator rules. Build with the Indicator Rule Builder below or paste valid JSON.",
                },
            ]
        },
        "scripts/entangledtickers.schwab.py": {
            "params": [
                {
                    "key": "primary_symbol",
                    "label": "Primary Symbol",
                    "type": "text",
                    "required": True,
                    "placeholder": "AAPL",
                    "help": "Ticker used to evaluate BUY/SELL signal from indicator rules.",
                    "normalize": "upper",
                },
                {
                    "key": "inverse_symbol",
                    "label": "Inverse Symbol",
                    "type": "text",
                    "required": True,
                    "placeholder": "MSFT",
                    "help": "Ticker that receives the inverse side of the primary signal.",
                    "normalize": "upper",
                },
                {
                    "key": "timeframe",
                    "label": "Timeframe",
                    "type": "select",
                    "default": "30m",
                    "options": [
                        {"value": "1m", "label": "1m"},
                        {"value": "5m", "label": "5m"},
                        {"value": "10m", "label": "10m"},
                        {"value": "15m", "label": "15m"},
                        {"value": "30m", "label": "30m"},
                        {"value": "1h", "label": "1h"},
                        {"value": "1d", "label": "1d"},
                    ],
                },
                {
                    "key": "include_extended_hours_data",
                    "label": "Include Extended Hours Candles",
                    "type": "boolean",
                    "default": True,
                },
                {
                    "key": "allow_extended_hours_orders",
                    "label": "Allow Extended Hours Trading (limit orders at bid/ask midpoint)",
                    "type": "boolean",
                    "default": False,
                },
                {"key": "sleep_duration", "label": "Sleep Duration (seconds)", "type": "number", "default": 30, "step": "1"},
                {"key": "shares_per_trade", "label": "Shares per Trade", "type": "number", "default": 1, "step": "1"},
                {
                    "key": "trailing_stop_amount",
                    "label": "Trailing Stop Amount ($)",
                    "type": "number",
                    "default": 0.10,
                    "step": "0.01",
                },
                {
                    "key": "trailing_stop_mode",
                    "label": "Trailing Stop Mode",
                    "type": "select",
                    "default": "fixed",
                    "options": [
                        {"value": "fixed", "label": "Fixed $ Amount"},
                        {"value": "atr", "label": "ATR Multiple"},
                    ],
                },
                {
                    "key": "trailing_stop_atr_mult",
                    "label": "ATR Multiplier",
                    "type": "number",
                    "default": 3.0,
                    "step": "0.1",
                },
                {
                    "key": "buy_order_type",
                    "label": "BUY Order Type",
                    "type": "select",
                    "default": "market",
                    "options": [
                        {"value": "market", "label": "Market"},
                        {"value": "trailing_stop", "label": "Trailing Stop"},
                        {"value": "limit_midpoint", "label": "Limit Midpoint (Bid/Ask)"},
                    ],
                },
                {
                    "key": "sell_order_type",
                    "label": "SELL Order Type",
                    "type": "select",
                    "default": "trailing_stop",
                    "options": [
                        {"value": "market", "label": "Market"},
                        {"value": "trailing_stop", "label": "Trailing Stop"},
                        {"value": "limit_midpoint", "label": "Limit Midpoint (Bid/Ask)"},
                    ],
                },
                {
                    "key": "stoploss_enabled",
                    "label": "Enable Stop-Loss Logic",
                    "type": "boolean",
                    "default": False,
                },
                {
                    "key": "portfolio_cap_rule_enabled",
                    "label": "Enable Portfolio Cap Rule",
                    "type": "boolean",
                    "default": False,
                },
                {
                    "key": "portfolio_cap_mode",
                    "label": "Portfolio Cap Mode",
                    "type": "select",
                    "default": "divisor_cash_slice",
                    "options": [
                        {"value": "divisor_cash_slice", "label": "Divisor + Cash Slice"},
                        {"value": "percent", "label": "Per-Ticker % Cap"},
                    ],
                },
                {
                    "key": "portfolio_cap_percent_by_symbol",
                    "label": "Per-Ticker Cap Map",
                    "type": "text",
                    "default": "{}",
                },
                {
                    "key": "portfolio_cash_percent",
                    "label": "Cash Position %",
                    "type": "number",
                    "default": 0,
                    "step": "0.01",
                    "help": "Minimum cash allocation to preserve before allowing new BUY orders. 0 keeps the divisor-derived cash slice.",
                },
                {
                    "key": "portfolio_cap_divisor",
                    "label": "Portfolio Cap Divisor (includes cash slice)",
                    "type": "number",
                    "default": 6,
                    "step": "1",
                },
                {
                    "key": "target_gain_pct",
                    "label": "Stop-Loss Arming %",
                    "type": "number",
                    "default": 0.5,
                    "step": "0.01",
                },
                {
                    "key": "stop_loss_pct",
                    "label": "Stop-Loss Trigger Percentage",
                    "type": "number",
                    "default": -0.5,
                    "step": "0.01",
                },
                {
                    "key": "indicator_rules_json",
                    "label": "Indicator Rules JSON",
                    "type": "textarea",
                    "placeholder": '[{\"name\":\"MA30\",\"kind\":\"ma\",\"params\":{\"ma_type\":\"sma\",\"length\":30,\"buy_relation\":\"above\",\"sell_relation\":\"below\"}}]',
                    "help": "Per-cryptid indicator rules. Build with the Indicator Rule Builder below or paste valid JSON.",
                },
            ]
        },
        "scripts/indicatorforge.crypto.robinhood.py": {
            "params": [
                {
                    "key": "symbols",
                    "label": "Symbols",
                    "type": "list",
                    "required": True,
                    "placeholder": "BTC, ETH",
                    "help": "Comma-separated crypto tickers.",
                    "normalize": "upper",
                },
                {
                    "key": "timeframe",
                    "label": "Timeframe",
                    "type": "select",
                    "default": "1h",
                    "options": [
                        {"value": "5m", "label": "5m"},
                        {"value": "10m", "label": "10m"},
                        {"value": "1h", "label": "1h"},
                        {"value": "1d", "label": "1d"},
                    ],
                },
                {"key": "sleep_duration", "label": "Sleep Duration (seconds)", "type": "number", "default": 30, "step": "1"},
                {
                    "key": "trade_amount",
                    "label": "Trade Amount ($)",
                    "type": "number",
                    "default": 10.0,
                    "step": "0.01",
                },
                {
                    "key": "trailing_stop_amount",
                    "label": "Trailing Stop Amount ($)",
                    "type": "number",
                    "default": 0.10,
                    "step": "0.01",
                },
                {
                    "key": "trailing_stop_mode",
                    "label": "Trailing Stop Mode",
                    "type": "select",
                    "default": "fixed",
                    "options": [
                        {"value": "fixed", "label": "Fixed $ Amount"},
                        {"value": "atr", "label": "ATR Multiple"},
                    ],
                },
                {
                    "key": "trailing_stop_atr_mult",
                    "label": "ATR Multiplier",
                    "type": "number",
                    "default": 3.0,
                    "step": "0.1",
                },
                {
                    "key": "buy_order_type",
                    "label": "BUY Order Type",
                    "type": "select",
                    "default": "local_trailing",
                    "options": [
                        {"value": "market", "label": "Market"},
                        {"value": "local_trailing", "label": "Local Trailing"},
                    ],
                },
                {
                    "key": "sell_order_type",
                    "label": "SELL Order Type",
                    "type": "select",
                    "default": "local_trailing",
                    "options": [
                        {"value": "market", "label": "Market"},
                        {"value": "local_trailing", "label": "Local Trailing"},
                    ],
                },
                {
                    "key": "stoploss_enabled",
                    "label": "Enable Stop-Loss Logic",
                    "type": "boolean",
                    "default": False,
                },
                {
                    "key": "target_gain_pct",
                    "label": "Stop-Loss Arming %",
                    "type": "number",
                    "default": 0.5,
                    "step": "0.01",
                },
                {
                    "key": "stop_loss_pct",
                    "label": "Stop-Loss Trigger Percentage",
                    "type": "number",
                    "default": -0.5,
                    "step": "0.01",
                },
                {
                    "key": "portfolio_cap_rule_enabled",
                    "label": "Enable Portfolio Cap Rule",
                    "type": "boolean",
                    "default": False,
                },
                {
                    "key": "portfolio_cap_mode",
                    "label": "Portfolio Cap Mode",
                    "type": "select",
                    "default": "divisor_cash_slice",
                    "options": [
                        {"value": "divisor_cash_slice", "label": "Divisor + Cash Slice"},
                        {"value": "percent", "label": "Per-Ticker % Cap"},
                    ],
                },
                {
                    "key": "portfolio_cap_percent_by_symbol",
                    "label": "Per-Ticker Cap Map",
                    "type": "text",
                    "default": "{}",
                },
                {
                    "key": "portfolio_cash_percent",
                    "label": "Cash Position %",
                    "type": "number",
                    "default": 0,
                    "step": "0.01",
                    "help": "Minimum cash allocation to preserve before allowing new BUY orders. 0 keeps the divisor-derived cash slice.",
                },
                {
                    "key": "portfolio_cap_divisor",
                    "label": "Portfolio Cap Divisor (includes cash slice)",
                    "type": "number",
                    "default": 6,
                    "step": "1",
                },
                {
                    "key": "indicator_rules_json",
                    "label": "Indicator Rules JSON",
                    "type": "textarea",
                    "placeholder": '[{\"name\":\"MA30\",\"kind\":\"ma\",\"params\":{\"ma_type\":\"sma\",\"length\":30,\"buy_relation\":\"above\",\"sell_relation\":\"below\"}}]',
                    "help": "Per-cryptid indicator rules. Build with the Indicator Rule Builder below or paste valid JSON.",
                },
            ]
        },
    }


def build_base_algo_form_defs(scripts: list[dict[str, Any]]) -> dict[str, Any]:
    defs_by_path = _base_algo_form_defs()
    out: dict[str, Any] = {}
    for s in scripts:
        path_key = _normalize_script_path(str(s.get("path", "")))
        base_def = defs_by_path.get(path_key, {"params": []})
        out[str(s["id"])] = {
            "id": s["id"],
            "name": s.get("name") or "",
            "path": s.get("path") or "",
            "description": s.get("description") or "",
            "params": base_def.get("params", []),
        }
    return out


def discover_base_scripts() -> None:
    ensure_dirs()
    conn = db()
    cur = conn.cursor()
    found: list[str] = []
    defs_by_path = _base_algo_form_defs()
    for f in sorted(SCRIPTS_DIR.glob("*.py")):
        rel = str(f.relative_to(APP_ROOT))
        found.append(rel)
        name = _base_script_name(rel)
        desc = _base_script_description(rel)
        schema_text = "{}"
        base_def = defs_by_path.get(_normalize_script_path(rel))
        if isinstance(base_def, dict):
            try:
                schema_text = json.dumps(base_def)
            except Exception:
                schema_text = "{}"
        else:
            schema_path = f.with_suffix(".schema.json")
            if schema_path.exists():
                try:
                    schema_text = schema_path.read_text(encoding="utf-8")
                except Exception:
                    schema_text = "{}"
        cur.execute("SELECT name, description, params_schema_json FROM base_scripts WHERE path=?", (rel,))
        row = cur.fetchone()
        if row:
            current_name = str(row["name"] or "")
            current_desc = str(row["description"] or "")
            current_schema = str(row["params_schema_json"] or "")
            if name and current_name != name:
                cur.execute("UPDATE base_scripts SET name=? WHERE path=?", (name, rel))
            if desc and current_desc != desc:
                cur.execute("UPDATE base_scripts SET description=? WHERE path=?", (desc, rel))
            if schema_text and current_schema != schema_text:
                cur.execute("UPDATE base_scripts SET params_schema_json=? WHERE path=?", (schema_text, rel))
            continue

        cur.execute(
            "INSERT INTO base_scripts (name, path, description, params_schema_json, created_ts) VALUES (?,?,?,?,?)",
            (name, rel, desc, schema_text, _utc_ts()),
        )

    # Prune base scripts that no longer exist on disk.
    keep = set(found)
    cur.execute("SELECT id, path FROM base_scripts")
    rows = cur.fetchall()
    stale_ids = [int(r["id"]) for r in rows if str(r["path"]) not in keep]
    if stale_ids:
        for i in range(0, len(stale_ids), 900):
            chunk = stale_ids[i : i + 900]
            cur.execute(
                f"DELETE FROM base_scripts WHERE id IN ({','.join(['?'] * len(chunk))})",
                chunk,
            )
    conn.commit()
    conn.close()


def get_base_scripts() -> list[dict[str, Any]]:
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT id, name, path, description, params_schema_json FROM base_scripts ORDER BY name ASC")
    rows = cur.fetchall()
    conn.close()

    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": int(r["id"]),
                "name": r["name"],
                "path": r["path"],
                "description": r["description"] or "",
                "params_schema": _safe_json(r["params_schema_json"], default={}),
            }
        )
    return out


# =========================
# Broker Connections (DB)
# =========================
def list_broker_connections() -> list[dict[str, Any]]:
    conn = db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, broker, label, status, metadata_json, created_ts, updated_ts FROM broker_connections ORDER BY id DESC"
    )
    rows = cur.fetchall()
    conn.close()

    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": int(r["id"]),
                "broker": r["broker"],
                "label": r["label"],
                "status": r["status"],
                "metadata": _safe_json(r["metadata_json"], default={}),
                "created_ts": int(r["created_ts"]),
                "updated_ts": int(r["updated_ts"]),
            }
        )
    return out


def upsert_broker_connection(
    *,
    broker: str,
    label: str,
    status: str,
    metadata: dict[str, Any] | None = None,
    secrets_json: dict[str, Any] | None = None,
    connection_id: int | None = None,
) -> int:
    conn = db()
    cur = conn.cursor()
    now = _utc_ts()
    metadata_json = json.dumps(metadata or {})
    secrets_txt = json.dumps(secrets_json or {})

    if connection_id is None:
        cur.execute(
            """
            INSERT INTO broker_connections (broker, label, status, metadata_json, secrets_json, created_ts, updated_ts)
            VALUES (?,?,?,?,?,?,?)
            """,
            (broker, label, status, metadata_json, secrets_txt, now, now),
        )
        conn.commit()
        cid = int(cur.lastrowid)
        conn.close()
        return cid

    cur.execute(
        """
        UPDATE broker_connections
        SET broker=?, label=?, status=?, metadata_json=?, secrets_json=?, updated_ts=?
        WHERE id=?
        """,
        (broker, label, status, metadata_json, secrets_txt, now, int(connection_id)),
    )
    conn.commit()
    conn.close()
    return int(connection_id)


def delete_broker_connection(connection_id: int) -> None:
    conn = db()
    cur = conn.cursor()
    cur.execute("DELETE FROM broker_connections WHERE id=?", (int(connection_id),))
    conn.commit()
    conn.close()


def _ensure_legacy_schwab_connection() -> None:
    """
    If TOKEN_PATH exists, ensure a corresponding broker_connections row exists/updated.
    This preserves backwards-compatibility with the older "single Schwab token file" approach.
    """
    if not _legacy_schwab_import_enabled():
        return
    tok = load_token()
    if not tok:
        return

    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM broker_connections WHERE broker='schwab' ORDER BY id DESC LIMIT 1")
    row = cur.fetchone()
    now = _utc_ts()

    metadata = {
        "storage": "file",
        "token_path": str(TOKEN_PATH),
        "obtained_at": tok.get("obtained_at"),
    }

    if row:
        cur.execute(
            "UPDATE broker_connections SET label=?, status=?, metadata_json=?, updated_ts=? WHERE id=?",
            ("Schwab", "connected", json.dumps(metadata), now, int(row["id"])),
        )
    else:
        cur.execute(
            """
            INSERT INTO broker_connections (broker, label, status, metadata_json, secrets_json, created_ts, updated_ts)
            VALUES (?,?,?,?,?,?,?)
            """,
            ("schwab", "Schwab", "connected", json.dumps(metadata), "{}", now, now),
        )

    conn.commit()
    conn.close()


# =========================
# Markets Helpers
# =========================
def _clean_symbol(raw: str) -> str:
    s = re.sub(r"[^A-Za-z0-9.\-_]", "", str(raw or "").upper()).strip()
    return s[:12]


def _clean_symbol_list(raw: Any) -> list[str]:
    syms: list[str] = []
    # Users often paste table/header text into the Symbols box. Treat common
    # separators and labels defensively so "SYMBOLS\tSIVR" becomes "SIVR".
    for token in re.split(r"[\s,;|]+", str(raw or "")):
        s = _clean_symbol(token)
        if not s or s in {"SYMBOL", "SYMBOLS", "TICKER", "TICKERS"}:
            continue
        if s not in syms:
            syms.append(s)
    return syms


def _list_markets_watchlist() -> list[str]:
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT symbol FROM markets_watchlist ORDER BY symbol ASC")
    rows = cur.fetchall()
    conn.close()
    return [str(r["symbol"]) for r in rows if str(r["symbol"]).strip()]


def _add_markets_symbol(symbol: str) -> bool:
    sym = _clean_symbol(symbol)
    if not sym:
        return False
    conn = db()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO markets_watchlist (symbol, created_ts) VALUES (?, ?)",
        (sym, _utc_ts()),
    )
    conn.commit()
    conn.close()
    return True


def _remove_markets_symbol(symbol: str) -> None:
    sym = _clean_symbol(symbol)
    if not sym:
        return
    conn = db()
    cur = conn.cursor()
    cur.execute("DELETE FROM markets_watchlist WHERE symbol=?", (sym,))
    conn.commit()
    conn.close()


def _to_float_opt(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def _to_int_opt(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None


def _list_indicator_rules(*, enabled_only: bool = False) -> list[dict[str, Any]]:
    conn = db()
    cur = conn.cursor()
    if enabled_only:
        cur.execute("SELECT * FROM markets_indicator_rules WHERE enabled=1 ORDER BY id ASC")
    else:
        cur.execute("SELECT * FROM markets_indicator_rules ORDER BY id ASC")
    rows = cur.fetchall()
    conn.close()
    out: list[dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": int(r["id"]),
                "name": str(r["name"] or ""),
                "kind": str(r["kind"] or ""),
                "timeframe": _rule_timeframe({"params": _safe_json(str(r["params_json"] or "{}"), default={})}, ""),
                "enabled": bool(int(r["enabled"] or 0)),
                "params": _safe_json(str(r["params_json"] or "{}"), default={}),
            }
        )
    return out


def _new_indicator_rule_id() -> str:
    return f"ifr_{uuid4().hex[:16]}"


def _ensure_saved_indicator_rules_have_ids() -> None:
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT id, params_json FROM markets_indicator_rules ORDER BY id ASC")
    rows = cur.fetchall()
    changed = 0
    for row in rows:
        params = _safe_json(str(row["params_json"] or "{}"), default={})
        if not isinstance(params, dict):
            params = {}
        rid = str(params.get("rule_id") or "").strip()
        if rid:
            continue
        params["rule_id"] = _new_indicator_rule_id()
        cur.execute(
            "UPDATE markets_indicator_rules SET params_json=? WHERE id=?",
            (json.dumps(params), int(row["id"])),
        )
        changed += 1
    if changed:
        conn.commit()
    conn.close()


def _saved_indicator_rule_target_entries(rules: Optional[list[dict[str, Any]]] = None) -> list[dict[str, str]]:
    src = rules if isinstance(rules, list) else _list_indicator_rules(enabled_only=False)
    out: list[dict[str, str]] = []
    for entry in _indicator_runtime_rule_entries(src):
        rid = str(entry.get("rule_id") or "").strip()
        if not rid:
            continue
        idx = int(entry.get("index") or 0) + 1
        name = str(entry.get("name") or "Rule").strip() or "Rule"
        kind = str(entry.get("kind") or "").strip().lower()
        display_kind = str(entry.get("display_kind") or kind.upper() or "RULE").strip() or "RULE"
        timeframe = _rule_timeframe(entry.get("rule") if isinstance(entry.get("rule"), dict) else {}, "")
        tf_label = f" [{timeframe}]" if timeframe else ""
        out.append(
            {
                "id": rid,
                "label": f"#{idx} {display_kind}{tf_label} - {name}",
                "kind": kind,
            }
        )
    return out


def _add_indicator_rule(name: str, kind: str, params: dict[str, Any], *, timeframe: str = "") -> None:
    nm = str(name or "").strip() or kind.upper()
    kd = str(kind or "").strip().lower()
    pr = dict(params or {}) if isinstance(params, dict) else {}
    if kd in ("bollinger", "bollinger_bands"):
        kd = "bb"
    elif kd in ("ichimoku", "ichimoku_cloud", "ichi"):
        kd = "ichimoku"
    elif kd in ("ttm", "ttm_squeeze", "squeeze_momentum"):
        kd = "ttm"
    elif kd in ("roc", "rate_of_change"):
        kd = "roc"
    elif kd in ("sar", "psar", "parabolic_sar", "parabolic"):
        kd = "sar"
    if kd not in ("ma", "ema", "rsi", "rsi_d", "macd", "heikin_ashi", "ha", "bb", "ichimoku", "ttm", "roc", "sar"):
        return
    if not str(pr.get("rule_id") or "").strip():
        pr["rule_id"] = _new_indicator_rule_id()
    tf = _normalize_indicator_rule_timeframe(timeframe or pr.get("timeframe"), default="")
    if tf:
        pr["timeframe"] = tf
    conn = db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO markets_indicator_rules (name, kind, params_json, enabled, created_ts) VALUES (?,?,?,?,?)",
        (nm, kd, json.dumps(pr), 1, _utc_ts()),
    )
    conn.commit()
    conn.close()


def _toggle_indicator_rule(rule_id: int) -> None:
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT enabled FROM markets_indicator_rules WHERE id=?", (int(rule_id),))
    row = cur.fetchone()
    if not row:
        conn.close()
        return
    new_val = 0 if int(row["enabled"] or 0) else 1
    cur.execute("UPDATE markets_indicator_rules SET enabled=? WHERE id=?", (new_val, int(rule_id)))
    conn.commit()
    conn.close()


def _delete_indicator_rule(rule_id: int) -> None:
    conn = db()
    cur = conn.cursor()
    cur.execute("DELETE FROM markets_indicator_rules WHERE id=?", (int(rule_id),))
    conn.commit()
    conn.close()


def _default_indicator_rules_if_empty() -> None:
    rules = _list_indicator_rules(enabled_only=False)
    if rules:
        return
    _add_indicator_rule(
        "MA30 Core",
        "ma",
        {
            "length": 30,
            "buy_relation": "above",
            "sell_relation": "above",
            "track_derivative": 1,
            "buy_derivative_min": 0.0,
            "sell_derivative_max": 0.0,
        },
    )
    _add_indicator_rule("MA78 Guard", "ma", {"length": 78, "buy_relation": "below", "sell_relation": "above"})
    _add_indicator_rule("MA190 Trend", "ma", {"length": 190, "buy_relation": "below", "sell_relation": "above"})
    _add_indicator_rule(
        "RSI Bands",
        "rsi",
        {
            "oversold": 30.0,
            "oversold_relation": "below",
            "oversold_action": "buy",
            "overbought": 70.0,
            "overbought_relation": "above",
            "overbought_action": "sell",
        },
    )
    _add_indicator_rule("RSI Derivative", "rsi_d", {"buy_above": 0.0, "sell_below": 0.0})


def _markets_timeframe(interval_key: str) -> tuple[str, str]:
    tf = str(interval_key or "1h").strip().lower()
    mapping = {
        "1m": ("1minute", "day"),
        "5m": ("5minute", "week"),
        "10m": ("10minute", "week"),
        "15m": ("15minute", "week"),
        "30m": ("30minute", "month"),
        "1h": ("hour", "3month"),
        "1d": ("day", "year"),
    }
    return mapping.get(tf, ("hour", "3month"))


_INDICATOR_RULE_TIMEFRAMES: tuple[str, ...] = ("1m", "5m", "10m", "15m", "30m", "1h", "1d")


def _normalize_indicator_rule_timeframe(value: Any, *, default: str = "1h") -> str:
    txt = str(value or "").strip().lower()
    aliases = {
        "1min": "1m",
        "1minute": "1m",
        "5min": "5m",
        "5minute": "5m",
        "10min": "10m",
        "10minute": "10m",
        "15min": "15m",
        "15minute": "15m",
        "30min": "30m",
        "30minute": "30m",
        "60m": "1h",
        "60min": "1h",
        "1hr": "1h",
        "1hour": "1h",
        "hour": "1h",
        "daily": "1d",
        "day": "1d",
    }
    txt = aliases.get(txt, txt)
    if txt in _INDICATOR_RULE_TIMEFRAMES:
        return txt
    fallback = str(default or "").strip().lower()
    fallback = aliases.get(fallback, fallback)
    if fallback in _INDICATOR_RULE_TIMEFRAMES:
        return fallback
    return "" if default in (None, "") else "1h"


def _rule_timeframe(rule: dict[str, Any], default_timeframe: str = "1h") -> str:
    params = rule.get("params") if isinstance(rule.get("params"), dict) else {}
    explicit = rule.get("timeframe")
    if explicit in (None, "", "None"):
        explicit = params.get("timeframe")
    if explicit in (None, "", "None"):
        explicit = params.get("rule_timeframe")
    return _normalize_indicator_rule_timeframe(explicit, default=default_timeframe)


def _rules_with_default_timeframe(rules: list[dict[str, Any]], default_timeframe: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        r = dict(rule)
        r["timeframe"] = _rule_timeframe(r, default_timeframe)
        out.append(r)
    return out


def _rules_by_timeframe(rules: list[dict[str, Any]], default_timeframe: str) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for rule in _rules_with_default_timeframe(rules, default_timeframe):
        tf = _rule_timeframe(rule, default_timeframe)
        grouped.setdefault(tf, []).append(rule)
    return grouped


def _normalize_market_source_hint(broker_hint: str) -> str:
    hint = str(broker_hint or "robinhood").strip().lower()
    if hint in ("robinhood_crypto", "crypto", "crypto_robinhood", "robinhood-crypto"):
        return "robinhood_crypto"
    if hint == "schwab":
        return "schwab"
    return "robinhood"


def _is_robinhood_crypto_source(broker_hint: str) -> bool:
    return _normalize_market_source_hint(broker_hint) == "robinhood_crypto"


def _market_source_label(source_hint: str, *, include_extended: bool = False) -> str:
    source = _normalize_market_source_hint(source_hint)
    if source == "robinhood_crypto":
        return "Robinhood crypto market data (24/7 candles)"
    if source == "schwab":
        return "Schwab API market data" + (" (extended candles on)" if include_extended else " (extended candles off)")
    return "Robinhood market data" + (" (extended candles on)" if include_extended else " (extended candles off)")


_MARKETS_SPAN_ORDER = ("day", "week", "month", "3month", "year", "5year")
_MARKETS_SPAN_DAYS = {
    "day": 1,
    "week": 7,
    "month": 30,
    "3month": 90,
    "year": 365,
    "5year": 1825,
}
_MARKETS_BARS_PER_DAY_STOCK = {
    "1minute": 390,
    "5minute": 78,
    "10minute": 39,
    "15minute": 26,
    "30minute": 13,
    "hour": 7,
    "day": 1,
}
_MARKETS_BARS_PER_DAY_CRYPTO = {
    "1minute": 1440,
    "5minute": 288,
    "10minute": 144,
    "15minute": 96,
    "30minute": 48,
    "hour": 24,
    "day": 1,
}
INDICATORFORGE_BACKTEST_DEFAULT_CANDLES = 1000
INDICATORFORGE_BACKTEST_MAX_CANDLES = 5000
_MARKETS_ALLOWED_STOCK_SPANS = {
    "1minute": ("day", "week"),
    "5minute": ("day", "week"),
    "10minute": ("day", "week"),
    "15minute": ("day", "week"),
    "30minute": ("day", "week", "month"),
    "hour": ("day", "week", "month", "3month"),
    "day": ("year", "5year"),
}
_MARKETS_ALLOWED_CRYPTO_SPANS = {
    "1minute": ("day", "week", "month", "3month"),
    "5minute": ("day", "week", "month", "3month"),
    "10minute": ("day", "week", "month", "3month"),
    "15minute": ("day", "week", "month", "3month"),
    "30minute": ("day", "week", "month", "3month"),
    "hour": ("day", "week", "month", "3month"),
    "day": ("year", "5year"),
}

_ROBINHOOD_STOCK_BACKTEST_CAPACITY = {
    # robin_stocks stock intraday spans are short. Keep requests inside what
    # the wrapper can plausibly return instead of running misleading partial sims.
    "1m": 390,
    "5m": 390,
    "10m": 195,
    "15m": 390,
    "30m": 390,
    "1h": 384,
    "1d": 1260,
}


def _robinhood_effective_backtest_lookback(timeframe: str, requested: int, min_required: int) -> tuple[int, Optional[str]]:
    tf = str(timeframe or "1h").strip().lower()
    req = max(40, int(requested or INDICATORFORGE_BACKTEST_DEFAULT_CANDLES))
    capacity = _ROBINHOOD_STOCK_BACKTEST_CAPACITY.get(tf)
    if capacity is None:
        return req, f"Robinhood backtest does not support timeframe '{tf}'."
    max_eval = int(capacity) - int(min_required) - 3
    if max_eval < 40:
        return 0, (
            f"Robinhood can return about {capacity} {tf} stock candles, but this rule set needs "
            f"{min_required} warmup candles plus at least 40 evaluation candles."
        )
    if req > max_eval:
        return max_eval, (
            f"Robinhood stock {tf} history can support about {max_eval} evaluation candles "
            f"after {min_required} warmup candles; requested {req}."
        )
    return req, None


def _market_span_candidates(
    *,
    interval: str,
    default_span: str,
    min_candles: int,
    is_crypto: bool,
) -> list[str]:
    interval_key = str(interval or "").strip().lower()
    allowed_map = _MARKETS_ALLOWED_CRYPTO_SPANS if is_crypto else _MARKETS_ALLOWED_STOCK_SPANS
    allowed = set(allowed_map.get(interval_key) or _MARKETS_SPAN_ORDER)
    order = [span for span in _MARKETS_SPAN_ORDER if span in allowed]
    if not order:
        return [str(default_span or "day").strip().lower() or "day"]
    default = str(default_span or "3month").strip().lower()
    try:
        base_idx = order.index(default)
    except Exception:
        base_idx = 0

    need = max(0, int(min_candles or 0))
    bars_per_day_map = _MARKETS_BARS_PER_DAY_CRYPTO if is_crypto else _MARKETS_BARS_PER_DAY_STOCK
    bars_per_day = int(bars_per_day_map.get(interval_key, 1))
    pick_idx = base_idx
    if need > 0:
        pick_idx = len(order) - 1
        for i in range(base_idx, len(order)):
            span_key = order[i]
            est = int(_MARKETS_SPAN_DAYS.get(span_key, 1)) * max(1, bars_per_day)
            if est >= need:
                pick_idx = i
                break
    candidates = order[pick_idx:]
    # If the largest candidate is rejected by Robinhood, retry shorter valid
    # spans before giving up and returning only the live quote.
    for span_key in reversed(order[:pick_idx]):
        if span_key not in candidates:
            candidates.append(span_key)
    return candidates


def _connected_robinhood_row() -> Optional[sqlite3.Row]:
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM broker_connections
        WHERE broker='robinhood' AND status IN ('connected','ok','')
        ORDER BY id DESC
        LIMIT 1
        """
    )
    row = cur.fetchone()
    conn.close()
    return row


def _connected_schwab_row() -> Optional[sqlite3.Row]:
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM broker_connections
        WHERE broker='schwab' AND status IN ('connected','ok','')
        ORDER BY id DESC
        LIMIT 1
        """
    )
    row = cur.fetchone()
    conn.close()
    return row


def _resolve_robinhood_pickle(row: sqlite3.Row) -> tuple[Optional[str], Optional[str]]:
    secrets = _db_read_connection_secrets(row, default={})
    pickle_dir = str(secrets.get("pickle_dir") or "").strip()
    pickle_name = str(secrets.get("pickle_name") or "").strip()
    pickle_path = str(secrets.get("pickle_path") or "").strip()
    if (not pickle_dir or not pickle_name) and pickle_path:
        p = Path(pickle_path).expanduser()
        if not pickle_dir:
            pickle_dir = str(p.parent)
        if not pickle_name:
            nm = p.name
            if nm.startswith("robinhood") and nm.endswith(".pickle"):
                pickle_name = nm[len("robinhood") : -len(".pickle")]
            elif nm.endswith(".pickle"):
                pickle_name = nm[: -len(".pickle")]
            else:
                pickle_name = nm
    if not pickle_dir:
        pickle_dir = str((DATA_DIR / "robinhood_sessions").resolve())
    if not pickle_name:
        pickle_name = f"_{int(row['id'])}"
    return pickle_dir, pickle_name


def _resolve_schwab_token_path(row: sqlite3.Row) -> Path:
    meta = _safe_json(str(row["metadata_json"] or "{}"), default={})
    raw = str((meta or {}).get("token_path") or "").strip()
    if raw:
        try:
            return Path(raw).expanduser().resolve()
        except Exception:
            pass
    return TOKEN_PATH.resolve()


def _schwab_market_base() -> Optional[str]:
    cfg = _schwab_config()
    market_base = str(cfg.get("market_data_base") or "").strip()
    trader_base = str(cfg.get("trader_api_base") or "").strip()
    base = market_base or trader_base
    if not base:
        return None
    if "/trader/" in base:
        base = base.replace("/trader/", "/marketdata/")
    if base.rstrip("/").endswith("/trader/v1"):
        base = base.rsplit("/trader/v1", 1)[0] + "/marketdata/v1"
    return base.rstrip("/")


def _schwab_preview_access_token(token_path: Path) -> Optional[str]:
    try:
        tok = json.loads(token_path.read_text(encoding="utf-8"))
        if not isinstance(tok, dict):
            return None
    except Exception:
        return None

    def _tok_access(token_obj: dict[str, Any]) -> Optional[str]:
        access = str(token_obj.get("access_token") or "").strip()
        return access or None

    age = _token_age_seconds(tok)
    expires_in = _to_int_opt(tok.get("expires_in"))
    if age is not None and expires_in and int(expires_in) > 0 and age < max(60, int(expires_in) - 60):
        return _tok_access(tok)

    cfg = _schwab_config()
    client_id = str(cfg.get("client_id") or "").strip()
    client_secret = str(cfg.get("client_secret") or "").strip()
    refresh_token = str(tok.get("refresh_token") or "").strip()
    if not client_id or not client_secret or not refresh_token:
        return _tok_access(tok)

    basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    headers = {"Authorization": f"Basic {basic}", "Content-Type": "application/x-www-form-urlencoded"}
    data = {"grant_type": "refresh_token", "refresh_token": refresh_token}
    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.post(SCHWAB_TOKEN_URL, headers=headers, data=data)
        if resp.status_code >= 400:
            return _tok_access(tok)
        new_tok = resp.json()
        if not isinstance(new_tok, dict):
            return _tok_access(tok)
        if not new_tok.get("refresh_token"):
            new_tok["refresh_token"] = refresh_token
        new_tok["obtained_at"] = _utc_now_iso()
        token_path.write_text(json.dumps(new_tok, indent=2), encoding="utf-8")
        return _tok_access(new_tok)
    except Exception:
        return _tok_access(tok)


def _market_fetch_closes_schwab(
    symbol: str,
    timeframe: str,
    *,
    min_candles: int = 0,
    include_extended: bool = False,
) -> list[float]:
    row = _connected_schwab_row()
    if not row:
        return []
    token_path = _resolve_schwab_token_path(row)
    if not token_path.exists():
        return []
    access = _schwab_preview_access_token(token_path)
    if not access:
        return []
    base = _schwab_market_base()
    if not base:
        return []

    tf = str(timeframe or "1h").strip().lower()
    mapping: dict[str, tuple[str, int, str, int]] = {
        "1m": ("day", 10, "minute", 1),
        "5m": ("day", 10, "minute", 5),
        "10m": ("day", 10, "minute", 10),
        "15m": ("day", 10, "minute", 15),
        "30m": ("day", 10, "minute", 30),
        "1h": ("day", 10, "minute", 15),
        "1d": ("year", 1, "daily", 1),
    }
    period_type, period, freq_type, freq = mapping.get(tf, mapping["1h"])
    needs_hourly_aggregation = (tf == "1h")

    headers = {"Authorization": f"Bearer {access}", "Accept": "application/json"}
    need_extended = bool(include_extended)

    def _fetch_history(
        *,
        symbol: str,
        period_type: str,
        period: int,
        frequency_type: str,
        frequency: int,
        need_extended: bool,
        start_date_ms: Optional[int] = None,
        end_date_ms: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "symbol": str(symbol).strip().upper(),
            "periodType": str(period_type),
            "period": int(period),
            "frequencyType": str(frequency_type),
            "frequency": int(frequency),
            "needExtendedHoursData": str(bool(need_extended)).lower(),
        }
        if start_date_ms is not None:
            params["startDate"] = int(start_date_ms)
        if end_date_ms is not None:
            params["endDate"] = int(end_date_ms)
        with httpx.Client(timeout=30.0) as client:
            resp = client.get(f"{base}/pricehistory", headers=headers, params=params)
        if resp.status_code >= 400:
            return []
        data = resp.json()
        candles = data.get("candles") if isinstance(data, dict) else None
        if not isinstance(candles, list):
            return []
        return [c for c in candles if isinstance(c, dict)]

    candles_rows: list[dict[str, Any]] = []
    target_candles = max(0, int(min_candles or 0))
    raw_target_candles = (target_candles * 4) if (needs_hourly_aggregation and target_candles > 0) else target_candles
    if _schwab_fetch_with_min_candles is not None and raw_target_candles > 0:
        try:
            candles_rows = _schwab_fetch_with_min_candles(
                fetch_fn=_fetch_history,
                symbol=str(symbol).strip().upper(),
                period_type=period_type,
                period=int(period),
                frequency_type=freq_type,
                frequency=int(freq),
                need_extended=need_extended,
                min_candles=raw_target_candles,
            )
        except Exception:
            candles_rows = []

    out: list[float] = []
    if not candles_rows:
        try:
            candles_rows = _fetch_history(
                symbol=str(symbol).strip().upper(),
                period_type=period_type,
                period=int(period),
                frequency_type=freq_type,
                frequency=int(freq),
                need_extended=need_extended,
            )
        except Exception:
            candles_rows = []

    # Aggregate 15-minute candles to hourly if needed
    if needs_hourly_aggregation and candles_rows:
        ordered_rows = [
            r for r in candles_rows
            if isinstance(r, dict) and _to_int_opt(r.get("datetime")) is not None
        ]
        ordered_rows.sort(key=lambda r: int(_to_int_opt(r.get("datetime")) or 0))
        aggregated: list[dict[str, Any]] = []
        bucket: list[dict[str, Any]] = []
        for row_obj in ordered_rows:
            if not isinstance(row_obj, dict):
                continue
            bucket.append(row_obj)
            if len(bucket) == 4:  # 4 x 15min = 60min
                # Aggregate the bucket
                highs = [float(r.get("high", 0)) for r in bucket if r.get("high") is not None]
                lows = [float(r.get("low", 0)) for r in bucket if r.get("low") is not None]
                closes = [r.get("close") for r in bucket if r.get("close") is not None]
                opens = [r.get("open") for r in bucket if r.get("open") is not None]
                volumes = [float(r.get("volume", 0)) for r in bucket if r.get("volume") is not None]

                if closes:
                    agg_candle = {
                        "datetime": bucket[0].get("datetime"),
                        "open": float(opens[0]) if opens else float(closes[0]),
                        "high": float(max(highs)) if highs else 0.0,
                        "low": float(min(lows)) if lows else 0.0,
                        "close": float(closes[-1]),
                        "volume": float(sum(volumes)) if volumes else 0.0,
                    }
                    aggregated.append(agg_candle)
                bucket = []
        candles_rows = aggregated
        if target_candles > 0 and len(candles_rows) > target_candles:
            candles_rows = candles_rows[-target_candles:]

    for row_obj in candles_rows:
        try:
            c = row_obj.get("close")
            if c is None:
                c = row_obj.get("close_price")
            if c is not None:
                out.append(float(c))
        except Exception:
            continue

    try:
        with httpx.Client(timeout=20.0) as client:
            qresp = client.get(
                f"{base}/quotes",
                headers=headers,
                params={
                    "symbols": str(symbol).strip().upper(),
                    "fields": "quote,regular",
                    "indicative": "false",
                },
            )
        if qresp.status_code < 400:
            qdata = qresp.json()
            if isinstance(qdata, dict):
                qobj = qdata.get(str(symbol).strip().upper()) or qdata.get(str(symbol).strip()) or {}
                if isinstance(qobj, dict):
                    quote = qobj.get("quote") if isinstance(qobj.get("quote"), dict) else qobj
                    regular = qobj.get("regular") if isinstance(qobj.get("regular"), dict) else {}
                    qv: Optional[float] = None
                    if not need_extended:
                        for key in ("regularMarketLastPrice",):
                            try:
                                if regular.get(key) is not None:
                                    qv = float(regular.get(key))
                                    break
                            except Exception:
                                pass
                    if qv is None:
                        for key in ("lastPrice", "closePrice", "askPrice", "bidPrice"):
                            try:
                                if quote.get(key) is not None:
                                    qv = float(quote.get(key))
                                    break
                            except Exception:
                                pass
                    if qv is None and need_extended:
                        for key in ("regularMarketLastPrice",):
                            try:
                                if regular.get(key) is not None:
                                    qv = float(regular.get(key))
                                    break
                            except Exception:
                                pass
                    if qv is not None and (not out or abs(out[-1] - qv) > 1e-9):
                        out.append(float(qv))
    except Exception:
        pass
    return out


def _ensure_robinhood_markets_session() -> tuple[bool, str]:
    if rh is None:
        return False, "robin_stocks unavailable"
    row = _connected_robinhood_row()
    if not row:
        return False, "No connected Robinhood session"
    try:
        pickle_dir, pickle_name = _resolve_robinhood_pickle(row)
        if not pickle_dir or not pickle_name:
            return False, "Robinhood pickle settings missing"
        pickle_file = Path(pickle_dir).expanduser() / f"robinhood{pickle_name}.pickle"
        if not pickle_file.exists():
            return False, "Robinhood session pickle missing"
        with open(pickle_file, "rb") as f:
            tok = pickle.load(f)
        if not isinstance(tok, dict):
            return False, "Robinhood session pickle unreadable"
        token_type = str(tok.get("token_type") or "").strip()
        access_token = str(tok.get("access_token") or "").strip()
        if not token_type or not access_token:
            return False, "Robinhood session token missing in pickle"
        from robin_stocks.robinhood import helper as rh_helper  # type: ignore
        rh_helper.set_login_state(True)
        rh_helper.update_session("Authorization", f"{token_type} {access_token}")
        probe = rh_helper.request_get("https://api.robinhood.com/user/")
        if not isinstance(probe, dict) or not probe:
            return False, "Robinhood session expired"
        return True, ""
    except Exception as e:
        return False, f"Robinhood login restore failed: {e}"


def _market_ma(prices: list[float], window: int) -> Optional[float]:
    if len(prices) < window:
        return None
    return sum(prices[-window:]) / float(window)


def _market_ma_series(prices: list[float], window: int) -> list[Optional[float]]:
    out: list[Optional[float]] = [None] * len(prices)
    if len(prices) < window:
        return out
    running = sum(prices[:window])
    out[window - 1] = running / float(window)
    for i in range(window, len(prices)):
        running += prices[i] - prices[i - window]
        out[i] = running / float(window)
    return out


def _market_ema_series(prices: list[float], window: int) -> list[Optional[float]]:
    out: list[Optional[float]] = [None] * len(prices)
    if len(prices) < window:
        return out
    alpha = 2.0 / (float(window) + 1.0)
    seed = sum(prices[:window]) / float(window)
    out[window - 1] = seed
    ema = seed
    for i in range(window, len(prices)):
        ema = (float(prices[i]) * alpha) + (ema * (1.0 - alpha))
        out[i] = ema
    return out


def _market_ema(prices: list[float], window: int) -> Optional[float]:
    s = _market_ema_series(prices, window)
    if not s:
        return None
    return s[-1]


def _market_ema_derivative(prices: list[float], window: int) -> Optional[float]:
    if len(prices) < window + 1:
        return None
    e0 = _market_ema(prices, window)
    e1 = _market_ema(prices[:-1], window)
    if e0 is None or e1 is None:
        return None
    return float(e0 - e1)


def _market_ichimoku_series(
    prices: list[float],
    *,
    tenkan_length: int = 9,
    kijun_length: int = 26,
    senkou_b_length: int = 52,
    displacement: int = 26,
    forward_projected: bool = False,
) -> dict[str, list[Optional[float]]]:
    n = len(prices)
    tenkan_len = max(1, int(tenkan_length))
    kijun_len = max(1, int(kijun_length))
    senkou_b_len = max(2, int(senkou_b_length))
    disp = max(1, int(displacement))
    out_len = n + disp if bool(forward_projected) else n
    out: dict[str, list[Optional[float]]] = {
        "tenkan": [None] * out_len,
        "kijun": [None] * out_len,
        "span_a": [None] * out_len,
        "span_b": [None] * out_len,
    }
    if n <= 0:
        return out

    values: list[float] = []
    for raw in prices:
        fv = _to_float_opt(raw)
        if fv is None:
            return out
        ff = float(fv)
        if not math.isfinite(ff):
            return out
        values.append(ff)

    def _midpoint(length: int, idx: int) -> Optional[float]:
        if idx < 0 or idx >= n:
            return None
        start = idx - int(length) + 1
        if start < 0:
            return None
        window = values[start : idx + 1]
        if not window:
            return None
        return (max(window) + min(window)) / 2.0

    raw_span_a: list[Optional[float]] = [None] * n
    raw_span_b: list[Optional[float]] = [None] * n
    for i in range(n):
        tenkan = _midpoint(tenkan_len, i)
        kijun = _midpoint(kijun_len, i)
        if tenkan is not None:
            out["tenkan"][i] = float(tenkan)
        if kijun is not None:
            out["kijun"][i] = float(kijun)
        if tenkan is not None and kijun is not None:
            raw_span_a[i] = (float(tenkan) + float(kijun)) / 2.0
        span_b_raw = _midpoint(senkou_b_len, i)
        if span_b_raw is not None:
            raw_span_b[i] = float(span_b_raw)

    if bool(forward_projected):
        for i in range(n):
            dst = i + disp
            if dst < 0 or dst >= out_len:
                continue
            span_a_now = raw_span_a[i]
            span_b_now = raw_span_b[i]
            if span_a_now is not None:
                out["span_a"][dst] = float(span_a_now)
            if span_b_now is not None:
                out["span_b"][dst] = float(span_b_now)
    else:
        # Rule evaluation expects the cloud value aligned with the current bar.
        for i in range(n):
            src = i - disp
            if src < 0 or src >= n:
                continue
            span_a_now = raw_span_a[src]
            span_b_now = raw_span_b[src]
            if span_a_now is not None:
                out["span_a"][i] = float(span_a_now)
            if span_b_now is not None:
                out["span_b"][i] = float(span_b_now)
    return out


def _market_line_value(prices: list[float], *, ma_type: str, length: int) -> Optional[float]:
    t = str(ma_type or "sma").strip().lower()
    if t == "ema":
        return _market_ema(prices, length)
    return _market_ma(prices, length)


def _market_line_derivative(prices: list[float], *, ma_type: str, length: int) -> Optional[float]:
    t = str(ma_type or "sma").strip().lower()
    if t == "ema":
        return _market_ema_derivative(prices, length)
    return _market_ma_derivative(prices, length)


def _market_macd_series(
    prices: list[float],
    *,
    fast_len: int = 12,
    slow_len: int = 26,
    signal_len: int = 9,
) -> tuple[list[Optional[float]], list[Optional[float]], list[Optional[float]]]:
    n = len(prices)
    out_macd: list[Optional[float]] = [None] * n
    out_sig: list[Optional[float]] = [None] * n
    out_hist: list[Optional[float]] = [None] * n
    if n < max(fast_len, slow_len, signal_len) + 2:
        return out_macd, out_sig, out_hist

    ema_fast = _market_ema_series(prices, max(2, int(fast_len)))
    ema_slow = _market_ema_series(prices, max(2, int(slow_len)))

    macd_vals: list[float] = []
    macd_idx: list[int] = []
    for i in range(n):
        ef = ema_fast[i]
        es = ema_slow[i]
        if ef is None or es is None:
            continue
        v = float(ef - es)
        out_macd[i] = v
        macd_vals.append(v)
        macd_idx.append(i)

    if len(macd_vals) < max(2, int(signal_len)):
        return out_macd, out_sig, out_hist

    sig_series = _market_ema_series(macd_vals, max(2, int(signal_len)))
    for j, idx in enumerate(macd_idx):
        sig = sig_series[j]
        if sig is None:
            continue
        out_sig[idx] = float(sig)
        if out_macd[idx] is not None:
            out_hist[idx] = float(out_macd[idx] - sig)
    return out_macd, out_sig, out_hist


def _market_rsi(prices: list[float], period: int = 14) -> Optional[float]:
    if len(prices) < period + 1:
        return None
    deltas = [float(prices[i] - prices[i - 1]) for i in range(1, len(prices))]
    gains = [d if d > 0 else 0.0 for d in deltas]
    losses = [-d if d < 0 else 0.0 for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(deltas)):
        avg_gain = ((avg_gain * (period - 1)) + gains[i]) / period
        avg_loss = ((avg_loss * (period - 1)) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def _market_rsi_derivative(prices: list[float], period: int = 14) -> Optional[float]:
    # Match IndicatorForge runtime scripts: two-step RSI slope.
    if len(prices) < period + 3:
        return None
    r0 = _market_rsi(prices, period)
    r2 = _market_rsi(prices[:-2], period)
    if r0 is None or r2 is None:
        return None
    return float((r0 - r2) / 2.0)


def _market_roc(prices: list[float], length: int = 12) -> Optional[float]:
    ln = max(1, int(length))
    if len(prices) <= ln:
        return None
    now = _to_float_opt(prices[-1])
    base = _to_float_opt(prices[-1 - ln])
    if now is None or base is None:
        return None
    if float(base) == 0.0:
        return None
    return ((float(now) - float(base)) / float(base)) * 100.0


def _market_roc_series(prices: list[float], length: int = 12) -> list[Optional[float]]:
    ln = max(1, int(length))
    out: list[Optional[float]] = [None] * len(prices)
    if len(prices) <= ln:
        return out
    for i in range(ln, len(prices)):
        try:
            now = float(prices[i])
            base = float(prices[i - ln])
        except Exception:
            continue
        if (not math.isfinite(now)) or (not math.isfinite(base)) or base == 0.0:
            continue
        out[i] = ((float(now) - float(base)) / float(base)) * 100.0
    return out


def _market_donchian_channels(
    highs: Optional[list[float]],
    lows: Optional[list[float]],
    lookback: int,
) -> tuple[list[Optional[float]], list[Optional[float]], list[Optional[float]]]:
    n = min(len(highs or []), len(lows or []))
    upper: list[Optional[float]] = [None] * n
    lower: list[Optional[float]] = [None] * n
    middle: list[Optional[float]] = [None] * n
    ln = max(1, int(lookback))
    if n <= 1:
        return upper, lower, middle
    high_vals: list[float] = []
    low_vals: list[float] = []
    try:
        high_vals = [float(v) for v in (highs or [])[:n]]
        low_vals = [float(v) for v in (lows or [])[:n]]
    except Exception:
        return upper, lower, middle
    for i in range(n):
        start = max(0, i - ln)
        if i - start < 1:
            continue
        upper[i] = max(high_vals[start:i])
        lower[i] = min(low_vals[start:i])
        middle[i] = (float(upper[i]) + float(lower[i])) / 2.0
    return upper, lower, middle


def _donchian_condition_hit(
    cond: str,
    *,
    close_now: float,
    high_now: Optional[float],
    low_now: Optional[float],
    upper: float,
    lower: float,
    middle: Optional[float] = None,
    prev_upper: Optional[float] = None,
    prev_lower: Optional[float] = None,
) -> bool:
    c = _normalize_donchian_condition(cond, default="hold")
    mid = (float(upper) + float(lower)) / 2.0 if middle is None else float(middle)
    inside = float(lower) <= float(close_now) <= float(upper)
    slope_up = (
        prev_upper is not None
        and prev_lower is not None
        and float(upper) > float(prev_upper)
        and float(lower) > float(prev_lower)
    )
    slope_down = (
        prev_upper is not None
        and prev_lower is not None
        and float(upper) < float(prev_upper)
        and float(lower) < float(prev_lower)
    )
    if c == "hold":
        return False
    if c == "close_above_upper":
        return float(close_now) > float(upper)
    if c == "high_above_upper":
        return float(high_now if high_now is not None else close_now) > float(upper)
    if c == "close_below_lower":
        return float(close_now) < float(lower)
    if c == "low_below_lower":
        return float(low_now if low_now is not None else close_now) < float(lower)
    if c == "inside_channel":
        return inside
    if c == "above_mid_inside":
        return inside and float(close_now) > mid
    if c == "below_mid_inside":
        return inside and float(close_now) < mid
    if c == "channel_slope_up":
        return slope_up
    if c == "channel_slope_down":
        return slope_down
    if c == "slope_up_above_mid_inside":
        return slope_up and inside and float(close_now) > mid
    if c == "slope_up_below_mid_inside":
        return slope_up and inside and float(close_now) < mid
    if c == "slope_down_above_mid_inside":
        return slope_down and inside and float(close_now) > mid
    if c == "slope_down_below_mid_inside":
        return slope_down and inside and float(close_now) < mid
    return False


def _market_true_range_series(
    highs: list[float],
    lows: list[float],
    closes: list[float],
) -> list[Optional[float]]:
    n = min(len(highs), len(lows), len(closes))
    out: list[Optional[float]] = [None] * n
    if n <= 0:
        return out
    try:
        high_vals = [float(v) for v in highs[:n]]
        low_vals = [float(v) for v in lows[:n]]
        close_vals = [float(v) for v in closes[:n]]
    except Exception:
        return out
    for i in range(n):
        prev_close = close_vals[i - 1] if i > 0 else close_vals[i]
        out[i] = max(
            high_vals[i] - low_vals[i],
            abs(high_vals[i] - prev_close),
            abs(low_vals[i] - prev_close),
        )
    return out


def _market_atr_series(
    highs: list[float],
    lows: list[float],
    closes: list[float],
    length: int,
) -> list[Optional[float]]:
    ln = max(1, int(length))
    tr = _market_true_range_series(highs, lows, closes)
    out: list[Optional[float]] = [None] * len(tr)
    if len(tr) < ln:
        return out
    for i in range(ln - 1, len(tr)):
        window = [v for v in tr[i - ln + 1 : i + 1] if isinstance(v, (int, float))]
        if len(window) == ln:
            out[i] = float(sum(float(v) for v in window) / float(ln))
    return out


def _market_supertrend_series(
    closes: list[float],
    *,
    highs: Optional[list[float]] = None,
    lows: Optional[list[float]] = None,
    atr_length: int = 10,
    multiplier: float = 3.0,
) -> tuple[list[Optional[float]], list[Optional[float]]]:
    points = _market_supertrend_points(
        closes,
        highs=highs,
        lows=lows,
        atr_length=atr_length,
        multiplier=multiplier,
    )
    return (
        [p.trend if p.trend is not None else None for p in points],
        [p.direction if p.direction is not None else None for p in points],
    )


def _market_supertrend_points(
    closes: list[float],
    *,
    highs: Optional[list[float]] = None,
    lows: Optional[list[float]] = None,
    atr_length: int = 10,
    multiplier: float = 3.0,
) -> list[SupertrendPoint]:
    n = len(closes)
    if n <= 0 or calculate_supertrend is None:
        return []
    try:
        close_vals = [float(v) for v in closes]
        high_vals = [
            float(highs[i]) if isinstance(highs, list) and i < len(highs) else float(close_vals[i])
            for i in range(n)
        ]
        low_vals = [
            float(lows[i]) if isinstance(lows, list) and i < len(lows) else float(close_vals[i])
            for i in range(n)
        ]
    except Exception:
        return []
    return calculate_supertrend(
        high_vals,
        low_vals,
        close_vals,
        max(1, int(atr_length)),
        max(0.1, float(multiplier)),
    )


def _supertrend_condition_hit(
    cond: str,
    *,
    close_now: float,
    close_prev: Optional[float],
    trend_now: float,
    trend_prev: Optional[float],
    direction_now: float,
    direction_prev: Optional[float],
) -> bool:
    c = _normalize_supertrend_condition(cond, default="hold")
    if c == "hold":
        return False
    if c == "trend_up":
        return float(direction_now) > 0.0
    if c == "trend_down":
        return float(direction_now) < 0.0
    if c == "close_above_trend":
        return float(close_now) > float(trend_now)
    if c == "close_below_trend":
        return float(close_now) < float(trend_now)
    if c == "flip_up":
        return direction_prev is not None and float(direction_prev) <= 0.0 and float(direction_now) > 0.0
    if c == "flip_down":
        return direction_prev is not None and float(direction_prev) >= 0.0 and float(direction_now) < 0.0
    return False


def _market_timestamp_day(raw: Any) -> str:
    txt = str(raw or "").strip()
    if not txt:
        return "all"
    if re.fullmatch(r"\d{10,13}", txt):
        try:
            sec = int(txt)
            if sec > 10_000_000_000:
                sec = int(sec / 1000)
            return datetime.fromtimestamp(sec, tz=timezone.utc).date().isoformat()
        except Exception:
            return "all"
    try:
        parsed = datetime.fromisoformat(txt.replace("Z", "+00:00"))
        return parsed.date().isoformat()
    except Exception:
        return txt[:10] if len(txt) >= 10 else "all"


def _market_vwap_series(
    closes: list[float],
    *,
    highs: Optional[list[float]] = None,
    lows: Optional[list[float]] = None,
    volumes: Optional[list[float]] = None,
    timestamps: Optional[list[str]] = None,
) -> list[Optional[float]]:
    n = len(closes)
    out: list[Optional[float]] = [None] * n
    if n <= 0 or not isinstance(volumes, list) or len(volumes) < n:
        return out
    try:
        close_vals = [float(v) for v in closes]
        high_vals = [
            float(highs[i]) if isinstance(highs, list) and i < len(highs) else float(close_vals[i])
            for i in range(n)
        ]
        low_vals = [
            float(lows[i]) if isinstance(lows, list) and i < len(lows) else float(close_vals[i])
            for i in range(n)
        ]
        volume_vals = [float(volumes[i]) for i in range(n)]
    except Exception:
        return out
    if not any(v > 0.0 and math.isfinite(v) for v in volume_vals):
        return out
    cum_pv = 0.0
    cum_vol = 0.0
    current_day = ""
    for i in range(n):
        day = (
            _market_timestamp_day(timestamps[i])
            if isinstance(timestamps, list) and i < len(timestamps)
            else "all"
        )
        if day != current_day:
            current_day = day
            cum_pv = 0.0
            cum_vol = 0.0
        vol = max(0.0, volume_vals[i])
        if vol > 0.0 and math.isfinite(vol):
            typical = (high_vals[i] + low_vals[i] + close_vals[i]) / 3.0
            cum_pv += typical * vol
            cum_vol += vol
        if cum_vol > 0.0:
            out[i] = cum_pv / cum_vol
    return out


def _vwap_condition_hit(
    cond: str,
    *,
    close_now: float,
    close_prev: Optional[float],
    vwap_now: float,
    vwap_prev: Optional[float],
    max_extension_pct: float,
    max_pullback_pct: float,
    exit_below_pct: float,
) -> bool:
    c = _normalize_vwap_condition(cond, default="hold")
    if c == "hold":
        return False
    if c == "price_above_vwap":
        return float(close_now) >= float(vwap_now)
    if c == "price_below_vwap":
        return float(close_now) <= float(vwap_now)
    if c == "within_band":
        return (
            float(close_now) >= float(vwap_now) * (1.0 - float(max_pullback_pct))
            and float(close_now) <= float(vwap_now) * (1.0 + float(max_extension_pct))
        )
    if c == "overextended_above":
        return float(close_now) > float(vwap_now) * (1.0 + float(max_extension_pct))
    if c == "extended_below":
        return float(close_now) < float(vwap_now) * (1.0 - float(max_pullback_pct))
    if c == "cross_above":
        return (
            close_prev is not None
            and vwap_prev is not None
            and float(close_prev) <= float(vwap_prev)
            and float(close_now) > float(vwap_now)
        )
    if c == "cross_below":
        return (
            close_prev is not None
            and vwap_prev is not None
            and float(close_prev) >= float(vwap_prev)
            and float(close_now) < float(vwap_now)
        )
    if c == "exit_below":
        return float(close_now) < float(vwap_now) * (1.0 - float(exit_below_pct))
    return False


def _market_relative_volume_series(volumes: Optional[list[float]], length: int = 20) -> list[Optional[float]]:
    if not isinstance(volumes, list):
        return []
    n = len(volumes)
    ln = max(1, int(length))
    out: list[Optional[float]] = [None] * n
    try:
        vals = [max(0.0, float(v)) for v in volumes]
    except Exception:
        return out
    if n < ln or not any(v > 0.0 for v in vals):
        return out
    running = sum(vals[:ln])
    avg = running / float(ln)
    if avg > 0.0:
        out[ln - 1] = vals[ln - 1] / avg
    for i in range(ln, n):
        running += vals[i] - vals[i - ln]
        avg = running / float(ln)
        if avg > 0.0:
            out[i] = vals[i] / avg
    return out


def _relative_volume_condition_hit(
    cond: str,
    *,
    rvol: float,
    prev_rvol: Optional[float],
    threshold: float,
) -> bool:
    c = _normalize_relative_volume_condition(cond, default="hold")
    if c == "hold":
        return False
    if c == "above_threshold":
        return float(rvol) >= float(threshold)
    if c == "below_threshold":
        return float(rvol) <= float(threshold)
    if c == "rising":
        return prev_rvol is not None and float(rvol) > float(prev_rvol)
    if c == "falling":
        return prev_rvol is not None and float(rvol) < float(prev_rvol)
    return False


def _market_sar_series_with_trend(
    closes: list[float],
    *,
    highs: Optional[list[float]] = None,
    lows: Optional[list[float]] = None,
    step: float = 0.02,
    max_step: float = 0.2,
) -> tuple[list[Optional[float]], list[Optional[bool]]]:
    n = len(closes)
    out: list[Optional[float]] = [None] * n
    trend: list[Optional[bool]] = [None] * n
    if n < 2:
        return out, trend
    high_vals: list[float] = []
    low_vals: list[float] = []
    close_vals: list[float] = []
    for i in range(n):
        c = _to_float_opt(closes[i] if i < len(closes) else None)
        if c is None or (not math.isfinite(float(c))):
            return out, trend
        cc = float(c)
        close_vals.append(cc)
        h = _to_float_opt(highs[i]) if isinstance(highs, list) and i < len(highs) else None
        l = _to_float_opt(lows[i]) if isinstance(lows, list) and i < len(lows) else None
        hv = float(h) if h is not None and math.isfinite(float(h)) else cc
        lv = float(l) if l is not None and math.isfinite(float(l)) else cc
        high_vals.append(max(hv, lv))
        low_vals.append(min(hv, lv))

    af_step = max(1.0e-6, float(step))
    af_max = max(af_step, float(max_step))
    af = af_step
    uptrend = close_vals[1] >= close_vals[0]
    ep = high_vals[0] if uptrend else low_vals[0]
    sar = low_vals[0] if uptrend else high_vals[0]
    out[0] = float(sar)
    trend[0] = bool(uptrend)

    for i in range(1, n):
        sar = float(sar) + (af * (float(ep) - float(sar)))
        if uptrend:
            clamp_1 = low_vals[i - 1]
            clamp_2 = low_vals[i - 2] if i > 1 else low_vals[i - 1]
            sar = min(sar, clamp_1, clamp_2)
            if low_vals[i] < sar:
                uptrend = False
                sar = float(ep)
                ep = low_vals[i]
                af = af_step
            else:
                if high_vals[i] > ep:
                    ep = high_vals[i]
                    af = min(af + af_step, af_max)
        else:
            clamp_1 = high_vals[i - 1]
            clamp_2 = high_vals[i - 2] if i > 1 else high_vals[i - 1]
            sar = max(sar, clamp_1, clamp_2)
            if high_vals[i] > sar:
                uptrend = True
                sar = float(ep)
                ep = high_vals[i]
                af = af_step
            else:
                if low_vals[i] < ep:
                    ep = low_vals[i]
                    af = min(af + af_step, af_max)
        out[i] = float(sar)
        trend[i] = bool(uptrend)
    return out, trend


def _market_sar_series(
    closes: list[float],
    *,
    highs: Optional[list[float]] = None,
    lows: Optional[list[float]] = None,
    step: float = 0.02,
    max_step: float = 0.2,
) -> list[Optional[float]]:
    values, _trend = _market_sar_series_with_trend(closes, highs=highs, lows=lows, step=step, max_step=max_step)
    return values


def _market_bollinger_series(
    closes: list[float],
    *,
    length: int = 20,
    std_mult: float = 2.0,
) -> dict[str, list[Optional[float]]]:
    ln = max(2, int(length))
    mult = max(0.1, float(std_mult))
    n = len(closes)
    mid: list[Optional[float]] = [None] * n
    upper: list[Optional[float]] = [None] * n
    lower: list[Optional[float]] = [None] * n
    if n < ln:
        return {"middle": mid, "upper": upper, "lower": lower}
    for i in range(ln - 1, n):
        try:
            window = [float(v) for v in closes[i - ln + 1 : i + 1]]
        except Exception:
            continue
        if not window:
            continue
        m = sum(window) / float(ln)
        variance = sum((float(v) - float(m)) ** 2 for v in window) / float(ln)
        std_dev = math.sqrt(max(0.0, float(variance)))
        mid[i] = float(m)
        upper[i] = float(m + (mult * std_dev))
        lower[i] = float(m - (mult * std_dev))
    return {"middle": mid, "upper": upper, "lower": lower}


def _market_ttm_series(
    closes: list[float],
    *,
    bb_length: int = 20,
    bb_mult: float = 2.0,
    kc_length: int = 20,
    kc_mult: float = 1.5,
    momentum_length: int = 20,
) -> dict[str, list[Optional[float]]]:
    n = len(closes)
    out_momentum: list[Optional[float]] = [None] * n
    out_sq_on: list[Optional[float]] = [None] * n
    out_sq_off: list[Optional[float]] = [None] * n
    out_sq_fired: list[Optional[float]] = [None] * n
    bb_len = max(2, int(bb_length))
    bb_mul = max(0.1, float(bb_mult))
    kc_len = max(2, int(kc_length))
    kc_mul = max(0.1, float(kc_mult))
    mom_len = max(2, int(momentum_length))
    need = max(bb_len, kc_len, mom_len)
    if n < need:
        return {
            "momentum": out_momentum,
            "squeeze_on": out_sq_on,
            "squeeze_off": out_sq_off,
            "squeeze_fired": out_sq_fired,
        }

    prev_on = False
    for i in range(n):
        sub = closes[: i + 1]
        if len(sub) < need:
            prev_on = False
            continue
        bb = _market_bollinger_snapshot(sub, length=bb_len, std_mult=bb_mul)
        kc_mid = _market_ttm_sma_tail(sub, kc_len)
        kc_atr = _market_ttm_atr(sub, highs=None, lows=None, length=kc_len)
        mom = _market_ttm_momentum(sub, highs=None, lows=None, length=mom_len)
        if bb is None or kc_mid is None or kc_atr is None or mom is None:
            prev_on = False
            continue
        kc_upper = float(kc_mid) + (kc_mul * float(kc_atr))
        kc_lower = float(kc_mid) - (kc_mul * float(kc_atr))
        bb_upper = float(bb["upper"])
        bb_lower = float(bb["lower"])
        sq_on = bool(bb_upper <= kc_upper and bb_lower >= kc_lower)
        sq_off = bool(bb_upper > kc_upper and bb_lower < kc_lower)
        sq_fired = bool(prev_on and sq_off)
        out_momentum[i] = float(mom)
        out_sq_on[i] = 1.0 if sq_on else 0.0
        out_sq_off[i] = 1.0 if sq_off else 0.0
        out_sq_fired[i] = 1.0 if sq_fired else 0.0
        prev_on = sq_on

    return {
        "momentum": out_momentum,
        "squeeze_on": out_sq_on,
        "squeeze_off": out_sq_off,
        "squeeze_fired": out_sq_fired,
    }


def _market_ma_derivative(prices: list[float], window: int) -> Optional[float]:
    if len(prices) < window + 1:
        return None
    m0 = _market_ma(prices, window)
    m1 = _market_ma(prices[:-1], window)
    if m0 is None or m1 is None:
        return None
    return float(m0 - m1)


def _market_fetch_quote(symbol: str, *, allow_crypto: bool = False) -> Optional[float]:
    if rh is None:
        return None
    stock_quote: Optional[dict[str, Any]] = None
    try:
        q = rh.stocks.get_stock_quote_by_symbol(symbol)
        if isinstance(q, dict):
            stock_quote = q
    except Exception:
        stock_quote = None

    if stock_quote is not None:
        for k in ("last_trade_price", "last_extended_hours_trade_price", "ask_price", "bid_price"):
            try:
                v = stock_quote.get(k)
                if v is not None:
                    return float(v)
            except Exception:
                continue

    if not allow_crypto:
        return None

    try:
        crypto_id = rh.crypto.get_crypto_id(symbol)
        if not crypto_id:
            return None
        cq = rh.crypto.get_crypto_quote_from_id(crypto_id)
        if not isinstance(cq, dict):
            return None
        for k in ("mark_price", "ask_price", "bid_price", "open_price"):
            try:
                v = cq.get(k)
                if v is not None:
                    return float(v)
            except Exception:
                continue
    except Exception:
        return None
    return None


def _market_fetch_crypto_quote(symbol: str) -> Optional[float]:
    if rh is None:
        return None
    try:
        cq = rh.crypto.get_crypto_quote(symbol)
    except Exception:
        cq = None
    if not isinstance(cq, dict):
        try:
            crypto_id = rh.crypto.get_crypto_id(symbol)
            if not crypto_id:
                return None
            cq = rh.crypto.get_crypto_quote_from_id(crypto_id)
        except Exception:
            cq = None
    if not isinstance(cq, dict):
        return None
    for key in ("mark_price", "ask_price", "bid_price", "open_price"):
        try:
            value = cq.get(key)
            if value is not None:
                price = float(value)
                if math.isfinite(price) and price > 0.0:
                    return price
        except Exception:
            continue
    return None


def _market_fetch_closes(
    symbol: str,
    timeframe: str,
    broker_hint: str = "robinhood",
    *,
    min_candles: int = 0,
    include_extended: bool = False,
    allow_crypto_fallback: bool = False,
    append_live_quote: bool = True,
) -> list[float]:
    hint = _normalize_market_source_hint(broker_hint)
    if hint == "schwab":
        return _market_fetch_closes_schwab(
            symbol,
            timeframe,
            min_candles=int(min_candles or 0),
            include_extended=bool(include_extended),
        )
    if rh is None:
        return []
    interval, span = _markets_timeframe(timeframe)
    if hint == "robinhood" and interval == "30minute":
        return []
    need = max(0, int(min_candles or 0))

    def _extract_closes(rows: Any) -> list[float]:
        out_local: list[float] = []
        if not isinstance(rows, list):
            return out_local
        for row in rows:
            if not isinstance(row, dict):
                continue
            try:
                c = row.get("close_price")
                if c is not None:
                    out_local.append(float(c))
            except Exception:
                continue
        return out_local

    out: list[float] = []
    best_stock_out: list[float] = []

    def _fetch_stock_history(cur_span: str, bounds: str) -> Any:
        if _rh_adapter_get_stock_historicals is None:
            return rh.stocks.get_stock_historicals(symbol, interval=interval, span=cur_span, bounds=bounds)
        if interval == "10minute" and bounds == "regular":
            try:
                return _rh_adapter_get_10m_stock_historicals(
                    symbol,
                    span=cur_span,
                    bounds=bounds,
                    min_candles=max(0, min(need, _ROBINHOOD_STOCK_BACKTEST_CAPACITY.get("10m", need))),
                    allow_partial=True,
                )
            except RuntimeError:
                return []
        return _rh_adapter_get_stock_historicals(symbol, interval=interval, span=cur_span, bounds=bounds)

    def _fetch_crypto_history(cur_span: str) -> Any:
        if _rh_adapter_get_crypto_historicals is None:
            return rh.crypto.get_crypto_historicals(symbol, interval=interval, span=cur_span, bounds="24_7")
        return _rh_adapter_get_crypto_historicals(symbol, interval=interval, span=cur_span, bounds="24_7")

    if hint == "robinhood_crypto":
        crypto_spans = _market_span_candidates(
            interval=interval,
            default_span=span,
            min_candles=need,
            is_crypto=True,
        )
        for cur_span in crypto_spans:
            try:
                ch = _fetch_crypto_history(cur_span)
            except Exception:
                ch = None
            out = _extract_closes(ch)
            if out and (len(out) >= need or cur_span == crypto_spans[-1]):
                break
        if bool(append_live_quote):
            q = _market_fetch_crypto_quote(symbol)
            if q is not None:
                out.append(float(q))
        return out

    # Try stock historicals first, escalating span when higher candle counts are requested.
    stock_spans = _market_span_candidates(
        interval=interval,
        default_span=span,
        min_candles=need,
        is_crypto=False,
    )
    for cur_span in stock_spans:
        try:
            hist = _fetch_stock_history(cur_span, "regular")
        except Exception:
            hist = None
        if not isinstance(hist, list) or not hist:
            continue

        if include_extended and interval not in ("day", "week"):
            # robin_stocks only supports extended/trading bounds for span="day".
            # For longer spans, merge same-day extended candles into regular history.
            try:
                extra = _fetch_stock_history("day", "extended")
            except Exception:
                extra = None
            if isinstance(extra, list) and extra:
                merged: dict[str, dict[str, Any]] = {}
                for row in hist:
                    if not isinstance(row, dict):
                        continue
                    key = row.get("begins_at")
                    if key:
                        merged[str(key)] = row
                for row in extra:
                    if not isinstance(row, dict):
                        continue
                    key = row.get("begins_at")
                    if not key:
                        continue
                    k = str(key)
                    if k in merged:
                        old = merged[k]
                        old_has_close = old.get("close_price") not in (None, "None", "")
                        new_has_close = row.get("close_price") not in (None, "None", "")
                        if new_has_close or not old_has_close:
                            merged[k] = row
                    else:
                        merged[k] = row
                if merged:
                    hist = [merged[k] for k in sorted(merged.keys())]

        candidate_out = _extract_closes(hist)
        if len(candidate_out) > len(best_stock_out):
            best_stock_out = candidate_out
        out = candidate_out
        if out and len(out) >= need:
            break
    if best_stock_out and (not out or len(best_stock_out) > len(out)):
        out = best_stock_out

    # Crypto fallback is opt-in. Stock IndicatorForge should never query
    # /marketdata/forex/... for ordinary stock/ETF symbols.
    if allow_crypto_fallback and (not out or (need > 0 and len(out) < need)):
        try:
            crypto_id = rh.crypto.get_crypto_id(symbol)
        except Exception:
            crypto_id = None
        if crypto_id:
            crypto_spans = _market_span_candidates(
                interval=interval,
                default_span=span,
                min_candles=need,
                is_crypto=True,
            )
            for cur_span in crypto_spans:
                try:
                    ch = _fetch_crypto_history(cur_span)
                except Exception:
                    ch = None
                out = _extract_closes(ch)
                if out and (len(out) >= need or cur_span == crypto_spans[-1]):
                    break

    if bool(append_live_quote):
        q = _market_fetch_quote(symbol, allow_crypto=allow_crypto_fallback)
        if q is not None:
            if not out or abs(out[-1] - q) > 1e-9:
                out.append(float(q))
    return out


def _market_row_ts(row: dict[str, Any]) -> str:
    return str(row.get("begins_at") or row.get("beginsAt") or row.get("time") or "")


def _market_row_session(row: dict[str, Any]) -> str:
    return str(row.get("session") or "").strip().lower()


def _market_extended_session_counts(rows: list[dict[str, Any]]) -> tuple[int, int]:
    pre = 0
    post = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        sess = _market_row_session(row)
        if sess in {"pre", "premarket", "pre_market"}:
            pre += 1
        elif sess in {"post", "afterhours", "after_hours", "postmarket"}:
            post += 1
    return pre, post


def _market_extract_ohlcv(
    rows: list[dict[str, Any]],
) -> tuple[list[float], list[float], list[float], list[float], list[float], list[str]]:
    opens: list[float] = []
    highs: list[float] = []
    lows: list[float] = []
    closes: list[float] = []
    volumes: list[float] = []
    timestamps: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            o = float(row.get("open_price") if row.get("open_price") is not None else row.get("open"))
            h = float(row.get("high_price") if row.get("high_price") is not None else row.get("high"))
            l = float(row.get("low_price") if row.get("low_price") is not None else row.get("low"))
            c = float(row.get("close_price") if row.get("close_price") is not None else row.get("close"))
        except Exception:
            continue
        if min(o, h, l, c) <= 0 or h < l or o < l or o > h or c < l or c > h:
            continue
        opens.append(o)
        highs.append(h)
        lows.append(l)
        closes.append(c)
        volume_raw = None
        for key in (
            "volume",
            "volume_traded",
            "volume_traded_units",
            "session_volume",
            "total_volume",
        ):
            if row.get(key) not in (None, "", "None"):
                volume_raw = row.get(key)
                break
        vol = _to_float_opt(volume_raw)
        volumes.append(float(vol) if vol is not None and math.isfinite(float(vol)) else 0.0)
        timestamps.append(_market_row_ts(row))
    return opens, highs, lows, closes, volumes, timestamps


def _market_extract_ohlc(rows: list[dict[str, Any]]) -> tuple[list[float], list[float], list[float], list[float]]:
    opens, highs, lows, closes, _volumes, _timestamps = _market_extract_ohlcv(rows)
    return opens, highs, lows, closes


def _market_merge_historical_rows(base: list[dict[str, Any]], extra: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for row in list(base or []) + list(extra or []):
        if not isinstance(row, dict):
            continue
        key = _market_row_ts(row)
        if not key:
            continue
        old = merged.get(key)
        if old is None:
            merged[key] = row
            continue
        old_has_close = old.get("close_price") not in (None, "None", "")
        new_has_close = row.get("close_price") not in (None, "None", "")
        if new_has_close or not old_has_close:
            merged[key] = row
    return [merged[k] for k in sorted(merged.keys())]


def _market_log_historical_candles(
    *,
    symbol: str,
    timeframe: str,
    extended_enabled: bool,
    requested_bounds: str,
    raw_rows: list[dict[str, Any]],
    chart_count: int,
    indicator_count: int,
    synthetically_modified: bool,
) -> None:
    pre_count, post_count = _market_extended_session_counts(raw_rows)
    first_ts = _market_row_ts(raw_rows[0]) if raw_rows else ""
    latest_ts = _market_row_ts(raw_rows[-1]) if raw_rows else ""
    logging.getLogger(__name__).info(
        "Historical candles symbol=%s timeframe=%s extended_hours_enabled=%s requested_bounds=%s "
        "raw_candle_count=%s chart_candle_count=%s indicator_candle_count=%s first_ts=%s latest_ts=%s "
        "premarket_candles=%s after_hours_candles=%s source=robin_stocks synthetically_modified=%s",
        symbol,
        timeframe,
        bool(extended_enabled),
        requested_bounds,
        len(raw_rows),
        chart_count,
        indicator_count,
        first_ts,
        latest_ts,
        pre_count,
        post_count,
        bool(synthetically_modified),
    )
    if (
        _market_should_note_missing_extended_candles(
            timeframe=timeframe,
            extended_enabled=extended_enabled,
            requested_bounds=requested_bounds,
        )
        and pre_count == 0
        and post_count == 0
    ):
        logging.getLogger(__name__).info(
            "Extended-hours candles not returned; using regular-session candles "
            "symbol=%s timeframe=%s requested_bounds=%s raw_candle_count=%s",
            symbol,
            timeframe,
            requested_bounds,
            len(raw_rows),
        )


def _market_should_note_missing_extended_candles(
    *,
    timeframe: str,
    extended_enabled: bool,
    requested_bounds: str,
) -> bool:
    if not bool(extended_enabled):
        return False
    requested = str(requested_bounds or "").strip().lower()
    if requested in ("", "regular", "24_7", "synthetic_from_non_robinhood_closes"):
        return False
    interval, _span = _markets_timeframe(timeframe)
    if str(interval or "").strip().lower() in ("day", "week"):
        return False
    return True


def _market_fetch_ohlc_rows(
    symbol: str,
    timeframe: str,
    broker_hint: str = "robinhood",
    *,
    min_candles: int = 0,
    include_extended: bool = False,
) -> tuple[list[dict[str, Any]], str]:
    hint = _normalize_market_source_hint(broker_hint)
    interval, default_span = _markets_timeframe(timeframe)
    need = max(0, int(min_candles or 0))

    if hint == "robinhood_crypto" and rh is not None:
        spans = _market_span_candidates(
            interval=interval,
            default_span=default_span,
            min_candles=need,
            is_crypto=True,
        )
        best_rows: list[dict[str, Any]] = []

        def _fetch_crypto(cur_span: str) -> list[dict[str, Any]]:
            if _rh_adapter_get_crypto_historicals is None:
                rows = rh.crypto.get_crypto_historicals(symbol, interval=interval, span=cur_span, bounds="24_7")
            else:
                rows = _rh_adapter_get_crypto_historicals(symbol, interval=interval, span=cur_span, bounds="24_7")
            return rows if isinstance(rows, list) else []

        for cur_span in spans:
            try:
                rows = _fetch_crypto(cur_span)
            except Exception:
                rows = []
            if len(rows) > len(best_rows):
                best_rows = rows
            if need and len(rows) >= need:
                break
        return best_rows, "24_7"

    if hint != "robinhood" or rh is None:
        closes = _market_fetch_closes(
            symbol,
            timeframe,
            broker_hint=broker_hint,
            min_candles=min_candles,
            include_extended=include_extended,
            append_live_quote=False,
        )
        o, h, l, c = _market_synthetic_ohlc_from_closes(closes)
        rows = [
            {"open_price": str(o[i]), "high_price": str(h[i]), "low_price": str(l[i]), "close_price": str(c[i])}
            for i in range(len(c))
        ]
        return rows, "synthetic_from_non_robinhood_closes"

    spans = _market_span_candidates(interval=interval, default_span=default_span, min_candles=need, is_crypto=False)
    best_rows: list[dict[str, Any]] = []
    requested_bounds = "extended" if bool(include_extended) and interval not in ("day", "week") else "regular"

    def _fetch(cur_span: str, bounds: str) -> list[dict[str, Any]]:
        if interval == "10minute" and _rh_adapter_get_10m_stock_historicals is not None:
            try:
                rows = _rh_adapter_get_10m_stock_historicals(
                    symbol,
                    span=cur_span,
                    bounds=bounds,
                    min_candles=max(0, min(need, _ROBINHOOD_STOCK_BACKTEST_CAPACITY.get("10m", need))),
                    allow_partial=True,
                )
            except RuntimeError:
                rows = []
        elif _rh_adapter_get_stock_historicals is not None:
            rows = _rh_adapter_get_stock_historicals(symbol, interval=interval, span=cur_span, bounds=bounds)
        else:
            rows = rh.stocks.get_stock_historicals(symbol, interval=interval, span=cur_span, bounds=bounds)
        return rows if isinstance(rows, list) else []

    for cur_span in spans:
        base_bounds = "regular"
        if bool(include_extended) and cur_span == "day" and interval not in ("day", "week"):
            base_bounds = "extended"
        rows = _fetch(cur_span, base_bounds)
        if bool(include_extended) and interval not in ("day", "week") and cur_span != "day":
            rows = _market_merge_historical_rows(rows, _fetch("day", "extended"))
        if len(rows) > len(best_rows):
            best_rows = rows
        if need and len(rows) >= need:
            break
    return best_rows, requested_bounds


def _market_fetch_ohlcv(
    symbol: str,
    timeframe: str,
    broker_hint: str = "robinhood",
    *,
    min_candles: int = 0,
    include_extended: bool = False,
) -> tuple[list[float], list[float], list[float], list[float], list[float], list[str], list[dict[str, Any]], str]:
    rows, requested_bounds = _market_fetch_ohlc_rows(
        symbol,
        timeframe,
        broker_hint=broker_hint,
        min_candles=min_candles,
        include_extended=include_extended,
    )
    opens, highs, lows, closes, volumes, timestamps = _market_extract_ohlcv(rows)
    return opens, highs, lows, closes, volumes, timestamps, rows, requested_bounds


def _market_fetch_ohlc(
    symbol: str,
    timeframe: str,
    broker_hint: str = "robinhood",
    *,
    min_candles: int = 0,
    include_extended: bool = False,
) -> tuple[list[float], list[float], list[float], list[float], list[dict[str, Any]], str]:
    opens, highs, lows, closes, _volumes, _timestamps, rows, requested_bounds = _market_fetch_ohlcv(
        symbol,
        timeframe,
        broker_hint=broker_hint,
        min_candles=min_candles,
        include_extended=include_extended,
    )
    return opens, highs, lows, closes, rows, requested_bounds


def _market_signal(
    *,
    price: float,
    ma30: Optional[float],
    ma78: Optional[float],
    ma190: Optional[float],
    rsi: Optional[float],
    drsi: Optional[float],
    d30: Optional[float],
) -> str:
    if None in (ma30, ma78, ma190, rsi, drsi, d30):
        return "HOLD"
    buy = bool(price > float(ma30) and price < float(ma78) and price < float(ma190) and float(drsi) > 0 and float(d30) > 0)
    sell = bool(
        (price > float(ma190) and price > float(ma78) and price > float(ma30) and float(rsi) > 70 and float(drsi) < 0)
        or (price > float(ma190) and price > float(ma78) and float(d30) < 0)
    )
    if sell:
        return "SELL"
    if buy:
        return "BUY"
    return "HOLD"


def _fmt_market_num(val: Any, digits: int = 3) -> str:
    try:
        if val is None:
            return "—"
        return f"{float(val):.{digits}f}"
    except Exception:
        return "—"


def _first_param_value(params: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in params:
            return params.get(key)
    return None


def _ichimoku_lengths_from_params(params: dict[str, Any]) -> tuple[int, int, int, int]:
    conversion = max(1, int(_to_int_opt(_first_param_value(params, "conversion_line_length", "tenkan_length")) or 9))
    base = max(1, int(_to_int_opt(_first_param_value(params, "base_line_length", "kijun_length")) or 26))
    leading_b = max(2, int(_to_int_opt(_first_param_value(params, "leading_span_b_length", "senkou_b_length")) or 52))
    displacement = max(
        1,
        int(
            _to_int_opt(
                _first_param_value(params, "lagging_line_displacement", "leading_line_displacement", "displacement")
            )
            or 26
        ),
    )
    return conversion, base, leading_b, displacement


def _ichimoku_base_bounce_tolerance_pct(params: dict[str, Any]) -> float:
    return float(
        _to_float_opt(_first_param_value(params, "base_line_bounce_tolerance_pct", "kijun_bounce_tolerance_pct"))
        or 0.35
    )


_MARKETS_LINE_PALETTE = [
    "#60a5fa",  # blue
    "#34d399",  # green
    "#f59e0b",  # amber
    "#f43f5e",  # rose
    "#a78bfa",  # violet
    "#22d3ee",  # cyan
    "#f97316",  # orange
    "#84cc16",  # lime
    "#e879f9",  # fuchsia
    "#fb7185",  # pink
    "#2dd4bf",  # teal
    "#facc15",  # yellow
]
_MARKETS_LINE_COLOR_CACHE: dict[str, str] = {}
_MARKETS_LINE_COLOR_NEXT = 0


def _markets_line_color(key: str) -> str:
    global _MARKETS_LINE_COLOR_NEXT
    s = str(key or "").strip().lower()
    if not s:
        s = "default"
    c = _MARKETS_LINE_COLOR_CACHE.get(s)
    if c:
        return c
    c = _MARKETS_LINE_PALETTE[_MARKETS_LINE_COLOR_NEXT % len(_MARKETS_LINE_PALETTE)]
    _MARKETS_LINE_COLOR_CACHE[s] = c
    _MARKETS_LINE_COLOR_NEXT += 1
    return c


def _rule_line_color(rule: dict[str, Any]) -> str:
    kind_raw = str(rule.get("kind") or "").strip().lower()
    if kind_raw in ("bollinger", "bollinger_bands"):
        kind = "bb"
    elif kind_raw in ("ichimoku", "ichimoku_cloud", "ichi"):
        kind = "ichimoku"
    elif kind_raw in ("ttm", "ttm_squeeze", "squeeze_momentum"):
        kind = "ttm"
    elif kind_raw in ("roc", "rate_of_change"):
        kind = "roc"
    elif kind_raw in ("sar", "psar", "parabolic_sar", "parabolic"):
        kind = "sar"
    elif kind_raw in ("donchian", "donchian_breakout", "donchian_channel", "donchian_channels"):
        kind = "donchian"
    elif kind_raw in ("pivot", "pivot_points", "pivots"):
        kind = "pivot"
    elif kind_raw in ("supertrend", "supertrend_trend"):
        kind = "supertrend"
    elif kind_raw in ("vwap", "vwap_filter"):
        kind = "vwap"
    elif kind_raw in ("relative_volume", "rvol", "rel_volume"):
        kind = "relative_volume"
    else:
        kind = kind_raw
    params = rule.get("params") if isinstance(rule.get("params"), dict) else {}
    if kind in ("ma", "ema"):
        if _normalize_ma_mode(params.get("mode"), default="single") == "ribbon":
            return "#93c5fd"
        ln = max(2, int(_to_int_opt(params.get("length")) or 30))
        mtype = _normalize_ma_type(params.get("ma_type"), default=("ema" if kind == "ema" else "sma"))
        prefix = "ema" if mtype == "ema" else "ma"
        return _markets_line_color(f"{prefix}:{ln}")
    if kind == "macd":
        f = max(2, int(_to_int_opt(params.get("fast_length")) or 12))
        s = max(2, int(_to_int_opt(params.get("slow_length")) or 26))
        g = max(2, int(_to_int_opt(params.get("signal_length")) or 9))
        return _markets_line_color(f"macd:{f}:{s}:{g}")
    if kind == "rsi":
        return "#a78bfa"
    if kind == "rsi_d":
        return "#22d3ee"
    if kind == "bb":
        return "#f472b6"
    if kind == "ichimoku":
        return "#34d399"
    if kind == "ttm":
        return "#06b6d4"
    if kind == "roc":
        return "#f59e0b"
    if kind == "sar":
        return "#f43f5e"
    if kind == "donchian":
        return "#38bdf8"
    if kind == "pivot":
        return "#f59e0b"
    if kind == "supertrend":
        return "#22c55e"
    if kind == "vwap":
        return "#facc15"
    if kind == "relative_volume":
        return "#fb7185"
    if kind in ("heikin_ashi", "ha"):
        return "#f97316"
    return "#e8ecff"


def _rule_name(rule: dict[str, Any]) -> str:
    n = str(rule.get("name") or "").strip()
    if n:
        return n
    kind = str(rule.get("kind") or "").strip().upper()
    return kind or "RULE"


_MACD_MODE_LABELS: dict[str, str] = {
    "signal_cross": "Signal Cross",
    "cross_regime": "Cross + Regime",
    "hist_momentum": "Histogram Momentum",
    "zero_reclaim_loss": "Zero-Line Reclaim/Loss",
    "macd_derivative_sign": "MACD Derivative Sign",
}

_MACD_MODE_SUMMARIES: dict[str, str] = {
    "signal_cross": "Buy when MACD is above signal; sell when MACD is below signal.",
    "cross_regime": (
        "Buy only on bullish MACD/signal cross while MACD is above zero; "
        "sell only on bearish cross while MACD is below zero."
    ),
    "hist_momentum": "Buy on positive-rising histogram; sell on negative-falling histogram.",
    "zero_reclaim_loss": (
        "Buy when MACD is above zero and above signal; sell when MACD is below signal while both are above zero."
    ),
    "macd_derivative_sign": (
        "Buy when MACD derivative is above the configured buy threshold; "
        "sell when MACD derivative is below the configured sell threshold, "
        "with selectable side mode (BUY only, SELL only, or both)."
    ),
}


def _normalize_rule_target_ids(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, str):
        return [s.strip() for s in raw.split(",") if s.strip()]
    if isinstance(raw, list):
        out: list[str] = []
        for item in raw:
            txt = str(item or "").strip()
            if txt:
                out.append(txt)
        return out
    return []


def _rule_override_scope_label(scope: Any) -> str:
    s = str(scope or "both").strip().lower()
    if s == "buy":
        return "BUY only"
    if s == "sell":
        return "SELL only"
    return "BUY and SELL"


_INDICATOR_OVERRIDE_NOTE_RE = re.compile(
    r"(?i)(rsi|macd|ha|heikin_ashi)\s+override\((both|buy|sell)\)->(buy|sell)\s+by\s+([^|]+)"
)


def _rule_summary_num(value: Any, digits: int = 4) -> str:
    v = _to_float_opt(value)
    if v is None:
        return "—"
    return _fmt_market_num(v, digits)


def _normalize_relation_mode(value: Any, *, default: str = "hold") -> str:
    s = str(value or "").strip().lower()
    if not s:
        s = str(default or "hold").strip().lower()
    if s == "ignore":
        s = "hold"
    if s in ("above", "below", "hold"):
        return s
    return "hold"


def _normalize_signal_action_mode(value: Any, *, default: str = "hold") -> str:
    s = str(value or "").strip().lower()
    if not s:
        s = str(default or "hold").strip().lower()
    if s == "ignore":
        s = "hold"
    if s in ("buy", "sell", "hold"):
        return s
    return "hold"


def _normalize_dual_signal_scope(value: Any, *, default: str = "both") -> str:
    s = str(value or "").strip().lower()
    if not s:
        s = str(default or "both").strip().lower()
    if s in ("buy", "sell"):
        return s
    return "both"


_MA_RIBBON_LEVEL_ORDER: tuple[str, str, str] = ("short", "medium", "long")
_MA_RIBBON_DEFAULT_LENGTHS: dict[str, int] = {"short": 30, "medium": 78, "long": 190}
_MA_RIBBON_LEVEL_LABELS: dict[str, str] = {"short": "Short", "medium": "Medium", "long": "Long"}


def _normalize_ma_mode(value: Any, *, default: str = "single") -> str:
    s = str(value or "").strip().lower()
    if not s:
        s = str(default or "single").strip().lower()
    if s in ("ribbon", "ma_ribbon"):
        return "ribbon"
    if s in ("mapped", "level", "action_map", "ribbon_level"):
        return "mapped"
    return "single"


def _normalize_ma_type(value: Any, *, default: str = "sma") -> str:
    s = str(value or "").strip().lower()
    if not s:
        s = str(default or "sma").strip().lower()
    return "ema" if s == "ema" else "sma"


def _ma_ribbon_levels_from_params(params: Any) -> list[dict[str, Any]]:
    cfg = params if isinstance(params, dict) else {}
    levels_raw = cfg.get("levels")
    levels_by_slot: dict[str, dict[str, Any]] = {}
    if isinstance(levels_raw, list):
        for raw in levels_raw:
            if not isinstance(raw, dict):
                continue
            slot = str(raw.get("slot") or raw.get("name") or "").strip().lower()
            if slot in _MA_RIBBON_LEVEL_ORDER and slot not in levels_by_slot:
                levels_by_slot[slot] = raw

    def _pick(raw_value: Any, *fallbacks: Any) -> Any:
        if raw_value not in (None, "", "None"):
            return raw_value
        for item in fallbacks:
            if item not in (None, "", "None"):
                return item
        return None

    out: list[dict[str, Any]] = []
    for slot in _MA_RIBBON_LEVEL_ORDER:
        raw = levels_by_slot.get(slot, {})
        raw_type = raw.get("ma_type") if isinstance(raw, dict) else None
        raw_length = raw.get("length") if isinstance(raw, dict) else None
        raw_above = raw.get("above_action") if isinstance(raw, dict) else None
        raw_below = raw.get("below_action") if isinstance(raw, dict) else None
        ma_type = _normalize_ma_type(
            _pick(raw_type, cfg.get(f"{slot}_type"), cfg.get(f"ribbon_{slot}_type")),
            default="sma",
        )
        length = max(
            2,
            int(
                _to_int_opt(
                    _pick(raw_length, cfg.get(f"{slot}_length"), cfg.get(f"ribbon_{slot}_length"))
                )
                or _MA_RIBBON_DEFAULT_LENGTHS[slot]
            ),
        )
        above_action = _normalize_signal_action_mode(
            _pick(raw_above, cfg.get(f"{slot}_above_action"), cfg.get(f"ribbon_{slot}_above_action")),
            default="hold",
        )
        below_action = _normalize_signal_action_mode(
            _pick(raw_below, cfg.get(f"{slot}_below_action"), cfg.get(f"ribbon_{slot}_below_action")),
            default="hold",
        )
        out.append(
            {
                "slot": slot,
                "label": _MA_RIBBON_LEVEL_LABELS[slot],
                "ma_type": ma_type,
                "length": length,
                "above_action": above_action,
                "below_action": below_action,
            }
        )
    return out


def _ma_ribbon_level_signal(
    price: float,
    line_value: float,
    *,
    above_action: str,
    below_action: str,
) -> tuple[str, str]:
    if float(price) > float(line_value):
        return above_action, "above"
    if float(price) < float(line_value):
        return below_action, "below"
    return "hold", "equal"


def _ma_ribbon_level_rule_id(parent_rule_id: Any, slot: Any) -> str:
    rid = str(parent_rule_id or "").strip()
    slot_key = str(slot or "").strip().lower()
    if not rid or slot_key not in _MA_RIBBON_LEVEL_ORDER:
        return ""
    return f"{rid}::{slot_key}"


def _ma_ribbon_level_name(parent_name: Any, level: dict[str, Any]) -> str:
    base_name = str(parent_name or "MA Ribbon").strip() or "MA Ribbon"
    slot = str(level.get("slot") or "").strip().lower()
    label = str(level.get("label") or _MA_RIBBON_LEVEL_LABELS.get(slot, slot.title())).strip() or "Level"
    ma_type = str(level.get("ma_type") or "sma").strip().lower()
    length = max(2, int(_to_int_opt(level.get("length")) or _MA_RIBBON_DEFAULT_LENGTHS.get(slot, 30)))
    line_tag = "EMA" if ma_type == "ema" else "SMA"
    return f"{base_name} - {label} {line_tag}{length}"


def _indicator_runtime_rule_entries(rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for idx, rule in enumerate(rules):
        if not isinstance(rule, dict):
            continue
        params = rule.get("params") if isinstance(rule.get("params"), dict) else {}
        kind = str(rule.get("kind") or "").strip().lower()
        name = _rule_name(rule)
        rid = str(params.get("rule_id") or "").strip()
        timeframe = _rule_timeframe(rule, "")
        if kind in ("ma", "ema") and _normalize_ma_mode(params.get("mode"), default="single") == "ribbon":
            for level in _ma_ribbon_levels_from_params(params):
                slot = str(level.get("slot") or "").strip().lower()
                child_id = _ma_ribbon_level_rule_id(rid, slot)
                color_rule = {
                    "kind": "ma",
                    "params": {
                        "mode": "single",
                        "ma_type": str(level.get("ma_type") or "sma"),
                        "length": int(_to_int_opt(level.get("length")) or _MA_RIBBON_DEFAULT_LENGTHS.get(slot, 30)),
                    },
                }
                out.append(
                    {
                        "key": f"idx:{idx}:{slot}",
                        "index": idx,
                        "name": _ma_ribbon_level_name(name, level),
                        "display_kind": "RIBBON LEVEL",
                        "kind": kind,
                        "timeframe": timeframe,
                        "rule": rule,
                        "rule_id": child_id,
                        "ribbon_slot": slot,
                        "color": _rule_line_color(color_rule),
                    }
                )
            continue
        out.append(
            {
                "key": f"idx:{idx}",
                "index": idx,
                "name": name,
                "display_kind": (kind.upper() or "RULE"),
                "kind": kind,
                "timeframe": timeframe,
                "rule": rule,
                "rule_id": rid,
                "ribbon_slot": "",
                "color": _rule_line_color(rule),
            }
        )
    return out


_BB_CONDITION_LABELS: dict[str, str] = {
    "hold": "hold",
    "touch_upper": "touch upper band",
    "touch_lower": "touch lower band",
    "close_outside_upper": "close outside upper band",
    "close_outside_lower": "close outside lower band",
    "reenter_from_above": "re-enter band from above",
    "reenter_from_below": "re-enter band from below",
    "width_increasing": "BB width increasing",
    "width_decreasing": "BB width decreasing",
    "width_below_threshold": "BB width below threshold (squeeze)",
    "width_expanding_from_squeeze": "BB width expanding from squeeze",
    "above_middle": "price above middle band",
    "below_middle": "price below middle band",
    "percent_b_above": "%B above threshold",
    "percent_b_below": "%B below threshold",
}


def _normalize_bb_condition(value: Any, *, default: str = "hold") -> str:
    raw = str(value or "").strip().lower()
    aliases: dict[str, str] = {
        "touch_upper_band": "touch_upper",
        "touch_lower_band": "touch_lower",
        "price_touches_upper_band": "touch_upper",
        "price_touches_lower_band": "touch_lower",
        "close_outside_band_upper": "close_outside_upper",
        "close_outside_band_lower": "close_outside_lower",
        "close_outside_upper_band": "close_outside_upper",
        "close_outside_lower_band": "close_outside_lower",
        "reenter_band_from_above": "reenter_from_above",
        "reenter_band_from_below": "reenter_from_below",
        "price_above_middle": "above_middle",
        "price_below_middle": "below_middle",
        "width_expanding_from_squeeze": "width_expanding_from_squeeze",
        "bb_width_expanding_from_squeeze": "width_expanding_from_squeeze",
    }
    s = aliases.get(raw, raw)
    if not s:
        s = str(default or "hold").strip().lower()
    if s not in _BB_CONDITION_LABELS:
        s = str(default or "hold").strip().lower()
    if s not in _BB_CONDITION_LABELS:
        s = "hold"
    return s


_ICHI_CONDITION_LABELS: dict[str, str] = {
    "hold": "hold",
    "price_above_cloud": "price above live cloud",
    "price_below_cloud": "price below live cloud",
    "price_inside_cloud": "price inside live cloud",
    "cloud_bullish": "live cloud bullish (Leading Span A > B)",
    "cloud_bearish": "live cloud bearish (Leading Span A < B)",
    "cloud_thickness_above": "live cloud thickness above threshold",
    "cloud_thickness_below": "live cloud thickness below threshold",
    "tenkan_above_kijun": "Conversion Line above Base Line",
    "tenkan_below_kijun": "Conversion Line below Base Line",
    "tenkan_cross_above": "Conversion Line crosses above Base Line",
    "tenkan_cross_below": "Conversion Line crosses below Base Line",
    "chikou_above_price": "Lagging Line above price (displacement back)",
    "chikou_below_price": "Lagging Line below price (displacement back)",
    "strong_long_confirm": "stacked long confirmation",
    "strong_short_confirm": "stacked short confirmation",
    "cloud_breakout_bullish": "bullish live cloud breakout",
    "cloud_breakout_bearish": "bearish live cloud breakdown",
    "future_twist_bullish": "future cloud twist bullish",
    "future_twist_bearish": "future cloud twist bearish",
    "approaching_future_twist_bullish": "approaching future bullish twist",
    "approaching_future_twist_bearish": "approaching future bearish twist",
    "delayed_bullish_cross_valid": "delayed bullish cross still valid",
    "delayed_bearish_cross_valid": "delayed bearish cross still valid",
    "bullish_cross_strong_above_cloud": "strong bullish cross (above live cloud)",
    "bullish_cross_medium_at_cloud": "medium bullish cross (at live cloud boundary)",
    "bullish_cross_weak_below_cloud": "weak bullish cross (below live cloud)",
    "bearish_cross_strong_below_cloud": "strong bearish cross (below live cloud)",
    "bearish_cross_medium_at_cloud": "medium bearish cross (at live cloud boundary)",
    "bearish_cross_weak_above_cloud": "weak bearish cross (above live cloud)",
    "price_above_tenkan_kijun_below_cloud": "price > Conversion Line/Base Line but below live cloud",
    "price_above_tenkan_kijun_inside_cloud": "price > Conversion Line/Base Line and inside live cloud",
    "price_below_tenkan_kijun_above_cloud": "price < Conversion Line/Base Line but above live cloud",
    "price_below_tenkan_kijun_inside_cloud": "price < Conversion Line/Base Line and inside live cloud",
    "price_extended_above_cloud": "price extended far above live cloud",
    "price_extended_below_cloud": "price extended far below live cloud",
    "price_stretched_above_kijun": "price stretched above Base Line",
    "price_stretched_below_kijun": "price stretched below Base Line",
    "kijun_flat": "flat Base Line",
    "kijun_rising": "rising Base Line",
    "kijun_falling": "falling Base Line",
    "tenkan_accelerating_up": "Conversion Line slope accelerating up",
    "tenkan_accelerating_down": "Conversion Line slope accelerating down",
    "shallow_cloud_entry_from_above": "shallow live cloud entry from above",
    "shallow_cloud_entry_from_below": "shallow live cloud entry from below",
    "deep_inside_cloud": "deep inside live cloud",
    "cloud_exit_up_with_momentum": "live cloud exit up with momentum",
    "cloud_exit_down_with_momentum": "live cloud exit down with momentum",
    "bullish_breakout_retest_hold": "bullish live cloud breakout retest holds",
    "bearish_breakdown_retest_fail": "bearish live cloud breakdown retest fails",
    "chikou_clears_past_cloud_bullish": "Lagging Line clears past cloud bullish",
    "chikou_clears_past_cloud_bearish": "Lagging Line clears past cloud bearish",
    "chikou_blocked_by_past_price": "Lagging Line blocked by past price",
    "chikou_in_congestion_zone": "Lagging Line in congestion zone",
    "cloud_expanding": "live cloud expanding",
    "cloud_contracting": "live cloud contracting",
    "cloud_thin_to_thick": "live cloud thin to thick transition",
    "bullish_to_neutral_transition": "bullish to neutral transition",
    "bearish_to_neutral_transition": "bearish to neutral transition",
    "partial_bullish_stack": "partial bullish stack (2/3)",
    "partial_bearish_stack": "partial bearish stack (2/3)",
    "full_bullish_stack": "full bullish stack (3/3)",
    "full_bearish_stack": "full bearish stack (3/3)",
    "bullish_stack_weakening": "bullish stack weakening",
    "bearish_stack_weakening": "bearish stack weakening",
    "kijun_bounce_bullish": "bullish Base Line bounce",
    "kijun_reject_bearish": "bearish Base Line rejection",
    "weak_cross_inside_cloud": "Conversion Line/Base Line cross inside live cloud (weak filter)",
    "cloud_rejection_bearish": "entered live cloud from above (rejection bearish)",
    "cloud_rejection_bullish": "entered live cloud from below (rejection bullish)",
}


_ICHI_ENGLISH_CONDITION_KEYS: dict[str, str] = {
    "cloud_bullish": "leading_span_a_above_leading_span_b",
    "cloud_bearish": "leading_span_a_below_leading_span_b",
    "tenkan_above_kijun": "conversion_line_above_base_line",
    "tenkan_below_kijun": "conversion_line_below_base_line",
    "tenkan_cross_above": "conversion_line_cross_above_base_line",
    "tenkan_cross_below": "conversion_line_cross_below_base_line",
    "chikou_above_price": "lagging_line_above_price",
    "chikou_below_price": "lagging_line_below_price",
    "price_above_tenkan_kijun_below_cloud": "price_above_conversion_base_below_cloud",
    "price_above_tenkan_kijun_inside_cloud": "price_above_conversion_base_inside_cloud",
    "price_below_tenkan_kijun_above_cloud": "price_below_conversion_base_above_cloud",
    "price_below_tenkan_kijun_inside_cloud": "price_below_conversion_base_inside_cloud",
    "price_stretched_above_kijun": "price_stretched_above_base_line",
    "price_stretched_below_kijun": "price_stretched_below_base_line",
    "kijun_flat": "base_line_flat",
    "kijun_rising": "base_line_rising",
    "kijun_falling": "base_line_falling",
    "tenkan_accelerating_up": "conversion_line_accelerating_up",
    "tenkan_accelerating_down": "conversion_line_accelerating_down",
    "chikou_clears_past_cloud_bullish": "lagging_line_clears_past_cloud_bullish",
    "chikou_clears_past_cloud_bearish": "lagging_line_clears_past_cloud_bearish",
    "chikou_blocked_by_past_price": "lagging_line_blocked_by_past_price",
    "chikou_in_congestion_zone": "lagging_line_in_congestion_zone",
    "kijun_bounce_bullish": "base_line_bounce_bullish",
    "kijun_reject_bearish": "base_line_reject_bearish",
    "weak_cross_inside_cloud": "conversion_base_cross_inside_cloud",
}


def _english_ichi_conditions(conditions: list[str]) -> list[str]:
    return [
        _ICHI_ENGLISH_CONDITION_KEYS.get(str(c or "").strip().lower(), str(c or "").strip().lower())
        for c in conditions
    ]


def _normalize_ichi_condition(value: Any, *, default: str = "hold") -> str:
    raw = str(value or "").strip().lower()
    aliases: dict[str, str] = {
        "above_cloud": "price_above_cloud",
        "below_cloud": "price_below_cloud",
        "inside_cloud": "price_inside_cloud",
        "span_a_above_span_b": "cloud_bullish",
        "span_a_below_span_b": "cloud_bearish",
        "leading_span_a_above_leading_span_b": "cloud_bullish",
        "leading_span_a_below_leading_span_b": "cloud_bearish",
        "leading_a_above_leading_b": "cloud_bullish",
        "leading_a_below_leading_b": "cloud_bearish",
        "senkou_a_above_senkou_b": "cloud_bullish",
        "senkou_a_below_senkou_b": "cloud_bearish",
        "conversion_line_crosses_above_base_line": "tenkan_cross_above",
        "conversion_line_crosses_below_base_line": "tenkan_cross_below",
        "conversion_line_cross_above_base_line": "tenkan_cross_above",
        "conversion_line_cross_below_base_line": "tenkan_cross_below",
        "conversion_line_above_base_line": "tenkan_above_kijun",
        "conversion_line_below_base_line": "tenkan_below_kijun",
        "price_above_conversion_base_below_cloud": "price_above_tenkan_kijun_below_cloud",
        "price_above_conversion_base_inside_cloud": "price_above_tenkan_kijun_inside_cloud",
        "price_below_conversion_base_above_cloud": "price_below_tenkan_kijun_above_cloud",
        "price_below_conversion_base_inside_cloud": "price_below_tenkan_kijun_inside_cloud",
        "price_stretched_above_base_line": "price_stretched_above_kijun",
        "price_stretched_below_base_line": "price_stretched_below_kijun",
        "base_line_flat": "kijun_flat",
        "base_line_rising": "kijun_rising",
        "base_line_falling": "kijun_falling",
        "conversion_line_accelerating_up": "tenkan_accelerating_up",
        "conversion_line_accelerating_down": "tenkan_accelerating_down",
        "base_line_bounce_bullish": "kijun_bounce_bullish",
        "base_line_reject_bearish": "kijun_reject_bearish",
        "conversion_base_cross_inside_cloud": "weak_cross_inside_cloud",
        "tenkan_crosses_above_kijun": "tenkan_cross_above",
        "tenkan_crosses_below_kijun": "tenkan_cross_below",
        "lagging_line_above": "chikou_above_price",
        "lagging_line_below": "chikou_below_price",
        "lagging_line_above_price": "chikou_above_price",
        "lagging_line_below_price": "chikou_below_price",
        "lagging_line_clears_past_cloud_bullish": "chikou_clears_past_cloud_bullish",
        "lagging_line_clears_past_cloud_bearish": "chikou_clears_past_cloud_bearish",
        "lagging_line_blocked_by_past_price": "chikou_blocked_by_past_price",
        "lagging_line_in_congestion_zone": "chikou_in_congestion_zone",
        "chikou_above": "chikou_above_price",
        "chikou_below": "chikou_below_price",
        "full_trend_confirmation": "strong_long_confirm",
        "full_trend_confirmation_long": "strong_long_confirm",
        "full_trend_confirmation_short": "strong_short_confirm",
        "cloud_breakout": "cloud_breakout_bullish",
        "cloud_breakdown": "cloud_breakout_bearish",
        "future_twist_up": "future_twist_bullish",
        "future_twist_down": "future_twist_bearish",
        "strong_bull_cross": "bullish_cross_strong_above_cloud",
        "medium_bull_cross": "bullish_cross_medium_at_cloud",
        "weak_bull_cross": "bullish_cross_weak_below_cloud",
        "strong_bear_cross": "bearish_cross_strong_below_cloud",
        "medium_bear_cross": "bearish_cross_medium_at_cloud",
        "weak_bear_cross": "bearish_cross_weak_above_cloud",
        "above_tk_kj_below_cloud": "price_above_tenkan_kijun_below_cloud",
        "above_tk_kj_inside_cloud": "price_above_tenkan_kijun_inside_cloud",
        "below_tk_kj_above_cloud": "price_below_tenkan_kijun_above_cloud",
        "below_tk_kj_inside_cloud": "price_below_tenkan_kijun_inside_cloud",
        "kijun_bounce": "kijun_bounce_bullish",
        "kijun_rejection": "kijun_reject_bearish",
        "weak_signal_filter": "weak_cross_inside_cloud",
        "cloud_rejection": "cloud_rejection_bearish",
    }
    s = aliases.get(raw, raw)
    if not s:
        s = str(default or "hold").strip().lower()
    if s not in _ICHI_CONDITION_LABELS:
        s = str(default or "hold").strip().lower()
    if s not in _ICHI_CONDITION_LABELS:
        s = "hold"
    return s


def _normalize_ichi_conditions(value: Any, *, default: str = "hold") -> list[str]:
    raw_items: list[Any] = []
    if isinstance(value, list):
        raw_items = list(value)
    elif isinstance(value, str):
        raw = str(value or "").strip()
        if "," in raw:
            raw_items = [part.strip() for part in raw.split(",")]
        elif raw:
            raw_items = [raw]
    elif value is not None:
        raw_items = [value]

    out: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        cond = _normalize_ichi_condition(item, default=default)
        if cond in seen:
            continue
        seen.add(cond)
        out.append(cond)

    if not out:
        out = [_normalize_ichi_condition(default, default="hold")]
    if len(out) > 1:
        out = [c for c in out if c != "hold"]
    if not out:
        out = ["hold"]
    return out


def _normalize_ichi_match_mode(value: Any, *, default: str = "all") -> str:
    mode = str(value or default or "all").strip().lower()
    return "any" if mode == "any" else "all"


_TTM_CONDITION_LABELS: dict[str, str] = {
    "hold": "hold",
    "squeeze_on": "squeeze on (BB inside KC)",
    "squeeze_off": "squeeze off (BB outside KC)",
    "squeeze_fired": "squeeze fired (on -> off)",
    "momentum_above_zero": "momentum above zero",
    "momentum_below_zero": "momentum below zero",
    "momentum_increasing": "momentum increasing",
    "momentum_decreasing": "momentum decreasing",
    "momentum_cross_up": "momentum crosses up through zero",
    "momentum_cross_down": "momentum crosses down through zero",
    "long_trend": "long trend (squeeze off + momentum > 0 + rising)",
    "short_trend": "short trend (squeeze off + momentum < 0 + falling)",
    "long_release": "long release (squeeze fired + momentum > 0)",
    "short_release": "short release (squeeze fired + momentum < 0)",
}


def _normalize_ttm_condition(value: Any, *, default: str = "hold") -> str:
    raw = str(value or "").strip().lower()
    aliases: dict[str, str] = {
        "ttm_squeeze_on": "squeeze_on",
        "ttm_squeeze_off": "squeeze_off",
        "squeeze_release": "squeeze_fired",
        "squeeze_fire": "squeeze_fired",
        "momentum_positive": "momentum_above_zero",
        "momentum_negative": "momentum_below_zero",
        "momentum_rising": "momentum_increasing",
        "momentum_falling": "momentum_decreasing",
        "mom_cross_up": "momentum_cross_up",
        "mom_cross_down": "momentum_cross_down",
        "trend_long": "long_trend",
        "trend_short": "short_trend",
        "release_long": "long_release",
        "release_short": "short_release",
    }
    s = aliases.get(raw, raw)
    if not s:
        s = str(default or "hold").strip().lower()
    if s not in _TTM_CONDITION_LABELS:
        s = str(default or "hold").strip().lower()
    if s not in _TTM_CONDITION_LABELS:
        s = "hold"
    return s


_ROC_CONDITION_LABELS: dict[str, str] = {
    "hold": "hold",
    "roc_above_threshold": "ROC above threshold",
    "roc_below_threshold": "ROC below threshold",
    "roc_cross_up_zero": "ROC crosses above zero",
    "roc_cross_down_zero": "ROC crosses below zero",
    "roc_cross_up_threshold": "ROC crosses above threshold",
    "roc_cross_down_threshold": "ROC crosses below threshold",
    "roc_increasing": "ROC increasing",
    "roc_decreasing": "ROC decreasing",
    "roc_positive": "ROC positive",
    "roc_negative": "ROC negative",
    "momentum_long": "momentum long (positive + increasing)",
    "momentum_short": "momentum short (negative + decreasing)",
}


def _normalize_roc_condition(value: Any, *, default: str = "hold") -> str:
    raw = str(value or "").strip().lower()
    aliases: dict[str, str] = {
        "above_threshold": "roc_above_threshold",
        "below_threshold": "roc_below_threshold",
        "cross_up_zero": "roc_cross_up_zero",
        "cross_down_zero": "roc_cross_down_zero",
        "cross_up_threshold": "roc_cross_up_threshold",
        "cross_down_threshold": "roc_cross_down_threshold",
        "increasing": "roc_increasing",
        "decreasing": "roc_decreasing",
        "positive": "roc_positive",
        "negative": "roc_negative",
        "long_trend": "momentum_long",
        "short_trend": "momentum_short",
    }
    s = aliases.get(raw, raw)
    if not s:
        s = str(default or "hold").strip().lower()
    if s not in _ROC_CONDITION_LABELS:
        s = str(default or "hold").strip().lower()
    if s not in _ROC_CONDITION_LABELS:
        s = "hold"
    return s


_SAR_CONDITION_LABELS: dict[str, str] = {
    "hold": "hold",
    "price_above_sar": "price above SAR",
    "price_below_sar": "price below SAR",
    "sar_cross_up": "price crosses above SAR",
    "sar_cross_down": "price crosses below SAR",
    "sar_rising": "SAR rising",
    "sar_falling": "SAR falling",
    "trend_long": "trend long (price above SAR + rising SAR)",
    "trend_short": "trend short (price below SAR + falling SAR)",
}


def _normalize_sar_condition(value: Any, *, default: str = "hold") -> str:
    raw = str(value or "").strip().lower()
    aliases: dict[str, str] = {
        "above_sar": "price_above_sar",
        "below_sar": "price_below_sar",
        "cross_up": "sar_cross_up",
        "cross_down": "sar_cross_down",
        "price_cross_up": "sar_cross_up",
        "price_cross_down": "sar_cross_down",
        "sar_up": "sar_rising",
        "sar_down": "sar_falling",
        "long": "trend_long",
        "short": "trend_short",
    }
    s = aliases.get(raw, raw)
    if not s:
        s = str(default or "hold").strip().lower()
    if s not in _SAR_CONDITION_LABELS:
        s = str(default or "hold").strip().lower()
    if s not in _SAR_CONDITION_LABELS:
        s = "hold"
    return s


_DONCHIAN_CONDITION_LABELS: dict[str, str] = {
    "hold": "hold",
    "close_above_upper": "close above prior upper channel",
    "high_above_upper": "high breaks prior upper channel",
    "close_below_lower": "close below prior lower channel",
    "low_below_lower": "low breaks prior lower channel",
    "inside_channel": "close inside prior channel",
    "above_mid_inside": "close above midpoint and inside channel",
    "below_mid_inside": "close below midpoint and inside channel",
    "channel_slope_up": "both channel bands are rising",
    "channel_slope_down": "both channel bands are falling",
    "slope_up_above_mid_inside": "channel rising and close above midpoint inside channel",
    "slope_up_below_mid_inside": "channel rising and close below midpoint inside channel",
    "slope_down_above_mid_inside": "channel falling and close above midpoint inside channel",
    "slope_down_below_mid_inside": "channel falling and close below midpoint inside channel",
}


_PIVOT_CONDITION_LABELS: dict[str, str] = {
    "hold": "Hold / Ignore Pivot Points",
    "above_p": "Price Above Pivot",
    "below_p": "Price Below Pivot",
    "above_r1": "Price Above R1",
    "below_s1": "Price Below S1",
    "cross_above_p": "Price Crosses Above Pivot",
    "cross_below_p": "Price Crosses Below Pivot",
    "cross_above_r1": "Price Crosses Above R1",
    "cross_below_s1": "Price Crosses Below S1",
    "near_resistance": "Price Near Resistance",
    "near_support": "Price Near Support",
}


def _normalize_pivot_condition(value: Any, *, default: str = "hold") -> str:
    raw = str(value or "").strip().lower()
    aliases: dict[str, str] = {
        "above_pivot": "above_p",
        "below_pivot": "below_p",
        "pivot_cross_up": "cross_above_p",
        "pivot_cross_down": "cross_below_p",
        "break_r1": "cross_above_r1",
        "break_s1": "cross_below_s1",
        "near_r": "near_resistance",
        "near_s": "near_support",
    }
    s = aliases.get(raw, raw)
    if not s:
        s = str(default or "hold").strip().lower()
    if s not in _PIVOT_CONDITION_LABELS:
        s = str(default or "hold").strip().lower()
    if s not in _PIVOT_CONDITION_LABELS:
        s = "hold"
    return s


def _pivot_condition_hit(
    cond: str,
    *,
    close_now: float,
    close_prev: Optional[float],
    levels: Any,
    tolerance_pct: float,
) -> bool:
    c = _normalize_pivot_condition(cond, default="hold")
    if c == "hold" or levels is None:
        return False
    level_map = levels.as_dict()
    tol = max(0.0, float(tolerance_pct)) / 100.0

    def _above(name: str) -> bool:
        return float(close_now) > float(level_map[name])

    def _below(name: str) -> bool:
        return float(close_now) < float(level_map[name])

    def _cross_above(name: str) -> bool:
        return close_prev is not None and float(close_prev) <= float(level_map[name]) < float(close_now)

    def _cross_below(name: str) -> bool:
        return close_prev is not None and float(close_prev) >= float(level_map[name]) > float(close_now)

    def _near(names: tuple[str, ...]) -> bool:
        for name in names:
            level = float(level_map[name])
            if level > 0.0 and abs(float(close_now) - level) / level <= tol:
                return True
        return False

    if c == "above_p":
        return _above("P")
    if c == "below_p":
        return _below("P")
    if c == "above_r1":
        return _above("R1")
    if c == "below_s1":
        return _below("S1")
    if c == "cross_above_p":
        return _cross_above("P")
    if c == "cross_below_p":
        return _cross_below("P")
    if c == "cross_above_r1":
        return _cross_above("R1")
    if c == "cross_below_s1":
        return _cross_below("S1")
    if c == "near_resistance":
        return _near(("R1", "R2", "R3"))
    if c == "near_support":
        return _near(("S1", "S2", "S3"))
    return False


def _normalize_donchian_condition(value: Any, *, default: str = "hold") -> str:
    raw = str(value or "").strip().lower()
    aliases: dict[str, str] = {
        "breakout": "close_above_upper",
        "close_breakout": "close_above_upper",
        "upper_break": "close_above_upper",
        "high_break": "high_above_upper",
        "use_high_break": "high_above_upper",
        "breakdown": "close_below_lower",
        "close_breakdown": "close_below_lower",
        "lower_break": "close_below_lower",
        "low_break": "low_below_lower",
        "above_middle": "above_mid_inside",
        "above_mid": "above_mid_inside",
        "close_above_mid": "above_mid_inside",
        "close_above_middle": "above_mid_inside",
        "midpoint_long": "above_mid_inside",
        "below_middle": "below_mid_inside",
        "below_mid": "below_mid_inside",
        "close_below_mid": "below_mid_inside",
        "close_below_middle": "below_mid_inside",
        "midpoint_short": "below_mid_inside",
        "slope_up": "channel_slope_up",
        "bands_rising": "channel_slope_up",
        "channel_up": "channel_slope_up",
        "rising_channel": "channel_slope_up",
        "slope_down": "channel_slope_down",
        "bands_falling": "channel_slope_down",
        "channel_down": "channel_slope_down",
        "falling_channel": "channel_slope_down",
        "channel_up_above_mid": "slope_up_above_mid_inside",
        "rising_above_mid": "slope_up_above_mid_inside",
        "channel_up_below_mid": "slope_up_below_mid_inside",
        "rising_below_mid": "slope_up_below_mid_inside",
        "channel_down_above_mid": "slope_down_above_mid_inside",
        "falling_above_mid": "slope_down_above_mid_inside",
        "channel_down_below_mid": "slope_down_below_mid_inside",
        "falling_below_mid": "slope_down_below_mid_inside",
    }
    s = aliases.get(raw, raw)
    if not s:
        s = str(default or "hold").strip().lower()
    if s not in _DONCHIAN_CONDITION_LABELS:
        s = str(default or "hold").strip().lower()
    if s not in _DONCHIAN_CONDITION_LABELS:
        s = "hold"
    return s


_SUPERTREND_CONDITION_LABELS: dict[str, str] = {
    "hold": "Hold / Ignore Supertrend",
    "trend_up": "Trend Is Bullish",
    "trend_down": "Trend Is Bearish",
    "close_above_trend": "Price Closes Above Supertrend",
    "close_below_trend": "Price Closes Below Supertrend",
    "flip_up": "Trend Flips Bullish",
    "flip_down": "Trend Flips Bearish",
}

_SUPERTREND_CONDITION_DESCRIPTIONS: dict[str, str] = {
    "hold": "Supertrend does not create a signal for this side of the rule.",
    "trend_up": "Persistent state: true on every candle while Supertrend is bullish and its active line is below price.",
    "trend_down": "Persistent state: true on every candle while Supertrend is bearish and its active line is above price.",
    "close_above_trend": "State comparison: true when the current close is above the active Supertrend line; it does not require a reversal.",
    "close_below_trend": "State comparison: true when the current close is below the active Supertrend line; it does not require a reversal.",
    "flip_up": "One-candle event: true only when direction changes from bearish to bullish.",
    "flip_down": "One-candle event: true only when direction changes from bullish to bearish.",
}


def _normalize_supertrend_condition(value: Any, *, default: str = "hold") -> str:
    raw = str(value or "").strip().lower()
    aliases: dict[str, str] = {
        "up": "trend_up",
        "down": "trend_down",
        "bullish": "trend_up",
        "bearish": "trend_down",
        "price_above": "close_above_trend",
        "price_below": "close_below_trend",
        "cross_up": "flip_up",
        "cross_down": "flip_down",
    }
    s = aliases.get(raw, raw)
    if not s:
        s = str(default or "hold").strip().lower()
    if s not in _SUPERTREND_CONDITION_LABELS:
        s = str(default or "hold").strip().lower()
    if s not in _SUPERTREND_CONDITION_LABELS:
        s = "hold"
    return s


_VWAP_CONDITION_LABELS: dict[str, str] = {
    "hold": "hold",
    "price_above_vwap": "price above VWAP",
    "price_below_vwap": "price below VWAP",
    "within_band": "price inside VWAP pullback/extension band",
    "overextended_above": "price overextended above VWAP",
    "extended_below": "price extended below VWAP",
    "cross_above": "price crosses above VWAP",
    "cross_below": "price crosses below VWAP",
    "exit_below": "price below VWAP exit band",
}


def _normalize_vwap_condition(value: Any, *, default: str = "hold") -> str:
    raw = str(value or "").strip().lower()
    aliases: dict[str, str] = {
        "above": "price_above_vwap",
        "below": "price_below_vwap",
        "filter": "within_band",
        "vwap_filter": "within_band",
        "near_vwap": "within_band",
        "not_overextended": "within_band",
        "cross_up": "cross_above",
        "cross_down": "cross_below",
        "sell_below": "exit_below",
    }
    s = aliases.get(raw, raw)
    if not s:
        s = str(default or "hold").strip().lower()
    if s not in _VWAP_CONDITION_LABELS:
        s = str(default or "hold").strip().lower()
    if s not in _VWAP_CONDITION_LABELS:
        s = "hold"
    return s


_RELATIVE_VOLUME_CONDITION_LABELS: dict[str, str] = {
    "hold": "hold",
    "above_threshold": "relative volume above threshold",
    "below_threshold": "relative volume below threshold",
    "rising": "relative volume rising",
    "falling": "relative volume falling",
}


def _normalize_relative_volume_condition(value: Any, *, default: str = "hold") -> str:
    raw = str(value or "").strip().lower()
    aliases: dict[str, str] = {
        "above": "above_threshold",
        "below": "below_threshold",
        "volume_spike": "above_threshold",
        "spike": "above_threshold",
        "increasing": "rising",
        "decreasing": "falling",
    }
    s = aliases.get(raw, raw)
    if not s:
        s = str(default or "hold").strip().lower()
    if s not in _RELATIVE_VOLUME_CONDITION_LABELS:
        s = str(default or "hold").strip().lower()
    if s not in _RELATIVE_VOLUME_CONDITION_LABELS:
        s = "hold"
    return s


def _indicator_pct_decimal(value: Any, *, default: float) -> float:
    raw = _to_float_opt(value)
    if raw is None:
        return float(default)
    out = float(raw)
    if abs(out) > 1.0:
        out = out / 100.0
    return float(out)


def _indicator_rule_summary_lines(
    rule: dict[str, Any],
    *,
    rule_ref_by_id: Optional[dict[str, str]] = None,
) -> list[str]:
    kind_raw = str(rule.get("kind") or "").strip().lower()
    if kind_raw in ("bollinger", "bollinger_bands"):
        kind = "bb"
    elif kind_raw in ("ichimoku", "ichimoku_cloud", "ichi"):
        kind = "ichimoku"
    elif kind_raw in ("ttm", "ttm_squeeze", "squeeze_momentum"):
        kind = "ttm"
    elif kind_raw in ("roc", "rate_of_change"):
        kind = "roc"
    elif kind_raw in ("sar", "psar", "parabolic_sar", "parabolic"):
        kind = "sar"
    elif kind_raw in ("donchian", "donchian_breakout", "donchian_channel", "donchian_channels"):
        kind = "donchian"
    elif kind_raw in ("pivot", "pivot_points", "pivots"):
        kind = "pivot"
    elif kind_raw in ("supertrend", "supertrend_trend"):
        kind = "supertrend"
    elif kind_raw in ("vwap", "vwap_filter"):
        kind = "vwap"
    elif kind_raw in ("relative_volume", "rvol", "rel_volume"):
        kind = "relative_volume"
    else:
        kind = kind_raw
    params = rule.get("params") if isinstance(rule.get("params"), dict) else {}
    lines: list[str] = []

    if kind in ("ma", "ema"):
        ma_mode = _normalize_ma_mode(params.get("mode"), default="single")
        if ma_mode == "ribbon":
            lines.append("MA Ribbon: BUY/SELL only when short, medium, and long all agree; otherwise HOLD.")
            for level in _ma_ribbon_levels_from_params(params):
                line_tag = "EMA" if level["ma_type"] == "ema" else "SMA"
                lines.append(
                    f"{level['label']} {line_tag}{level['length']}: "
                    f"above -> {str(level['above_action']).upper()}; "
                    f"below -> {str(level['below_action']).upper()}."
                )
            return lines
        if ma_mode == "mapped":
            ma_type = _normalize_ma_type(params.get("ma_type"), default=("ema" if kind == "ema" else "sma"))
            line_tag = "EMA" if ma_type == "ema" else "SMA"
            length = max(2, int(_to_int_opt(params.get("length")) or 30))
            above_action = str(_normalize_signal_action_mode(params.get("above_action"), default="hold")).upper()
            below_action = str(_normalize_signal_action_mode(params.get("below_action"), default="hold")).upper()
            lines.append(f"{line_tag}{length}: above -> {above_action}; below -> {below_action}.")
            return lines

        ma_type = _normalize_ma_type(params.get("ma_type"), default=("ema" if kind == "ema" else "sma"))
        line_tag = "EMA" if ma_type == "ema" else "SMA"
        length = max(2, int(_to_int_opt(params.get("length")) or 30))
        buy_rel = _normalize_relation_mode(params.get("buy_relation"), default="hold")
        sell_rel = _normalize_relation_mode(params.get("sell_relation"), default="hold")
        buy_txt = "hold" if buy_rel == "hold" else f"price {buy_rel}"
        sell_txt = "hold" if sell_rel == "hold" else f"price {sell_rel}"
        lines.append(f"{line_tag}{length}: BUY={buy_txt}; SELL={sell_txt}.")

        track_d = bool(int(params.get("track_derivative") or 0))
        if track_d:
            dtag = "dEMA" if ma_type == "ema" else "dMA"
            buy_d = _to_float_opt(params.get("buy_derivative_min"))
            sell_d = _to_float_opt(params.get("sell_derivative_max"))
            buy_d_txt = "any" if buy_d is None else f">= {_rule_summary_num(buy_d, 4)}"
            sell_d_txt = "any" if sell_d is None else f"<= {_rule_summary_num(sell_d, 4)}"
            lines.append(f"{dtag}{length} filter: BUY {buy_d_txt}; SELL {sell_d_txt}.")

        unless_enabled = bool(int(params.get("unless_enabled") or 0))
        if unless_enabled:
            unless_rel = str(params.get("unless_relation") or "above").strip().lower()
            unless_type = _normalize_ma_type(params.get("unless_type"), default="sma")
            unless_length = max(2, int(_to_int_opt(params.get("unless_length")) or 30))
            unless_action = str(params.get("unless_action") or "sell").strip().lower()
            utag = "EMA" if unless_type == "ema" else "SMA"
            lines.append(
                f"Unless override: if {line_tag}{length} {unless_rel} {utag}{unless_length} "
                f"-> force {unless_action.upper()}."
            )
        return lines

    if kind == "bb":
        length = max(2, int(_to_int_opt(params.get("length")) or 20))
        std_mult = max(0.1, float(_to_float_opt(params.get("std_mult")) or 2.0))
        buy_cond = _normalize_bb_condition(params.get("buy_condition"), default="hold")
        sell_cond = _normalize_bb_condition(params.get("sell_condition"), default="hold")
        squeeze_threshold_pct = float(_to_float_opt(params.get("squeeze_threshold_pct")) or 5.0)
        pb_buy = float(_to_float_opt(params.get("percent_b_buy_threshold")) or 0.2)
        pb_sell = float(_to_float_opt(params.get("percent_b_sell_threshold")) or 0.8)
        buy_txt = _BB_CONDITION_LABELS.get(buy_cond, buy_cond)
        sell_txt = _BB_CONDITION_LABELS.get(sell_cond, sell_cond)
        lines.append(
            f"Bollinger ({length}, {std_mult:.2f}σ): BUY={buy_txt}; SELL={sell_txt}."
        )
        lines.append(
            f"Squeeze threshold: <= {_rule_summary_num(squeeze_threshold_pct, 3)}% width. "
            f"%B thresholds: BUY {_rule_summary_num(pb_buy, 3)}; SELL {_rule_summary_num(pb_sell, 3)}."
        )
        return lines

    if kind == "ichimoku":
        conversion, base, leading_b, displacement = _ichimoku_lengths_from_params(params)
        delayed_cross_lookback = max(1, int(_to_int_opt(params.get("delayed_cross_lookback")) or 3))
        buy_mode = _normalize_ichi_match_mode(params.get("buy_match_mode"), default="all")
        sell_mode = _normalize_ichi_match_mode(params.get("sell_match_mode"), default="all")
        block_mode = _normalize_ichi_match_mode(params.get("block_match_mode"), default="all")
        buy_conds = _normalize_ichi_conditions(params.get("buy_conditions", params.get("buy_condition")), default="hold")
        sell_conds = _normalize_ichi_conditions(params.get("sell_conditions", params.get("sell_condition")), default="hold")
        block_conds = _normalize_ichi_conditions(params.get("block_conditions", params.get("block_condition")), default="hold")
        thickness = float(_to_float_opt(params.get("cloud_thickness_threshold_pct")) or 1.0)
        bounce_tol = _ichimoku_base_bounce_tolerance_pct(params)
        buy_active = [c for c in buy_conds if c != "hold"]
        sell_active = [c for c in sell_conds if c != "hold"]
        block_active = [c for c in block_conds if c != "hold"]
        buy_txt = "hold" if not buy_active else " + ".join(_ICHI_CONDITION_LABELS.get(c, c) for c in buy_active)
        sell_txt = "hold" if not sell_active else " + ".join(_ICHI_CONDITION_LABELS.get(c, c) for c in sell_active)
        block_txt = "hold" if not block_active else " + ".join(_ICHI_CONDITION_LABELS.get(c, c) for c in block_active)
        lines.append(
            f"Ichimoku (Conversion/Base/Leading B {conversion}/{base}/{leading_b}, disp {displacement}): "
            f"BUY({buy_mode.upper()})={buy_txt}; "
            f"SELL({sell_mode.upper()})={sell_txt}; "
            f"BLOCK/HOLD({block_mode.upper()})={block_txt}."
        )
        lines.append(
            f"Cloud thickness threshold: {_rule_summary_num(thickness, 3)}%. "
            f"Base Line bounce tolerance: {_rule_summary_num(bounce_tol, 3)}%."
        )
        lines.append(f"Delayed cross lookback: {delayed_cross_lookback} bars.")
        return lines

    if kind == "ttm":
        bb_len = max(2, int(_to_int_opt(params.get("bb_length")) or 20))
        bb_mult = max(0.1, float(_to_float_opt(params.get("bb_mult")) or 2.0))
        kc_len = max(2, int(_to_int_opt(params.get("kc_length")) or 20))
        kc_mult = max(0.1, float(_to_float_opt(params.get("kc_mult")) or 1.5))
        mom_len = max(2, int(_to_int_opt(params.get("momentum_length")) or 20))
        buy_cond = _normalize_ttm_condition(params.get("buy_condition"), default="hold")
        sell_cond = _normalize_ttm_condition(params.get("sell_condition"), default="hold")
        buy_txt = _TTM_CONDITION_LABELS.get(buy_cond, buy_cond)
        sell_txt = _TTM_CONDITION_LABELS.get(sell_cond, sell_cond)
        lines.append(
            f"TTM Squeeze (BB {bb_len}/{bb_mult:.2f}, KC {kc_len}/{kc_mult:.2f}, MOM {mom_len}): "
            f"BUY={buy_txt}; SELL={sell_txt}."
        )
        lines.append("Squeeze is BB-inside-KC. Momentum uses linreg endpoint over centered price.")
        return lines

    if kind == "roc":
        length = max(1, int(_to_int_opt(params.get("length")) or 12))
        buy_cond = _normalize_roc_condition(params.get("buy_condition"), default="hold")
        sell_cond = _normalize_roc_condition(params.get("sell_condition"), default="hold")
        buy_thr = float(_to_float_opt(params.get("buy_threshold_pct")) or 0.0)
        sell_thr = float(_to_float_opt(params.get("sell_threshold_pct")) or 0.0)
        buy_txt = _ROC_CONDITION_LABELS.get(buy_cond, buy_cond)
        sell_txt = _ROC_CONDITION_LABELS.get(sell_cond, sell_cond)
        lines.append(f"ROC ({length}): BUY={buy_txt}; SELL={sell_txt}.")
        lines.append(
            f"Thresholds (%): BUY {_rule_summary_num(buy_thr, 3)}; "
            f"SELL {_rule_summary_num(sell_thr, 3)}."
        )
        return lines

    if kind == "sar":
        step = max(0.0001, float(_to_float_opt(params.get("step")) or 0.02))
        max_step = max(step, float(_to_float_opt(params.get("max_step")) or 0.2))
        buy_cond = _normalize_sar_condition(params.get("buy_condition"), default="hold")
        sell_cond = _normalize_sar_condition(params.get("sell_condition"), default="hold")
        buy_txt = _SAR_CONDITION_LABELS.get(buy_cond, buy_cond)
        sell_txt = _SAR_CONDITION_LABELS.get(sell_cond, sell_cond)
        lines.append(
            f"Parabolic SAR (step {_rule_summary_num(step, 4)}, max {_rule_summary_num(max_step, 4)}): "
            f"BUY={buy_txt}; SELL={sell_txt}."
        )
        lines.append(
            "SAR trails trend direction; cross conditions trigger only on the transition bar."
        )
        return lines

    if kind == "donchian":
        lookback = max(1, int(_to_int_opt(params.get("lookback")) or 20))
        default_buy = "high_above_upper" if bool(params.get("use_high_break")) else "close_above_upper"
        buy_cond = _normalize_donchian_condition(params.get("buy_condition"), default=default_buy)
        sell_cond = _normalize_donchian_condition(params.get("sell_condition"), default="close_below_lower")
        lines.append(
            f"Donchian ({lookback} prior bars): "
            f"BUY={_DONCHIAN_CONDITION_LABELS.get(buy_cond, buy_cond)}; "
            f"SELL={_DONCHIAN_CONDITION_LABELS.get(sell_cond, sell_cond)}."
        )
        return lines

    if kind == "pivot":
        buy_cond = _normalize_pivot_condition(params.get("buy_condition"), default="above_p")
        sell_cond = _normalize_pivot_condition(params.get("sell_condition"), default="below_p")
        tolerance_pct = max(0.0, float(_to_float_opt(params.get("tolerance_pct")) or 0.25))
        lines.append(
            f"Pivot Points (previous candle): "
            f"BUY={_PIVOT_CONDITION_LABELS.get(buy_cond, buy_cond)}; "
            f"SELL={_PIVOT_CONDITION_LABELS.get(sell_cond, sell_cond)}; "
            f"near tolerance {tolerance_pct:g}%."
        )
        return lines

    if kind == "supertrend":
        atr_length = max(1, int(_to_int_opt(params.get("atr_length")) or 10))
        multiplier = max(0.1, float(_to_float_opt(params.get("multiplier")) or 3.0))
        buy_cond = _normalize_supertrend_condition(params.get("buy_condition"), default="trend_up")
        sell_cond = _normalize_supertrend_condition(params.get("sell_condition"), default="trend_down")
        lines.append(
            f"ST({atr_length},{_rule_summary_num(multiplier, 3)}): "
            f"BUY={_SUPERTREND_CONDITION_LABELS.get(buy_cond, buy_cond)}; "
            f"SELL={_SUPERTREND_CONDITION_LABELS.get(sell_cond, sell_cond)}."
        )
        lines.append(
            "State rules can remain true for multiple candles; flip rules are one-candle trend-change events."
        )
        return lines

    if kind == "vwap":
        buy_cond = _normalize_vwap_condition(params.get("buy_condition"), default="within_band")
        sell_cond = _normalize_vwap_condition(params.get("sell_condition"), default="exit_below")
        max_extension = _indicator_pct_decimal(params.get("max_extension_pct"), default=0.015)
        max_pullback = _indicator_pct_decimal(params.get("max_pullback_pct"), default=0.010)
        exit_below = _indicator_pct_decimal(params.get("exit_below_pct"), default=0.012)
        lines.append(
            f"VWAP: BUY={_VWAP_CONDITION_LABELS.get(buy_cond, buy_cond)}; "
            f"SELL={_VWAP_CONDITION_LABELS.get(sell_cond, sell_cond)}."
        )
        lines.append(
            f"Band: pullback {_rule_summary_num(max_pullback * 100.0, 3)}%, "
            f"extension {_rule_summary_num(max_extension * 100.0, 3)}%, "
            f"exit below {_rule_summary_num(exit_below * 100.0, 3)}%."
        )
        return lines

    if kind == "relative_volume":
        length = max(1, int(_to_int_opt(params.get("length")) or 20))
        threshold = max(0.0, float(_to_float_opt(params.get("threshold")) or 1.2))
        buy_cond = _normalize_relative_volume_condition(params.get("buy_condition"), default="above_threshold")
        sell_cond = _normalize_relative_volume_condition(params.get("sell_condition"), default="below_threshold")
        lines.append(
            f"Relative Volume ({length}): "
            f"BUY={_RELATIVE_VOLUME_CONDITION_LABELS.get(buy_cond, buy_cond)}; "
            f"SELL={_RELATIVE_VOLUME_CONDITION_LABELS.get(sell_cond, sell_cond)}; "
            f"threshold {_rule_summary_num(threshold, 3)}."
        )
        return lines

    if kind == "rsi":
        oversold = _to_float_opt(params.get("oversold"))
        overbought = _to_float_opt(params.get("overbought"))
        os_rel = str(params.get("oversold_relation") or "below").strip().lower()
        ob_rel = str(params.get("overbought_relation") or "above").strip().lower()
        os_action = _normalize_signal_action_mode(params.get("oversold_action"), default="buy")
        ob_action = _normalize_signal_action_mode(params.get("overbought_action"), default="sell")
        lines.append(
            "RSI thresholds: "
            f"oversold {os_rel} {_rule_summary_num(oversold, 2)} -> {os_action.upper()}, "
            f"overbought {ob_rel} {_rule_summary_num(overbought, 2)} -> {ob_action.upper()}."
        )
        if bool(int(params.get("signal_override_enabled") or 0)):
            scope_txt = _rule_override_scope_label(params.get("signal_override_scope"))
            target_ids = _normalize_rule_target_ids(params.get("signal_override_targets"))
            target_labels: list[str] = []
            for rid in target_ids:
                if rule_ref_by_id and rid in rule_ref_by_id:
                    target_labels.append(rule_ref_by_id[rid])
                else:
                    target_labels.append(rid)
            targets_txt = ", ".join(target_labels) if target_labels else "none selected"
            lines.append(f"Override enabled: {scope_txt} on {targets_txt}.")
        return lines

    if kind == "rsi_d":
        buy_above = _to_float_opt(params.get("buy_above"))
        sell_below = _to_float_opt(params.get("sell_below"))
        lines.append(
            f"dRSI lock: BUY >= {_rule_summary_num(buy_above, 4)}; "
            f"SELL <= {_rule_summary_num(sell_below, 4)}."
        )
        return lines

    if kind == "macd":
        fast = max(2, int(_to_int_opt(params.get("fast_length")) or 12))
        slow = max(2, int(_to_int_opt(params.get("slow_length")) or 26))
        signal = max(2, int(_to_int_opt(params.get("signal_length")) or 9))
        mode = str(params.get("mode") or "signal_cross").strip().lower()
        mode_label = _MACD_MODE_LABELS.get(mode, mode or "signal_cross")
        lines.append(f"MACD mode: {mode_label} ({fast}/{slow}/{signal}).")
        lines.append(_MACD_MODE_SUMMARIES.get(mode, _MACD_MODE_SUMMARIES["signal_cross"]))
        if mode == "macd_derivative_sign":
            buy_above_raw = _to_float_opt(params.get("derivative_buy_above"))
            sell_below_raw = _to_float_opt(params.get("derivative_sell_below"))
            d_scope = _normalize_dual_signal_scope(params.get("derivative_signal_scope"), default="both")
            buy_above = float(buy_above_raw) if buy_above_raw is not None else 0.0
            sell_below = float(sell_below_raw) if sell_below_raw is not None else 0.0
            lines.append(
                f"dMACD thresholds: BUY > {_rule_summary_num(buy_above, 4)}; "
                f"SELL < {_rule_summary_num(sell_below, 4)}."
            )
            lines.append(f"dMACD side mode: {_rule_override_scope_label(d_scope)}.")
        if bool(int(params.get("signal_override_enabled") or 0)):
            scope_txt = _rule_override_scope_label(params.get("signal_override_scope"))
            target_ids = _normalize_rule_target_ids(params.get("signal_override_targets"))
            target_labels: list[str] = []
            for rid in target_ids:
                if rule_ref_by_id and rid in rule_ref_by_id:
                    target_labels.append(rule_ref_by_id[rid])
                else:
                    target_labels.append(rid)
            targets_txt = ", ".join(target_labels) if target_labels else "none selected"
            lines.append(f"Override enabled: {scope_txt} on {targets_txt}.")
        return lines

    if kind in ("heikin_ashi", "ha"):
        mode = str(params.get("mode") or "transition").strip().lower()
        if mode not in ("transition", "state"):
            mode = "transition"
        doji_tol_raw = _to_float_opt(params.get("doji_tolerance_pct"))
        if mode == "state":
            lines.append("Heikin Ashi state: BUY when HA candle is bullish; SELL when HA candle is bearish.")
        else:
            lines.append("Heikin Ashi transition: BUY on bearish→bullish flip; SELL on bullish→bearish flip.")
        if doji_tol_raw is None:
            lines.append("Doji tolerance: disabled (only exact HA open=close is doji).")
        else:
            doji_tol = max(0.0, float(doji_tol_raw))
            lines.append(f"Doji tolerance: candle body <= {_rule_summary_num(doji_tol, 3)}% treated as doji.")
        if bool(int(params.get("signal_override_enabled") or 0)):
            scope_txt = _rule_override_scope_label(params.get("signal_override_scope"))
            target_ids = _normalize_rule_target_ids(params.get("signal_override_targets"))
            target_labels: list[str] = []
            for rid in target_ids:
                if rule_ref_by_id and rid in rule_ref_by_id:
                    target_labels.append(rule_ref_by_id[rid])
                else:
                    target_labels.append(rid)
            targets_txt = ", ".join(target_labels) if target_labels else "none selected"
            lines.append(f"Override enabled: {scope_txt} on {targets_txt}.")
        return lines

    lines.append("Unsupported rule configuration.")
    return lines


def _render_indicator_rule_summary_panel(
    rules: list[dict[str, Any]],
    *,
    title: str = "Rule Summary",
) -> str:
    if not rules:
        return ""

    rule_ref_by_id: dict[str, str] = {}
    for idx, rule in enumerate(rules, start=1):
        params = rule.get("params") if isinstance(rule.get("params"), dict) else {}
        rid = str(params.get("rule_id") or "").strip()
        if rid and rid not in rule_ref_by_id:
            rule_ref_by_id[rid] = f"#{idx} {_rule_name(rule)}"
        kind = str(rule.get("kind") or "").strip().lower()
        if kind in ("ma", "ema") and _normalize_ma_mode(params.get("mode"), default="single") == "ribbon":
            for level in _ma_ribbon_levels_from_params(params):
                child_id = _ma_ribbon_level_rule_id(rid, level.get("slot"))
                if not child_id or child_id in rule_ref_by_id:
                    continue
                rule_ref_by_id[child_id] = f"#{idx} {_ma_ribbon_level_name(_rule_name(rule), level)}"

    out: list[str] = [
        f"<div class='small' style='margin-top:8px'>{html.escape(title)}</div>",
        "<div class='status-table-wrap'><table>",
        "<thead><tr><th>#</th><th>Rule</th><th>TF</th><th>Summary</th></tr></thead><tbody>",
    ]
    for idx, rule in enumerate(rules, start=1):
        name = _rule_name(rule)
        kind = str(rule.get("kind") or "").strip().upper() or "RULE"
        params = rule.get("params") if isinstance(rule.get("params"), dict) else {}
        rid = str(params.get("rule_id") or "").strip()
        timeframe = _rule_timeframe(rule, "")
        tf_html = f"<span class='badge'>TF {html.escape(timeframe)}</span>" if timeframe else ""
        color = _rule_line_color(rule)
        dot = (
            "<span style='display:inline-block;width:8px;height:8px;border-radius:999px;"
            f"background:{html.escape(color)};margin-right:6px;vertical-align:middle;'></span>"
        )
        lines = _indicator_rule_summary_lines(rule, rule_ref_by_id=rule_ref_by_id)
        summary_html = "<br>".join(html.escape(line) for line in lines)
        rid_html = f"<div class='small'><code>{html.escape(rid)}</code></div>" if rid else ""
        out.append(
            "<tr>"
            f"<td>{idx}</td>"
            f"<td>{dot}<b>{html.escape(name)}</b><div class='small'>{html.escape(kind)}</div>{rid_html}</td>"
            f"<td>{tf_html}</td>"
            f"<td class='small'>{summary_html}</td>"
            "</tr>"
        )
        if str(rule.get("kind") or "").strip().lower() in ("ma", "ema") and _normalize_ma_mode(params.get("mode"), default="single") == "ribbon":
            for level in _ma_ribbon_levels_from_params(params):
                child_id = _ma_ribbon_level_rule_id(rid, level.get("slot"))
                child_name = _ma_ribbon_level_name(name, level)
                line_tag = "EMA" if str(level.get("ma_type") or "sma").strip().lower() == "ema" else "SMA"
                length = max(2, int(_to_int_opt(level.get("length")) or 30))
                above_action = str(level.get("above_action") or "hold").strip().upper()
                below_action = str(level.get("below_action") or "hold").strip().upper()
                child_summary = (
                    f"{str(level.get('label') or 'Level')} {line_tag}{length}: "
                    f"above -> {above_action}; below -> {below_action}."
                )
                child_id_html = (
                    f"<div class='small'><code>{html.escape(child_id)}</code></div>" if child_id else ""
                )
                out.append(
                    "<tr>"
                    f"<td>{idx}.{html.escape(str(level.get('label') or 'L')[0].upper())}</td>"
                    f"<td>{dot}<b>{html.escape(child_name)}</b><div class='small'>RIBBON LEVEL</div>{child_id_html}</td>"
                    f"<td>{tf_html}</td>"
                    f"<td class='small'>{html.escape(child_summary)}</td>"
                    "</tr>"
                )
    out.append("</tbody></table></div>")
    return "".join(out)


def _market_synthetic_ohlc_from_closes(closes: list[float]) -> tuple[list[float], list[float], list[float], list[float]]:
    if not closes:
        return [], [], [], []
    out_opens: list[float] = []
    out_highs: list[float] = []
    out_lows: list[float] = []
    out_closes: list[float] = []
    for idx, raw in enumerate(closes):
        try:
            c = float(raw)
        except Exception:
            continue
        if not math.isfinite(c) or c <= 0:
            continue
        if idx == 0:
            o = c
        else:
            try:
                o = float(closes[idx - 1])
            except Exception:
                o = c
            if (not math.isfinite(o)) or o <= 0:
                o = c
        h = max(o, c)
        l = min(o, c)
        out_opens.append(float(o))
        out_highs.append(float(h))
        out_lows.append(float(l))
        out_closes.append(float(c))
    return out_opens, out_highs, out_lows, out_closes


def _market_heikin_ashi_series(
    opens: list[float],
    highs: list[float],
    lows: list[float],
    closes: list[float],
) -> tuple[list[float], list[float], list[float], list[float]]:
    return shared_heikin_ashi_series(opens, highs, lows, closes)


def _market_ha_candle_state(ha_open: float, ha_close: float, *, doji_tolerance_pct: float = 0.0) -> str:
    body = float(ha_close) - float(ha_open)
    tol_pct = max(0.0, float(doji_tolerance_pct))
    if tol_pct > 0.0:
        ref = max(abs(float(ha_open)), abs(float(ha_close)), 1.0e-9)
        if abs(body) <= (ref * (tol_pct / 100.0)):
            return "doji"
    if body > 0.0:
        return "bullish"
    if body < 0.0:
        return "bearish"
    return "doji"


def _market_bollinger_snapshot(
    closes: list[float],
    *,
    length: int = 20,
    std_mult: float = 2.0,
) -> Optional[dict[str, float]]:
    ln = max(2, int(length))
    if len(closes) < ln:
        return None
    try:
        window = [float(v) for v in closes[-ln:]]
    except Exception:
        return None
    if not window:
        return None
    middle = sum(window) / float(ln)
    variance = sum((float(v) - float(middle)) ** 2 for v in window) / float(ln)
    std_dev = math.sqrt(max(0.0, float(variance)))
    mult = max(0.1, float(std_mult))
    upper = float(middle) + (mult * float(std_dev))
    lower = float(middle) - (mult * float(std_dev))
    band_range = float(upper) - float(lower)
    close_now = float(closes[-1])
    width_pct = ((float(upper) - float(lower)) / float(middle) * 100.0) if float(middle) != 0.0 else 0.0
    percent_b = ((float(close_now) - float(lower)) / float(band_range)) if band_range > 0.0 else 0.5
    return {
        "middle": float(middle),
        "upper": float(upper),
        "lower": float(lower),
        "width_pct": float(width_pct),
        "percent_b": float(percent_b),
    }


def _bb_condition_hit(
    cond: str,
    *,
    close_now: float,
    high_now: Optional[float],
    low_now: Optional[float],
    upper: float,
    lower: float,
    middle: float,
    prev_close: Optional[float],
    prev_upper: Optional[float],
    prev_lower: Optional[float],
    width_pct: float,
    prev_width_pct: Optional[float],
    squeeze_threshold_pct: float,
    percent_b: float,
    percent_b_threshold: float,
) -> bool:
    c = _normalize_bb_condition(cond, default="hold")
    if c == "hold":
        return False
    if c == "touch_upper":
        touch_price = float(high_now) if high_now is not None else float(close_now)
        return touch_price >= float(upper)
    if c == "touch_lower":
        touch_price = float(low_now) if low_now is not None else float(close_now)
        return touch_price <= float(lower)
    if c == "close_outside_upper":
        return float(close_now) > float(upper)
    if c == "close_outside_lower":
        return float(close_now) < float(lower)
    if c == "reenter_from_above":
        if prev_close is None or prev_upper is None:
            return False
        return float(prev_close) > float(prev_upper) and float(close_now) <= float(upper)
    if c == "reenter_from_below":
        if prev_close is None or prev_lower is None:
            return False
        return float(prev_close) < float(prev_lower) and float(close_now) >= float(lower)
    if c == "width_increasing":
        return prev_width_pct is not None and float(width_pct) > float(prev_width_pct)
    if c == "width_decreasing":
        return prev_width_pct is not None and float(width_pct) < float(prev_width_pct)
    if c == "width_below_threshold":
        return float(width_pct) <= float(squeeze_threshold_pct)
    if c == "width_expanding_from_squeeze":
        return (
            prev_width_pct is not None
            and float(prev_width_pct) <= float(squeeze_threshold_pct)
            and float(width_pct) > float(prev_width_pct)
        )
    if c == "above_middle":
        return float(close_now) > float(middle)
    if c == "below_middle":
        return float(close_now) < float(middle)
    if c == "percent_b_above":
        return float(percent_b) >= float(percent_b_threshold)
    if c == "percent_b_below":
        return float(percent_b) <= float(percent_b_threshold)
    return False


def _market_ichimoku_state(
    closes: list[float],
    *,
    highs: Optional[list[float]] = None,
    lows: Optional[list[float]] = None,
    tenkan_length: int = 9,
    kijun_length: int = 26,
    senkou_b_length: int = 52,
    displacement: int = 26,
) -> Optional[dict[str, float]]:
    n = len(closes)
    if n <= 0:
        return None

    close_vals: list[float] = []
    for raw in closes:
        fv = _to_float_opt(raw)
        if fv is None:
            return None
        close_vals.append(float(fv))
    if not close_vals:
        return None

    high_vals: list[float] = []
    low_vals: list[float] = []
    for idx, close_now in enumerate(close_vals):
        h = _to_float_opt(highs[idx]) if isinstance(highs, list) and idx < len(highs) else None
        l = _to_float_opt(lows[idx]) if isinstance(lows, list) and idx < len(lows) else None
        high_vals.append(float(h) if h is not None else float(close_now))
        low_vals.append(float(l) if l is not None else float(close_now))

    tenkan_len = max(1, int(tenkan_length))
    kijun_len = max(1, int(kijun_length))
    senkou_b_len = max(2, int(senkou_b_length))
    disp = max(1, int(displacement))

    def _midpoint(length: int, idx: int) -> Optional[float]:
        if idx < 0 or idx >= n:
            return None
        start = idx - int(length) + 1
        if start < 0:
            return None
        hi_window = high_vals[start : idx + 1]
        lo_window = low_vals[start : idx + 1]
        if not hi_window or not lo_window:
            return None
        return (max(hi_window) + min(lo_window)) / 2.0

    def _span_a_raw(idx: int) -> Optional[float]:
        tenkan = _midpoint(tenkan_len, idx)
        kijun = _midpoint(kijun_len, idx)
        if tenkan is None or kijun is None:
            return None
        return (float(tenkan) + float(kijun)) / 2.0

    def _span_b_raw(idx: int) -> Optional[float]:
        return _midpoint(senkou_b_len, idx)

    curr_idx = n - 1
    prev_idx = curr_idx - 1
    # Price-vs-cloud rules compare the live candle with the displaced cloud drawn at
    # that candle. The raw current spans are only the projected forward cloud.
    cloud_idx = curr_idx - disp
    prev_cloud_idx = prev_idx - disp if prev_idx >= 0 else None
    chikou_ref_idx = curr_idx - disp
    chikou_prev_ref_idx = prev_idx - disp if prev_idx >= 0 else None

    tenkan_now = _midpoint(tenkan_len, curr_idx)
    kijun_now = _midpoint(kijun_len, curr_idx)
    if tenkan_now is None or kijun_now is None:
        return None
    tenkan_prev = _midpoint(tenkan_len, prev_idx) if prev_idx >= 0 else None
    kijun_prev = _midpoint(kijun_len, prev_idx) if prev_idx >= 0 else None
    tenkan_prev2 = _midpoint(tenkan_len, prev_idx - 1) if prev_idx >= 1 else None
    kijun_prev2 = _midpoint(kijun_len, prev_idx - 1) if prev_idx >= 1 else None

    span_a = _span_a_raw(cloud_idx) if cloud_idx >= 0 else None
    span_b = _span_b_raw(cloud_idx) if cloud_idx >= 0 else None
    future_span_a = _span_a_raw(curr_idx)
    future_span_b = _span_b_raw(curr_idx)
    if span_a is None or span_b is None or future_span_a is None or future_span_b is None:
        return None
    prev_span_a = _span_a_raw(prev_cloud_idx) if (prev_cloud_idx is not None and prev_cloud_idx >= 0) else None
    prev_span_b = _span_b_raw(prev_cloud_idx) if (prev_cloud_idx is not None and prev_cloud_idx >= 0) else None
    future_span_a_prev = _span_a_raw(prev_idx) if prev_idx >= 0 else None
    future_span_b_prev = _span_b_raw(prev_idx) if prev_idx >= 0 else None

    close_now = float(close_vals[curr_idx])
    close_prev = float(close_vals[prev_idx]) if prev_idx >= 0 else None
    cloud_top = max(float(span_a), float(span_b))
    cloud_bottom = min(float(span_a), float(span_b))
    cloud_top_prev = max(float(prev_span_a), float(prev_span_b)) if (prev_span_a is not None and prev_span_b is not None) else None
    cloud_bottom_prev = min(float(prev_span_a), float(prev_span_b)) if (prev_span_a is not None and prev_span_b is not None) else None
    cloud_mid = (float(span_a) + float(span_b)) / 2.0
    cloud_thickness_pct = (abs(float(span_a) - float(span_b)) / max(abs(cloud_mid), 1.0e-9)) * 100.0
    cloud_thickness_prev_pct: Optional[float] = None
    if prev_span_a is not None and prev_span_b is not None:
        prev_mid = (float(prev_span_a) + float(prev_span_b)) / 2.0
        cloud_thickness_prev_pct = (abs(float(prev_span_a) - float(prev_span_b)) / max(abs(prev_mid), 1.0e-9)) * 100.0

    chikou_ref_price = float(close_vals[chikou_ref_idx]) if chikou_ref_idx >= 0 else None
    chikou_prev_ref_price = float(close_vals[chikou_prev_ref_idx]) if (chikou_prev_ref_idx is not None and chikou_prev_ref_idx >= 0) else None
    chikou_cloud_idx = chikou_ref_idx - disp
    chikou_span_a = _span_a_raw(chikou_cloud_idx) if chikou_cloud_idx >= 0 else None
    chikou_span_b = _span_b_raw(chikou_cloud_idx) if chikou_cloud_idx >= 0 else None
    chikou_cloud_top = max(float(chikou_span_a), float(chikou_span_b)) if (chikou_span_a is not None and chikou_span_b is not None) else None
    chikou_cloud_bottom = min(float(chikou_span_a), float(chikou_span_b)) if (chikou_span_a is not None and chikou_span_b is not None) else None

    last_cross_up_idx: Optional[int] = None
    last_cross_down_idx: Optional[int] = None
    prev_tk: Optional[float] = None
    prev_kj: Optional[float] = None
    for j in range(n):
        tk_j = _midpoint(tenkan_len, j)
        kj_j = _midpoint(kijun_len, j)
        if tk_j is None or kj_j is None:
            prev_tk = tk_j
            prev_kj = kj_j
            continue
        if prev_tk is not None and prev_kj is not None:
            if float(prev_tk) <= float(prev_kj) and float(tk_j) > float(kj_j):
                last_cross_up_idx = j
            if float(prev_tk) >= float(prev_kj) and float(tk_j) < float(kj_j):
                last_cross_down_idx = j
        prev_tk = tk_j
        prev_kj = kj_j
    bars_since_cross_up = (curr_idx - last_cross_up_idx) if last_cross_up_idx is not None else None
    bars_since_cross_down = (curr_idx - last_cross_down_idx) if last_cross_down_idx is not None else None

    return {
        "close_now": float(close_now),
        "close_prev": float(close_prev) if close_prev is not None else None,
        "tenkan": float(tenkan_now),
        "tenkan_prev": float(tenkan_prev) if tenkan_prev is not None else None,
        "tenkan_prev2": float(tenkan_prev2) if tenkan_prev2 is not None else None,
        "kijun": float(kijun_now),
        "kijun_prev": float(kijun_prev) if kijun_prev is not None else None,
        "kijun_prev2": float(kijun_prev2) if kijun_prev2 is not None else None,
        "span_a": float(span_a),
        "span_b": float(span_b),
        "span_a_prev": float(prev_span_a) if prev_span_a is not None else None,
        "span_b_prev": float(prev_span_b) if prev_span_b is not None else None,
        "future_span_a": float(future_span_a),
        "future_span_b": float(future_span_b),
        "future_span_a_prev": float(future_span_a_prev) if future_span_a_prev is not None else None,
        "future_span_b_prev": float(future_span_b_prev) if future_span_b_prev is not None else None,
        "cloud_top": float(cloud_top),
        "cloud_bottom": float(cloud_bottom),
        "cloud_top_prev": float(cloud_top_prev) if cloud_top_prev is not None else None,
        "cloud_bottom_prev": float(cloud_bottom_prev) if cloud_bottom_prev is not None else None,
        "cloud_thickness_pct": float(cloud_thickness_pct),
        "cloud_thickness_prev_pct": float(cloud_thickness_prev_pct) if cloud_thickness_prev_pct is not None else None,
        "chikou_ref_price": float(chikou_ref_price) if chikou_ref_price is not None else None,
        "chikou_prev_ref_price": float(chikou_prev_ref_price) if chikou_prev_ref_price is not None else None,
        "chikou_cloud_top": float(chikou_cloud_top) if chikou_cloud_top is not None else None,
        "chikou_cloud_bottom": float(chikou_cloud_bottom) if chikou_cloud_bottom is not None else None,
        "bars_since_cross_up": int(bars_since_cross_up) if bars_since_cross_up is not None else None,
        "bars_since_cross_down": int(bars_since_cross_down) if bars_since_cross_down is not None else None,
    }


def _ichimoku_condition_hit(
    cond: str,
    *,
    state: dict[str, float],
    cloud_thickness_threshold_pct: float,
    kijun_bounce_tolerance_pct: float,
    delayed_cross_lookback: int,
) -> bool:
    c = _normalize_ichi_condition(cond, default="hold")
    if c == "hold":
        return False

    close_now = float(state.get("close_now") or 0.0)
    close_prev = _to_float_opt(state.get("close_prev"))
    tenkan = float(state.get("tenkan") or 0.0)
    tenkan_prev = _to_float_opt(state.get("tenkan_prev"))
    tenkan_prev2 = _to_float_opt(state.get("tenkan_prev2"))
    kijun = float(state.get("kijun") or 0.0)
    kijun_prev = _to_float_opt(state.get("kijun_prev"))
    kijun_prev2 = _to_float_opt(state.get("kijun_prev2"))
    cloud_top = float(state.get("cloud_top") or 0.0)
    cloud_bottom = float(state.get("cloud_bottom") or 0.0)
    cloud_top_prev = _to_float_opt(state.get("cloud_top_prev"))
    cloud_bottom_prev = _to_float_opt(state.get("cloud_bottom_prev"))
    span_a = float(state.get("span_a") or 0.0)
    span_b = float(state.get("span_b") or 0.0)
    future_span_a = float(state.get("future_span_a") or 0.0)
    future_span_b = float(state.get("future_span_b") or 0.0)
    future_span_a_prev = _to_float_opt(state.get("future_span_a_prev"))
    future_span_b_prev = _to_float_opt(state.get("future_span_b_prev"))
    cloud_thickness_pct = float(state.get("cloud_thickness_pct") or 0.0)
    cloud_thickness_prev_pct = _to_float_opt(state.get("cloud_thickness_prev_pct"))
    chikou_ref_price = _to_float_opt(state.get("chikou_ref_price"))
    chikou_prev_ref_price = _to_float_opt(state.get("chikou_prev_ref_price"))
    chikou_cloud_top = _to_float_opt(state.get("chikou_cloud_top"))
    chikou_cloud_bottom = _to_float_opt(state.get("chikou_cloud_bottom"))
    bars_since_cross_up = _to_int_opt(state.get("bars_since_cross_up"))
    bars_since_cross_down = _to_int_opt(state.get("bars_since_cross_down"))

    inside_cloud = float(cloud_bottom) <= float(close_now) <= float(cloud_top)
    price_above_cloud = float(close_now) > float(cloud_top)
    price_below_cloud = float(close_now) < float(cloud_bottom)
    cloud_bullish = float(span_a) > float(span_b)
    cloud_bearish = float(span_a) < float(span_b)
    tenkan_above = float(tenkan) > float(kijun)
    tenkan_below = float(tenkan) < float(kijun)
    price_above_tenkan = float(close_now) > float(tenkan)
    price_below_tenkan = float(close_now) < float(tenkan)
    price_above_kijun = float(close_now) > float(kijun)
    price_below_kijun = float(close_now) < float(kijun)
    chikou_above = chikou_ref_price is not None and float(close_now) > float(chikou_ref_price)
    chikou_below = chikou_ref_price is not None and float(close_now) < float(chikou_ref_price)
    cross_up = (
        tenkan_prev is not None
        and kijun_prev is not None
        and float(tenkan_prev) <= float(kijun_prev)
        and tenkan_above
    )
    cross_down = (
        tenkan_prev is not None
        and kijun_prev is not None
        and float(tenkan_prev) >= float(kijun_prev)
        and tenkan_below
    )
    distance_to_kijun_pct = (abs(float(close_now) - float(kijun)) / max(abs(float(kijun)), 1.0e-9)) * 100.0
    bounce_tol = max(0.0, float(kijun_bounce_tolerance_pct))
    boundary_tol = max(0.05, bounce_tol)
    cloud_ext_thr = max(0.5, float(cloud_thickness_threshold_pct))
    kijun_ext_thr = max(0.5, bounce_tol * 2.0)
    delayed_bars = max(1, int(delayed_cross_lookback))
    close_ref = max(abs(float(close_now)), 1.0e-9)
    dist_to_top_pct = (abs(float(close_now) - float(cloud_top)) / close_ref) * 100.0
    dist_to_bottom_pct = (abs(float(close_now) - float(cloud_bottom)) / close_ref) * 100.0
    near_cloud_boundary = min(dist_to_top_pct, dist_to_bottom_pct) <= boundary_tol
    cloud_range = max(float(cloud_top) - float(cloud_bottom), 1.0e-9)
    depth_ratio = (float(close_now) - float(cloud_bottom)) / cloud_range
    shallow_band = 0.25
    deep_low = 0.4
    deep_high = 0.6
    inside_prev = (
        close_prev is not None
        and cloud_top_prev is not None
        and cloud_bottom_prev is not None
        and float(cloud_bottom_prev) <= float(close_prev) <= float(cloud_top_prev)
    )
    price_momentum_up = close_prev is not None and float(close_now) > float(close_prev)
    price_momentum_down = close_prev is not None and float(close_now) < float(close_prev)
    above_cloud_prev = close_prev is not None and cloud_top_prev is not None and float(close_prev) > float(cloud_top_prev)
    below_cloud_prev = close_prev is not None and cloud_bottom_prev is not None and float(close_prev) < float(cloud_bottom_prev)
    price_above_tenkan = float(close_now) > float(tenkan)
    price_below_tenkan = float(close_now) < float(tenkan)
    price_above_kijun = float(close_now) > float(kijun)
    price_below_kijun = float(close_now) < float(kijun)
    future_diff = float(future_span_a) - float(future_span_b)
    future_prev_diff: Optional[float] = None
    if future_span_a_prev is not None and future_span_b_prev is not None:
        future_prev_diff = float(future_span_a_prev) - float(future_span_b_prev)
    twist_prox_pct = (abs(float(future_diff)) / close_ref) * 100.0
    approaching_bull_twist = (
        future_prev_diff is not None
        and float(future_prev_diff) < 0.0
        and float(future_diff) < 0.0
        and abs(float(future_diff)) < abs(float(future_prev_diff))
        and twist_prox_pct <= boundary_tol
    )
    approaching_bear_twist = (
        future_prev_diff is not None
        and float(future_prev_diff) > 0.0
        and float(future_diff) > 0.0
        and abs(float(future_diff)) < abs(float(future_prev_diff))
        and twist_prox_pct <= boundary_tol
    )
    future_twist_bullish = future_prev_diff is not None and float(future_prev_diff) <= 0.0 and float(future_diff) > 0.0
    future_twist_bearish = future_prev_diff is not None and float(future_prev_diff) >= 0.0 and float(future_diff) < 0.0
    delayed_bull_valid = (
        bars_since_cross_up is not None
        and int(bars_since_cross_up) <= delayed_bars
        and tenkan_above
        and price_above_tenkan
        and price_above_kijun
    )
    delayed_bear_valid = (
        bars_since_cross_down is not None
        and int(bars_since_cross_down) <= delayed_bars
        and tenkan_below
        and price_below_tenkan
        and price_below_kijun
    )
    above_cloud_ext_pct = (
        ((float(close_now) - float(cloud_top)) / max(abs(float(cloud_top)), 1.0e-9)) * 100.0
        if price_above_cloud
        else 0.0
    )
    below_cloud_ext_pct = (
        ((float(cloud_bottom) - float(close_now)) / max(abs(float(cloud_bottom)), 1.0e-9)) * 100.0
        if price_below_cloud
        else 0.0
    )
    kijun_flat = (
        kijun_prev is not None
        and (abs(float(kijun) - float(kijun_prev)) / max(abs(float(kijun_prev)), 1.0e-9)) * 100.0 <= max(0.05, bounce_tol / 2.0)
    )
    kijun_rising = kijun_prev is not None and float(kijun) > float(kijun_prev)
    kijun_falling = kijun_prev is not None and float(kijun) < float(kijun_prev)
    tenkan_slope_now = (float(tenkan) - float(tenkan_prev)) if tenkan_prev is not None else None
    tenkan_slope_prev = (float(tenkan_prev) - float(tenkan_prev2)) if tenkan_prev is not None and tenkan_prev2 is not None else None
    tenkan_accel_up = (
        tenkan_slope_now is not None
        and tenkan_slope_prev is not None
        and float(tenkan_slope_now) > 0.0
        and float(tenkan_slope_now) > float(tenkan_slope_prev)
    )
    tenkan_accel_down = (
        tenkan_slope_now is not None
        and tenkan_slope_prev is not None
        and float(tenkan_slope_now) < 0.0
        and float(tenkan_slope_now) < float(tenkan_slope_prev)
    )
    shallow_entry_from_above = inside_cloud and depth_ratio >= (1.0 - shallow_band) and above_cloud_prev
    shallow_entry_from_below = inside_cloud and depth_ratio <= shallow_band and below_cloud_prev
    deep_inside_cloud = inside_cloud and deep_low <= depth_ratio <= deep_high
    cloud_exit_up_momentum = inside_prev and price_above_cloud and price_momentum_up and tenkan_above
    cloud_exit_down_momentum = inside_prev and price_below_cloud and price_momentum_down and tenkan_below
    bullish_retest_hold = (
        above_cloud_prev
        and float(close_now) >= float(cloud_top)
        and dist_to_top_pct <= boundary_tol
        and tenkan_above
    )
    bearish_retest_fail = (
        below_cloud_prev
        and float(close_now) <= float(cloud_bottom)
        and dist_to_bottom_pct <= boundary_tol
        and tenkan_below
    )
    chikou_clears_past_cloud_bull = chikou_cloud_top is not None and float(close_now) > float(chikou_cloud_top)
    chikou_clears_past_cloud_bear = chikou_cloud_bottom is not None and float(close_now) < float(chikou_cloud_bottom)
    chikou_blocked = (
        chikou_ref_price is not None
        and (abs(float(close_now) - float(chikou_ref_price)) / max(abs(float(chikou_ref_price)), 1.0e-9)) * 100.0 <= boundary_tol
    )
    chikou_congestion = (
        chikou_blocked
        and chikou_cloud_top is not None
        and chikou_cloud_bottom is not None
        and float(chikou_cloud_bottom) <= float(close_now) <= float(chikou_cloud_top)
    )
    cloud_expanding = cloud_thickness_prev_pct is not None and cloud_thickness_pct > float(cloud_thickness_prev_pct)
    cloud_contracting = cloud_thickness_prev_pct is not None and cloud_thickness_pct < float(cloud_thickness_prev_pct)
    cloud_thin_to_thick = (
        cloud_thickness_prev_pct is not None
        and float(cloud_thickness_prev_pct) <= float(cloud_thickness_threshold_pct)
        and cloud_thickness_pct > float(cloud_thickness_threshold_pct)
    )
    bullish_to_neutral = above_cloud_prev and inside_cloud
    bearish_to_neutral = below_cloud_prev and inside_cloud
    bullish_components = [
        price_above_cloud,
        tenkan_above,
        bool(chikou_above),
    ]
    bearish_components = [
        price_below_cloud,
        tenkan_below,
        bool(chikou_below),
    ]
    bull_count = sum(1 for ok in bullish_components if bool(ok))
    bear_count = sum(1 for ok in bearish_components if bool(ok))
    prev_price_above_cloud = above_cloud_prev
    prev_price_below_cloud = below_cloud_prev
    prev_tenkan_above = tenkan_prev is not None and kijun_prev is not None and float(tenkan_prev) > float(kijun_prev)
    prev_tenkan_below = tenkan_prev is not None and kijun_prev is not None and float(tenkan_prev) < float(kijun_prev)
    prev_chikou_above = close_prev is not None and chikou_prev_ref_price is not None and float(close_prev) > float(chikou_prev_ref_price)
    prev_chikou_below = close_prev is not None and chikou_prev_ref_price is not None and float(close_prev) < float(chikou_prev_ref_price)
    bull_prev_count = sum(1 for ok in (prev_price_above_cloud, prev_tenkan_above, prev_chikou_above) if bool(ok))
    bear_prev_count = sum(1 for ok in (prev_price_below_cloud, prev_tenkan_below, prev_chikou_below) if bool(ok))

    if c == "price_above_cloud":
        return price_above_cloud
    if c == "price_below_cloud":
        return price_below_cloud
    if c == "price_inside_cloud":
        return inside_cloud
    if c == "cloud_bullish":
        return cloud_bullish
    if c == "cloud_bearish":
        return cloud_bearish
    if c == "cloud_thickness_above":
        return cloud_thickness_pct >= float(cloud_thickness_threshold_pct)
    if c == "cloud_thickness_below":
        return cloud_thickness_pct <= float(cloud_thickness_threshold_pct)
    if c == "tenkan_above_kijun":
        return tenkan_above
    if c == "tenkan_below_kijun":
        return tenkan_below
    if c == "tenkan_cross_above":
        return bool(cross_up)
    if c == "tenkan_cross_below":
        return bool(cross_down)
    if c == "chikou_above_price":
        return bool(chikou_above)
    if c == "chikou_below_price":
        return bool(chikou_below)
    if c == "strong_long_confirm":
        return price_above_cloud and tenkan_above and bool(chikou_above)
    if c == "strong_short_confirm":
        return price_below_cloud and tenkan_below and bool(chikou_below)
    if c == "future_twist_bullish":
        return bool(future_twist_bullish)
    if c == "future_twist_bearish":
        return bool(future_twist_bearish)
    if c == "approaching_future_twist_bullish":
        return bool(approaching_bull_twist)
    if c == "approaching_future_twist_bearish":
        return bool(approaching_bear_twist)
    if c == "delayed_bullish_cross_valid":
        return bool(delayed_bull_valid)
    if c == "delayed_bearish_cross_valid":
        return bool(delayed_bear_valid)
    if c == "bullish_cross_strong_above_cloud":
        return bool(cross_up) and price_above_cloud
    if c == "bullish_cross_medium_at_cloud":
        return bool(cross_up) and (inside_cloud or near_cloud_boundary)
    if c == "bullish_cross_weak_below_cloud":
        return bool(cross_up) and price_below_cloud
    if c == "bearish_cross_strong_below_cloud":
        return bool(cross_down) and price_below_cloud
    if c == "bearish_cross_medium_at_cloud":
        return bool(cross_down) and (inside_cloud or near_cloud_boundary)
    if c == "bearish_cross_weak_above_cloud":
        return bool(cross_down) and price_above_cloud
    if c == "cloud_breakout_bullish":
        return (
            close_prev is not None
            and cloud_top_prev is not None
            and float(close_prev) <= float(cloud_top_prev)
            and price_above_cloud
            and float(future_span_a) > float(future_span_b)
        )
    if c == "cloud_breakout_bearish":
        return (
            close_prev is not None
            and cloud_bottom_prev is not None
            and float(close_prev) >= float(cloud_bottom_prev)
            and price_below_cloud
            and float(future_span_a) < float(future_span_b)
        )
    if c == "price_above_tenkan_kijun_below_cloud":
        return price_above_tenkan and price_above_kijun and price_below_cloud
    if c == "price_above_tenkan_kijun_inside_cloud":
        return price_above_tenkan and price_above_kijun and inside_cloud
    if c == "price_below_tenkan_kijun_above_cloud":
        return price_below_tenkan and price_below_kijun and price_above_cloud
    if c == "price_below_tenkan_kijun_inside_cloud":
        return price_below_tenkan and price_below_kijun and inside_cloud
    if c == "price_extended_above_cloud":
        return above_cloud_ext_pct >= cloud_ext_thr
    if c == "price_extended_below_cloud":
        return below_cloud_ext_pct >= cloud_ext_thr
    if c == "price_stretched_above_kijun":
        return float(close_now) > float(kijun) and distance_to_kijun_pct >= kijun_ext_thr
    if c == "price_stretched_below_kijun":
        return float(close_now) < float(kijun) and distance_to_kijun_pct >= kijun_ext_thr
    if c == "kijun_flat":
        return bool(kijun_flat)
    if c == "kijun_rising":
        return bool(kijun_rising)
    if c == "kijun_falling":
        return bool(kijun_falling)
    if c == "tenkan_accelerating_up":
        return bool(tenkan_accel_up)
    if c == "tenkan_accelerating_down":
        return bool(tenkan_accel_down)
    if c == "shallow_cloud_entry_from_above":
        return bool(shallow_entry_from_above)
    if c == "shallow_cloud_entry_from_below":
        return bool(shallow_entry_from_below)
    if c == "deep_inside_cloud":
        return bool(deep_inside_cloud)
    if c == "cloud_exit_up_with_momentum":
        return bool(cloud_exit_up_momentum)
    if c == "cloud_exit_down_with_momentum":
        return bool(cloud_exit_down_momentum)
    if c == "bullish_breakout_retest_hold":
        return bool(bullish_retest_hold)
    if c == "bearish_breakdown_retest_fail":
        return bool(bearish_retest_fail)
    if c == "chikou_clears_past_cloud_bullish":
        return bool(chikou_clears_past_cloud_bull)
    if c == "chikou_clears_past_cloud_bearish":
        return bool(chikou_clears_past_cloud_bear)
    if c == "chikou_blocked_by_past_price":
        return bool(chikou_blocked)
    if c == "chikou_in_congestion_zone":
        return bool(chikou_congestion)
    if c == "cloud_expanding":
        return bool(cloud_expanding)
    if c == "cloud_contracting":
        return bool(cloud_contracting)
    if c == "cloud_thin_to_thick":
        return bool(cloud_thin_to_thick)
    if c == "bullish_to_neutral_transition":
        return bool(bullish_to_neutral)
    if c == "bearish_to_neutral_transition":
        return bool(bearish_to_neutral)
    if c == "partial_bullish_stack":
        return bull_count >= 2
    if c == "partial_bearish_stack":
        return bear_count >= 2
    if c == "full_bullish_stack":
        return bull_count == 3
    if c == "full_bearish_stack":
        return bear_count == 3
    if c == "bullish_stack_weakening":
        return bull_prev_count >= 2 and bull_count < bull_prev_count
    if c == "bearish_stack_weakening":
        return bear_prev_count >= 2 and bear_count < bear_prev_count
    if c == "kijun_bounce_bullish":
        return (
            price_above_cloud
            and tenkan_above
            and float(close_now) >= float(kijun)
            and distance_to_kijun_pct <= bounce_tol
        )
    if c == "kijun_reject_bearish":
        return (
            price_below_cloud
            and tenkan_below
            and float(close_now) <= float(kijun)
            and distance_to_kijun_pct <= bounce_tol
        )
    if c == "weak_cross_inside_cloud":
        return inside_cloud and (bool(cross_up) or bool(cross_down))
    if c == "cloud_rejection_bearish":
        return (
            close_prev is not None
            and cloud_top_prev is not None
            and float(close_prev) > float(cloud_top_prev)
            and inside_cloud
        )
    if c == "cloud_rejection_bullish":
        return (
            close_prev is not None
            and cloud_bottom_prev is not None
            and float(close_prev) < float(cloud_bottom_prev)
            and inside_cloud
        )
    return False


def _ichimoku_conditions_hit(
    conditions: list[str],
    *,
    state: dict[str, float],
    cloud_thickness_threshold_pct: float,
    kijun_bounce_tolerance_pct: float,
    delayed_cross_lookback: int,
    mode: str = "all",
) -> bool:
    active = [c for c in _normalize_ichi_conditions(conditions, default="hold") if c != "hold"]
    if not active:
        return False
    match_mode = _normalize_ichi_match_mode(mode, default="all")
    results = [
        _ichimoku_condition_hit(
            cond,
            state=state,
            cloud_thickness_threshold_pct=cloud_thickness_threshold_pct,
            kijun_bounce_tolerance_pct=kijun_bounce_tolerance_pct,
            delayed_cross_lookback=delayed_cross_lookback,
        )
        for cond in active
    ]
    if match_mode == "any":
        return any(results)
    return all(results)


def _market_ttm_sma_tail(values: list[float], length: int) -> Optional[float]:
    ln = max(1, int(length))
    if len(values) < ln:
        return None
    try:
        window = [float(v) for v in values[-ln:]]
    except Exception:
        return None
    if not window:
        return None
    return float(sum(window) / float(ln))


def _market_ttm_atr(
    closes: list[float],
    *,
    highs: Optional[list[float]] = None,
    lows: Optional[list[float]] = None,
    length: int = 20,
) -> Optional[float]:
    ln = max(1, int(length))
    n = len(closes)
    if n < ln:
        return None
    close_vals: list[float] = []
    high_vals: list[float] = []
    low_vals: list[float] = []
    for i, raw_close in enumerate(closes):
        close_now = _to_float_opt(raw_close)
        if close_now is None:
            return None
        c = float(close_now)
        h = _to_float_opt(highs[i]) if isinstance(highs, list) and i < len(highs) else None
        l = _to_float_opt(lows[i]) if isinstance(lows, list) and i < len(lows) else None
        close_vals.append(c)
        high_vals.append(float(h) if h is not None else c)
        low_vals.append(float(l) if l is not None else c)

    trs: list[float] = []
    for i in range(n):
        hi = float(high_vals[i])
        lo = float(low_vals[i])
        prev_close = float(close_vals[i - 1]) if i > 0 else float(close_vals[i])
        tr = max(hi - lo, abs(hi - prev_close), abs(lo - prev_close))
        trs.append(float(tr))
    if len(trs) < ln:
        return None
    window = trs[-ln:]
    return float(sum(window) / float(ln))


def _market_ttm_linreg_endpoint(values: list[float]) -> Optional[float]:
    n = len(values)
    if n <= 0:
        return None
    try:
        ys = [float(v) for v in values]
    except Exception:
        return None
    x_sum = float((n - 1) * n / 2)
    x2_sum = float((n - 1) * n * (2 * n - 1) / 6)
    y_sum = float(sum(ys))
    xy_sum = float(sum((i * ys[i]) for i in range(n)))
    denom = (float(n) * x2_sum) - (x_sum * x_sum)
    slope = 0.0 if abs(denom) <= 1.0e-12 else ((float(n) * xy_sum) - (x_sum * y_sum)) / denom
    intercept = (y_sum - (slope * x_sum)) / float(n)
    return float(intercept + (slope * float(n - 1)))


def _market_ttm_momentum(
    closes: list[float],
    *,
    highs: Optional[list[float]] = None,
    lows: Optional[list[float]] = None,
    length: int = 20,
) -> Optional[float]:
    ln = max(2, int(length))
    if len(closes) < ln:
        return None
    close_window = closes[-ln:]
    try:
        close_vals = [float(v) for v in close_window]
    except Exception:
        return None
    high_vals: list[float] = []
    low_vals: list[float] = []
    for i, c in enumerate(close_vals):
        source_idx = len(closes) - ln + i
        h = _to_float_opt(highs[source_idx]) if isinstance(highs, list) and source_idx < len(highs) else None
        l = _to_float_opt(lows[source_idx]) if isinstance(lows, list) and source_idx < len(lows) else None
        high_vals.append(float(h) if h is not None else float(c))
        low_vals.append(float(l) if l is not None else float(c))

    highest = max(high_vals) if high_vals else None
    lowest = min(low_vals) if low_vals else None
    if highest is None or lowest is None:
        return None
    sma_close = sum(close_vals) / float(ln)
    ref = ((float(highest) + float(lowest)) / 2.0 + float(sma_close)) / 2.0
    centered = [float(c - ref) for c in close_vals]
    return _market_ttm_linreg_endpoint(centered)


def _market_ttm_state(
    closes: list[float],
    *,
    highs: Optional[list[float]] = None,
    lows: Optional[list[float]] = None,
    bb_length: int = 20,
    bb_mult: float = 2.0,
    kc_length: int = 20,
    kc_mult: float = 1.5,
    momentum_length: int = 20,
) -> Optional[dict[str, float]]:
    bb_len = max(2, int(bb_length))
    bb_mul = max(0.1, float(bb_mult))
    kc_len = max(2, int(kc_length))
    kc_mul = max(0.1, float(kc_mult))
    mom_len = max(2, int(momentum_length))
    need = max(bb_len, kc_len, mom_len)
    if len(closes) < need:
        return None

    def _single_state(
        closes_local: list[float],
        highs_local: Optional[list[float]],
        lows_local: Optional[list[float]],
    ) -> Optional[dict[str, float]]:
        bb = _market_bollinger_snapshot(closes_local, length=bb_len, std_mult=bb_mul)
        if bb is None:
            return None
        kc_mid = _market_ttm_sma_tail(closes_local, kc_len)
        kc_atr = _market_ttm_atr(closes_local, highs=highs_local, lows=lows_local, length=kc_len)
        mom = _market_ttm_momentum(closes_local, highs=highs_local, lows=lows_local, length=mom_len)
        if kc_mid is None or kc_atr is None or mom is None:
            return None
        kc_upper = float(kc_mid) + (kc_mul * float(kc_atr))
        kc_lower = float(kc_mid) - (kc_mul * float(kc_atr))
        bb_upper = float(bb["upper"])
        bb_lower = float(bb["lower"])
        squeeze_on = bool(bb_upper <= kc_upper and bb_lower >= kc_lower)
        squeeze_off = bool(bb_upper > kc_upper and bb_lower < kc_lower)
        return {
            "bb_middle": float(bb["middle"]),
            "bb_upper": float(bb_upper),
            "bb_lower": float(bb_lower),
            "kc_middle": float(kc_mid),
            "kc_upper": float(kc_upper),
            "kc_lower": float(kc_lower),
            "momentum": float(mom),
            "squeeze_on": 1.0 if squeeze_on else 0.0,
            "squeeze_off": 1.0 if squeeze_off else 0.0,
        }

    current = _single_state(closes, highs, lows)
    if current is None:
        return None
    prev_state: Optional[dict[str, float]] = None
    if len(closes) >= (need + 1):
        prev_state = _single_state(
            closes[:-1],
            highs[:-1] if isinstance(highs, list) and highs else None,
            lows[:-1] if isinstance(lows, list) and lows else None,
        )
    prev_momentum = _to_float_opt(prev_state.get("momentum")) if isinstance(prev_state, dict) else None
    prev_squeeze_on = bool(prev_state.get("squeeze_on")) if isinstance(prev_state, dict) else False
    current["prev_momentum"] = float(prev_momentum) if prev_momentum is not None else None
    current["prev_squeeze_on"] = 1.0 if prev_squeeze_on else 0.0
    return current


def _ttm_condition_hit(cond: str, *, state: dict[str, float]) -> bool:
    c = _normalize_ttm_condition(cond, default="hold")
    if c == "hold":
        return False
    momentum = float(state.get("momentum") or 0.0)
    prev_momentum = _to_float_opt(state.get("prev_momentum"))
    squeeze_on = bool(state.get("squeeze_on"))
    squeeze_off = bool(state.get("squeeze_off"))
    prev_squeeze_on = bool(state.get("prev_squeeze_on"))

    if c == "squeeze_on":
        return squeeze_on
    if c == "squeeze_off":
        return squeeze_off
    if c == "squeeze_fired":
        return prev_squeeze_on and squeeze_off
    if c == "momentum_above_zero":
        return momentum > 0.0
    if c == "momentum_below_zero":
        return momentum < 0.0
    if c == "momentum_increasing":
        return prev_momentum is not None and momentum > float(prev_momentum)
    if c == "momentum_decreasing":
        return prev_momentum is not None and momentum < float(prev_momentum)
    if c == "momentum_cross_up":
        return prev_momentum is not None and float(prev_momentum) <= 0.0 and momentum > 0.0
    if c == "momentum_cross_down":
        return prev_momentum is not None and float(prev_momentum) >= 0.0 and momentum < 0.0
    if c == "long_trend":
        return squeeze_off and (momentum > 0.0) and (prev_momentum is not None and momentum > float(prev_momentum))
    if c == "short_trend":
        return squeeze_off and (momentum < 0.0) and (prev_momentum is not None and momentum < float(prev_momentum))
    if c == "long_release":
        return (prev_squeeze_on and squeeze_off) and (momentum > 0.0)
    if c == "short_release":
        return (prev_squeeze_on and squeeze_off) and (momentum < 0.0)
    return False


def _roc_condition_hit(
    cond: str,
    *,
    roc: float,
    prev_roc: Optional[float],
    threshold: float,
) -> bool:
    c = _normalize_roc_condition(cond, default="hold")
    if c == "hold":
        return False
    if c == "roc_above_threshold":
        return float(roc) >= float(threshold)
    if c == "roc_below_threshold":
        return float(roc) <= float(threshold)
    if c == "roc_cross_up_zero":
        return prev_roc is not None and float(prev_roc) <= 0.0 and float(roc) > 0.0
    if c == "roc_cross_down_zero":
        return prev_roc is not None and float(prev_roc) >= 0.0 and float(roc) < 0.0
    if c == "roc_cross_up_threshold":
        return prev_roc is not None and float(prev_roc) <= float(threshold) and float(roc) > float(threshold)
    if c == "roc_cross_down_threshold":
        return prev_roc is not None and float(prev_roc) >= float(threshold) and float(roc) < float(threshold)
    if c == "roc_increasing":
        return prev_roc is not None and float(roc) > float(prev_roc)
    if c == "roc_decreasing":
        return prev_roc is not None and float(roc) < float(prev_roc)
    if c == "roc_positive":
        return float(roc) > 0.0
    if c == "roc_negative":
        return float(roc) < 0.0
    if c == "momentum_long":
        return float(roc) > 0.0 and prev_roc is not None and float(roc) > float(prev_roc)
    if c == "momentum_short":
        return float(roc) < 0.0 and prev_roc is not None and float(roc) < float(prev_roc)
    return False


def _sar_condition_hit(
    cond: str,
    *,
    close_now: float,
    close_prev: Optional[float],
    sar_now: float,
    prev_sar: Optional[float],
    trend_up: Optional[bool] = None,
) -> bool:
    c = _normalize_sar_condition(cond, default="hold")
    if c == "hold":
        return False
    if c == "price_above_sar":
        return float(close_now) > float(sar_now)
    if c == "price_below_sar":
        return float(close_now) < float(sar_now)
    if c == "sar_cross_up":
        return close_prev is not None and prev_sar is not None and float(close_prev) <= float(prev_sar) and float(close_now) > float(sar_now)
    if c == "sar_cross_down":
        return close_prev is not None and prev_sar is not None and float(close_prev) >= float(prev_sar) and float(close_now) < float(sar_now)
    if c == "sar_rising":
        if trend_up is not None:
            return bool(trend_up)
        return prev_sar is not None and float(sar_now) > float(prev_sar)
    if c == "sar_falling":
        if trend_up is not None:
            return not bool(trend_up)
        return prev_sar is not None and float(sar_now) < float(prev_sar)
    if c == "trend_long":
        dir_ok = bool(trend_up) if trend_up is not None else (prev_sar is not None and float(sar_now) > float(prev_sar))
        return float(close_now) > float(sar_now) and bool(dir_ok)
    if c == "trend_short":
        dir_ok = (not bool(trend_up)) if trend_up is not None else (prev_sar is not None and float(sar_now) < float(prev_sar))
        return float(close_now) < float(sar_now) and bool(dir_ok)
    return False


def _eval_indicator_rule(
    rule: dict[str, Any],
    closes: list[float],
    price: float,
    *,
    opens: Optional[list[float]] = None,
    highs: Optional[list[float]] = None,
    lows: Optional[list[float]] = None,
    volumes: Optional[list[float]] = None,
    timestamps: Optional[list[str]] = None,
) -> dict[str, Any]:
    kind_raw = str(rule.get("kind") or "").strip().lower()
    if kind_raw == "ha":
        kind = "heikin_ashi"
    elif kind_raw in ("bollinger", "bollinger_bands"):
        kind = "bb"
    elif kind_raw in ("ichimoku", "ichimoku_cloud", "ichi"):
        kind = "ichimoku"
    elif kind_raw in ("ttm", "ttm_squeeze", "squeeze_momentum"):
        kind = "ttm"
    elif kind_raw in ("roc", "rate_of_change"):
        kind = "roc"
    elif kind_raw in ("sar", "psar", "parabolic_sar", "parabolic"):
        kind = "sar"
    elif kind_raw in ("donchian", "donchian_breakout", "donchian_channel", "donchian_channels"):
        kind = "donchian"
    elif kind_raw in ("pivot", "pivot_points", "pivots"):
        kind = "pivot"
    elif kind_raw in ("supertrend", "supertrend_trend"):
        kind = "supertrend"
    elif kind_raw in ("vwap", "vwap_filter"):
        kind = "vwap"
    elif kind_raw in ("relative_volume", "rvol", "rel_volume"):
        kind = "relative_volume"
    else:
        kind = kind_raw
    params = rule.get("params") if isinstance(rule.get("params"), dict) else {}
    out: dict[str, Any] = {
        "name": _rule_name(rule),
        "kind": kind,
        "_rule_kind": kind,
        "_rule_params": params,
        "_rule_id": str(params.get("rule_id") or "").strip(),
        "buy_ok": False,
        "sell_ok": False,
        "value": "—",
        "detail": "",
    }

    if kind in ("ma", "ema"):
        ma_mode = _normalize_ma_mode(params.get("mode"), default="single")
        if ma_mode == "ribbon":
            level_values: list[str] = []
            level_details: list[str] = []
            level_actions: list[str] = []
            level_checks: list[dict[str, Any]] = []
            unavailable: list[str] = []
            for level in _ma_ribbon_levels_from_params(params):
                ma_type = str(level["ma_type"])
                length = int(level["length"])
                line_val = _market_line_value(closes, ma_type=ma_type, length=length)
                line_tag = "EMA" if ma_type == "ema" else "MA"
                if line_val is None:
                    unavailable.append(f"{level['label']} {line_tag}{length}")
                    continue
                action, relation = _ma_ribbon_level_signal(
                    price,
                    float(line_val),
                    above_action=str(level["above_action"]),
                    below_action=str(level["below_action"]),
                )
                level_actions.append(action)
                level_value = f"{level['label']} {line_tag}{length}={_fmt_market_num(line_val, 2)}"
                level_detail = f"{level['label']} {relation}->{str(action).upper()}"
                level_values.append(level_value)
                level_details.append(level_detail)
                level_checks.append(
                    {
                        "name": _ma_ribbon_level_name(out.get("name"), level),
                        "kind": kind,
                        "_rule_kind": kind,
                        "_rule_params": params,
                        "_rule_id": _ma_ribbon_level_rule_id(out.get("_rule_id"), level.get("slot")),
                        "_ribbon_parent_rule_id": str(out.get("_rule_id") or "").strip(),
                        "_ribbon_slot": str(level.get("slot") or "").strip().lower(),
                        "buy_ok": action == "buy",
                        "sell_ok": action == "sell",
                        "buy_ignored": False,
                        "sell_ignored": False,
                        "value": level_value,
                        "detail": level_detail,
                    }
                )
            if unavailable:
                out["detail"] = f"Ribbon unavailable: {', '.join(unavailable)}"
                return out
            buy_ok = bool(level_actions) and all(action == "buy" for action in level_actions)
            sell_ok = bool(level_actions) and all(action == "sell" for action in level_actions)
            agreement = "BUY" if buy_ok else ("SELL" if sell_ok else "HOLD")
            out["buy_ok"] = bool(buy_ok)
            out["sell_ok"] = bool(sell_ok)
            out["buy_ignored"] = False
            out["sell_ignored"] = False
            out["value"] = " | ".join(level_values) if level_values else "Ribbon unavailable"
            out["detail"] = "; ".join(level_details) + f" => {agreement} (all levels must agree)"
            out["_ribbon_level_checks"] = level_checks
            return out
        if ma_mode == "mapped":
            ma_type = _normalize_ma_type(params.get("ma_type"), default=("ema" if kind == "ema" else "sma"))
            length = max(2, int(_to_int_opt(params.get("length")) or 30))
            line_val = _market_line_value(closes, ma_type=ma_type, length=length)
            line_tag = "EMA" if ma_type == "ema" else "MA"
            if line_val is None:
                out["detail"] = f"{line_tag}{length} unavailable"
                return out
            action, relation = _ma_ribbon_level_signal(
                price,
                float(line_val),
                above_action=_normalize_signal_action_mode(params.get("above_action"), default="hold"),
                below_action=_normalize_signal_action_mode(params.get("below_action"), default="hold"),
            )
            out["buy_ok"] = action == "buy"
            out["sell_ok"] = action == "sell"
            out["buy_ignored"] = False
            out["sell_ignored"] = False
            out["value"] = f"{line_tag}{length}={_fmt_market_num(line_val, 2)}"
            out["detail"] = f"{relation}->{str(action).upper()}"
            return out

        ma_type = _normalize_ma_type(params.get("ma_type"), default=("ema" if kind == "ema" else "sma"))
        length = max(2, int(_to_int_opt(params.get("length")) or 30))
        line_val = _market_line_value(closes, ma_type=ma_type, length=length)
        line_tag = "EMA" if ma_type == "ema" else "MA"
        if line_val is None:
            out["detail"] = f"{line_tag}{length} unavailable"
            return out
        buy_rel = _normalize_relation_mode(params.get("buy_relation"), default="hold")
        sell_rel = _normalize_relation_mode(params.get("sell_relation"), default="hold")

        def _rel_ok(rel: str, current: float, ref: float) -> bool:
            if rel == "above":
                return current > ref
            if rel == "below":
                return current < ref
            return False

        buy_ignored = buy_rel == "hold"
        sell_ignored = sell_rel == "hold"
        buy_ok = False if buy_ignored else _rel_ok(buy_rel, price, line_val)
        sell_ok = False if sell_ignored else _rel_ok(sell_rel, price, line_val)

        track_d = bool(int(params.get("track_derivative") or 0))
        dval = _market_line_derivative(closes, ma_type=ma_type, length=length) if track_d else None
        buy_d_min = _to_float_opt(params.get("buy_derivative_min"))
        sell_d_max = _to_float_opt(params.get("sell_derivative_max"))
        if track_d:
            if dval is None:
                if not buy_ignored:
                    buy_ok = False
                if not sell_ignored:
                    sell_ok = False
            else:
                if (not buy_ignored) and buy_d_min is not None:
                    buy_ok = buy_ok and (float(dval) >= float(buy_d_min))
                if (not sell_ignored) and sell_d_max is not None:
                    sell_ok = sell_ok and (float(dval) <= float(sell_d_max))

        # Optional override: if this line is above/below another selected MA/EMA, force action.
        unless_enabled = bool(int(params.get("unless_enabled") or 0))
        other_val: Optional[float] = None
        unless_hit = False
        unless_rel = str(params.get("unless_relation") or "above").strip().lower()
        unless_type = _normalize_ma_type(params.get("unless_type"), default="sma")
        unless_length = max(2, int(_to_int_opt(params.get("unless_length")) or 30))
        unless_action = str(params.get("unless_action") or "sell").strip().lower()
        if unless_enabled:
            other_val = _market_line_value(closes, ma_type=unless_type, length=unless_length)
            if other_val is not None:
                if unless_rel == "above":
                    unless_hit = float(line_val) > float(other_val)
                else:
                    unless_hit = float(line_val) < float(other_val)
            if unless_hit:
                if unless_action == "buy":
                    buy_ok = True
                    buy_ignored = False
                    sell_ok = False
                elif unless_action == "sell":
                    buy_ok = False
                    sell_ok = True
                    sell_ignored = False

        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = bool(buy_ignored)
        out["sell_ignored"] = bool(sell_ignored)
        out["value"] = f"{line_tag}{length}={_fmt_market_num(line_val, 2)}"
        unless_detail = ""
        if unless_enabled:
            utag = "EMA" if unless_type == "ema" else "MA"
            out["value"] += f" | U:{utag}{unless_length}={_fmt_market_num(other_val, 2)}"
            unless_detail = (
                f" unless {line_tag}{length} {unless_rel} {utag}{unless_length}->{unless_action}"
                f" (hit={'yes' if unless_hit else 'no'})"
            )
        if track_d:
            dtag = "dEMA" if ma_type == "ema" else "dMA"
            out["detail"] = f"{dtag}{length}={_fmt_market_num(dval, 4)}{unless_detail}"
        else:
            out["detail"] = unless_detail.strip()
        return out

    if kind == "bb":
        length = max(2, int(_to_int_opt(params.get("length")) or 20))
        std_mult = max(0.1, float(_to_float_opt(params.get("std_mult")) or 2.0))
        buy_cond = _normalize_bb_condition(params.get("buy_condition"), default="hold")
        sell_cond = _normalize_bb_condition(params.get("sell_condition"), default="hold")
        buy_ignored = buy_cond == "hold"
        sell_ignored = sell_cond == "hold"
        squeeze_threshold_pct = float(_to_float_opt(params.get("squeeze_threshold_pct")) or 5.0)
        pb_buy = float(_to_float_opt(params.get("percent_b_buy_threshold")) or 0.2)
        pb_sell = float(_to_float_opt(params.get("percent_b_sell_threshold")) or 0.8)

        curr = _market_bollinger_snapshot(closes, length=length, std_mult=std_mult)
        if curr is None:
            out["detail"] = f"BB({length},{_fmt_market_num(std_mult,2)}σ) unavailable"
            return out
        prev = _market_bollinger_snapshot(closes[:-1], length=length, std_mult=std_mult) if len(closes) >= (length + 1) else None
        prev_close = float(closes[-2]) if len(closes) >= 2 else None
        prev_upper = float(prev["upper"]) if isinstance(prev, dict) else None
        prev_lower = float(prev["lower"]) if isinstance(prev, dict) else None
        prev_width_pct = float(prev["width_pct"]) if isinstance(prev, dict) else None

        close_now = float(_to_float_opt(price) or closes[-1])
        high_now: Optional[float] = None
        low_now: Optional[float] = None
        if isinstance(highs, list) and highs:
            high_now = _to_float_opt(highs[-1])
        if isinstance(lows, list) and lows:
            low_now = _to_float_opt(lows[-1])

        upper = float(curr["upper"])
        lower = float(curr["lower"])
        middle = float(curr["middle"])
        width_pct = float(curr["width_pct"])
        percent_b = float(curr["percent_b"])

        buy_ok = True if buy_ignored else _bb_condition_hit(
            buy_cond,
            close_now=close_now,
            high_now=high_now,
            low_now=low_now,
            upper=upper,
            lower=lower,
            middle=middle,
            prev_close=prev_close,
            prev_upper=prev_upper,
            prev_lower=prev_lower,
            width_pct=width_pct,
            prev_width_pct=prev_width_pct,
            squeeze_threshold_pct=squeeze_threshold_pct,
            percent_b=percent_b,
            percent_b_threshold=pb_buy,
        )
        sell_ok = True if sell_ignored else _bb_condition_hit(
            sell_cond,
            close_now=close_now,
            high_now=high_now,
            low_now=low_now,
            upper=upper,
            lower=lower,
            middle=middle,
            prev_close=prev_close,
            prev_upper=prev_upper,
            prev_lower=prev_lower,
            width_pct=width_pct,
            prev_width_pct=prev_width_pct,
            squeeze_threshold_pct=squeeze_threshold_pct,
            percent_b=percent_b,
            percent_b_threshold=pb_sell,
        )

        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = bool(buy_ignored)
        out["sell_ignored"] = bool(sell_ignored)
        out["value"] = (
            f"BB{length} M={_fmt_market_num(middle,3)} U={_fmt_market_num(upper,3)} "
            f"L={_fmt_market_num(lower,3)} W={_fmt_market_num(width_pct,3)}% %B={_fmt_market_num(percent_b,3)}"
        )
        out["detail"] = (
            f"buy={buy_cond} sell={sell_cond} "
            f"squeeze<={_fmt_market_num(squeeze_threshold_pct,3)}% "
            f"pb_buy={_fmt_market_num(pb_buy,3)} pb_sell={_fmt_market_num(pb_sell,3)} "
            f"prevW={_fmt_market_num(prev_width_pct,3)}%"
        )
        return out

    if kind == "ichimoku":
        conversion_len, base_len, leading_b_len, displacement = _ichimoku_lengths_from_params(params)
        delayed_cross_lookback = max(1, int(_to_int_opt(params.get("delayed_cross_lookback")) or 3))
        buy_mode = _normalize_ichi_match_mode(params.get("buy_match_mode"), default="all")
        sell_mode = _normalize_ichi_match_mode(params.get("sell_match_mode"), default="all")
        block_mode = _normalize_ichi_match_mode(params.get("block_match_mode"), default="all")
        buy_conds = _normalize_ichi_conditions(params.get("buy_conditions", params.get("buy_condition")), default="hold")
        sell_conds = _normalize_ichi_conditions(params.get("sell_conditions", params.get("sell_condition")), default="hold")
        block_conds = _normalize_ichi_conditions(params.get("block_conditions", params.get("block_condition")), default="hold")
        buy_active = [c for c in buy_conds if c != "hold"]
        sell_active = [c for c in sell_conds if c != "hold"]
        block_active = [c for c in block_conds if c != "hold"]
        buy_ignored = not buy_active
        sell_ignored = not sell_active
        block_ignored = not block_active
        cloud_thickness_threshold_pct = float(_to_float_opt(params.get("cloud_thickness_threshold_pct")) or 1.0)
        base_line_bounce_tolerance_pct = _ichimoku_base_bounce_tolerance_pct(params)

        state = _market_ichimoku_state(
            closes,
            highs=highs,
            lows=lows,
            tenkan_length=conversion_len,
            kijun_length=base_len,
            senkou_b_length=leading_b_len,
            displacement=displacement,
        )
        if state is None:
            out["detail"] = (
                f"ICHI(Conversion/Base/LeadingB={conversion_len}/{base_len}/{leading_b_len},disp={displacement}) unavailable"
            )
            return out

        buy_ok = True if buy_ignored else _ichimoku_conditions_hit(
            buy_active,
            state=state,
            cloud_thickness_threshold_pct=cloud_thickness_threshold_pct,
            kijun_bounce_tolerance_pct=base_line_bounce_tolerance_pct,
            delayed_cross_lookback=delayed_cross_lookback,
            mode=buy_mode,
        )
        sell_ok = True if sell_ignored else _ichimoku_conditions_hit(
            sell_active,
            state=state,
            cloud_thickness_threshold_pct=cloud_thickness_threshold_pct,
            kijun_bounce_tolerance_pct=base_line_bounce_tolerance_pct,
            delayed_cross_lookback=delayed_cross_lookback,
            mode=sell_mode,
        )
        block_ok = False if block_ignored else _ichimoku_conditions_hit(
            block_active,
            state=state,
            cloud_thickness_threshold_pct=cloud_thickness_threshold_pct,
            kijun_bounce_tolerance_pct=base_line_bounce_tolerance_pct,
            delayed_cross_lookback=delayed_cross_lookback,
            mode=block_mode,
        )
        if block_ok:
            buy_ok = False
            sell_ok = False

        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = bool(buy_ignored)
        out["sell_ignored"] = bool(sell_ignored)
        out["block_ok"] = bool(block_ok)
        out["block_ignored"] = bool(block_ignored)
        out["value"] = (
            f"ICHI Conversion={_fmt_market_num(state.get('tenkan'),3)} Base={_fmt_market_num(state.get('kijun'),3)} "
            f"LiveCloudTop={_fmt_market_num(state.get('cloud_top'),3)} LiveCloudBottom={_fmt_market_num(state.get('cloud_bottom'),3)} "
            f"ProjectedA={_fmt_market_num(state.get('future_span_a'),3)} ProjectedB={_fmt_market_num(state.get('future_span_b'),3)} "
            f"Thickness={_fmt_market_num(state.get('cloud_thickness_pct'),3)}%"
        )
        buy_txt = "hold" if not buy_active else "+".join(buy_active)
        sell_txt = "hold" if not sell_active else "+".join(sell_active)
        block_txt = "hold" if not block_active else "+".join(block_active)
        out["detail"] = (
            f"buy({buy_mode})={buy_txt} sell({sell_mode})={sell_txt} block({block_mode})={block_txt} "
            f"thick_thr={_fmt_market_num(cloud_thickness_threshold_pct,3)}% "
            f"base_tol={_fmt_market_num(base_line_bounce_tolerance_pct,3)}% "
            f"delay={delayed_cross_lookback} "
            f"lagging_ref={_fmt_market_num(state.get('chikou_ref_price'),3)}"
        )
        return out

    if kind == "ttm":
        bb_len = max(2, int(_to_int_opt(params.get("bb_length")) or 20))
        bb_mult = max(0.1, float(_to_float_opt(params.get("bb_mult")) or 2.0))
        kc_len = max(2, int(_to_int_opt(params.get("kc_length")) or 20))
        kc_mult = max(0.1, float(_to_float_opt(params.get("kc_mult")) or 1.5))
        mom_len = max(2, int(_to_int_opt(params.get("momentum_length")) or 20))
        buy_cond = _normalize_ttm_condition(params.get("buy_condition"), default="hold")
        sell_cond = _normalize_ttm_condition(params.get("sell_condition"), default="hold")
        buy_ignored = buy_cond == "hold"
        sell_ignored = sell_cond == "hold"

        state = _market_ttm_state(
            closes,
            highs=highs,
            lows=lows,
            bb_length=bb_len,
            bb_mult=bb_mult,
            kc_length=kc_len,
            kc_mult=kc_mult,
            momentum_length=mom_len,
        )
        if state is None:
            out["detail"] = f"TTM(BB {bb_len}/{_fmt_market_num(bb_mult,2)}, KC {kc_len}/{_fmt_market_num(kc_mult,2)}, MOM {mom_len}) unavailable"
            return out

        buy_ok = True if buy_ignored else _ttm_condition_hit(buy_cond, state=state)
        sell_ok = True if sell_ignored else _ttm_condition_hit(sell_cond, state=state)
        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = bool(buy_ignored)
        out["sell_ignored"] = bool(sell_ignored)
        out["value"] = (
            f"TTM SQ={'ON' if bool(state.get('squeeze_on')) else 'OFF'} "
            f"MOM={_fmt_market_num(state.get('momentum'),4)} "
            f"BBU={_fmt_market_num(state.get('bb_upper'),3)} BBL={_fmt_market_num(state.get('bb_lower'),3)} "
            f"KCU={_fmt_market_num(state.get('kc_upper'),3)} KCL={_fmt_market_num(state.get('kc_lower'),3)}"
        )
        out["detail"] = (
            f"buy={buy_cond} sell={sell_cond} "
            f"prev_mom={_fmt_market_num(state.get('prev_momentum'),4)} "
            f"prev_sq={'ON' if bool(state.get('prev_squeeze_on')) else 'OFF'}"
        )
        return out

    if kind == "rsi":
        rsi = _market_rsi(closes, 14)
        if rsi is None:
            out["detail"] = "RSI unavailable"
            return out
        oversold = _to_float_opt(params.get("oversold"))
        overbought = _to_float_opt(params.get("overbought"))
        os_rel = str(params.get("oversold_relation") or "below").strip().lower()
        ob_rel = str(params.get("overbought_relation") or "above").strip().lower()
        os_action = _normalize_signal_action_mode(params.get("oversold_action"), default="buy")
        ob_action = _normalize_signal_action_mode(params.get("overbought_action"), default="sell")

        def _rel_match(value: float, threshold: float, relation: str) -> bool:
            if relation == "above":
                return value >= threshold
            return value <= threshold

        buy_checks: list[bool] = []
        sell_checks: list[bool] = []
        if oversold is not None and os_action in ("buy", "sell"):
            hit = _rel_match(float(rsi), float(oversold), os_rel)
            if os_action == "buy":
                buy_checks.append(hit)
            else:
                sell_checks.append(hit)
        if overbought is not None and ob_action in ("buy", "sell"):
            hit = _rel_match(float(rsi), float(overbought), ob_rel)
            if ob_action == "buy":
                buy_checks.append(hit)
            else:
                sell_checks.append(hit)

        buy_signal = bool(buy_checks) and all(buy_checks)
        sell_signal = bool(sell_checks) and all(sell_checks)
        buy_ok = all(buy_checks) if buy_checks else True
        sell_ok = all(sell_checks) if sell_checks else True
        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = False
        out["sell_ignored"] = False
        out["rsi_buy_signal"] = bool(buy_signal)
        out["rsi_sell_signal"] = bool(sell_signal)
        out["value"] = f"RSI={_fmt_market_num(rsi, 2)}"
        out["detail"] = (
            f"OS={_fmt_market_num(oversold,2)} {os_rel}->{os_action} "
            f"OB={_fmt_market_num(overbought,2)} {ob_rel}->{ob_action}"
        )
        return out

    if kind == "rsi_d":
        drsi = _market_rsi_derivative(closes)
        if drsi is None:
            out["detail"] = "dRSI unavailable"
            return out
        buy_above = _to_float_opt(params.get("buy_above"))
        sell_below = _to_float_opt(params.get("sell_below"))
        buy_ok = True if buy_above is None else (float(drsi) >= float(buy_above))
        sell_ok = True if sell_below is None else (float(drsi) <= float(sell_below))
        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = False
        out["sell_ignored"] = False
        out["value"] = f"dRSI={_fmt_market_num(drsi, 4)}"
        out["detail"] = f"buy>={_fmt_market_num(buy_above,4)} sell<={_fmt_market_num(sell_below,4)}"
        return out

    if kind == "roc":
        length = max(1, int(_to_int_opt(params.get("length")) or 12))
        roc = _market_roc(closes, length)
        if roc is None:
            out["detail"] = f"ROC({length}) unavailable"
            return out
        prev_roc = _market_roc(closes[:-1], length) if len(closes) >= (length + 2) else None
        buy_cond = _normalize_roc_condition(params.get("buy_condition"), default="hold")
        sell_cond = _normalize_roc_condition(params.get("sell_condition"), default="hold")
        buy_thr = float(_to_float_opt(params.get("buy_threshold_pct")) or 0.0)
        sell_thr = float(_to_float_opt(params.get("sell_threshold_pct")) or 0.0)
        buy_ignored = buy_cond == "hold"
        sell_ignored = sell_cond == "hold"
        buy_ok = True if buy_ignored else _roc_condition_hit(
            buy_cond,
            roc=float(roc),
            prev_roc=prev_roc,
            threshold=buy_thr,
        )
        sell_ok = True if sell_ignored else _roc_condition_hit(
            sell_cond,
            roc=float(roc),
            prev_roc=prev_roc,
            threshold=sell_thr,
        )
        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = bool(buy_ignored)
        out["sell_ignored"] = bool(sell_ignored)
        out["value"] = f"ROC{length}={_fmt_market_num(roc,4)}%"
        out["detail"] = (
            f"buy={buy_cond} sell={sell_cond} "
            f"buy_thr={_fmt_market_num(buy_thr,3)}% "
            f"sell_thr={_fmt_market_num(sell_thr,3)}% "
            f"prev={_fmt_market_num(prev_roc,4)}%"
        )
        return out

    if kind == "sar":
        step = max(0.0001, float(_to_float_opt(params.get("step")) or 0.02))
        max_step = max(step, float(_to_float_opt(params.get("max_step")) or 0.2))
        buy_cond = _normalize_sar_condition(params.get("buy_condition"), default="hold")
        sell_cond = _normalize_sar_condition(params.get("sell_condition"), default="hold")
        buy_ignored = buy_cond == "hold"
        sell_ignored = sell_cond == "hold"
        sar_series, sar_trends = _market_sar_series_with_trend(closes, highs=highs, lows=lows, step=step, max_step=max_step)
        sar_now = sar_series[-1] if sar_series else None
        prev_sar = sar_series[-2] if len(sar_series) > 1 else None
        trend_up = sar_trends[-1] if sar_trends else None
        close_now = _to_float_opt(price)
        if close_now is None:
            close_now = _to_float_opt(closes[-1] if closes else None)
        close_prev = _to_float_opt(closes[-2] if len(closes) >= 2 else None)
        if sar_now is None or close_now is None:
            out["detail"] = f"SAR(step={_fmt_market_num(step,4)},max={_fmt_market_num(max_step,4)}) unavailable"
            return out
        buy_ok = True if buy_ignored else _sar_condition_hit(
            buy_cond,
            close_now=float(close_now),
            close_prev=close_prev,
            sar_now=float(sar_now),
            prev_sar=prev_sar,
            trend_up=trend_up,
        )
        sell_ok = True if sell_ignored else _sar_condition_hit(
            sell_cond,
            close_now=float(close_now),
            close_prev=close_prev,
            sar_now=float(sar_now),
            prev_sar=prev_sar,
            trend_up=trend_up,
        )
        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = bool(buy_ignored)
        out["sell_ignored"] = bool(sell_ignored)
        trend_txt = "up" if trend_up is True else ("down" if trend_up is False else "n/a")
        out["value"] = (
            f"SAR={_fmt_market_num(sar_now,4)} "
            f"P={_fmt_market_num(close_now,4)} "
            f"prevSAR={_fmt_market_num(prev_sar,4)} "
            f"trend={trend_txt}"
        )
        out["detail"] = (
            f"buy={buy_cond} sell={sell_cond} "
            f"step={_fmt_market_num(step,4)} "
            f"max={_fmt_market_num(max_step,4)}"
        )
        return out

    if kind == "donchian":
        lookback = max(1, int(_to_int_opt(params.get("lookback")) or 20))
        default_buy = "high_above_upper" if bool(params.get("use_high_break")) else "close_above_upper"
        buy_cond = _normalize_donchian_condition(params.get("buy_condition"), default=default_buy)
        sell_cond = _normalize_donchian_condition(params.get("sell_condition"), default="close_below_lower")
        buy_ignored = buy_cond == "hold"
        sell_ignored = sell_cond == "hold"
        src_highs = highs if isinstance(highs, list) and len(highs) >= len(closes) else closes
        src_lows = lows if isinstance(lows, list) and len(lows) >= len(closes) else closes
        upper_series, lower_series, middle_series = _market_donchian_channels(src_highs, src_lows, lookback)
        upper = upper_series[-1] if upper_series else None
        lower = lower_series[-1] if lower_series else None
        middle = middle_series[-1] if middle_series else None
        prev_upper = upper_series[-2] if len(upper_series) >= 2 else None
        prev_lower = lower_series[-2] if len(lower_series) >= 2 else None
        if upper is None or lower is None or middle is None:
            out["detail"] = f"Donchian({lookback}) unavailable"
            return out
        close_now = float(_to_float_opt(price) or closes[-1])
        high_now = _to_float_opt(src_highs[-1] if src_highs else None)
        low_now = _to_float_opt(src_lows[-1] if src_lows else None)
        buy_ok = True if buy_ignored else _donchian_condition_hit(
            buy_cond,
            close_now=close_now,
            high_now=high_now,
            low_now=low_now,
            upper=float(upper),
            lower=float(lower),
            middle=float(middle),
            prev_upper=prev_upper,
            prev_lower=prev_lower,
        )
        sell_ok = True if sell_ignored else _donchian_condition_hit(
            sell_cond,
            close_now=close_now,
            high_now=high_now,
            low_now=low_now,
            upper=float(upper),
            lower=float(lower),
            middle=float(middle),
            prev_upper=prev_upper,
            prev_lower=prev_lower,
        )
        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = bool(buy_ignored)
        out["sell_ignored"] = bool(sell_ignored)
        out["value"] = (
            f"DC{lookback} U={_fmt_market_num(upper,4)} M={_fmt_market_num(middle,4)} L={_fmt_market_num(lower,4)} "
            f"P={_fmt_market_num(close_now,4)}"
        )
        out["detail"] = f"buy={buy_cond} sell={sell_cond}"
        return out

    if kind == "pivot":
        buy_cond = _normalize_pivot_condition(params.get("buy_condition"), default="above_p")
        sell_cond = _normalize_pivot_condition(params.get("sell_condition"), default="below_p")
        buy_ignored = buy_cond == "hold"
        sell_ignored = sell_cond == "hold"
        src_highs = highs if isinstance(highs, list) and len(highs) >= len(closes) else closes
        src_lows = lows if isinstance(lows, list) and len(lows) >= len(closes) else closes
        close_now = float(_to_float_opt(price) or closes[-1])
        close_prev = _to_float_opt(closes[-2] if len(closes) >= 2 else None)
        tolerance_pct = max(0.0, float(_to_float_opt(params.get("tolerance_pct")) or 0.25))
        if calculate_pivot_points is None or len(src_highs) < 2 or len(src_lows) < 2 or len(closes) < 2:
            out["detail"] = "Pivot Points unavailable"
            return out
        levels = calculate_pivot_points(src_highs, src_lows, closes, source_index=-2)
        if levels is None:
            out["detail"] = "Pivot Points unavailable"
            return out
        buy_ok = True if buy_ignored else _pivot_condition_hit(
            buy_cond,
            close_now=close_now,
            close_prev=close_prev,
            levels=levels,
            tolerance_pct=tolerance_pct,
        )
        sell_ok = True if sell_ignored else _pivot_condition_hit(
            sell_cond,
            close_now=close_now,
            close_prev=close_prev,
            levels=levels,
            tolerance_pct=tolerance_pct,
        )
        level_map = levels.as_dict()
        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = bool(buy_ignored)
        out["sell_ignored"] = bool(sell_ignored)
        out["value"] = (
            f"PIV P={_fmt_market_num(level_map.get('P'),4)} "
            f"S1={_fmt_market_num(level_map.get('S1'),4)} "
            f"R1={_fmt_market_num(level_map.get('R1'),4)} "
            f"Price={_fmt_market_num(close_now,4)}"
        )
        out["detail"] = (
            f"source=previous candle "
            f"buy={_PIVOT_CONDITION_LABELS.get(buy_cond, buy_cond)} "
            f"sell={_PIVOT_CONDITION_LABELS.get(sell_cond, sell_cond)} "
            f"near={_fmt_market_num(tolerance_pct,3)}%"
        )
        return out

    if kind == "supertrend":
        atr_length = max(1, int(_to_int_opt(params.get("atr_length")) or 10))
        multiplier = max(0.1, float(_to_float_opt(params.get("multiplier")) or 3.0))
        buy_cond = _normalize_supertrend_condition(params.get("buy_condition"), default="trend_up")
        sell_cond = _normalize_supertrend_condition(params.get("sell_condition"), default="trend_down")
        buy_ignored = buy_cond == "hold"
        sell_ignored = sell_cond == "hold"
        src_highs = highs if isinstance(highs, list) and len(highs) >= len(closes) else closes
        src_lows = lows if isinstance(lows, list) and len(lows) >= len(closes) else closes
        points = _market_supertrend_points(
            closes,
            highs=src_highs,
            lows=src_lows,
            atr_length=atr_length,
            multiplier=multiplier,
        )
        point_now = points[-1] if points else None
        point_prev = points[-2] if len(points) >= 2 else None
        trend_now = point_now.trend if point_now else None
        direction_now = point_now.direction if point_now else None
        trend_prev = point_prev.trend if point_prev else None
        direction_prev = point_prev.direction if point_prev else None
        close_now = float(_to_float_opt(price) or closes[-1])
        close_prev = _to_float_opt(closes[-2] if len(closes) >= 2 else None)
        if trend_now is None or direction_now is None:
            out["detail"] = f"ST({atr_length},{_fmt_market_num(multiplier,2)}) unavailable"
            return out
        buy_ok = True if buy_ignored else _supertrend_condition_hit(
            buy_cond,
            close_now=close_now,
            close_prev=close_prev,
            trend_now=float(trend_now),
            trend_prev=trend_prev,
            direction_now=float(direction_now),
            direction_prev=direction_prev,
        )
        sell_ok = True if sell_ignored else _supertrend_condition_hit(
            sell_cond,
            close_now=close_now,
            close_prev=close_prev,
            trend_now=float(trend_now),
            trend_prev=trend_prev,
            direction_now=float(direction_now),
            direction_prev=direction_prev,
        )
        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = bool(buy_ignored)
        out["sell_ignored"] = bool(sell_ignored)
        direction_txt = "Up" if float(direction_now) > 0.0 else "Down"
        out["value"] = (
            f"ST({atr_length},{_fmt_market_num(multiplier,3)})={_fmt_market_num(trend_now,4)} "
            f"Price={_fmt_market_num(close_now,4)} Direction={direction_txt}"
        )
        atr_txt = _fmt_market_num(point_now.atr if point_now else None, 4)
        upper_txt = _fmt_market_num(point_now.final_upper if point_now else None, 4)
        lower_txt = _fmt_market_num(point_now.final_lower if point_now else None, 4)
        prev_dir_txt = "Up" if direction_prev is not None and float(direction_prev) > 0.0 else ("Down" if direction_prev is not None else "—")
        out["detail"] = (
            f"Period={atr_length} Factor={_fmt_market_num(multiplier,3)} ATR={atr_txt} "
            f"Final Upper={upper_txt} Final Lower={lower_txt} Previous Direction={prev_dir_txt} "
            f"buy={_SUPERTREND_CONDITION_LABELS.get(buy_cond, buy_cond)} "
            f"sell={_SUPERTREND_CONDITION_LABELS.get(sell_cond, sell_cond)}"
        )
        return out

    if kind == "vwap":
        buy_cond = _normalize_vwap_condition(params.get("buy_condition"), default="within_band")
        sell_cond = _normalize_vwap_condition(params.get("sell_condition"), default="exit_below")
        buy_ignored = buy_cond == "hold"
        sell_ignored = sell_cond == "hold"
        src_highs = highs if isinstance(highs, list) and len(highs) >= len(closes) else closes
        src_lows = lows if isinstance(lows, list) and len(lows) >= len(closes) else closes
        vwap_series = _market_vwap_series(
            closes,
            highs=src_highs,
            lows=src_lows,
            volumes=volumes,
            timestamps=timestamps,
        )
        vwap_now = vwap_series[-1] if vwap_series else None
        vwap_prev = vwap_series[-2] if len(vwap_series) >= 2 else None
        close_now = float(_to_float_opt(price) or closes[-1])
        close_prev = _to_float_opt(closes[-2] if len(closes) >= 2 else None)
        max_extension = _indicator_pct_decimal(params.get("max_extension_pct"), default=0.015)
        max_pullback = _indicator_pct_decimal(params.get("max_pullback_pct"), default=0.010)
        exit_below = _indicator_pct_decimal(params.get("exit_below_pct"), default=0.012)
        if vwap_now is None or float(vwap_now) <= 0.0:
            out["detail"] = "VWAP unavailable: volume data unavailable"
            return out
        buy_ok = True if buy_ignored else _vwap_condition_hit(
            buy_cond,
            close_now=close_now,
            close_prev=close_prev,
            vwap_now=float(vwap_now),
            vwap_prev=vwap_prev,
            max_extension_pct=max_extension,
            max_pullback_pct=max_pullback,
            exit_below_pct=exit_below,
        )
        sell_ok = True if sell_ignored else _vwap_condition_hit(
            sell_cond,
            close_now=close_now,
            close_prev=close_prev,
            vwap_now=float(vwap_now),
            vwap_prev=vwap_prev,
            max_extension_pct=max_extension,
            max_pullback_pct=max_pullback,
            exit_below_pct=exit_below,
        )
        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = bool(buy_ignored)
        out["sell_ignored"] = bool(sell_ignored)
        dist_pct = ((close_now - float(vwap_now)) / max(abs(float(vwap_now)), 1.0e-9)) * 100.0
        out["value"] = (
            f"VWAP={_fmt_market_num(vwap_now,4)} "
            f"P={_fmt_market_num(close_now,4)} dist={_fmt_market_num(dist_pct,3)}%"
        )
        out["detail"] = (
            f"buy={buy_cond} sell={sell_cond} "
            f"ext={_fmt_market_num(max_extension * 100.0,3)}% "
            f"pull={_fmt_market_num(max_pullback * 100.0,3)}% "
            f"exit={_fmt_market_num(exit_below * 100.0,3)}%"
        )
        return out

    if kind == "relative_volume":
        length = max(1, int(_to_int_opt(params.get("length")) or 20))
        threshold = max(0.0, float(_to_float_opt(params.get("threshold")) or 1.2))
        buy_cond = _normalize_relative_volume_condition(params.get("buy_condition"), default="above_threshold")
        sell_cond = _normalize_relative_volume_condition(params.get("sell_condition"), default="below_threshold")
        buy_ignored = buy_cond == "hold"
        sell_ignored = sell_cond == "hold"
        rvol_series = _market_relative_volume_series(volumes, length=length)
        rvol = rvol_series[-1] if rvol_series else None
        prev_rvol = rvol_series[-2] if len(rvol_series) >= 2 else None
        if rvol is None:
            out["detail"] = f"RVOL({length}) unavailable: volume data unavailable"
            return out
        buy_ok = True if buy_ignored else _relative_volume_condition_hit(
            buy_cond,
            rvol=float(rvol),
            prev_rvol=prev_rvol,
            threshold=threshold,
        )
        sell_ok = True if sell_ignored else _relative_volume_condition_hit(
            sell_cond,
            rvol=float(rvol),
            prev_rvol=prev_rvol,
            threshold=threshold,
        )
        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = bool(buy_ignored)
        out["sell_ignored"] = bool(sell_ignored)
        latest_volume = volumes[-1] if isinstance(volumes, list) and volumes else None
        out["value"] = (
            f"RVOL{length}={_fmt_market_num(rvol,3)} "
            f"V={_fmt_market_num(latest_volume,0)}"
        )
        out["detail"] = (
            f"buy={buy_cond} sell={sell_cond} "
            f"threshold={_fmt_market_num(threshold,3)} prev={_fmt_market_num(prev_rvol,3)}"
        )
        return out

    if kind == "macd":
        fast = max(2, int(_to_int_opt(params.get("fast_length")) or 12))
        slow = max(2, int(_to_int_opt(params.get("slow_length")) or 26))
        signal = max(2, int(_to_int_opt(params.get("signal_length")) or 9))
        mode = str(params.get("mode") or "signal_cross").strip().lower()
        macd_s, sig_s, hist_s = _market_macd_series(closes, fast_len=fast, slow_len=slow, signal_len=signal)
        if len(macd_s) < 2:
            out["detail"] = "MACD unavailable"
            return out
        m0 = macd_s[-1]
        m1 = macd_s[-2] if len(macd_s) > 1 else None
        s0 = sig_s[-1]
        s1 = sig_s[-2] if len(sig_s) > 1 else None
        h0 = hist_s[-1]
        h1 = hist_s[-2] if len(hist_s) > 1 else None
        if None in (m0, m1, s0, s1, h0, h1):
            out["detail"] = "MACD warmup incomplete"
            return out
        m0f = float(m0)
        m1f = float(m1)
        s0f = float(s0)
        s1f = float(s1)
        h0f = float(h0)
        h1f = float(h1)

        bull_cross = m1f <= s1f and m0f > s0f
        bear_cross = m1f >= s1f and m0f < s0f
        cross_up_zero = m1f <= 0.0 and m0f > 0.0
        cross_down_zero = m1f >= 0.0 and m0f < 0.0

        buy_ok = False
        sell_ok = False
        buy_ignored = False
        sell_ignored = False
        derivative_buy_above_raw = _to_float_opt(params.get("derivative_buy_above"))
        derivative_sell_below_raw = _to_float_opt(params.get("derivative_sell_below"))
        derivative_scope = _normalize_dual_signal_scope(params.get("derivative_signal_scope"), default="both")
        derivative_buy_above = float(derivative_buy_above_raw) if derivative_buy_above_raw is not None else 0.0
        derivative_sell_below = float(derivative_sell_below_raw) if derivative_sell_below_raw is not None else 0.0
        if mode == "signal_cross":
            buy_ok = m0f > s0f
            sell_ok = m0f < s0f
        elif mode == "cross_regime":
            buy_ok = bull_cross and (m0f > 0.0)
            sell_ok = bear_cross and (m0f < 0.0)
        elif mode == "hist_momentum":
            buy_ok = (h0f > 0.0) and (h0f > h1f)
            sell_ok = (h0f < 0.0) and (h0f < h1f)
        elif mode == "zero_reclaim_loss":
            buy_ok = (m0f > 0.0) and (m0f > s0f)
            sell_ok = (m0f > 0.0) and (s0f > 0.0) and (m0f < s0f)
        elif mode == "macd_derivative_sign":
            dmacd = m0f - m1f
            buy_ignored = derivative_scope == "sell"
            sell_ignored = derivative_scope == "buy"
            buy_ok = (not buy_ignored) and (dmacd > derivative_buy_above)
            sell_ok = (not sell_ignored) and (dmacd < derivative_sell_below)
        else:
            buy_ok = bull_cross
            sell_ok = bear_cross

        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = bool(buy_ignored)
        out["sell_ignored"] = bool(sell_ignored)
        out["macd_buy_signal"] = bool((not buy_ignored) and buy_ok)
        out["macd_sell_signal"] = bool((not sell_ignored) and sell_ok)
        if mode == "macd_derivative_sign":
            out["value"] = f"dMACD={_fmt_market_num(m0f - m1f, 4)}"
            out["detail"] = (
                f"{mode} ({fast}/{slow}/{signal}) "
                f"buy>{_fmt_market_num(derivative_buy_above,4)} "
                f"sell<{_fmt_market_num(derivative_sell_below,4)} "
                f"side={derivative_scope}"
            )
        else:
            out["value"] = (
                f"MACD={_fmt_market_num(m0f,4)} SIG={_fmt_market_num(s0f,4)} HIST={_fmt_market_num(h0f,4)}"
            )
            out["detail"] = f"{mode} ({fast}/{slow}/{signal})"
        return out

    if kind == "heikin_ashi":
        if isinstance(opens, list) and isinstance(highs, list) and isinstance(lows, list):
            n = min(len(opens), len(highs), len(lows), len(closes))
            if n >= 2:
                try:
                    src_opens = [float(opens[i]) for i in range(n)]
                    src_highs = [float(highs[i]) for i in range(n)]
                    src_lows = [float(lows[i]) for i in range(n)]
                    src_closes = [float(closes[i]) for i in range(n)]
                except Exception:
                    src_opens, src_highs, src_lows, src_closes = _market_synthetic_ohlc_from_closes(closes)
            else:
                src_opens, src_highs, src_lows, src_closes = _market_synthetic_ohlc_from_closes(closes)
        else:
            src_opens, src_highs, src_lows, src_closes = _market_synthetic_ohlc_from_closes(closes)

        if len(src_closes) < 2:
            out["detail"] = "Heikin Ashi unavailable"
            return out

        ha_open, _ha_high, _ha_low, ha_close = _market_heikin_ashi_series(
            src_opens, src_highs, src_lows, src_closes
        )
        if len(ha_close) < 2:
            out["detail"] = "Heikin Ashi unavailable"
            return out

        mode = str(params.get("mode") or "transition").strip().lower()
        if mode not in ("transition", "state"):
            mode = "transition"
        doji_tol = max(0.0, float(_to_float_opt(params.get("doji_tolerance_pct")) or 0.0))
        prev_state = _market_ha_candle_state(ha_open[-2], ha_close[-2], doji_tolerance_pct=doji_tol)
        curr_state = _market_ha_candle_state(ha_open[-1], ha_close[-1], doji_tolerance_pct=doji_tol)

        if mode == "state":
            buy_ok = curr_state == "bullish"
            sell_ok = curr_state == "bearish"
        else:
            buy_ok = prev_state == "bearish" and curr_state == "bullish"
            sell_ok = prev_state == "bullish" and curr_state == "bearish"

        out["buy_ok"] = bool(buy_ok)
        out["sell_ok"] = bool(sell_ok)
        out["buy_ignored"] = False
        out["sell_ignored"] = False
        out["ha_buy_signal"] = bool(buy_ok)
        out["ha_sell_signal"] = bool(sell_ok)
        out["value"] = f"HA_O={_fmt_market_num(ha_open[-1],4)} HA_C={_fmt_market_num(ha_close[-1],4)}"
        out["detail"] = f"{mode} prev={prev_state} curr={curr_state} doji_tol={_fmt_market_num(doji_tol,3)}%"
        return out

    out["detail"] = "unsupported rule kind"
    return out


def _is_ichimoku_rule_item(item: Any) -> bool:
    if not isinstance(item, dict):
        return False
    kind = str(item.get("_rule_kind") or item.get("kind") or "").strip().lower()
    return kind in ("ichimoku", "ichimoku_cloud", "ichi")


def _apply_indicator_signal_overrides(checks: list[dict[str, Any]]) -> None:
    if not checks:
        return

    def _target_matches(item: Any, target_exact: set[str], target_name_set: set[str]) -> bool:
        if not isinstance(item, dict):
            return False
        item_name = str(item.get("name") or "").strip()
        item_id = str(item.get("_rule_id") or "").strip()
        return bool(item_id and item_id in target_exact) or bool(item_name and item_name.upper() in target_name_set)

    def _apply_override_to_item(
        item: dict[str, Any],
        *,
        apply_buy: bool,
        apply_sell: bool,
        scope: str,
        forced_side: str,
        source_tag: str,
        source_name: str,
    ) -> None:
        if apply_buy:
            item["buy_ok"] = True
            if scope == "both":
                item["sell_ok"] = False
        if apply_sell:
            item["buy_ok"] = False
            item["sell_ok"] = True
        item["_override_applied"] = True
        item["_override_scope"] = scope
        item["_override_forced_side"] = forced_side
        item["_override_source_tag"] = source_tag
        item["_override_source_name"] = source_name
        note = f"{source_tag} override({scope})->{forced_side} by {source_name}"
        detail = str(item.get("detail") or "")
        if note not in detail:
            item["detail"] = f"{detail} | {note}".strip(" |")

    def _effective_state(item: Any) -> str:
        if not isinstance(item, dict):
            return "HOLD"
        if not _is_ichimoku_rule_item(item):
            override_meta = _indicator_override_meta(item)
            if override_meta is not None:
                forced = str(override_meta.get("forced_side") or "").strip().upper()
                if forced in ("BUY", "SELL"):
                    return forced
        buy_ignored = bool(item.get("buy_ignored"))
        sell_ignored = bool(item.get("sell_ignored"))
        if bool(item.get("sell_ok")) and not sell_ignored:
            return "SELL"
        if bool(item.get("buy_ok")) and not buy_ignored:
            return "BUY"
        return "HOLD"

    for src in checks:
        src_kind = str(src.get("_rule_kind") or "").strip().lower()
        if src_kind == "ha":
            src_kind = "heikin_ashi"
        if src_kind not in ("rsi", "macd", "heikin_ashi"):
            continue
        params = src.get("_rule_params") if isinstance(src.get("_rule_params"), dict) else {}
        if not _coerce_bool(params.get("signal_override_enabled"), default=False):
            continue

        targets = _normalize_rule_target_ids(params.get("signal_override_targets"))
        target_exact = {str(t).strip() for t in targets if str(t).strip()}
        target_name_set = {str(t).strip().upper() for t in targets if str(t).strip()}
        if not target_exact and not target_name_set:
            continue

        scope = str(params.get("signal_override_scope") or "both").strip().lower()
        if scope not in ("both", "buy", "sell"):
            scope = "both"

        if src_kind == "rsi":
            buy_signal = bool(src.get("rsi_buy_signal"))
            sell_signal = bool(src.get("rsi_sell_signal"))
            source_tag = "RSI"
        elif src_kind == "macd":
            buy_signal = bool(src.get("macd_buy_signal"))
            sell_signal = bool(src.get("macd_sell_signal"))
            source_tag = "MACD"
        else:
            buy_signal = bool(src.get("ha_buy_signal")) or (
                bool(src.get("buy_ok")) and not bool(src.get("buy_ignored"))
            )
            sell_signal = bool(src.get("ha_sell_signal")) or (
                bool(src.get("sell_ok")) and not bool(src.get("sell_ignored"))
            )
            source_tag = "HA"

        if buy_signal == sell_signal:
            continue

        apply_buy = bool(buy_signal) and scope in ("both", "buy")
        apply_sell = bool(sell_signal) and scope in ("both", "sell")
        if not (apply_buy or apply_sell):
            continue

        source_name = str(src.get("name") or source_tag)
        forced_side = "BUY" if apply_buy else "SELL"
        for dst in checks:
            if dst is src:
                continue
            if _target_matches(dst, target_exact, target_name_set) and not _is_ichimoku_rule_item(dst):
                _apply_override_to_item(
                    dst,
                    apply_buy=apply_buy,
                    apply_sell=apply_sell,
                    scope=scope,
                    forced_side=forced_side,
                    source_tag=source_tag,
                    source_name=source_name,
                )
                if isinstance(dst.get("_ribbon_level_checks"), list):
                    dst["_ribbon_parent_override_applied"] = True
            level_checks = dst.get("_ribbon_level_checks")
            if not isinstance(level_checks, list):
                continue
            for child in level_checks:
                if not _target_matches(child, target_exact, target_name_set):
                    continue
                if _is_ichimoku_rule_item(child):
                    continue
                _apply_override_to_item(
                    child,
                    apply_buy=apply_buy,
                    apply_sell=apply_sell,
                    scope=scope,
                    forced_side=forced_side,
                    source_tag=source_tag,
                    source_name=source_name,
                )

    for parent in checks:
        level_checks = parent.get("_ribbon_level_checks")
        if not isinstance(level_checks, list) or not level_checks:
            continue
        if bool(parent.get("_ribbon_parent_override_applied")):
            continue
        states = [_effective_state(child) for child in level_checks]
        buy_ok = bool(states) and all(state == "BUY" for state in states)
        sell_ok = bool(states) and all(state == "SELL" for state in states)
        agreement = "BUY" if buy_ok else ("SELL" if sell_ok else "HOLD")
        parent["buy_ok"] = bool(buy_ok)
        parent["sell_ok"] = bool(sell_ok)
        parent["buy_ignored"] = False
        parent["sell_ignored"] = False
        value_parts = [str(child.get("value") or "").strip() for child in level_checks if str(child.get("value") or "").strip()]
        detail_parts = [str(child.get("detail") or "").strip() for child in level_checks if str(child.get("detail") or "").strip()]
        if value_parts:
            parent["value"] = " | ".join(value_parts)
        parent["detail"] = (
            "; ".join(detail_parts) + f" => {agreement} (all levels must agree)"
            if detail_parts
            else f"Ribbon => {agreement} (all levels must agree)"
        )


def _build_indicator_rule_checks(
    rules: list[dict[str, Any]],
    closes: list[float],
    price: float,
    *,
    opens: Optional[list[float]] = None,
    highs: Optional[list[float]] = None,
    lows: Optional[list[float]] = None,
    volumes: Optional[list[float]] = None,
    timestamps: Optional[list[str]] = None,
    apply_overrides: bool = True,
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    for rule in rules:
        check = _eval_indicator_rule(
            rule,
            closes,
            price,
            opens=opens,
            highs=highs,
            lows=lows,
            volumes=volumes,
            timestamps=timestamps,
        )
        params = rule.get("params") if isinstance(rule.get("params"), dict) else {}
        base_name = str(check.get("name") or _rule_name(rule))
        base_kind = str(rule.get("kind") or "").strip().lower()
        base_rule_id = str(params.get("rule_id") or "").strip()
        level_checks = check.get("_ribbon_level_checks")
        if isinstance(level_checks, list) and level_checks:
            for child in level_checks:
                if not isinstance(child, dict):
                    continue
                child["name"] = str(child.get("name") or _ma_ribbon_level_name(base_name, child))
                child["_rule_kind"] = str(child.get("_rule_kind") or base_kind)
                child["_rule_params"] = params
                if not str(child.get("_rule_id") or "").strip():
                    child["_rule_id"] = _ma_ribbon_level_rule_id(base_rule_id, child.get("_ribbon_slot"))
                checks.append(child)
            continue
        check["name"] = base_name
        check["_rule_kind"] = base_kind
        check["_rule_params"] = params
        check["_rule_id"] = base_rule_id
        checks.append(check)
    if apply_overrides:
        _apply_indicator_signal_overrides(checks)
    return checks


def _build_indicator_rule_checks_by_timeframe(
    rules: list[dict[str, Any]],
    ohlc_by_timeframe: dict[str, tuple[list[float], ...]],
    *,
    default_timeframe: str,
    apply_overrides: bool = True,
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        tf = _rule_timeframe(rule, default_timeframe)
        ohlc = ohlc_by_timeframe.get(tf)
        if ohlc is None:
            check = {
                "buy_ok": False,
                "sell_ok": False,
                "buy_ignored": False,
                "sell_ignored": False,
                "value": "—",
                "detail": f"{tf} data unavailable",
            }
            params = rule.get("params") if isinstance(rule.get("params"), dict) else {}
            check["name"] = _rule_name(rule)
            check["_rule_kind"] = str(rule.get("kind") or "").strip().lower()
            check["_rule_params"] = params
            check["_rule_id"] = str(params.get("rule_id") or "").strip()
            check["_timeframe"] = tf
            checks.append(check)
            continue
        opens, highs, lows, closes = ohlc[:4]
        volumes = ohlc[4] if len(ohlc) >= 5 else None
        timestamps = ohlc[5] if len(ohlc) >= 6 else None
        if len(closes) < 2:
            continue
        item_checks = _build_indicator_rule_checks(
            [rule],
            closes,
            float(closes[-1]),
            opens=opens,
            highs=highs,
            lows=lows,
            volumes=volumes if isinstance(volumes, list) else None,
            timestamps=timestamps if isinstance(timestamps, list) else None,
            apply_overrides=False,
        )
        for item in item_checks:
            item["_timeframe"] = tf
            checks.append(item)
    if apply_overrides:
        _apply_indicator_signal_overrides(checks)
    return checks


def _indicator_override_meta_from_detail(detail: Any) -> Optional[dict[str, str]]:
    detail_txt = str(detail or "")
    if not detail_txt:
        return None
    match: Optional[re.Match[str]] = None
    for m in _INDICATOR_OVERRIDE_NOTE_RE.finditer(detail_txt):
        match = m
    if match is None:
        return None
    scope = str(match.group(2) or "both").strip().lower()
    if scope not in ("both", "buy", "sell"):
        scope = "both"
    forced_side = str(match.group(3) or "").strip().upper()
    if forced_side not in ("BUY", "SELL"):
        return None
    source_tag = str(match.group(1) or "RULE").strip().upper() or "RULE"
    source_name = str(match.group(4) or "").strip() or source_tag
    return {
        "source_tag": source_tag,
        "scope": scope,
        "forced_side": forced_side,
        "source_name": source_name,
    }


def _indicator_override_meta(item: dict[str, Any]) -> Optional[dict[str, str]]:
    if _is_ichimoku_rule_item(item):
        return None
    if isinstance(item.get("_ribbon_level_checks"), list) and not bool(item.get("_override_applied")):
        return None
    forced_side = str(item.get("_override_forced_side") or "").strip().upper()
    if forced_side in ("BUY", "SELL") and bool(item.get("_override_applied")):
        scope = str(item.get("_override_scope") or "both").strip().lower()
        if scope not in ("both", "buy", "sell"):
            scope = "both"
        source_tag = str(item.get("_override_source_tag") or "RULE").strip().upper() or "RULE"
        source_name = str(item.get("_override_source_name") or source_tag).strip() or source_tag
        return {
            "source_tag": source_tag,
            "scope": scope,
            "forced_side": forced_side,
            "source_name": source_name,
        }
    return _indicator_override_meta_from_detail(item.get("detail"))


def _indicator_rule_state_html(item: dict[str, Any]) -> str:
    override_meta = _indicator_override_meta(item)
    if override_meta is not None:
        forced_side = str(override_meta.get("forced_side") or "BUY").upper()
        source_name = str(override_meta.get("source_name") or override_meta.get("source_tag") or "RULE").strip()
        scope = str(override_meta.get("scope") or "both").strip().lower()
        css_cls = "indicator-buy" if forced_side == "BUY" else "indicator-sell"
        title_txt = f"Signal overridden to {forced_side} by {source_name} (scope: {scope})"
        return (
            f"<span class='small {css_cls}' title='{html.escape(title_txt)}'>"
            f" OVERRIDDEN-&gt;{html.escape(forced_side)}"
            "</span>"
        )

    buy_ignored = bool(item.get("buy_ignored"))
    sell_ignored = bool(item.get("sell_ignored"))
    if bool(item.get("block_ok")) and not bool(item.get("block_ignored")):
        return "<span class='small'> BLOCK/HOLD</span>"
    if buy_ignored and sell_ignored:
        return "<span class='small'> HOLD</span>"
    if bool(item.get("sell_ok")) and not sell_ignored:
        return "<span class='small indicator-sell'> SELL</span>"
    if bool(item.get("buy_ok")) and not buy_ignored:
        return "<span class='small indicator-buy'> BUY</span>"
    return "<span class='small'> HOLD</span>"


def _market_chart_svg(
    *,
    closes: list[float],
    opens: Optional[list[float]] = None,
    highs: Optional[list[float]] = None,
    lows: Optional[list[float]] = None,
    ma_lengths: list[int],
    ema_lengths: list[int],
    macd_configs: list[tuple[int, int, int]],
    bb_configs: list[tuple[int, float]],
    ttm_configs: list[tuple[int, float, int, float, int]],
    roc_lengths: list[int],
    sar_configs: list[tuple[float, float]],
    heikin_ashi_mode: bool,
    required_points: int,
    show_price: bool,
    show_rsi: bool,
    show_drsi: bool,
    d_ma_lengths: list[int],
    d_ema_lengths: list[int],
    ichimoku_configs: list[tuple[int, int, int, int]],
    css_class: str = "chart-spark markets-chart",
    display_width: int = 700,
    display_height: int = 330,
    show_price_markers: bool = False,
    volumes: Optional[list[float]] = None,
    timestamps: Optional[list[str]] = None,
    donchian_lookbacks: Optional[list[int]] = None,
    supertrend_configs: Optional[list[tuple[int, float]]] = None,
    pivot_enabled: bool = False,
    pivot_include_half_levels: bool = False,
    vwap_enabled: bool = False,
    rvol_lengths: Optional[list[int]] = None,
) -> str:
    if len(closes) < 2:
        return "<span class='small'>—</span>"

    # Keep enough candles for long MA/EMA overlays while still capping render cost.
    max_pts = max(180, int(required_points) + 80)
    max_pts = min(max_pts, 1200)
    data = closes[-max_pts:]
    vol_data = volumes[-max_pts:] if isinstance(volumes, list) else None
    ts_data = timestamps[-max_pts:] if isinstance(timestamps, list) else None
    if isinstance(opens, list) and isinstance(highs, list) and isinstance(lows, list):
        o_data = opens[-max_pts:]
        h_data = highs[-max_pts:]
        l_data = lows[-max_pts:]
        if len(o_data) == len(data) and len(h_data) == len(data) and len(l_data) == len(data):
            opens_syn, highs_syn, lows_syn, closes_syn = (
                [float(v) for v in o_data],
                [float(v) for v in h_data],
                [float(v) for v in l_data],
                [float(v) for v in data],
            )
        else:
            opens_syn, highs_syn, lows_syn, closes_syn = _market_synthetic_ohlc_from_closes(data)
    else:
        opens_syn, highs_syn, lows_syn, closes_syn = _market_synthetic_ohlc_from_closes(data)
    n = len(data)
    width = 520.0
    height = 260.0
    top_h = 172.0
    bot_h = 82.0
    axis_points = n

    def _path(values: list[Optional[float]], y_min: float, y_max: float, y_offset: float, h: float) -> str:
        if not values:
            return ""
        rng = max(y_max - y_min, 1e-9)
        d: list[str] = []
        for i, v in enumerate(values):
            if i >= axis_points:
                break
            if v is None:
                continue
            x = (i / max(1, axis_points - 1)) * width
            y = y_offset + h - ((float(v) - y_min) / rng) * h
            d.append(("M" if not d else "L") + f"{x:.2f},{y:.2f}")
        return " ".join(d)

    price_series: dict[str, list[Optional[float]]] = {}
    if show_price:
        price_series["price"] = [float(x) for x in data]
    for ln in sorted(set(int(x) for x in ma_lengths if int(x) >= 2)):
        price_series[f"ma{ln}"] = _market_ma_series(data, ln)
    for ln in sorted(set(int(x) for x in ema_lengths if int(x) >= 2)):
        price_series[f"ema{ln}"] = _market_ema_series(data, ln)
    ichimoku_cfgs: list[tuple[int, int, int, int]] = sorted(
        set(
            (
                max(1, int(cfg[0])),
                max(1, int(cfg[1])),
                max(2, int(cfg[2])),
                max(1, int(cfg[3])),
            )
            for cfg in ichimoku_configs
            if isinstance(cfg, (list, tuple)) and len(cfg) >= 4
        )
    )
    max_forward_points = max((int(cfg[3]) for cfg in ichimoku_cfgs), default=0)
    base_right_buffer = max(4, min(24, int(math.ceil(float(n) * 0.04)))) if bool(show_price) else 0
    axis_points = max(n, n + max(base_right_buffer, max_forward_points))
    ichimoku_series_map: dict[tuple[int, int, int, int], dict[str, list[Optional[float]]]] = {}
    for cfg in ichimoku_cfgs:
        tenkan_len, kijun_len, senkou_b_len, displacement = cfg
        ichimoku_series_map[cfg] = _market_ichimoku_series(
            data,
            tenkan_length=tenkan_len,
            kijun_length=kijun_len,
            senkou_b_length=senkou_b_len,
            displacement=displacement,
            forward_projected=True,
        )
    bb_cfgs: list[tuple[int, float]] = sorted(
        set(
            (
                max(2, int(cfg[0])),
                max(0.1, float(cfg[1])),
            )
            for cfg in bb_configs
            if isinstance(cfg, (list, tuple)) and len(cfg) >= 2
        )
    )
    bb_series_map: dict[tuple[int, float], dict[str, list[Optional[float]]]] = {}
    for cfg in bb_cfgs:
        bb_series_map[cfg] = _market_bollinger_series(data, length=cfg[0], std_mult=cfg[1])
    ttm_cfgs: list[tuple[int, float, int, float, int]] = sorted(
        set(
            (
                max(2, int(cfg[0])),
                max(0.1, float(cfg[1])),
                max(2, int(cfg[2])),
                max(0.1, float(cfg[3])),
                max(2, int(cfg[4])),
            )
            for cfg in ttm_configs
            if isinstance(cfg, (list, tuple)) and len(cfg) >= 5
        )
    )
    ttm_series_map: dict[tuple[int, float, int, float, int], dict[str, list[Optional[float]]]] = {}
    for cfg in ttm_cfgs:
        ttm_series_map[cfg] = _market_ttm_series(
            data,
            bb_length=cfg[0],
            bb_mult=cfg[1],
            kc_length=cfg[2],
            kc_mult=cfg[3],
            momentum_length=cfg[4],
        )
    roc_lens_raw: list[int] = []
    for raw_len in roc_lengths:
        try:
            ln = max(1, int(raw_len))
        except Exception:
            continue
        roc_lens_raw.append(ln)
    roc_lens: list[int] = sorted(set(roc_lens_raw))
    roc_series_map: dict[int, list[Optional[float]]] = {}
    for ln in roc_lens:
        roc_series_map[ln] = _market_roc_series(data, length=ln)
    sar_cfgs: list[tuple[float, float]] = sorted(
        set(
            (
                max(0.0001, float(cfg[0])),
                max(max(0.0001, float(cfg[0])), float(cfg[1])),
            )
            for cfg in sar_configs
            if isinstance(cfg, (list, tuple)) and len(cfg) >= 2
        )
    )
    sar_series_map: dict[tuple[float, float], list[Optional[float]]] = {}
    for cfg in sar_cfgs:
        sar_series_map[cfg] = _market_sar_series(
            data,
            highs=highs_syn,
            lows=lows_syn,
            step=cfg[0],
            max_step=cfg[1],
        )
    donchian_lens: list[int] = sorted(
        set(max(1, int(x)) for x in (donchian_lookbacks or []) if int(x) >= 1)
    )
    donchian_series_map: dict[int, tuple[list[Optional[float]], list[Optional[float]], list[Optional[float]]]] = {}
    for ln in donchian_lens:
        donchian_series_map[ln] = _market_donchian_channels(highs_syn, lows_syn, ln)
    supertrend_cfgs: list[tuple[int, float]] = sorted(
        set(
            (
                max(1, int(cfg[0])),
                max(0.1, float(cfg[1])),
            )
            for cfg in (supertrend_configs or [])
            if isinstance(cfg, (list, tuple)) and len(cfg) >= 2
        )
    )
    supertrend_series_map: dict[tuple[int, float], list[SupertrendPoint]] = {}
    for cfg in supertrend_cfgs:
        supertrend_series_map[cfg] = _market_supertrend_points(
            data,
            highs=highs_syn,
            lows=lows_syn,
            atr_length=cfg[0],
            multiplier=cfg[1],
        )
    pivot_levels = (
        calculate_pivot_points(highs_syn, lows_syn, closes_syn, source_index=-2)
        if bool(pivot_enabled) and calculate_pivot_points is not None and len(closes_syn) >= 2
        else None
    )
    pivot_level_pairs = (
        pivot_level_sequence(pivot_levels, include_half_levels=bool(pivot_include_half_levels))
        if pivot_levels is not None and pivot_level_sequence is not None
        else []
    )
    vwap_series = (
        _market_vwap_series(data, highs=highs_syn, lows=lows_syn, volumes=vol_data, timestamps=ts_data)
        if bool(vwap_enabled)
        else []
    )
    rvol_lens: list[int] = sorted(
        set(max(1, int(x)) for x in (rvol_lengths or []) if int(x) >= 1)
    )
    rvol_series_map: dict[int, list[Optional[float]]] = {}
    for ln in rvol_lens:
        rvol_series_map[ln] = _market_relative_volume_series(vol_data, length=ln)

    ha_open: list[float] = []
    ha_close: list[float] = []
    ha_high: list[float] = []
    ha_low: list[float] = []
    if bool(heikin_ashi_mode):
        ho, hh, hl, hc = _market_heikin_ashi_series(opens_syn, highs_syn, lows_syn, closes_syn)
        if len(ho) == n and len(hc) == n:
            ha_open = [float(v) for v in ho]
            ha_high = [float(v) for v in hh]
            ha_low = [float(v) for v in hl]
            ha_close = [float(v) for v in hc]

    price_vals: list[float] = []
    for vals in price_series.values():
        for v in vals:
            if isinstance(v, (int, float)):
                price_vals.append(float(v))
    for ichi_series in ichimoku_series_map.values():
        for key in ("tenkan", "kijun", "span_a", "span_b"):
            vals = ichi_series.get(key)
            if not isinstance(vals, list):
                continue
            for v in vals:
                if isinstance(v, (int, float)):
                    price_vals.append(float(v))
    for bb_series in bb_series_map.values():
        for key in ("upper", "middle", "lower"):
            vals = bb_series.get(key)
            if not isinstance(vals, list):
                continue
            for v in vals:
                if isinstance(v, (int, float)):
                    price_vals.append(float(v))
    for vals in sar_series_map.values():
        for v in vals:
            if isinstance(v, (int, float)):
                price_vals.append(float(v))
    for upper, lower, middle in donchian_series_map.values():
        for vals in (upper, lower, middle):
            for v in vals:
                if isinstance(v, (int, float)):
                    price_vals.append(float(v))
    for points in supertrend_series_map.values():
        for point in points:
            if point.trend is not None:
                price_vals.append(float(point.trend))
    for _name, value in pivot_level_pairs:
        if isinstance(value, (int, float)):
            price_vals.append(float(value))
    for v in vwap_series:
        if isinstance(v, (int, float)):
            price_vals.append(float(v))
    if not price_vals:
        return "<span class='small'>—</span>"
    pmin, pmax = min(price_vals), max(price_vals)
    if math.isclose(pmin, pmax):
        pad = max(0.01, abs(pmin) * 0.005)
        pmin -= pad
        pmax += pad
    prng = max(pmax - pmin, 1e-9)

    def _price_y(v: float) -> float:
        y = top_h - ((float(v) - pmin) / prng) * top_h
        return min(max(0.0, y), top_h)

    def _candle_paths() -> str:
        if n < 1 or not show_price:
            return ""
        candle_opens = ha_open if bool(heikin_ashi_mode) and len(ha_open) == n else opens_syn
        candle_highs = ha_high if bool(heikin_ashi_mode) and len(ha_high) == n else highs_syn
        candle_lows = ha_low if bool(heikin_ashi_mode) and len(ha_low) == n else lows_syn
        candle_closes = ha_close if bool(heikin_ashi_mode) and len(ha_close) == n else closes_syn
        if not (
            len(candle_opens) == n
            and len(candle_highs) == n
            and len(candle_lows) == n
            and len(candle_closes) == n
        ):
            return ""
        slot_w = width / max(1, axis_points - 1)
        body_w = max(1.1, min(5.5, slot_w * 0.58))
        wick_w = "0.75" if n > 90 else "0.95"
        parts: list[str] = ["<g class='chart-price-candles'>"]
        for i in range(n):
            try:
                o = float(candle_opens[i])
                h = float(candle_highs[i])
                l = float(candle_lows[i])
                c = float(candle_closes[i])
            except Exception:
                continue
            if not all(math.isfinite(v) and v > 0.0 for v in (o, h, l, c)):
                continue
            x = (i / max(1, axis_points - 1)) * width
            yo = _price_y(o)
            yh = _price_y(max(h, o, c))
            yl = _price_y(min(l, o, c))
            yc = _price_y(c)
            y_top = min(yo, yc)
            body_h = max(1.0, abs(yc - yo))
            color = "#22c55e" if c >= o else "#ef4444"
            opacity = "0.78" if n > 120 else "0.9"
            parts.append(
                f"<line x1='{x:.2f}' y1='{yh:.2f}' x2='{x:.2f}' y2='{yl:.2f}' "
                f"stroke='{color}' stroke-width='{wick_w}' opacity='{opacity}'/>"
            )
            parts.append(
                f"<rect x='{(x - (body_w / 2.0)):.2f}' y='{y_top:.2f}' width='{body_w:.2f}' height='{body_h:.2f}' "
                f"fill='{color}' stroke='{color}' stroke-width='0.55' opacity='{opacity}'/>"
            )
        parts.append("</g>")
        return "".join(parts)

    rsi_series: Optional[list[Optional[float]]] = None
    if show_rsi:
        rsi_full: list[Optional[float]] = []
        for i in range(len(data)):
            rsi_full.append(_market_rsi(data[: i + 1], 14))
        rsi_series = rsi_full

    drsi_series: Optional[list[Optional[float]]] = None
    if show_drsi:
        dvals: list[Optional[float]] = []
        for i in range(len(data)):
            dvals.append(_market_rsi_derivative(data[: i + 1]))
        drsi_series = dvals

    d_ma_series_map: dict[int, list[Optional[float]]] = {}
    for ln in sorted(set(int(x) for x in d_ma_lengths if int(x) >= 2)):
        dvals2: list[Optional[float]] = []
        for i in range(len(data)):
            dvals2.append(_market_ma_derivative(data[: i + 1], ln))
        d_ma_series_map[ln] = dvals2
    d_ema_series_map: dict[int, list[Optional[float]]] = {}
    for ln in sorted(set(int(x) for x in d_ema_lengths if int(x) >= 2)):
        dvals3: list[Optional[float]] = []
        for i in range(len(data)):
            dvals3.append(_market_ema_derivative(data[: i + 1], ln))
        d_ema_series_map[ln] = dvals3

    macd_map: dict[tuple[int, int, int], tuple[list[Optional[float]], list[Optional[float]], list[Optional[float]]]] = {}
    for cfg in sorted(set(macd_configs)):
        f, s, g = cfg
        macd_map[cfg] = _market_macd_series(data, fast_len=f, slow_len=s, signal_len=g)

    osc_vals: list[float] = []
    if rsi_series:
        for v in rsi_series:
            if isinstance(v, (int, float)):
                osc_vals.append((float(v) - 50.0) / 50.0)
    if drsi_series:
        for v in drsi_series:
            if isinstance(v, (int, float)):
                osc_vals.append(float(v))
    for dseries in d_ma_series_map.values():
        for v in dseries:
            if isinstance(v, (int, float)):
                osc_vals.append(float(v))
    for dseries in d_ema_series_map.values():
        for v in dseries:
            if isinstance(v, (int, float)):
                osc_vals.append(float(v))
    for m_s, s_s, h_s in macd_map.values():
        for vals in (m_s, s_s, h_s):
            for v in vals:
                if isinstance(v, (int, float)):
                    osc_vals.append(float(v))
    for vals in roc_series_map.values():
        for v in vals:
            if isinstance(v, (int, float)):
                osc_vals.append(float(v))
    for ttm_series in ttm_series_map.values():
        for v in ttm_series.get("momentum", []):
            if isinstance(v, (int, float)):
                osc_vals.append(float(v))
    for vals in rvol_series_map.values():
        for v in vals:
            if isinstance(v, (int, float)):
                osc_vals.append(float(v))
    omin, omax = (-1.0, 1.0)
    if osc_vals:
        oabs = max(abs(min(osc_vals)), abs(max(osc_vals)), 1e-6)
        omin, omax = (-oabs, oabs)

    paths: list[str] = []
    panel_bg_paths: list[str] = []
    cloud_paths: list[str] = []
    marker_bg: list[str] = []
    marker_fg: list[str] = []

    def _band_area_paths(
        upper: list[Optional[float]],
        lower: list[Optional[float]],
        *,
        fill: str,
    ) -> list[str]:
        out_paths: list[str] = []
        if len(upper) != len(lower):
            return out_paths
        limit = min(len(upper), axis_points)
        if limit < 2:
            return out_paths

        seg_start: Optional[int] = None
        for i in range(limit):
            uv = upper[i]
            lv = lower[i]
            valid = isinstance(uv, (int, float)) and isinstance(lv, (int, float))
            if valid:
                if seg_start is None:
                    seg_start = i
                continue
            if seg_start is None:
                continue
            seg_end = i - 1
            if seg_end - seg_start >= 1:
                upper_pts: list[tuple[float, float]] = []
                lower_pts: list[tuple[float, float]] = []
                for j in range(seg_start, seg_end + 1):
                    av = upper[j]
                    bv = lower[j]
                    if not isinstance(av, (int, float)) or not isinstance(bv, (int, float)):
                        continue
                    x = (j / max(1, axis_points - 1)) * width
                    upper_pts.append((x, _price_y(float(av))))
                    lower_pts.append((x, _price_y(float(bv))))
                if len(upper_pts) >= 2 and len(lower_pts) >= 2:
                    coords = upper_pts + list(reversed(lower_pts))
                    d = "M" + " L".join(f"{x:.2f},{y:.2f}" for x, y in coords) + " Z"
                    out_paths.append(f"<path d='{d}' style='fill:{fill};stroke:none;'/>")
            seg_start = None

        if seg_start is not None:
            seg_end = limit - 1
            if seg_end - seg_start >= 1:
                upper_pts = []
                lower_pts = []
                for j in range(seg_start, seg_end + 1):
                    av = upper[j]
                    bv = lower[j]
                    if not isinstance(av, (int, float)) or not isinstance(bv, (int, float)):
                        continue
                    x = (j / max(1, axis_points - 1)) * width
                    upper_pts.append((x, _price_y(float(av))))
                    lower_pts.append((x, _price_y(float(bv))))
                if len(upper_pts) >= 2 and len(lower_pts) >= 2:
                    coords = upper_pts + list(reversed(lower_pts))
                    d = "M" + " L".join(f"{x:.2f},{y:.2f}" for x, y in coords) + " Z"
                    out_paths.append(f"<path d='{d}' style='fill:{fill};stroke:none;'/>")
        return out_paths

    def _cloud_area_paths(
        span_a: list[Optional[float]],
        span_b: list[Optional[float]],
        *,
        bullish_fill: str,
        bearish_fill: str,
    ) -> list[str]:
        out_paths: list[str] = []
        if len(span_a) != len(span_b):
            return out_paths
        limit = min(len(span_a), axis_points)
        if limit < 2:
            return out_paths

        def _emit_segment(start_idx: int, end_idx: int, bullish: bool) -> None:
            if end_idx - start_idx < 1:
                return
            upper_pts: list[tuple[float, float]] = []
            lower_pts: list[tuple[float, float]] = []
            for j in range(start_idx, end_idx + 1):
                av = span_a[j]
                bv = span_b[j]
                if not isinstance(av, (int, float)) or not isinstance(bv, (int, float)):
                    continue
                x = (j / max(1, axis_points - 1)) * width
                upper_pts.append((x, _price_y(float(av))))
                lower_pts.append((x, _price_y(float(bv))))
            if len(upper_pts) < 2 or len(lower_pts) < 2:
                return
            coords = upper_pts + list(reversed(lower_pts))
            d = "M" + " L".join(f"{x:.2f},{y:.2f}" for x, y in coords) + " Z"
            fill = bullish_fill if bullish else bearish_fill
            out_paths.append(f"<path d='{d}' style='fill:{fill};stroke:none;'/>")

        seg_start: Optional[int] = None
        seg_bullish = False
        for i in range(limit):
            av = span_a[i]
            bv = span_b[i]
            valid = isinstance(av, (int, float)) and isinstance(bv, (int, float))
            if not valid:
                if seg_start is not None:
                    _emit_segment(seg_start, i - 1, seg_bullish)
                    seg_start = None
                continue
            bullish_now = float(av) >= float(bv)
            if seg_start is None:
                seg_start = i
                seg_bullish = bullish_now
            elif bullish_now != seg_bullish:
                _emit_segment(seg_start, i - 1, seg_bullish)
                seg_start = i
                seg_bullish = bullish_now
        if seg_start is not None:
            _emit_segment(seg_start, limit - 1, seg_bullish)
        return out_paths

    for idx, cfg in enumerate(bb_cfgs):
        bb_series = bb_series_map.get(cfg, {})
        upper = bb_series.get("upper", [None] * n)
        middle = bb_series.get("middle", [None] * n)
        lower = bb_series.get("lower", [None] * n)
        panel_bg_paths.extend(
            _band_area_paths(
                upper,
                lower,
                fill="rgba(56,189,248,0.11)" if idx == 0 else "rgba(56,189,248,0.06)",
            )
        )
        extra_dash = "" if idx == 0 else " stroke-dasharray='3 2'"
        extra_opacity = "0.96" if idx == 0 else "0.72"
        upper_path = _path(upper, pmin, pmax, 0.0, top_h)
        if upper_path:
            paths.append(
                f"<path d='{upper_path}' stroke='#38bdf8' stroke-width='1.05' fill='none' opacity='{extra_opacity}'{extra_dash}/>"
            )
        middle_path = _path(middle, pmin, pmax, 0.0, top_h)
        if middle_path:
            paths.append(
                f"<path d='{middle_path}' stroke='#94a3b8' stroke-width='0.95' fill='none' opacity='{extra_opacity}'{extra_dash}/>"
            )
        lower_path = _path(lower, pmin, pmax, 0.0, top_h)
        if lower_path:
            paths.append(
                f"<path d='{lower_path}' stroke='#38bdf8' stroke-width='1.05' fill='none' opacity='{extra_opacity}'{extra_dash}/>"
            )

    for idx, ln in enumerate(donchian_lens):
        upper, lower, middle = donchian_series_map.get(ln, ([None] * n, [None] * n, [None] * n))
        panel_bg_paths.extend(
            _band_area_paths(
                upper,
                lower,
                fill="rgba(20,184,166,0.10)" if idx == 0 else "rgba(20,184,166,0.05)",
            )
        )
        dash = "" if idx == 0 else " stroke-dasharray='4 3'"
        opacity = "0.92" if idx == 0 else "0.70"
        upper_path = _path(upper, pmin, pmax, 0.0, top_h)
        middle_path = _path(middle, pmin, pmax, 0.0, top_h)
        lower_path = _path(lower, pmin, pmax, 0.0, top_h)
        if upper_path:
            paths.append(
                f"<path d='{upper_path}' stroke='#14b8a6' stroke-width='1.05' fill='none' opacity='{opacity}'{dash}/>"
            )
        if middle_path:
            paths.append(
                f"<path d='{middle_path}' stroke='#facc15' stroke-width='0.95' fill='none' opacity='{opacity}' stroke-dasharray='5 3'/>"
            )
        if lower_path:
            paths.append(
                f"<path d='{lower_path}' stroke='#14b8a6' stroke-width='1.05' fill='none' opacity='{opacity}'{dash}/>"
            )

    for idx, cfg in enumerate(ichimoku_cfgs):
        series = ichimoku_series_map.get(cfg, {})
        span_a = series.get("span_a", [None] * n)
        span_b = series.get("span_b", [None] * n)
        cloud_paths.extend(
            _cloud_area_paths(
                span_a,
                span_b,
                bullish_fill="rgba(34,197,94,0.16)" if idx == 0 else "rgba(34,197,94,0.08)",
                bearish_fill="rgba(244,63,94,0.14)" if idx == 0 else "rgba(244,63,94,0.07)",
            )
        )
        extra_dash = "" if idx == 0 else " stroke-dasharray='3 2'"
        extra_opacity = "1.0" if idx == 0 else "0.78"
        tenkan_path = _path(series.get("tenkan", [None] * n), pmin, pmax, 0.0, top_h)
        if tenkan_path:
            paths.append(
                f"<path d='{tenkan_path}' stroke='#fb7185' stroke-width='1.1' fill='none' opacity='{extra_opacity}'{extra_dash}/>"
            )
        kijun_path = _path(series.get("kijun", [None] * n), pmin, pmax, 0.0, top_h)
        if kijun_path:
            paths.append(
                f"<path d='{kijun_path}' stroke='#60a5fa' stroke-width='1.1' fill='none' opacity='{extra_opacity}'{extra_dash}/>"
            )
        span_a_path = _path(span_a, pmin, pmax, 0.0, top_h)
        if span_a_path:
            paths.append(
                f"<path d='{span_a_path}' stroke='#22c55e' stroke-width='1.0' fill='none' opacity='{extra_opacity}'{extra_dash}/>"
            )
        span_b_path = _path(span_b, pmin, pmax, 0.0, top_h)
        if span_b_path:
            paths.append(
                f"<path d='{span_b_path}' stroke='#f59e0b' stroke-width='1.0' fill='none' opacity='{extra_opacity}'{extra_dash}/>"
            )

    if show_price_markers:
        marker_vals = [pmax, (pmax + pmin) / 2.0, pmin]
        marker_dash = ["", "2 2", ""]
        for i, mv in enumerate(marker_vals):
            yv = _price_y(float(mv))
            dash_attr = f" stroke-dasharray='{marker_dash[i]}'" if marker_dash[i] else ""
            marker_bg.append(
                f"<line x1='0' y1='{yv:.2f}' x2='{width:.1f}' y2='{yv:.2f}' "
                f"stroke='rgba(255,255,255,0.16)' stroke-width='0.8'{dash_attr}/>"
            )
            marker_fg.append(
                f"<text x='{(width - 3.0):.2f}' y='{(yv - 2.0):.2f}' text-anchor='end' "
                "font-size='10' fill='rgba(255,255,255,0.72)' font-family='ui-monospace, SFMono-Regular, Menlo, Consolas, monospace'>"
                f"{html.escape(_fmt_market_num(mv, 2))}"
                "</text>"
            )
        last_price: Optional[float] = None
        price_line = price_series.get("price", [])
        for v in reversed(price_line):
            if isinstance(v, (int, float)):
                last_price = float(v)
                break
        if last_price is not None:
            y_last = _price_y(last_price)
            marker_bg.append(
                f"<line x1='0' y1='{y_last:.2f}' x2='{width:.1f}' y2='{y_last:.2f}' "
                "stroke='rgba(96,165,250,0.65)' stroke-width='0.95' stroke-dasharray='4 2'/>"
            )
            marker_fg.append(
                f"<text x='3' y='{(y_last - 2.0):.2f}' text-anchor='start' "
                "font-size='10' fill='rgba(96,165,250,0.95)' font-family='ui-monospace, SFMono-Regular, Menlo, Consolas, monospace'>"
                f"LAST {html.escape(_fmt_market_num(last_price, 2))}"
                "</text>"
            )

    price_vals_for_path = price_series.get("price", [None] * n)
    candle_price_paths = _candle_paths()
    if candle_price_paths:
        paths.append(candle_price_paths)
    if bool(heikin_ashi_mode) and len(ha_open) == n and len(ha_close) == n:
        seg_path: list[str] = []
        seg_color: Optional[str] = None

        def _ha_seg_color(idx: int) -> str:
            if idx < 0 or idx >= n:
                return "#f8fafc"
            if float(ha_close[idx]) > float(ha_open[idx]):
                return "#22c55e"
            if float(ha_close[idx]) < float(ha_open[idx]):
                return "#ef4444"
            return "#f8fafc"

        def _flush_seg() -> None:
            nonlocal seg_path, seg_color
            if seg_path and seg_color:
                paths.append(
                    f"<path d='{' '.join(seg_path)}' stroke='{seg_color}' stroke-width='1.9' fill='none' opacity='0.96' class='chart-price-line'/>"
                )
            seg_path = []
            seg_color = None

        for i in range(1, n):
            prev_v = price_vals_for_path[i - 1]
            curr_v = price_vals_for_path[i]
            if not isinstance(prev_v, (int, float)) or not isinstance(curr_v, (int, float)):
                _flush_seg()
                continue
            x0 = ((i - 1) / max(1, axis_points - 1)) * width
            y0 = _price_y(float(prev_v))
            x1 = (i / max(1, axis_points - 1)) * width
            y1 = _price_y(float(curr_v))
            c = _ha_seg_color(i)
            if seg_color != c:
                _flush_seg()
                seg_color = c
                seg_path.append(f"M{x0:.2f},{y0:.2f}")
            seg_path.append(f"L{x1:.2f},{y1:.2f}")
        _flush_seg()
    else:
        price_path = _path(price_vals_for_path, pmin, pmax, 0.0, top_h)
        if price_path:
            paths.append(f"<path d='{price_path}' stroke='#f8fafc' stroke-width='1.8' fill='none' class='chart-price-line'/>")
    for idx, cfg in enumerate(supertrend_cfgs):
        points = supertrend_series_map.get(cfg, [])
        opacity = "0.96" if idx == 0 else "0.70"
        dash = "" if idx == 0 else " stroke-dasharray='3 2'"
        segments = segment_supertrend_runs(points) if segment_supertrend_runs is not None else []
        for direction, start, end in segments:
            seg: list[str] = []
            for i in range(start, end + 1):
                if i >= len(points):
                    continue
                raw_v = points[i].trend
                if raw_v is None:
                    continue
                x = (i / max(1, axis_points - 1)) * width
                y = _price_y(float(raw_v))
                seg.append(("M" if not seg else "L") + f"{x:.2f},{y:.2f}")
            if not seg:
                continue
            color = "#22c55e" if direction >= 0.0 else "#ef4444"
            paths.append(
                f"<path d='{' '.join(seg)}' stroke='{color}' stroke-width='1.35' fill='none' opacity='{opacity}'{dash}/>"
            )
        for i, point in enumerate(points):
            if point.trend is None or (not point.flip_up and not point.flip_down):
                continue
            x = (i / max(1, axis_points - 1)) * width
            stem = 30.0 if display_height <= 200 else 24.0
            head = 10.0 if display_height <= 200 else 8.0
            half = 8.0 if display_height <= 200 else 6.6
            stroke_w = 3.0 if display_height <= 200 else 2.4
            outline_w = stroke_w + 2.4
            trend_y = _price_y(float(point.trend))
            if point.flip_up:
                y_tip = max(3.0, min(top_h - 3.0, trend_y))
                y_tail = min(top_h - 3.0, y_tip + stem)
                y_head_base = min(top_h - 3.0, y_tip + head)
                marker = (
                    f"<g opacity='{opacity}'>"
                    f"<line x1='{x:.2f}' y1='{y_tail:.2f}' x2='{x:.2f}' y2='{y_tip:.2f}' "
                    f"stroke='#0f172a' stroke-width='{outline_w:.2f}' stroke-linecap='round'/>"
                    f"<line x1='{x:.2f}' y1='{y_tail:.2f}' x2='{x:.2f}' y2='{y_tip:.2f}' "
                    f"stroke='#22c55e' stroke-width='{stroke_w:.2f}' stroke-linecap='round'/>"
                    f"<path d='M{x:.2f},{y_tip:.2f} L{x - half:.2f},{y_head_base:.2f} "
                    f"L{x + half:.2f},{y_head_base:.2f} Z' fill='#22c55e' "
                    f"stroke='#0f172a' stroke-width='1.2'/>"
                    f"</g>"
                )
            else:
                y_tip = max(3.0, min(top_h - 3.0, trend_y))
                y_tail = max(3.0, y_tip - stem)
                y_head_base = max(3.0, y_tip - head)
                marker = (
                    f"<g opacity='{opacity}'>"
                    f"<line x1='{x:.2f}' y1='{y_tail:.2f}' x2='{x:.2f}' y2='{y_tip:.2f}' "
                    f"stroke='#0f172a' stroke-width='{outline_w:.2f}' stroke-linecap='round'/>"
                    f"<line x1='{x:.2f}' y1='{y_tail:.2f}' x2='{x:.2f}' y2='{y_tip:.2f}' "
                    f"stroke='#ef4444' stroke-width='{stroke_w:.2f}' stroke-linecap='round'/>"
                    f"<path d='M{x:.2f},{y_tip:.2f} L{x - half:.2f},{y_head_base:.2f} "
                    f"L{x + half:.2f},{y_head_base:.2f} Z' fill='#ef4444' "
                    f"stroke='#0f172a' stroke-width='1.2'/>"
                    f"</g>"
                )
            marker_fg.append(marker)
    if pivot_level_pairs:
        for name, value in pivot_level_pairs:
            if not isinstance(value, (int, float)):
                continue
            y = _price_y(float(value))
            major = "/" not in str(name)
            color = "#f59e0b" if str(name).startswith("R") else ("#22c55e" if str(name).startswith("S") else "#facc15")
            opacity = "0.72" if major else "0.42"
            dash = "6 4" if major else "2 5"
            sw = "0.95" if major else "0.7"
            paths.append(
                f"<line x1='0' y1='{y:.2f}' x2='{width:.1f}' y2='{y:.2f}' "
                f"stroke='{color}' stroke-width='{sw}' stroke-dasharray='{dash}' opacity='{opacity}'/>"
            )
            if major:
                paths.append(
                    f"<text x='4' y='{max(10.0, y - 3.0):.2f}' fill='{color}' font-size='9' "
                    f"font-family='system-ui, sans-serif' opacity='0.9'>{html.escape(str(name))}</text>"
                )
    if vwap_series:
        vwap_path = _path(vwap_series, pmin, pmax, 0.0, top_h)
        if vwap_path:
            paths.append(
                f"<path d='{vwap_path}' stroke='#facc15' stroke-width='1.25' fill='none' stroke-dasharray='6 2'/>"
            )
    for ln in sorted(set(int(x) for x in ma_lengths if int(x) >= 2)):
        ma_path = _path(price_series.get(f"ma{ln}", [None] * n), pmin, pmax, 0.0, top_h)
        if ma_path:
            c = _markets_line_color(f"ma:{ln}")
            paths.append(f"<path d='{ma_path}' stroke='{c}' stroke-width='1.25' fill='none'/>")
    for ln in sorted(set(int(x) for x in ema_lengths if int(x) >= 2)):
        ema_path = _path(price_series.get(f"ema{ln}", [None] * n), pmin, pmax, 0.0, top_h)
        if ema_path:
            c = _markets_line_color(f"ema:{ln}")
            paths.append(f"<path d='{ema_path}' stroke='{c}' stroke-width='1.25' fill='none' stroke-dasharray='4 2'/>")
    for idx, cfg in enumerate(sar_cfgs):
        sar_vals = sar_series_map.get(cfg, [None] * n)
        dot_parts: list[str] = []
        for i, raw_v in enumerate(sar_vals):
            if not isinstance(raw_v, (int, float)):
                continue
            x = (i / max(1, axis_points - 1)) * width
            y = _price_y(float(raw_v))
            dot_parts.append(f"M{x:.2f},{y:.2f} l0,0")
        if dot_parts:
            opacity = "0.98" if idx == 0 else "0.72"
            sw = "2.1" if idx == 0 else "1.7"
            paths.append(
                f"<path d='{' '.join(dot_parts)}' stroke='#f43f5e' stroke-width='{sw}' fill='none' "
                f"stroke-linecap='round' opacity='{opacity}'/>"
            )

    if show_rsi and rsi_series:
        rsi_norm = [((float(v) - 50.0) / 50.0) if isinstance(v, (int, float)) else None for v in rsi_series]
        rsi_path = _path(rsi_norm, omin, omax, top_h + 2.0, bot_h)
        if rsi_path:
            paths.append(f"<path d='{rsi_path}' stroke='#a78bfa' stroke-width='1.1' fill='none'/>")
    if show_drsi and drsi_series:
        drsi_path = _path(drsi_series, omin, omax, top_h + 2.0, bot_h)
        if drsi_path:
            paths.append(f"<path d='{drsi_path}' stroke='#22d3ee' stroke-width='1.1' fill='none' stroke-dasharray='2 2'/>")
    for idx, ln in enumerate(roc_lens):
        roc_path = _path(roc_series_map.get(ln, [None] * n), omin, omax, top_h + 2.0, bot_h)
        if roc_path:
            dash = "" if idx == 0 else " stroke-dasharray='3 2'"
            opacity = "0.95" if idx == 0 else "0.76"
            paths.append(
                f"<path d='{roc_path}' stroke='#f59e0b' stroke-width='1.15' fill='none' opacity='{opacity}'{dash}/>"
            )
    for idx, cfg in enumerate(ttm_cfgs):
        ttm_series = ttm_series_map.get(cfg, {})
        mom_vals = ttm_series.get("momentum", [None] * n)
        mom_path = _path(mom_vals, omin, omax, top_h + 2.0, bot_h)
        if mom_path:
            dash = "" if idx == 0 else " stroke-dasharray='2 2'"
            opacity = "0.9" if idx == 0 else "0.68"
            paths.append(
                f"<path d='{mom_path}' stroke='#06b6d4' stroke-width='1.0' fill='none' opacity='{opacity}'{dash}/>"
            )
    for idx, ln in enumerate(rvol_lens):
        rvol_path = _path(rvol_series_map.get(ln, [None] * n), omin, omax, top_h + 2.0, bot_h)
        if rvol_path:
            dash = "" if idx == 0 else " stroke-dasharray='3 2'"
            opacity = "0.95" if idx == 0 else "0.72"
            paths.append(
                f"<path d='{rvol_path}' stroke='#fb7185' stroke-width='1.05' fill='none' opacity='{opacity}'{dash}/>"
            )
    for ln in sorted(set(int(x) for x in d_ma_lengths if int(x) >= 2)):
        d_path = _path(d_ma_series_map.get(ln, [None] * n), omin, omax, top_h + 2.0, bot_h)
        if d_path:
            c = _markets_line_color(f"ma:{ln}")
            paths.append(f"<path d='{d_path}' stroke='{c}' stroke-width='1.05' fill='none' stroke-dasharray='2 2'/>")
    for ln in sorted(set(int(x) for x in d_ema_lengths if int(x) >= 2)):
        d_path = _path(d_ema_series_map.get(ln, [None] * n), omin, omax, top_h + 2.0, bot_h)
        if d_path:
            c = _markets_line_color(f"ema:{ln}")
            paths.append(f"<path d='{d_path}' stroke='{c}' stroke-width='1.05' fill='none' stroke-dasharray='4 3'/>")
    for cfg in sorted(set(macd_configs)):
        m_s, s_s, h_s = macd_map.get(cfg, ([None] * n, [None] * n, [None] * n))
        c = _markets_line_color(f"macd:{cfg[0]}:{cfg[1]}:{cfg[2]}")
        m_path = _path(m_s, omin, omax, top_h + 2.0, bot_h)
        s_path = _path(s_s, omin, omax, top_h + 2.0, bot_h)
        h_path = _path(h_s, omin, omax, top_h + 2.0, bot_h)
        if h_path:
            paths.append(f"<path d='{h_path}' stroke='{c}' stroke-width='0.95' fill='none' stroke-dasharray='1 2' opacity='0.75'/>")
        if m_path:
            paths.append(f"<path d='{m_path}' stroke='{c}' stroke-width='1.25' fill='none'/>")
        if s_path:
            paths.append(f"<path d='{s_path}' stroke='{c}' stroke-width='1.1' fill='none' stroke-dasharray='5 2'/>")

    osc_rng = max(omax - omin, 1e-9)

    def _osc_y(v: float) -> float:
        return top_h + 2.0 + bot_h - ((float(v) - omin) / osc_rng) * bot_h

    zero_y = _osc_y(0.0)
    squeeze_marker_paths: list[str] = []
    for idx, cfg in enumerate(ttm_cfgs):
        ttm_series = ttm_series_map.get(cfg, {})
        on_vals = ttm_series.get("squeeze_on", [None] * n)
        off_vals = ttm_series.get("squeeze_off", [None] * n)
        fired_vals = ttm_series.get("squeeze_fired", [None] * n)
        y_dot = _osc_y(0.0) + (idx * 2.5)
        d_on: list[str] = []
        d_off: list[str] = []
        d_fired: list[str] = []
        for i in range(n):
            x = (i / max(1, axis_points - 1)) * width
            if isinstance(off_vals[i], (int, float)) and float(off_vals[i]) >= 0.5:
                d_off.append(f"M{x:.2f},{y_dot:.2f} l0,0")
            if isinstance(on_vals[i], (int, float)) and float(on_vals[i]) >= 0.5:
                d_on.append(f"M{x:.2f},{y_dot:.2f} l0,0")
            if isinstance(fired_vals[i], (int, float)) and float(fired_vals[i]) >= 0.5:
                d_fired.append(f"M{x:.2f},{y_dot:.2f} l0,0")
        if d_off:
            squeeze_marker_paths.append(
                f"<path d='{' '.join(d_off)}' stroke='rgba(148,163,184,0.62)' stroke-width='2.4' fill='none' stroke-linecap='round'/>"
            )
        if d_on:
            squeeze_marker_paths.append(
                f"<path d='{' '.join(d_on)}' stroke='rgba(244,63,94,0.88)' stroke-width='2.6' fill='none' stroke-linecap='round'/>"
            )
        if d_fired:
            squeeze_marker_paths.append(
                f"<path d='{' '.join(d_fired)}' stroke='rgba(245,158,11,0.96)' stroke-width='3.0' fill='none' stroke-linecap='round'/>"
            )
    bottom_y = top_h + 2.0
    svg_class = html.escape(str(css_class or "chart-spark markets-chart"))
    render_w = max(1, int(display_width))
    render_h = max(1, int(display_height))
    return (
        f"<svg class='{svg_class}' viewBox='0 0 {int(width)} {int(height)}' "
        f"preserveAspectRatio='none' width='{render_w}' height='{render_h}' "
        f"style='width:{render_w}px;height:{render_h}px;display:block;max-width:none;'>"
        f"<rect x='0' y='0' width='{width:.1f}' height='{top_h:.1f}' fill='rgba(255,255,255,0.02)'/>"
        f"<rect x='0' y='{bottom_y:.1f}' width='{width:.1f}' height='{bot_h:.1f}' fill='rgba(255,255,255,0.02)'/>"
        + "".join(panel_bg_paths)
        + "".join(cloud_paths)
        + f"<line x1='0' y1='{zero_y:.2f}' x2='{width:.1f}' y2='{zero_y:.2f}' stroke='rgba(255,255,255,0.16)' stroke-width='0.8'/>"
        + "".join(marker_bg)
        + "".join(squeeze_marker_paths)
        + "".join(paths)
        + "".join(marker_fg)
        + "</svg>"
    )


def _render_markets_indicators_html(
    *,
    timeframe: str,
    symbol: str = "",
) -> str:
    wl = _list_markets_watchlist()
    ad_hoc = _clean_symbol(symbol)
    symbols: list[str] = list(wl)
    if ad_hoc and ad_hoc not in symbols:
        symbols.append(ad_hoc)
    if not symbols:
        return "<div class='small'>Watchlist is empty. Add symbols to start scanning.</div>"

    ok, msg = _ensure_robinhood_markets_session()
    if not ok:
        return f"<div class='small'>Indicator scan unavailable: {html.escape(msg)}</div>"

    rules = _list_indicator_rules(enabled_only=True)
    if not rules:
        return "<div class='small'>No enabled indicator rules. Add rules in the Indicator Rules panel.</div>"

    chart_cfg = _indicator_rules_chart_config(rules)
    ma_lengths = chart_cfg["ma_lengths"]
    ema_lengths = chart_cfg["ema_lengths"]
    macd_configs = chart_cfg["macd_configs"]
    d_ma_lengths = chart_cfg["d_ma_lengths"]
    d_ema_lengths = chart_cfg["d_ema_lengths"]
    bb_configs = chart_cfg["bb_configs"]
    ttm_configs = chart_cfg["ttm_configs"]
    roc_lengths = chart_cfg["roc_lengths"]
    sar_configs = chart_cfg["sar_configs"]
    donchian_lookbacks = chart_cfg.get("donchian_lookbacks") or []
    supertrend_configs = chart_cfg.get("supertrend_configs") or []
    rvol_lengths = chart_cfg.get("rvol_lengths") or []
    ichimoku_configs = chart_cfg["ichimoku_configs"]
    has_rsi = bool(chart_cfg["has_rsi"])
    has_drsi = bool(chart_cfg["has_drsi"])
    has_heikin_ashi = bool(chart_cfg["has_heikin_ashi"])
    has_vwap = bool(chart_cfg.get("has_vwap"))
    has_pivot = bool(chart_cfg.get("has_pivot"))
    pivot_include_half_levels = bool(chart_cfg.get("pivot_include_half_levels"))
    min_required = int(chart_cfg["min_required"])
    rule_columns = _indicator_runtime_rule_entries(rules)

    headers = ["<th>Symbol</th>", "<th>Signal</th>", "<th>Price</th>"]
    for entry in rule_columns:
        headers.append(f"<th>{html.escape(str(entry.get('name') or 'RULE'))}</th>")
    headers.append("<th>Chart</th>")

    rows: list[str] = [
        "<div class='status-table-wrap'><table>",
        "<thead><tr>" + "".join(headers) + "</tr></thead><tbody>",
    ]
    for sym in symbols:
        opens, highs, lows, closes, volumes, timestamps, _raw_rows, _requested_bounds = _market_fetch_ohlcv(
            sym,
            timeframe,
            min_candles=min_required,
        )
        if len(closes) < min_required:
            rows.append(
                "<tr>"
                f"<td><b>{html.escape(sym)}</b></td>"
                "<td class='signal-hold'>NO_DATA</td>"
                f"<td colspan='{max(1, len(headers)-2)}' class='small'>insufficient candles "
                f"(need {min_required}, got {len(closes)})</td>"
                "</tr>"
            )
            continue
        price = float(closes[-1])
        checks = _build_indicator_rule_checks(
            rules,
            closes,
            price,
            opens=opens,
            highs=highs,
            lows=lows,
            volumes=volumes,
            timestamps=timestamps,
        )
        buy_pass = sum(1 for c in checks if (not bool(c.get("buy_ignored"))) and bool(c.get("buy_ok")))
        sell_pass = sum(1 for c in checks if (not bool(c.get("sell_ignored"))) and bool(c.get("sell_ok")))
        buy_total = sum(1 for c in checks if not bool(c.get("buy_ignored")))
        sell_total = sum(1 for c in checks if not bool(c.get("sell_ignored")))
        buy_all = buy_total > 0 and buy_pass == buy_total
        sell_all = sell_total > 0 and sell_pass == sell_total
        signal = "SELL" if sell_all else ("BUY" if buy_all else "HOLD")
        cls = "signal-hold"
        if signal == "BUY":
            cls = "signal-buy"
        elif signal == "SELL":
            cls = "signal-sell"
        chart = _market_chart_svg(
            closes=closes,
            opens=opens,
            highs=highs,
            lows=lows,
            ma_lengths=ma_lengths,
            ema_lengths=ema_lengths,
            macd_configs=macd_configs,
            bb_configs=bb_configs,
            ttm_configs=ttm_configs,
            roc_lengths=roc_lengths,
            sar_configs=sar_configs,
            heikin_ashi_mode=has_heikin_ashi,
            required_points=min_required,
            show_price=True,
            show_rsi=has_rsi,
            show_drsi=has_drsi,
            d_ma_lengths=d_ma_lengths,
            d_ema_lengths=d_ema_lengths,
            ichimoku_configs=ichimoku_configs,
            volumes=volumes,
            timestamps=timestamps,
            donchian_lookbacks=donchian_lookbacks,
            supertrend_configs=supertrend_configs,
            pivot_enabled=has_pivot,
            pivot_include_half_levels=pivot_include_half_levels,
            vwap_enabled=has_vwap,
            rvol_lengths=rvol_lengths,
        )
        cells = [
            f"<td><b>{html.escape(sym)}</b></td>",
            f"<td class='{cls}'>{signal}</td>",
            f"<td>{_fmt_market_num(price, 2)}</td>",
        ]
        for i, c in enumerate(checks):
            v_txt = html.escape(str(c.get("value") or "—"))
            d_txt = html.escape(str(c.get("detail") or ""))
            rule_color = str(rule_columns[i].get("color") or "#e8ecff") if i < len(rule_columns) else "#e8ecff"
            buy_ignored = bool(c.get("buy_ignored"))
            sell_ignored = bool(c.get("sell_ignored"))
            if bool(c.get("sell_ok")) and not sell_ignored:
                state_txt = "<span class='small indicator-sell'> SELL</span>"
            elif bool(c.get("buy_ok")) and not buy_ignored:
                state_txt = "<span class='small indicator-buy'> BUY</span>"
            elif buy_ignored and sell_ignored:
                state_txt = "<span class='small'> HOLD</span>"
            else:
                state_txt = ""
            cell_inner = (
                f"<div style='color:{html.escape(rule_color)}; font-weight:650'>{v_txt}</div>"
                f"{state_txt}"
            )
            if d_txt:
                cell_inner += f"<div class='small'>{d_txt}</div>"
            cells.append(f"<td>{cell_inner}</td>")
        cells.append(f"<td>{chart}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")
    rows.append("</tbody></table></div>")
    rows.append(
        f"<div class='small' style='margin-top:6px;'>Source: Robinhood market data · timeframe {html.escape(str(timeframe))}</div>"
    )
    return "".join(rows)


def _normalize_indicator_rules_payload(raw: Any, *, default_timeframe: str = "") -> list[dict[str, Any]]:
    obj: Any = raw
    if isinstance(obj, str):
        try:
            obj = json.loads(obj)
        except Exception:
            return []
    if not isinstance(obj, list):
        return []
    out: list[dict[str, Any]] = []
    for item in obj:
        if not isinstance(item, dict):
            continue
        kind_raw = str(item.get("kind") or "").strip().lower()
        if kind_raw == "ha":
            kind = "heikin_ashi"
        elif kind_raw in ("bollinger", "bollinger_bands"):
            kind = "bb"
        elif kind_raw in ("ichimoku", "ichimoku_cloud", "ichi"):
            kind = "ichimoku"
        elif kind_raw in ("ttm", "ttm_squeeze", "squeeze_momentum"):
            kind = "ttm"
        elif kind_raw in ("roc", "rate_of_change"):
            kind = "roc"
        elif kind_raw in ("sar", "psar", "parabolic_sar", "parabolic"):
            kind = "sar"
        elif kind_raw in ("donchian", "donchian_breakout", "donchian_channel", "donchian_channels"):
            kind = "donchian"
        elif kind_raw in ("pivot", "pivot_points", "pivots"):
            kind = "pivot"
        elif kind_raw in ("supertrend", "supertrend_trend"):
            kind = "supertrend"
        elif kind_raw in ("vwap", "vwap_filter"):
            kind = "vwap"
        elif kind_raw in ("relative_volume", "rvol", "rel_volume"):
            kind = "relative_volume"
        else:
            kind = kind_raw
        if kind not in (
            "ma",
            "ema",
            "rsi",
            "rsi_d",
            "macd",
            "heikin_ashi",
            "bb",
            "ichimoku",
            "ttm",
            "roc",
            "sar",
            "donchian",
            "pivot",
            "supertrend",
            "vwap",
            "relative_volume",
        ):
            continue
        params = item.get("params") if isinstance(item.get("params"), dict) else {}
        rule: dict[str, Any] = {
            "name": str(item.get("name") or "").strip() or kind.upper(),
            "kind": kind,
            "params": params,
        }
        tf_raw = item.get("timeframe")
        if tf_raw in (None, "", "None"):
            tf_raw = params.get("timeframe") if isinstance(params, dict) else None
        if tf_raw not in (None, "", "None") or default_timeframe:
            rule["timeframe"] = _normalize_indicator_rule_timeframe(tf_raw, default=default_timeframe or "1h")
        out.append(rule)
    return out


def _indicator_rules_chart_config(rules: list[dict[str, Any]]) -> dict[str, Any]:
    ma_lengths: list[int] = []
    ema_lengths: list[int] = []
    macd_configs: list[tuple[int, int, int]] = []
    d_ma_lengths: list[int] = []
    d_ema_lengths: list[int] = []
    ichimoku_configs: list[tuple[int, int, int, int]] = []
    bb_configs: list[tuple[int, float]] = []
    ttm_configs: list[tuple[int, float, int, float, int]] = []
    roc_lengths: list[int] = []
    sar_configs: list[tuple[float, float]] = []
    donchian_lookbacks: list[int] = []
    supertrend_configs: list[tuple[int, float]] = []
    rvol_lengths: list[int] = []
    has_rsi = False
    has_drsi = False
    has_heikin_ashi = False
    has_vwap = False
    has_pivot = False
    pivot_include_half_levels = False
    longest_bb = 0
    longest_ichimoku = 0
    longest_ttm = 0
    longest_roc = 0
    longest_sar = 0
    longest_donchian = 0
    longest_supertrend = 0
    longest_rvol = 0

    def _flag(v: Any) -> bool:
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return float(v) != 0.0
        txt = str(v or "").strip().lower()
        return txt in ("1", "true", "yes", "on", "y")

    for rule in rules:
        kind_raw = str(rule.get("kind") or "").strip().lower()
        if kind_raw == "ha":
            kind = "heikin_ashi"
        elif kind_raw in ("bollinger", "bollinger_bands"):
            kind = "bb"
        elif kind_raw in ("ichimoku", "ichimoku_cloud", "ichi"):
            kind = "ichimoku"
        elif kind_raw in ("ttm", "ttm_squeeze", "squeeze_momentum"):
            kind = "ttm"
        elif kind_raw in ("roc", "rate_of_change"):
            kind = "roc"
        elif kind_raw in ("sar", "psar", "parabolic_sar", "parabolic"):
            kind = "sar"
        elif kind_raw in ("donchian", "donchian_breakout", "donchian_channel", "donchian_channels"):
            kind = "donchian"
        elif kind_raw in ("pivot", "pivot_points", "pivots"):
            kind = "pivot"
        elif kind_raw in ("supertrend", "supertrend_trend"):
            kind = "supertrend"
        elif kind_raw in ("vwap", "vwap_filter"):
            kind = "vwap"
        elif kind_raw in ("relative_volume", "rvol", "rel_volume"):
            kind = "relative_volume"
        else:
            kind = kind_raw
        params = rule.get("params") if isinstance(rule.get("params"), dict) else {}
        if kind in ("ma", "ema"):
            if _normalize_ma_mode(params.get("mode"), default="single") == "ribbon":
                for level in _ma_ribbon_levels_from_params(params):
                    if level["ma_type"] == "ema":
                        ema_lengths.append(int(level["length"]))
                    else:
                        ma_lengths.append(int(level["length"]))
            else:
                ln = max(2, int(_to_int_opt(params.get("length")) or 30))
                mtype = _normalize_ma_type(params.get("ma_type"), default=("ema" if kind == "ema" else "sma"))
                if mtype == "ema":
                    ema_lengths.append(ln)
                else:
                    ma_lengths.append(ln)
                if _flag(params.get("track_derivative")):
                    if mtype == "ema":
                        d_ema_lengths.append(ln)
                    else:
                        d_ma_lengths.append(ln)
                if _flag(params.get("unless_enabled")):
                    ulen = max(2, int(_to_int_opt(params.get("unless_length")) or 30))
                    utype = _normalize_ma_type(params.get("unless_type"), default="sma")
                    if utype == "ema":
                        ema_lengths.append(ulen)
                    else:
                        ma_lengths.append(ulen)
        elif kind == "macd":
            f = max(2, int(_to_int_opt(params.get("fast_length")) or 12))
            s = max(2, int(_to_int_opt(params.get("slow_length")) or 26))
            g = max(2, int(_to_int_opt(params.get("signal_length")) or 9))
            macd_configs.append((f, s, g))
        elif kind == "rsi":
            has_rsi = True
        elif kind == "rsi_d":
            has_drsi = True
        elif kind in ("heikin_ashi", "ha"):
            has_heikin_ashi = True
        elif kind == "bb":
            ln = max(2, int(_to_int_opt(params.get("length")) or 20))
            std_mult = max(0.1, float(_to_float_opt(params.get("std_mult")) or 2.0))
            bb_configs.append((ln, std_mult))
            longest_bb = max(longest_bb, ln + 1)
        elif kind == "ichimoku":
            conversion_len, base_len, leading_b_len, displacement = _ichimoku_lengths_from_params(params)
            ichimoku_configs.append((conversion_len, base_len, leading_b_len, displacement))
            longest_ichimoku = max(
                longest_ichimoku,
                max(conversion_len, base_len, leading_b_len) + displacement + 1,
            )
        elif kind == "ttm":
            bb_len = max(2, int(_to_int_opt(params.get("bb_length")) or 20))
            bb_mult = max(0.1, float(_to_float_opt(params.get("bb_mult")) or 2.0))
            kc_len = max(2, int(_to_int_opt(params.get("kc_length")) or 20))
            kc_mult = max(0.1, float(_to_float_opt(params.get("kc_mult")) or 1.5))
            mom_len = max(2, int(_to_int_opt(params.get("momentum_length")) or 20))
            ttm_configs.append((bb_len, bb_mult, kc_len, kc_mult, mom_len))
            longest_ttm = max(longest_ttm, max(bb_len, kc_len, mom_len) + 1)
        elif kind == "roc":
            ln = max(1, int(_to_int_opt(params.get("length")) or 12))
            roc_lengths.append(ln)
            longest_roc = max(longest_roc, ln + 2)
        elif kind == "sar":
            step = max(0.0001, float(_to_float_opt(params.get("step")) or 0.02))
            max_step = max(step, float(_to_float_opt(params.get("max_step")) or 0.2))
            sar_configs.append((step, max_step))
            longest_sar = max(longest_sar, 3)
        elif kind == "donchian":
            lookback = max(1, int(_to_int_opt(params.get("lookback")) or 20))
            donchian_lookbacks.append(lookback)
            longest_donchian = max(longest_donchian, lookback + 1)
        elif kind == "pivot":
            has_pivot = True
            pivot_include_half_levels = pivot_include_half_levels or _flag(params.get("include_half_levels"))
        elif kind == "supertrend":
            atr_length = max(1, int(_to_int_opt(params.get("atr_length")) or 10))
            multiplier = max(0.1, float(_to_float_opt(params.get("multiplier")) or 3.0))
            supertrend_configs.append((atr_length, multiplier))
            longest_supertrend = max(longest_supertrend, atr_length + 2)
        elif kind == "vwap":
            has_vwap = True
        elif kind == "relative_volume":
            length = max(1, int(_to_int_opt(params.get("length")) or 20))
            rvol_lengths.append(length)
            longest_rvol = max(longest_rvol, length + 1)

    longest_ma = max(ma_lengths) if ma_lengths else 0
    longest_ema = max(ema_lengths) if ema_lengths else 0
    longest_dma = max(d_ma_lengths) if d_ma_lengths else 0
    longest_dema = max(d_ema_lengths) if d_ema_lengths else 0
    longest_macd = 0
    if macd_configs:
        longest_macd = max((max(f, s) + g + 2) for (f, s, g) in macd_configs)
    min_required = max(
        30,
        longest_ma,
        longest_ema,
        longest_dma + 1,
        longest_dema + 1,
        longest_macd,
        longest_bb,
        longest_ichimoku,
        longest_ttm,
        longest_roc,
        longest_sar,
        longest_donchian,
        longest_supertrend,
        longest_rvol,
    )

    return {
        "ma_lengths": ma_lengths,
        "ema_lengths": ema_lengths,
        "macd_configs": macd_configs,
        "d_ma_lengths": d_ma_lengths,
        "d_ema_lengths": d_ema_lengths,
        "bb_configs": sorted(set(bb_configs)),
        "ttm_configs": sorted(set(ttm_configs)),
        "roc_lengths": sorted(set(roc_lengths)),
        "sar_configs": sorted(set(sar_configs)),
        "donchian_lookbacks": sorted(set(donchian_lookbacks)),
        "supertrend_configs": sorted(set(supertrend_configs)),
        "rvol_lengths": sorted(set(rvol_lengths)),
        "ichimoku_configs": sorted(set(ichimoku_configs)),
        "has_rsi": has_rsi,
        "has_drsi": has_drsi,
        "has_heikin_ashi": has_heikin_ashi,
        "has_vwap": has_vwap,
        "has_pivot": has_pivot,
        "pivot_include_half_levels": pivot_include_half_levels,
        "min_required": min_required,
    }


def _render_indicatorforge_preview_html(
    *,
    timeframe: str,
    symbols: str,
    rules_json: str,
    broker_hint: str = "robinhood",
    include_extended_hours_data: bool = False,
    use_current_candle: bool = True,
    entangled_mode: bool = False,
    entangled_primary_symbol: str = "",
    entangled_inverse_symbol: str = "",
) -> str:
    rules = _normalize_indicator_rules_payload(rules_json, default_timeframe=timeframe)
    if not rules:
        return "<div class='small'>No valid indicator rules yet. Add at least one rule.</div>"
    rules = _rules_with_default_timeframe(rules, timeframe)
    rules_by_tf = _rules_by_timeframe(rules, timeframe)

    primary_symbol = _clean_symbol(entangled_primary_symbol)
    inverse_symbol = _clean_symbol(entangled_inverse_symbol)
    entangled_enabled = bool(entangled_mode)

    syms = _clean_symbol_list(symbols)

    if entangled_enabled:
        if not primary_symbol or not inverse_symbol:
            return "<div class='small'>Enter both primary and inverse symbols to preview entanglement output.</div>"
        if primary_symbol == inverse_symbol:
            return "<div class='small'>Primary and inverse symbols must be different.</div>"
        syms = [primary_symbol, inverse_symbol]

    if not syms:
        return "<div class='small'>Enter one or more symbols to preview scanner output.</div>"

    source_hint = _normalize_market_source_hint(broker_hint)
    include_history_extended = (
        True if source_hint == "robinhood_crypto"
        else (bool(include_extended_hours_data) if source_hint in ("robinhood", "schwab") else False)
    )
    # IndicatorForge always evaluates the latest available/current candle.
    # Keep the function parameter for backward-compatible requests, but do not
    # allow saved params or hidden UI fields to switch indicators to closed-only.
    use_current = True
    if source_hint in ("robinhood", "robinhood_crypto"):
        ok, msg = _ensure_robinhood_markets_session()
        if not ok:
            return f"<div class='small'>Indicator preview unavailable: {html.escape(msg)}</div>"

    tf_key = str(timeframe or "").strip().lower()
    chart_cfg_by_tf = {tf: _indicator_rules_chart_config(tf_rules) for tf, tf_rules in rules_by_tf.items()}
    preview_eval_target_by_tf = {tf: int(cfg["min_required"]) for tf, cfg in chart_cfg_by_tf.items()}
    preview_candle_target_by_tf: dict[str, int] = {}
    for tf, cfg in chart_cfg_by_tf.items():
        min_required = int(cfg["min_required"])
        if source_hint == "schwab":
            if str(tf or "").strip().lower() == "1h":
                preview_candle_target_by_tf[tf] = max(min_required + 80, min_required)
            else:
                preview_candle_target_by_tf[tf] = max(min_required, 600)
        else:
            preview_candle_target_by_tf[tf] = min_required
    summary_html = _render_indicator_rule_summary_panel(rules, title="Configured Indicator Rules")
    rule_columns = _indicator_runtime_rule_entries(rules)

    headers = ["<th>Symbol</th>", "<th>Signal</th>", "<th>Price</th>"]
    for entry in rule_columns:
        kind_txt = str(entry.get("display_kind") or "").strip().upper()
        tf_txt = _rule_timeframe(entry.get("rule") if isinstance(entry.get("rule"), dict) else {}, timeframe)
        kind_detail = " · ".join([part for part in (kind_txt, f"TF {tf_txt}" if tf_txt else "") if part])
        kind_html = f"<div class='small'>{html.escape(kind_detail)}</div>" if kind_detail else ""
        headers.append(f"<th>{html.escape(str(entry.get('name') or 'RULE'))}{kind_html}</th>")
    headers.append("<th>Charts</th>")

    rows: list[str] = [
        "<div class='status-table-wrap'><table>",
        "<thead><tr>" + "".join(headers) + "</tr></thead><tbody>",
    ]

    def _signal_from_checks(checks: list[dict[str, Any]]) -> tuple[str, int, int, int]:
        rule_states: list[str] = []
        for c in checks:
            override_meta = _indicator_override_meta(c)
            if override_meta is not None:
                forced = str(override_meta.get("forced_side") or "").strip().upper()
                if forced in ("BUY", "SELL"):
                    rule_states.append(forced)
                    continue

            buy_ignored = bool(c.get("buy_ignored"))
            sell_ignored = bool(c.get("sell_ignored"))
            if bool(c.get("sell_ok")) and not sell_ignored:
                rule_states.append("SELL")
            elif bool(c.get("buy_ok")) and not buy_ignored:
                rule_states.append("BUY")
            elif buy_ignored and sell_ignored:
                rule_states.append("HOLD")
            else:
                rule_states.append("HOLD")

        total_rules = len(rule_states)
        buy_votes = sum(1 for s in rule_states if s == "BUY")
        sell_votes = sum(1 for s in rule_states if s == "SELL")
        buy_all = total_rules > 0 and buy_votes == total_rules
        sell_all = total_rules > 0 and sell_votes == total_rules
        signal = "SELL" if sell_all else ("BUY" if buy_all else "HOLD")
        return signal, buy_votes, sell_votes, total_rules

    def _signal_class(signal: str) -> str:
        if signal == "BUY":
            return "signal-buy"
        if signal == "SELL":
            return "signal-sell"
        return "signal-hold"

    def _render_no_data_row(sym: str, *, reason: str) -> None:
        rows.append(
            "<tr>"
            f"<td><b>{html.escape(sym)}</b></td>"
            "<td class='signal-hold'>NO_DATA</td>"
            f"<td colspan='{max(1, len(headers)-2)}' class='small'>{html.escape(reason)}</td>"
            "</tr>"
        )

    def _render_signal_row(
        *,
        sym: str,
        opens: list[float],
        highs: list[float],
        lows: list[float],
        closes: list[float],
        ohlc_by_tf: dict[str, tuple[list[float], ...]],
        checks: list[dict[str, Any]],
        signal: str,
        buy_votes: int,
        sell_votes: int,
        total_rules: int,
        signal_note: str = "",
        signal_note_class: str = "",
    ) -> None:
        price = float(closes[-1])
        cls = _signal_class(signal)
        chart_parts: list[str] = []
        for tf, tf_rules in rules_by_tf.items():
            ohlc = ohlc_by_tf.get(tf)
            if ohlc is None:
                continue
            tf_opens, tf_highs, tf_lows, tf_closes = ohlc[:4]
            tf_volumes = ohlc[4] if len(ohlc) >= 5 and isinstance(ohlc[4], list) else None
            tf_timestamps = ohlc[5] if len(ohlc) >= 6 and isinstance(ohlc[5], list) else None
            cfg = chart_cfg_by_tf.get(tf) or _indicator_rules_chart_config(tf_rules)
            chart = _market_chart_svg(
                closes=tf_closes,
                opens=tf_opens,
                highs=tf_highs,
                lows=tf_lows,
                ma_lengths=cfg["ma_lengths"],
                ema_lengths=cfg["ema_lengths"],
                macd_configs=cfg["macd_configs"],
                bb_configs=cfg["bb_configs"],
                ttm_configs=cfg["ttm_configs"],
                roc_lengths=cfg["roc_lengths"],
                sar_configs=cfg["sar_configs"],
                heikin_ashi_mode=bool(cfg["has_heikin_ashi"]),
                required_points=int(preview_candle_target_by_tf.get(tf, cfg["min_required"])),
                show_price=True,
                show_rsi=bool(cfg["has_rsi"]),
                show_drsi=bool(cfg["has_drsi"]),
                d_ma_lengths=cfg["d_ma_lengths"],
                d_ema_lengths=cfg["d_ema_lengths"],
                ichimoku_configs=cfg["ichimoku_configs"],
                display_width=420,
                display_height=180,
                show_price_markers=True,
                volumes=tf_volumes,
                timestamps=tf_timestamps,
                donchian_lookbacks=cfg.get("donchian_lookbacks") or [],
                supertrend_configs=cfg.get("supertrend_configs") or [],
                pivot_enabled=bool(cfg.get("has_pivot")),
                pivot_include_half_levels=bool(cfg.get("pivot_include_half_levels")),
                vwap_enabled=bool(cfg.get("has_vwap")),
                rvol_lengths=cfg.get("rvol_lengths") or [],
            )
            chart_parts.append(
                "<div class='indicatorforge-tf-chart'>"
                f"<div class='small'><b>TF {html.escape(tf)}</b></div>"
                f"{chart}"
                "</div>"
            )
        chart = "<div class='indicatorforge-chart-row'>" + "".join(chart_parts) + "</div>" if chart_parts else "<span class='small'>—</span>"
        note_css = str(signal_note_class or "").strip()
        if signal_note:
            if note_css:
                note_html = f"<div class='small {html.escape(note_css)}'>{html.escape(signal_note)}</div>"
            else:
                note_html = f"<div class='small'>{html.escape(signal_note)}</div>"
        else:
            note_html = ""
        vote_html = f"<div class='small'>BUY {buy_votes}/{total_rules} · SELL {sell_votes}/{total_rules}</div>"

        cells = [
            f"<td><b>{html.escape(sym)}</b></td>",
            (
                f"<td class='{cls}'>{signal}"
                f"{vote_html}"
                f"{note_html}"
                "</td>"
            ),
            f"<td>{_fmt_market_num(price, 2)}</td>",
        ]
        for i, c in enumerate(checks):
            v_txt = html.escape(str(c.get("value") or "—"))
            d_txt = html.escape(str(c.get("detail") or ""))
            rule_color = str(rule_columns[i].get("color") or "#e8ecff") if i < len(rule_columns) else "#e8ecff"
            state_txt = _indicator_rule_state_html(c)
            cells.append(
                "<td>"
                f"<div style='color:{rule_color}'>{v_txt}{state_txt}</div>"
                f"<div class='small'>{d_txt}</div>"
                "</td>"
            )
        cells.append(f"<td>{chart}</td>")
        rows.append("<tr>" + "".join(cells) + "</tr>")

    def _fetch_ohlc_for_preview(
        sym: str,
        tf: str,
    ) -> tuple[list[float], list[float], list[float], list[float], list[float], list[str], int]:
        candle_target = int(preview_candle_target_by_tf.get(tf, 30))
        raw_opens, raw_highs, raw_lows, raw_closes, raw_volumes, raw_timestamps, raw_rows, requested_bounds = _market_fetch_ohlcv(
            sym,
            tf,
            broker_hint=source_hint,
            min_candles=candle_target,
            include_extended=include_history_extended,
        )
        policy = apply_final_candle_policy(
            opens=list(raw_opens),
            highs=list(raw_highs),
            lows=list(raw_lows),
            closes=list(raw_closes),
            use_current_candle=use_current,
        )
        historical_count = len(policy.closes)
        preview_opens = list(policy.opens)
        preview_highs = list(policy.highs)
        preview_lows = list(policy.lows)
        preview_closes = list(policy.closes)
        used_count = len(preview_closes)
        if used_count > 0 and len(raw_volumes) >= used_count:
            if bool(policy.latest_excluded) and len(raw_volumes) >= used_count:
                preview_volumes = list(raw_volumes[:used_count])
                preview_timestamps = list(raw_timestamps[:used_count])
            else:
                preview_volumes = list(raw_volumes[-used_count:])
                preview_timestamps = list(raw_timestamps[-used_count:])
        else:
            preview_volumes = [0.0] * used_count
            preview_timestamps = [""] * used_count
        quote_appended = False
        if source_hint == "robinhood_crypto":
            quote = _market_fetch_crypto_quote(sym)
            if quote is not None:
                q = float(quote)
                if not preview_closes:
                    preview_opens.append(q)
                    preview_highs.append(q)
                    preview_lows.append(q)
                    preview_closes.append(q)
                    preview_volumes.append(0.0)
                    preview_timestamps.append(datetime.now(timezone.utc).isoformat())
                    quote_appended = True
                elif abs(float(preview_closes[-1]) - q) > 1e-9:
                    prev_close = float(preview_closes[-1])
                    preview_opens.append(prev_close)
                    preview_highs.append(max(prev_close, q))
                    preview_lows.append(min(prev_close, q))
                    preview_closes.append(q)
                    preview_volumes.append(0.0)
                    preview_timestamps.append(datetime.now(timezone.utc).isoformat())
                    quote_appended = True
                else:
                    if preview_opens and preview_highs and preview_lows:
                        preview_opens[-1] = float(preview_opens[-1])
                        preview_highs[-1] = max(float(preview_highs[-1]), q)
                        preview_lows[-1] = min(float(preview_lows[-1]), q)
                    preview_closes[-1] = q
        _market_log_historical_candles(
            symbol=sym,
            timeframe=tf,
            extended_enabled=include_history_extended,
            requested_bounds=requested_bounds,
            raw_rows=raw_rows,
            chart_count=len(preview_closes),
            indicator_count=len(preview_closes),
            synthetically_modified=quote_appended,
        )
        latest_ohlc = None
        if preview_closes:
            if quote_appended and len(preview_closes) >= 2:
                prev_close = float(preview_closes[-2])
                latest_close = float(preview_closes[-1])
                latest_ohlc = (
                    prev_close,
                    max(prev_close, latest_close),
                    min(prev_close, latest_close),
                    latest_close,
                )
            elif preview_closes and preview_opens and preview_highs and preview_lows:
                latest_ohlc = (preview_opens[-1], preview_highs[-1], preview_lows[-1], preview_closes[-1])
        log_indicator_policy(
            mode="PREVIEW",
            symbol=sym,
            timeframe=tf,
            session="preview",
            extended_hours=include_history_extended,
            use_current_candle=use_current,
            total_fetched=len(raw_closes),
            total_used=len(preview_closes),
            latest_ohlc=latest_ohlc,
            latest_included=policy.latest_included,
            latest_excluded=policy.latest_excluded,
            final_signal="PENDING",
        )
        return preview_opens, preview_highs, preview_lows, preview_closes, preview_volumes, preview_timestamps, historical_count

    if entangled_enabled:
        pair_label = f"{primary_symbol} (inverse {inverse_symbol})"
        primary_ohlc_by_tf: dict[str, tuple[list[float], ...]] = {}
        primary_shortages: list[str] = []
        for tf in rules_by_tf:
            (
                primary_opens,
                primary_highs,
                primary_lows,
                primary_closes,
                primary_volumes,
                primary_timestamps,
                primary_historical_count,
            ) = _fetch_ohlc_for_preview(primary_symbol, tf)
            primary_available_count = primary_historical_count if source_hint == "robinhood_crypto" else len(primary_closes)
            preview_eval_target = int(preview_eval_target_by_tf.get(tf, 30))
            if primary_available_count < preview_eval_target:
                primary_shortages.append(f"{tf} need {preview_eval_target}, got {primary_available_count}")
                continue
            primary_ohlc_by_tf[tf] = (
                primary_opens,
                primary_highs,
                primary_lows,
                primary_closes,
                primary_volumes,
                primary_timestamps,
            )
        if primary_shortages:
            _render_no_data_row(
                pair_label,
                reason=(
                    f"insufficient candles for primary {primary_symbol} "
                    f"({'; '.join(primary_shortages)}); inverse action unavailable"
                ),
            )
        else:
            primary_tf = tf_key if tf_key in primary_ohlc_by_tf else next(iter(primary_ohlc_by_tf.keys()))
            primary_opens, primary_highs, primary_lows, primary_closes = primary_ohlc_by_tf[primary_tf][:4]
            primary_checks = _build_indicator_rule_checks_by_timeframe(
                rules,
                primary_ohlc_by_tf,
                default_timeframe=timeframe,
                apply_overrides=source_hint != "robinhood_crypto",
            )
            primary_signal, primary_buy_votes, primary_sell_votes, primary_total = _signal_from_checks(primary_checks)
            if primary_signal == "BUY":
                inverse_signal = "SELL"
            elif primary_signal == "SELL":
                inverse_signal = "BUY"
            else:
                inverse_signal = "HOLD"
            _render_signal_row(
                sym=pair_label,
                opens=primary_opens,
                highs=primary_highs,
                lows=primary_lows,
                closes=primary_closes,
                ohlc_by_tf=primary_ohlc_by_tf,
                checks=primary_checks,
                signal=primary_signal,
                buy_votes=primary_buy_votes,
                sell_votes=primary_sell_votes,
                total_rules=primary_total,
                signal_note=f"Inverse {inverse_symbol}: {inverse_signal}",
                signal_note_class=_signal_class(inverse_signal),
            )
    else:
        for sym in syms:
            ohlc_by_tf: dict[str, tuple[list[float], ...]] = {}
            shortages: list[str] = []
            for tf in rules_by_tf:
                opens, highs, lows, closes, volumes, timestamps, historical_count = _fetch_ohlc_for_preview(sym, tf)
                available_count = historical_count if source_hint == "robinhood_crypto" else len(closes)
                preview_eval_target = int(preview_eval_target_by_tf.get(tf, 30))
                if available_count < preview_eval_target:
                    shortages.append(f"{tf} need {preview_eval_target}, got {available_count}")
                    continue
                ohlc_by_tf[tf] = (opens, highs, lows, closes, volumes, timestamps)
            if shortages:
                _render_no_data_row(
                    sym,
                    reason=f"insufficient candles ({'; '.join(shortages)})",
                )
                continue

            primary_tf = tf_key if tf_key in ohlc_by_tf else next(iter(ohlc_by_tf.keys()))
            opens, highs, lows, closes = ohlc_by_tf[primary_tf][:4]
            checks = _build_indicator_rule_checks_by_timeframe(
                rules,
                ohlc_by_tf,
                default_timeframe=timeframe,
                apply_overrides=source_hint != "robinhood_crypto",
            )
            signal, buy_votes, sell_votes, total_rules = _signal_from_checks(checks)
            _render_signal_row(
                sym=sym,
                opens=opens,
                highs=highs,
                lows=lows,
                closes=closes,
                ohlc_by_tf=ohlc_by_tf,
                checks=checks,
                signal=signal,
                buy_votes=buy_votes,
                sell_votes=sell_votes,
                total_rules=total_rules,
            )

    rows.append("</tbody></table></div>")
    src_label = _market_source_label(source_hint, include_extended=include_history_extended)
    tf_list = ", ".join(rules_by_tf.keys()) or str(timeframe)
    rows.append(
        f"<div class='small' style='margin-top:6px;'>Source: {html.escape(src_label)} · timeframes {html.escape(tf_list)}</div>"
    )
    return summary_html + "".join(rows)


def _indicator_signal_from_checks_for_backtest(checks: list[dict[str, Any]]) -> str:
    rule_states: list[str] = []
    for c in checks:
        override_meta = _indicator_override_meta(c)
        if override_meta is not None:
            forced = str(override_meta.get("forced_side") or "").strip().upper()
            if forced in ("BUY", "SELL"):
                rule_states.append(forced)
                continue

        buy_ignored = bool(c.get("buy_ignored"))
        sell_ignored = bool(c.get("sell_ignored"))
        if bool(c.get("sell_ok")) and not sell_ignored:
            rule_states.append("SELL")
        elif bool(c.get("buy_ok")) and not buy_ignored:
            rule_states.append("BUY")
        else:
            rule_states.append("HOLD")

    total_rules = len(rule_states)
    buy_votes = sum(1 for s in rule_states if s == "BUY")
    sell_votes = sum(1 for s in rule_states if s == "SELL")
    buy_all = total_rules > 0 and buy_votes == total_rules
    sell_all = total_rules > 0 and sell_votes == total_rules
    return "SELL" if sell_all else ("BUY" if buy_all else "HOLD")


def _simulate_signal_series_backtest(
    closes: list[float],
    *,
    signals_by_index: dict[int, str],
    start_idx: int,
    end_idx: int,
    stoploss_enabled: bool = False,
    stoploss_arm_gain_pct: Optional[float] = None,
    stoploss_trigger_pct: Optional[float] = None,
) -> dict[str, Any]:
    if len(closes) < 3 or end_idx <= start_idx:
        return {"ok": False, "reason": "insufficient candles for simulation"}

    stoploss_on = bool(stoploss_enabled)
    arm_gain_pct = _to_float_opt(stoploss_arm_gain_pct)
    trigger_pct = _to_float_opt(stoploss_trigger_pct)
    if stoploss_on:
        if arm_gain_pct is None:
            arm_gain_pct = 0.5
        if trigger_pct is None:
            trigger_pct = -0.5

    buy_signals = 0
    sell_signals = 0
    hold_signals = 0
    last_signal = "HOLD"

    # Position model: each BUY bar adds one unit at execution price.
    # Each SELL bar exits one unit only if profitable versus current average entry.
    open_units = 0
    open_cost = 0.0
    buy_executions = 0
    sell_executions = 0
    blocked_loss_sells = 0
    trade_returns: list[float] = []
    total_buy_cost = 0.0
    realized_gain_total = 0.0
    realized_sale_value_total = 0.0
    cash_units_balance = 0.0
    min_cash_units_balance = 0.0
    stoploss_armed = False
    stoploss_arm_events = 0
    stoploss_trigger_events = 0
    stoploss_forced_sell_units = 0
    stoploss_blocked_disarms = 0

    for i in range(start_idx, end_idx + 1):
        signal = str(signals_by_index.get(i) or "HOLD").strip().upper()
        if signal not in ("BUY", "SELL", "HOLD"):
            signal = "HOLD"
        last_signal = signal
        if signal == "BUY":
            buy_signals += 1
        elif signal == "SELL":
            sell_signals += 1
        else:
            hold_signals += 1

        try:
            next_price = float(closes[i + 1])
        except Exception:
            continue
        if next_price <= 0:
            continue

        if stoploss_on and open_units > 0 and open_cost > 0.0 and arm_gain_pct is not None and trigger_pct is not None:
            avg_entry_for_stop = float(open_cost) / float(open_units) if open_units > 0 else 0.0
            if avg_entry_for_stop > 0.0:
                percentage_gain = ((float(next_price) / float(avg_entry_for_stop)) - 1.0) * 100.0
                if (not stoploss_armed) and percentage_gain >= float(arm_gain_pct):
                    stoploss_armed = True
                    stoploss_arm_events += 1
                if stoploss_armed:
                    trigger_price = float(avg_entry_for_stop) * (1.0 + (float(trigger_pct) / 100.0))
                    if float(next_price) <= float(trigger_price):
                        # Backtest assumption: if bar close breaches trigger, stop is
                        # treated as filled intrabar at trigger price (not close).
                        stop_exec_price = float(trigger_price)
                        stop_ret = (float(stop_exec_price) / float(avg_entry_for_stop)) - 1.0
                        if stop_ret <= 0.0:
                            stoploss_armed = False
                            stoploss_blocked_disarms += 1
                        else:
                            qty_to_sell = int(open_units)
                            if qty_to_sell > 0:
                                realized_gain_total += float(qty_to_sell) * (float(stop_exec_price) - float(avg_entry_for_stop))
                                realized_sale_value_total += float(qty_to_sell) * float(stop_exec_price)
                                trade_returns.extend([float(stop_ret)] * qty_to_sell)
                                sell_executions += qty_to_sell
                                cash_units_balance += float(qty_to_sell) * (1.0 + float(stop_ret))
                                stoploss_trigger_events += 1
                                stoploss_forced_sell_units += qty_to_sell
                            open_units = 0
                            open_cost = 0.0
                            stoploss_armed = False

        if signal == "BUY":
            open_units += 1
            open_cost += float(next_price)
            total_buy_cost += float(next_price)
            buy_executions += 1
            cash_units_balance -= 1.0
            min_cash_units_balance = min(float(min_cash_units_balance), float(cash_units_balance))
            continue

        if signal == "SELL" and open_units > 0:
            if open_cost <= 0.0:
                open_units = 0
                open_cost = 0.0
                stoploss_armed = False
                continue
            avg_entry = float(open_cost) / float(open_units)
            if avg_entry <= 0.0:
                continue
            ret = (float(next_price) / float(avg_entry)) - 1.0
            if ret <= 0.0:
                blocked_loss_sells += 1
                continue
            realized_gain_total += float(next_price) - float(avg_entry)
            realized_sale_value_total += float(next_price)
            trade_returns.append(float(ret))
            sell_executions += 1
            cash_units_balance += 1.0 + float(ret)
            open_units -= 1
            open_cost -= float(avg_entry)
            if open_units <= 0:
                open_units = 0
                open_cost = 0.0
                stoploss_armed = False
            elif open_cost < 0.0:
                open_cost = 0.0

    realized_sells = len(trade_returns)
    realized_total_return = sum(float(r) for r in trade_returns)
    avg_win_pct: Optional[float] = None
    realized_gain_pct_of_sales: Optional[float] = None
    if realized_sells > 0:
        avg_win_pct = 100.0 * (float(realized_total_return) / float(realized_sells))
    if realized_sale_value_total > 0.0:
        realized_gain_pct_of_sales = (float(realized_gain_total) / float(realized_sale_value_total)) * 100.0

    # Completed wins are profitable SELL executions of previously acquired units.
    # Loss-selling is blocked, so realized losing exits remain zero in this model.
    trades = int(buy_executions)
    wins = int(realized_sells)
    losses = 0

    open_avg_entry_price = (float(open_cost) / float(open_units)) if open_units > 0 else None
    open_mark_price: Optional[float] = None
    try:
        last_close = float(closes[-1])
        if last_close > 0:
            open_mark_price = last_close
    except Exception:
        open_mark_price = None

    open_unrealized_pct: Optional[float] = None
    if open_units > 0 and open_avg_entry_price and open_mark_price and open_avg_entry_price > 0:
        open_unrealized_pct = ((float(open_mark_price) / float(open_avg_entry_price)) - 1.0) * 100.0

    open_unrealized_gain_total: Optional[float] = None
    if open_units > 0 and open_mark_price is not None and open_cost > 0.0:
        open_unrealized_gain_total = (float(open_units) * float(open_mark_price)) - float(open_cost)

    required_capital_units: Optional[float] = None
    ending_equity_units: Optional[float] = None
    cumulative_gain_pct: Optional[float] = None
    if buy_executions > 0:
        required_capital_units = max(0.0, -float(min_cash_units_balance))
        if required_capital_units > 0.0:
            open_mark_value_units = 0.0
            if open_units > 0 and open_avg_entry_price and open_mark_price and open_avg_entry_price > 0:
                open_mark_value_units = float(open_units) * (float(open_mark_price) / float(open_avg_entry_price))
            ending_equity_units = float(required_capital_units) + float(cash_units_balance) + float(open_mark_value_units)
            cumulative_gain_pct = ((float(ending_equity_units) / float(required_capital_units)) - 1.0) * 100.0

    # Net cumulative gain uses actual simulated buy cost as the denominator and
    # includes completed realized profit plus MTM gain/loss on still-open units.
    net_total_gain_dollars: Optional[float] = None
    net_cumulative_gain_pct: Optional[float] = None
    if total_buy_cost > 0.0:
        net_total_gain_dollars = float(realized_gain_total)
        if open_unrealized_gain_total is not None:
            net_total_gain_dollars += float(open_unrealized_gain_total)
        net_cumulative_gain_pct = (100.0 * float(net_total_gain_dollars)) / float(total_buy_cost)

    return {
        "ok": True,
        "bars_tested": max(0, end_idx - start_idx + 1),
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "avg_win_pct": avg_win_pct,
        "realized_gain_total": realized_gain_total,
        "realized_sale_value_total": realized_sale_value_total,
        "realized_gain_pct_of_sales": realized_gain_pct_of_sales,
        "cumulative_gain_pct": cumulative_gain_pct,
        "required_capital_units": required_capital_units,
        "ending_equity_units": ending_equity_units,
        "last_signal": last_signal,
        "buy_signals": buy_signals,
        "sell_signals": sell_signals,
        "hold_signals": hold_signals,
        "buy_executions": buy_executions,
        "sell_executions": sell_executions,
        "blocked_loss_sells": blocked_loss_sells,
        "stoploss_enabled": bool(stoploss_on),
        "stoploss_arm_gain_pct": arm_gain_pct,
        "stoploss_trigger_pct": trigger_pct,
        "stoploss_armed": bool(stoploss_armed),
        "stoploss_arm_events": stoploss_arm_events,
        "stoploss_trigger_events": stoploss_trigger_events,
        "stoploss_forced_sell_units": stoploss_forced_sell_units,
        "stoploss_blocked_disarms": stoploss_blocked_disarms,
        "open_units": open_units,
        "open_avg_entry_price": open_avg_entry_price,
        "open_mark_price": open_mark_price,
        "open_unrealized_pct": open_unrealized_pct,
        "open_unrealized_gain_total": open_unrealized_gain_total,
        "total_buy_cost": total_buy_cost,
        "net_total_gain_dollars": net_total_gain_dollars,
        "net_cumulative_gain_pct": net_cumulative_gain_pct,
    }


def _invert_signal(signal: str) -> str:
    s = str(signal or "").strip().upper()
    if s == "BUY":
        return "SELL"
    if s == "SELL":
        return "BUY"
    return "HOLD"


def _render_indicatorforge_backtest_html(
    *,
    timeframe: str,
    symbols: str,
    rules_json: str,
    broker_hint: str = "robinhood",
    include_extended_hours_data: bool = False,
    entangled_mode: bool = False,
    entangled_primary_symbol: str = "",
    entangled_inverse_symbol: str = "",
    backtest_candles: int = INDICATORFORGE_BACKTEST_DEFAULT_CANDLES,
    stoploss_enabled: bool = False,
    target_gain_pct: Optional[float] = None,
    stop_loss_pct: Optional[float] = None,
    include_held_end_column: bool = False,
) -> str:
    rules = _normalize_indicator_rules_payload(rules_json, default_timeframe=timeframe)
    if not rules:
        return "<div class='small'>No valid indicator rules yet. Add at least one rule.</div>"
    rules = _rules_with_default_timeframe(rules, timeframe)
    rules_by_tf = _rules_by_timeframe(rules, timeframe)

    source_hint = _normalize_market_source_hint(broker_hint)
    include_history_extended = (
        True if source_hint == "robinhood_crypto"
        else (bool(include_extended_hours_data) if source_hint in ("robinhood", "schwab") else False)
    )
    if source_hint in ("robinhood", "robinhood_crypto"):
        ok, msg = _ensure_robinhood_markets_session()
        if not ok:
            return f"<div class='small'>Backtest unavailable: {html.escape(msg)}</div>"

    stoploss_on = bool(stoploss_enabled)
    stoploss_arm_gain = _to_float_opt(target_gain_pct)
    stoploss_trigger = _to_float_opt(stop_loss_pct)
    if stoploss_on:
        if stoploss_arm_gain is None:
            stoploss_arm_gain = 0.5
        if stoploss_trigger is None:
            stoploss_trigger = -0.5

    requested_lookback = max(40, min(int(backtest_candles or INDICATORFORGE_BACKTEST_DEFAULT_CANDLES), INDICATORFORGE_BACKTEST_MAX_CANDLES))
    lookback = requested_lookback
    tf_key = str(timeframe or "").strip().lower()
    chart_cfg_by_tf = {tf: _indicator_rules_chart_config(tf_rules) for tf, tf_rules in rules_by_tf.items()}
    min_required_by_tf = {
        tf: int(cfg.get("min_required") or 30) for tf, cfg in chart_cfg_by_tf.items()
    }
    min_required = max(min_required_by_tf.values()) if min_required_by_tf else 30
    lookback_note: Optional[str] = None
    if source_hint == "robinhood":
        execution_min_required = int(min_required_by_tf.get(tf_key, min_required))
        adjusted_lookback, adjust_note = _robinhood_effective_backtest_lookback(
            tf_key,
            requested_lookback,
            execution_min_required,
        )
        lookback = int(adjusted_lookback)
        lookback_note = adjust_note

    primary_symbol = _clean_symbol(entangled_primary_symbol)
    inverse_symbol = _clean_symbol(entangled_inverse_symbol)
    entangled_enabled = bool(entangled_mode)

    syms = _clean_symbol_list(symbols)
    if entangled_enabled:
        if not primary_symbol or not inverse_symbol:
            return "<div class='small'>Enter both primary and inverse symbols to run backtest.</div>"
        if primary_symbol == inverse_symbol:
            return "<div class='small'>Primary and inverse symbols must be different.</div>"
        syms = [primary_symbol, inverse_symbol]
    if not syms:
        return "<div class='small'>Enter one or more symbols to run backtest.</div>"

    if lookback <= 0:
        return f"<div class='small'>Backtest unavailable: {html.escape(lookback_note or 'insufficient Robinhood historical candle capacity')}</div>"

    fetch_target_by_tf: dict[str, int] = {}
    for tf, tf_min_required in min_required_by_tf.items():
        target = int(tf_min_required + lookback + 3)
        if source_hint == "schwab" and str(tf or "").strip().lower() == "1h":
            target += 80
        fetch_target_by_tf[tf] = target
    fetch_target = int(fetch_target_by_tf.get(tf_key) or max(fetch_target_by_tf.values() or [min_required + lookback + 3]))

    held_end_header = "<th>Held End</th>" if include_held_end_column else ""
    rows: list[str] = [
        "<div class='status-table-wrap'><table>",
        (
            "<thead><tr>"
            "<th>Symbol</th><th>Candles</th><th>Eval Bars</th><th>Trades</th><th>Wins</th><th>Avg Win %</th>"
            f"<th>Realized Gain</th><th>Net Cum Gain %</th>{held_end_header}<th>Last Signal</th><th>Notes</th>"
            "</tr></thead><tbody>"
        ),
    ]

    def _fmt_pct(v: Optional[float], *, digits: int = 2) -> str:
        if v is None:
            return "—"
        return f"{float(v):.{digits}f}%"

    def _fmt_money(v: Optional[float], *, digits: int = 2) -> str:
        if v is None:
            return "—"
        amt = float(v)
        sign = "-" if amt < 0 else ""
        return f"{sign}${abs(amt):,.{digits}f}"

    def _signal_class(signal: str) -> str:
        s = str(signal or "").strip().upper()
        if s == "BUY":
            return "signal-buy"
        if s == "SELL":
            return "signal-sell"
        return "signal-hold"

    def _render_stats_row(sym_label: str, stats: dict[str, Any], *, note: str = "") -> None:
        def _render_held_end(stats_map: dict[str, Any]) -> tuple[str, str]:
            open_units = int(stats_map.get("open_units") or 0)
            if open_units <= 0:
                return ("signal-hold", "0")
            open_gain = _to_float_opt(stats_map.get("open_unrealized_gain_total"))
            open_pct = _to_float_opt(stats_map.get("open_unrealized_pct"))
            cls = "signal-hold"
            basis = open_gain if open_gain is not None else open_pct
            if basis is not None:
                if float(basis) > 0.0:
                    cls = "signal-buy"
                elif float(basis) < 0.0:
                    cls = "signal-sell"
            extras: list[str] = []
            if open_gain is not None:
                extras.append(_fmt_money(open_gain))
            if open_pct is not None:
                extras.append(_fmt_pct(open_pct))
            txt = str(open_units)
            if extras:
                txt += f" ({', '.join(extras)})"
            return (cls, txt)

        def _fmt_realized_gain(stats_map: dict[str, Any]) -> str:
            gain_total = _to_float_opt(stats_map.get("realized_gain_total"))
            gain_pct = _to_float_opt(stats_map.get("realized_gain_pct_of_sales"))
            sale_total = _to_float_opt(stats_map.get("realized_sale_value_total"))
            if gain_total is None or gain_pct is None or sale_total is None or sale_total <= 0.0:
                return "—"
            return f"{_fmt_money(gain_total)} ({_fmt_pct(gain_pct)})"

        def _render_note_stack(lines: list[str]) -> str:
            if not lines:
                return "<span class='small'>—</span>"
            return (
                "<div class='small' style='display:flex; flex-direction:column; gap:2px;'>"
                + "".join(f"<div>{html.escape(str(line or ''))}</div>" for line in lines)
                + "</div>"
            )

        if not bool(stats.get("ok")):
            reason = html.escape(str(stats.get("reason") or "insufficient data"))
            held_end_empty = "<td>—</td>" if include_held_end_column else ""
            rows.append(
                "<tr>"
                f"<td><b>{html.escape(sym_label)}</b></td>"
                f"<td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td>{held_end_empty}<td class='signal-hold'>NO_DATA</td>"
                f"<td class='small'>{reason}</td>"
                "</tr>"
            )
            return

        sig = str(stats.get("last_signal") or "HOLD").strip().upper()
        notes: list[str] = [
            f"Signals BUY/SELL/HOLD: {int(stats.get('buy_signals') or 0)}/{int(stats.get('sell_signals') or 0)}/{int(stats.get('hold_signals') or 0)}"
        ]
        notes.append(
            f"Executed BUY/SELL: {int(stats.get('buy_executions') or 0)}/{int(stats.get('sell_executions') or 0)}"
        )
        notes.append("Wins = completed profitable SELL executions of previously bought units")
        notes.append("Avg Win % = average realized % gain across executed SELL units")
        notes.append("Realized Gain = total estimated $ profit on completed profitable SELL units; % is profit as share of sale proceeds")
        notes.append("Net Cum Gain % = realized profit plus open-unit MTM gain/loss, divided by total simulated BUY cost")
        if include_held_end_column:
            notes.append("Held End = simulated units still open at the end of the backtest, with mark-to-market $ and % gain/loss")
        realized_gain_total = _to_float_opt(stats.get("realized_gain_total"))
        realized_sale_value_total = _to_float_opt(stats.get("realized_sale_value_total"))
        realized_gain_pct_of_sales = _to_float_opt(stats.get("realized_gain_pct_of_sales"))
        if (
            realized_gain_total is not None
            and realized_sale_value_total is not None
            and realized_sale_value_total > 0.0
            and realized_gain_pct_of_sales is not None
        ):
            notes.append(
                f"Realized winning sales: {_fmt_money(realized_gain_total)} profit on {_fmt_money(realized_sale_value_total)} sold ({_fmt_pct(realized_gain_pct_of_sales)} of sale proceeds)"
            )
        if bool(stats.get("stoploss_enabled")):
            notes.append(
                f"Stop-loss sim (arm/trigger): {_fmt_pct(_to_float_opt(stats.get('stoploss_arm_gain_pct')))} / {_fmt_pct(_to_float_opt(stats.get('stoploss_trigger_pct')))}"
            )
            notes.append(
                f"Stop-loss arm/trigger events: {int(stats.get('stoploss_arm_events') or 0)}/{int(stats.get('stoploss_trigger_events') or 0)}"
            )
            forced_units = int(stats.get("stoploss_forced_sell_units") or 0)
            if forced_units > 0:
                notes.append(f"Forced stop-loss sell units: {forced_units}")
            blocked_disarms = int(stats.get("stoploss_blocked_disarms") or 0)
            if blocked_disarms > 0:
                notes.append(f"Stop-loss disarms from no-loss guard: {blocked_disarms}")
        open_units = int(stats.get("open_units") or 0)
        if open_units > 0:
            avg_entry = _to_float_opt(stats.get("open_avg_entry_price"))
            mark_px = _to_float_opt(stats.get("open_mark_price"))
            if avg_entry is not None:
                if mark_px is not None:
                    notes.append(
                        f"Open units: {open_units} @ avg {_fmt_market_num(avg_entry, 4)} (mark {_fmt_market_num(mark_px, 4)})"
                    )
                else:
                    notes.append(f"Open units: {open_units} @ avg {_fmt_market_num(avg_entry, 4)}")
            else:
                notes.append(f"Open units: {open_units}")
            open_unrealized_pct = _to_float_opt(stats.get("open_unrealized_pct"))
            if open_unrealized_pct is not None:
                notes.append(f"Open MTM: {open_unrealized_pct:.2f}%")
        required_capital_units = _to_float_opt(stats.get("required_capital_units"))
        ending_equity_units = _to_float_opt(stats.get("ending_equity_units"))
        if required_capital_units is not None and ending_equity_units is not None:
            notes.append(
                f"Capital model: start {required_capital_units:.2f} units -> end {ending_equity_units:.2f} units"
            )
        blocked_sells = int(stats.get("blocked_loss_sells") or 0)
        if blocked_sells > 0:
            notes.append(f"Blocked SELL-at-loss bars: {blocked_sells}")
        if note:
            notes.append(note)
        notes_html = _render_note_stack(notes)
        held_end_class, held_end_txt = _render_held_end(stats)
        held_end_cell = (
            f"<td class='{held_end_class}'>{html.escape(held_end_txt)}</td>" if include_held_end_column else ""
        )
        rows.append(
            "<tr>"
            f"<td><b>{html.escape(sym_label)}</b></td>"
            f"<td>{int(stats.get('candles_fetched') or 0)}</td>"
            f"<td>{int(stats.get('bars_tested') or 0)}</td>"
            f"<td>{int(stats.get('trades') or 0)}</td>"
            f"<td>{int(stats.get('wins') or 0)}</td>"
            f"<td>{_fmt_pct(_to_float_opt(stats.get('avg_win_pct')))}</td>"
            f"<td>{_fmt_realized_gain(stats)}</td>"
            f"<td>{_fmt_pct(_to_float_opt(stats.get('net_cumulative_gain_pct')))}</td>"
            f"{held_end_cell}"
            f"<td class='{_signal_class(sig)}'>{html.escape(sig)}</td>"
            f"<td>{notes_html}</td>"
            "</tr>"
        )

    def _clean_backtest_ohlcv(
        opens: list[float],
        highs: list[float],
        lows: list[float],
        closes: list[float],
        volumes: list[float],
        timestamps: list[str],
    ) -> tuple[list[float], list[float], list[float], list[float], list[float], list[str]]:
        clean_o: list[float] = []
        clean_h: list[float] = []
        clean_l: list[float] = []
        clean_c: list[float] = []
        clean_v: list[float] = []
        clean_t: list[str] = []
        n = min(len(opens), len(highs), len(lows), len(closes))
        for i in range(n):
            o = _to_float_opt(opens[i])
            h = _to_float_opt(highs[i])
            l = _to_float_opt(lows[i])
            c = _to_float_opt(closes[i])
            if (
                o is None
                or h is None
                or l is None
                or c is None
                or not math.isfinite(float(c))
                or float(c) <= 0.0
            ):
                continue
            clean_o.append(float(o))
            clean_h.append(float(h))
            clean_l.append(float(l))
            clean_c.append(float(c))
            v = _to_float_opt(volumes[i] if i < len(volumes) else None)
            clean_v.append(float(v) if v is not None and math.isfinite(float(v)) else 0.0)
            clean_t.append(str(timestamps[i] if i < len(timestamps) else ""))
        return clean_o, clean_h, clean_l, clean_c, clean_v, clean_t

    def _fetch_backtest_ohlcv_by_timeframe(sym: str, timeframe_keys: list[str]) -> dict[str, tuple[list[float], ...]]:
        out: dict[str, tuple[list[float], ...]] = {}
        for tf in timeframe_keys:
            opens, highs, lows, closes, volumes, timestamps, _raw_rows, _requested_bounds = _market_fetch_ohlcv(
                sym,
                tf,
                broker_hint=source_hint,
                min_candles=int(fetch_target_by_tf.get(tf) or fetch_target),
                include_extended=include_history_extended,
            )
            out[tf] = _clean_backtest_ohlcv(opens, highs, lows, closes, volumes, timestamps)
        return out

    def _simulate_rule_series_by_timeframe(
        ohlcv_by_tf: dict[str, tuple[list[float], ...]],
        *,
        forced_signals_by_offset: Optional[dict[int, str]] = None,
    ) -> dict[str, Any]:
        if not ohlcv_by_tf:
            return {
                "ok": False,
                "candles_fetched": 0,
                "reason": "no candle data available",
            }
        execution_tf = tf_key if tf_key in ohlcv_by_tf else next(iter(ohlcv_by_tf.keys()))
        execution_closes = list((ohlcv_by_tf.get(execution_tf) or ([], [], [], []))[3])
        required_execution = int(min_required_by_tf.get(execution_tf, min_required))
        rule_timeframes_for_eval = [] if forced_signals_by_offset is not None else list(rules_by_tf.keys())
        shortage_lines: list[str] = []
        for tf in rule_timeframes_for_eval:
            tf_rules = rules_by_tf.get(tf) or []
            tf_closes = list((ohlcv_by_tf.get(tf) or ([], [], [], []))[3])
            tf_min_required = int(min_required_by_tf.get(tf, _indicator_rules_chart_config(tf_rules).get("min_required") or 30))
            if len(tf_closes) < (tf_min_required + 2):
                shortage_lines.append(f"{tf} need >= {tf_min_required + 2}, got {len(tf_closes)}")
        if len(execution_closes) < (required_execution + 2):
            shortage_lines.append(
                f"execution {execution_tf} need >= {required_execution + 2}, got {len(execution_closes)}"
            )
        if shortage_lines:
            return {
                "ok": False,
                "candles_fetched": len(execution_closes),
                "reason": "; ".join(shortage_lines),
            }

        eval_capacity = len(execution_closes) - 1 - required_execution
        for tf in rule_timeframes_for_eval:
            tf_rules = rules_by_tf.get(tf) or []
            tf_closes = list((ohlcv_by_tf.get(tf) or ([], [], [], []))[3])
            tf_min_required = int(min_required_by_tf.get(tf, _indicator_rules_chart_config(tf_rules).get("min_required") or 30))
            eval_capacity = min(eval_capacity, len(tf_closes) - 1 - tf_min_required)
        if forced_signals_by_offset is not None and forced_signals_by_offset:
            eval_capacity = min(eval_capacity, max(int(offset) for offset in forced_signals_by_offset))

        eval_bars = min(int(lookback), int(eval_capacity))
        if eval_bars <= 0:
            return {
                "ok": False,
                "candles_fetched": len(execution_closes),
                "reason": "not enough aligned evaluation bars after warmup",
            }

        end_idx = len(execution_closes) - 2
        start_idx = end_idx - eval_bars + 1
        if end_idx <= start_idx:
            return {
                "ok": False,
                "candles_fetched": len(execution_closes),
                "reason": "not enough evaluation bars after warmup",
            }

        signals: dict[int, str] = {}
        signals_by_offset: dict[int, str] = {}
        for i in range(start_idx, end_idx + 1):
            offset_from_end = int(len(execution_closes) - 1 - i)
            if forced_signals_by_offset is not None:
                signal = str(forced_signals_by_offset.get(offset_from_end) or "HOLD").strip().upper()
                if signal not in ("BUY", "SELL", "HOLD"):
                    signal = "HOLD"
            else:
                ohlc_windows: dict[str, tuple[list[float], ...]] = {}
                for tf in rules_by_tf:
                    tf_data = ohlcv_by_tf.get(tf) or ([], [], [], [], [], [])
                    tf_opens = list(tf_data[0]) if len(tf_data) >= 1 else []
                    tf_highs = list(tf_data[1]) if len(tf_data) >= 2 else []
                    tf_lows = list(tf_data[2]) if len(tf_data) >= 3 else []
                    tf_closes = list(tf_data[3]) if len(tf_data) >= 4 else []
                    tf_volumes = list(tf_data[4]) if len(tf_data) >= 5 else []
                    tf_timestamps = list(tf_data[5]) if len(tf_data) >= 6 else []
                    tf_end_idx = int(len(tf_closes) - 1 - offset_from_end)
                    if tf_end_idx < 1:
                        continue
                    ohlc_windows[tf] = (
                        tf_opens[: tf_end_idx + 1],
                        tf_highs[: tf_end_idx + 1],
                        tf_lows[: tf_end_idx + 1],
                        tf_closes[: tf_end_idx + 1],
                        tf_volumes[: tf_end_idx + 1],
                        tf_timestamps[: tf_end_idx + 1],
                    )
                checks = _build_indicator_rule_checks_by_timeframe(
                    rules,
                    ohlc_windows,
                    default_timeframe=timeframe,
                    apply_overrides=True,
                )
                signal = _indicator_signal_from_checks_for_backtest(checks)
            signals[i] = signal
            signals_by_offset[offset_from_end] = signal
        stats = _simulate_signal_series_backtest(
            execution_closes,
            signals_by_index=signals,
            start_idx=start_idx,
            end_idx=end_idx,
            stoploss_enabled=stoploss_on,
            stoploss_arm_gain_pct=stoploss_arm_gain,
            stoploss_trigger_pct=stoploss_trigger,
        )
        stats["candles_fetched"] = len(execution_closes)
        stats["base_timeframe"] = execution_tf
        stats["timeframes"] = list(rules_by_tf.keys())
        stats["_signals_by_offset"] = signals_by_offset
        return stats

    if entangled_enabled:
        timeframe_keys = list(rules_by_tf.keys())
        if tf_key not in timeframe_keys:
            timeframe_keys = [tf_key] + timeframe_keys
        primary_ohlcv_by_tf = _fetch_backtest_ohlcv_by_timeframe(primary_symbol, timeframe_keys)
        inverse_ohlcv_by_tf = _fetch_backtest_ohlcv_by_timeframe(inverse_symbol, [tf_key])
        primary_stats = _simulate_rule_series_by_timeframe(primary_ohlcv_by_tf)
        primary_offsets = primary_stats.get("_signals_by_offset") if isinstance(primary_stats, dict) else {}
        if isinstance(primary_stats, dict) and bool(primary_stats.get("ok")) and isinstance(primary_offsets, dict):
            inverse_forced = {
                int(offset): _invert_signal(signal)
                for offset, signal in primary_offsets.items()
            }
            inverse_stats = _simulate_rule_series_by_timeframe(inverse_ohlcv_by_tf, forced_signals_by_offset=inverse_forced)
        else:
            inverse_stats = {
                "ok": False,
                "candles_fetched": len((inverse_ohlcv_by_tf.get(tf_key) or ([], [], [], []))[3]),
                "reason": "primary signal unavailable",
            }
        tf_note = "Rule timeframes: " + ", ".join(rules_by_tf.keys())
        primary_note = f"Primary rules drive signal. {tf_note}."
        inverse_note = f"Inverse follows opposite of {primary_symbol} on {tf_key}. {tf_note}."
        if source_hint == "robinhood":
            primary_short = [
                f"{tf} returned {len((primary_ohlcv_by_tf.get(tf) or ([], [], [], []))[3])}/{int(fetch_target_by_tf.get(tf) or fetch_target)}"
                for tf in timeframe_keys
                if len((primary_ohlcv_by_tf.get(tf) or ([], [], [], []))[3]) < int(fetch_target_by_tf.get(tf) or fetch_target)
            ]
            inverse_len = len((inverse_ohlcv_by_tf.get(tf_key) or ([], [], [], []))[3])
            if primary_short:
                primary_note += " Robinhood " + "; ".join(primary_short) + "."
            if inverse_len < fetch_target:
                inverse_note += f" Robinhood returned {inverse_len}/{fetch_target} requested execution candles."
        _render_stats_row(primary_symbol, primary_stats, note=primary_note)
        _render_stats_row(inverse_symbol, inverse_stats, note=inverse_note)
    else:
        for sym in syms:
            timeframe_keys = list(rules_by_tf.keys())
            if tf_key not in timeframe_keys:
                timeframe_keys = [tf_key] + timeframe_keys
            ohlcv_by_tf = _fetch_backtest_ohlcv_by_timeframe(sym, timeframe_keys)
            stats = _simulate_rule_series_by_timeframe(ohlcv_by_tf)
            note = ""
            if lookback_note:
                note = lookback_note
            tf_note = "Rule timeframes: " + ", ".join(rules_by_tf.keys())
            note = (note + " " if note else "") + tf_note + "."
            if source_hint == "robinhood":
                short_parts = [
                    f"{tf} returned {len((ohlcv_by_tf.get(tf) or ([], [], [], []))[3])}/{int(fetch_target_by_tf.get(tf) or fetch_target)}"
                    for tf in timeframe_keys
                    if len((ohlcv_by_tf.get(tf) or ([], [], [], []))[3]) < int(fetch_target_by_tf.get(tf) or fetch_target)
                ]
                if short_parts:
                    note += " Robinhood " + "; ".join(short_parts) + "."
            _render_stats_row(sym, stats, note=note)

    rows.append("</tbody></table></div>")
    src_label = _market_source_label(source_hint, include_extended=include_history_extended)
    rows.append(
        "<div class='small' style='margin-top:6px;'>"
        f"Quick Backtest · lookback={lookback} candles · execution timeframe {html.escape(str(timeframe))} · source={html.escape(src_label)}"
        f" · warmup={min_required} candles"
        f" · rule timeframes={html.escape(', '.join(rules_by_tf.keys()) or str(timeframe))}"
        f"{' · requested=' + str(requested_lookback) if requested_lookback != lookback else ''}"
        "</div>"
    )
    if lookback_note:
        rows.append(f"<div class='small'>{html.escape(lookback_note)}</div>")
    rows.append(
        "<div class='small'>Model: long-only; each BUY bar adds one unit at next-candle close; each SELL bar exits one existing unit only when profitable vs current average entry; no final force-close; Wins = profitable SELL executions; Avg Win % = average realized gain across executed SELL units; Realized Gain = total estimated dollar profit on completed profitable SELL units, with % shown as profit share of those sale proceeds; Net Cum Gain % = realized profit plus still-open unit MTM gain/loss, divided by total simulated BUY cost; Held End = units still open at the end of the sim whether marked at gain or loss; no fees/slippage/spread.</div>"
        if include_held_end_column
        else "<div class='small'>Model: long-only; each BUY bar adds one unit at next-candle close; each SELL bar exits one existing unit only when profitable vs current average entry; no final force-close; Wins = profitable SELL executions; Avg Win % = average realized gain across executed SELL units; Realized Gain = total estimated dollar profit on completed profitable SELL units, with % shown as profit share of those sale proceeds; Net Cum Gain % = realized profit plus still-open unit MTM gain/loss, divided by total simulated BUY cost; no fees/slippage/spread.</div>"
    )
    if stoploss_on:
        rows.append(
            "<div class='small'>"
            f"Stop-loss model: armed at gain >= {_fmt_pct(stoploss_arm_gain)}; when armed and price <= trigger {_fmt_pct(stoploss_trigger)}, backtest assumes intrabar fill at trigger price and force-sells all open units if that trigger fill remains above average entry; otherwise disarms (no-loss guard).</div>"
        )
    else:
        rows.append("<div class='small'>Stop-loss model: disabled in this backtest run.</div>")
    return "".join(rows)


def _fetch_robinhood_news(symbol: str, limit: int) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    if rh is None:
        return out
    try:
        items = rh.stocks.get_news(symbol)
    except Exception:
        return out
    if not isinstance(items, list):
        return out
    for it in items[: max(1, int(limit))]:
        if not isinstance(it, dict):
            continue
        title = str(it.get("title") or "").strip()
        if not title:
            continue
        out.append(
            {
                "symbol": symbol,
                "title": title,
                "url": str(it.get("relay_url") or it.get("url") or "").strip(),
                "source": str(it.get("source") or "Robinhood"),
                "published_at": str(it.get("published_at") or ""),
                "summary": str(it.get("summary") or "").strip(),
                "provider": "Robinhood",
            }
        )
    return out


def _fetch_rss_news(symbol: str, limit: int) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    feed_url = f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={quote_plus(symbol)}&region=US&lang=en-US"
    try:
        resp = httpx.get(feed_url, timeout=10.0)
        if resp.status_code >= 400:
            return out
        root = ET.fromstring(resp.text)
    except Exception:
        return out
    for item in root.findall(".//item")[: max(1, int(limit))]:
        title = (item.findtext("title") or "").strip()
        if not title:
            continue
        out.append(
            {
                "symbol": symbol,
                "title": title,
                "url": (item.findtext("link") or "").strip(),
                "source": (item.findtext("source") or "Yahoo RSS").strip(),
                "published_at": (item.findtext("pubDate") or "").strip(),
                "summary": (item.findtext("description") or "").strip(),
                "provider": "RSS",
            }
        )
    return out


def _render_markets_news_html(*, limit: int) -> str:
    symbols = _list_markets_watchlist()
    if not symbols:
        return "<div class='small'>Watchlist is empty. Add symbols to read ticker news.</div>"

    rh_ok, _ = _ensure_robinhood_markets_session()
    blocks: list[str] = []
    for sym in symbols:
        per_sym: list[dict[str, str]] = []
        if rh_ok:
            per_sym.extend(_fetch_robinhood_news(sym, limit))
        if len(per_sym) < max(1, int(limit)):
            need = max(1, int(limit)) - len(per_sym)
            per_sym.extend(_fetch_rss_news(sym, need))
        if not per_sym:
            blocks.append(f"<div class='small' style='margin-top:8px;'><b>{html.escape(sym)}:</b> no recent articles found.</div>")
            continue

        rows = [f"<div class='small' style='margin-top:10px;'><b>{html.escape(sym)}</b></div>", "<table><thead><tr><th>Headline</th><th>Source</th><th>When</th></tr></thead><tbody>"]
        for n in per_sym[: max(1, int(limit))]:
            title = html.escape(str(n.get("title") or ""))
            src = html.escape(f"{n.get('provider')}: {n.get('source') or ''}".strip(": "))
            when = html.escape(str(n.get("published_at") or "—"))
            url = str(n.get("url") or "").strip()
            if url:
                head = f"<a class='link' href='{html.escape(url)}' target='_blank' rel='noopener noreferrer'>{title}</a>"
            else:
                head = title
            rows.append(f"<tr><td>{head}</td><td class='small'>{src}</td><td class='small'>{when}</td></tr>")
        rows.append("</tbody></table>")
        blocks.append("".join(rows))
    blocks.append("<div class='small' style='margin-top:8px;'>News sources: Robinhood first, Yahoo Finance RSS fallback.</div>")
    return "".join(blocks)


def _render_markets_watchlist_html() -> str:
    symbols = _list_markets_watchlist()
    if not symbols:
        return "<div class='small'>No watchlist symbols yet.</div>"
    rows = ["<table><thead><tr><th>Symbol</th><th></th></tr></thead><tbody>"]
    for sym in symbols:
        rows.append(
            "<tr>"
            f"<td><b>{html.escape(sym)}</b></td>"
            "<td>"
            f"<form method='post' action='/markets/watchlist/{html.escape(sym)}/remove' "
            "onsubmit=\"return confirm('Remove symbol from watchlist?');\">"
            "<button class='btn danger' type='submit'>Remove</button>"
            "</form>"
            "</td>"
            "</tr>"
        )
    rows.append("</tbody></table>")
    return "".join(rows)


def _render_indicator_rules_html() -> str:
    rules = _list_indicator_rules(enabled_only=False)
    if not rules:
        return "<div class='small'>No indicator rules yet.</div>"
    out = [
        "<table><thead><tr><th>Name</th><th>Type</th><th>Timeframe</th><th>Config</th><th>State</th><th></th></tr></thead><tbody>"
    ]
    for r in rules:
        rid = int(r["id"])
        kind_raw = str(r["kind"])
        kind = "heikin_ashi" if kind_raw.strip().lower() == "ha" else kind_raw
        if str(kind).strip().lower() in ("bollinger", "bollinger_bands"):
            kind = "bb"
        if str(kind).strip().lower() in ("ichimoku", "ichimoku_cloud", "ichi"):
            kind = "ichimoku"
        if str(kind).strip().lower() in ("roc", "rate_of_change"):
            kind = "roc"
        if str(kind).strip().lower() in ("ttm", "ttm_squeeze", "squeeze_momentum"):
            kind = "ttm"
        if str(kind).strip().lower() in ("sar", "psar", "parabolic_sar", "parabolic"):
            kind = "sar"
        if str(kind).strip().lower() in ("donchian", "donchian_breakout", "donchian_channel", "donchian_channels"):
            kind = "donchian"
        if str(kind).strip().lower() in ("supertrend", "supertrend_trend"):
            kind = "supertrend"
        if str(kind).strip().lower() in ("vwap", "vwap_filter"):
            kind = "vwap"
        if str(kind).strip().lower() in ("relative_volume", "rvol", "rel_volume"):
            kind = "relative_volume"
        params = r.get("params") if isinstance(r.get("params"), dict) else {}
        timeframe = _rule_timeframe(r, "")
        tf_html = f"<span class='badge'>TF {html.escape(timeframe)}</span>" if timeframe else ""
        cfg = ""
        if kind == "ma":
            if _normalize_ma_mode(params.get("mode"), default="single") == "ribbon":
                parts: list[str] = []
                for level in _ma_ribbon_levels_from_params(params):
                    parts.append(
                        f"{str(level['label']).lower()}={str(level['ma_type']).upper()}{int(level['length'])} "
                        f"above->{str(level['above_action'])} below->{str(level['below_action'])}"
                    )
                cfg = "mode=ribbon " + " | ".join(parts) + " | agreement=all-or-hold"
            else:
                ma_type = _normalize_ma_type(params.get("ma_type"), default="sma")
                ln = int(_to_int_opt(params.get("length")) or 30)
                cfg = (
                    f"L={ln} buy={_normalize_relation_mode(params.get('buy_relation'), default='hold')} "
                    f"sell={_normalize_relation_mode(params.get('sell_relation'), default='hold')} "
                    f"type={ma_type.upper()} dMA={'on' if int(params.get('track_derivative') or 0) else 'off'}"
                )
                if int(params.get("track_derivative") or 0):
                    cfg += (
                        f" dBuy>={_fmt_market_num(params.get('buy_derivative_min'),4)}"
                        f" dSell<={_fmt_market_num(params.get('sell_derivative_max'),4)}"
                    )
                if int(params.get("unless_enabled") or 0):
                    utype = _normalize_ma_type(params.get("unless_type"), default="sma").upper()
                    ulen = int(_to_int_opt(params.get("unless_length")) or 30)
                    urel = str(params.get("unless_relation") or "above")
                    uact = str(params.get("unless_action") or "sell")
                    cfg += f" unless self {urel} {utype}{ulen}->{uact}"
        elif kind == "ema":
            ln = int(_to_int_opt(params.get("length")) or 30)
            cfg = (
                f"L={ln} buy={_normalize_relation_mode(params.get('buy_relation'), default='hold')} "
                f"sell={_normalize_relation_mode(params.get('sell_relation'), default='hold')} "
                f"dEMA={'on' if int(params.get('track_derivative') or 0) else 'off'}"
            )
            if int(params.get("track_derivative") or 0):
                cfg += (
                    f" dBuy>={_fmt_market_num(params.get('buy_derivative_min'),4)}"
                    f" dSell<={_fmt_market_num(params.get('sell_derivative_max'),4)}"
                )
        elif kind == "macd":
            fast = int(_to_int_opt(params.get("fast_length")) or 12)
            slow = int(_to_int_opt(params.get("slow_length")) or 26)
            sig = int(_to_int_opt(params.get("signal_length")) or 9)
            mode = str(params.get("mode") or "signal_cross")
            cfg = f"mode={mode} ({fast}/{slow}/{sig})"
            if str(mode or "").strip().lower() == "macd_derivative_sign":
                buy_above_raw = _to_float_opt(params.get("derivative_buy_above"))
                sell_below_raw = _to_float_opt(params.get("derivative_sell_below"))
                d_scope = _normalize_dual_signal_scope(params.get("derivative_signal_scope"), default="both")
                buy_above = float(buy_above_raw) if buy_above_raw is not None else 0.0
                sell_below = float(sell_below_raw) if sell_below_raw is not None else 0.0
                cfg += (
                    f" dBuy>{_fmt_market_num(buy_above,4)}"
                    f" dSell<{_fmt_market_num(sell_below,4)}"
                    f" dSide={d_scope}"
                )
            if int(params.get("signal_override_enabled") or 0):
                override_targets = _normalize_rule_target_ids(params.get("signal_override_targets"))
                override_scope = _rule_override_scope_label(params.get("signal_override_scope"))
                cfg += f" override={override_scope} targets={len(override_targets)}"
        elif kind == "rsi":
            os_rel = str(params.get("oversold_relation") or "below")
            ob_rel = str(params.get("overbought_relation") or "above")
            os_action = _normalize_signal_action_mode(params.get("oversold_action"), default="buy")
            ob_action = _normalize_signal_action_mode(params.get("overbought_action"), default="sell")
            cfg = (
                f"oversold={_fmt_market_num(params.get('oversold'),2)} {os_rel}->{os_action} "
                f"overbought={_fmt_market_num(params.get('overbought'),2)} {ob_rel}->{ob_action}"
            )
            if int(params.get("signal_override_enabled") or 0):
                override_targets = _normalize_rule_target_ids(params.get("signal_override_targets"))
                override_scope = _rule_override_scope_label(params.get("signal_override_scope"))
                cfg += f" override={override_scope} targets={len(override_targets)}"
        elif kind == "rsi_d":
            cfg = f"buy>={_fmt_market_num(params.get('buy_above'),4)} sell<={_fmt_market_num(params.get('sell_below'),4)}"
        elif kind == "roc":
            length = max(1, int(_to_int_opt(params.get("length")) or 12))
            buy_cond = _normalize_roc_condition(params.get("buy_condition"), default="hold")
            sell_cond = _normalize_roc_condition(params.get("sell_condition"), default="hold")
            buy_thr = float(_to_float_opt(params.get("buy_threshold_pct")) or 0.0)
            sell_thr = float(_to_float_opt(params.get("sell_threshold_pct")) or 0.0)
            cfg = (
                f"len={length} buy={buy_cond} sell={sell_cond} "
                f"buy_thr={_fmt_market_num(buy_thr,3)}% sell_thr={_fmt_market_num(sell_thr,3)}%"
            )
        elif kind == "heikin_ashi":
            mode = str(params.get("mode") or "transition").strip().lower()
            if mode not in ("transition", "state"):
                mode = "transition"
            doji_tol_raw = _to_float_opt(params.get("doji_tolerance_pct"))
            if doji_tol_raw is None:
                cfg = f"mode={mode} doji_tol=off"
            else:
                doji_tol = max(0.0, float(doji_tol_raw))
                cfg = f"mode={mode} doji_tol={_fmt_market_num(doji_tol,3)}%"
            if int(params.get("signal_override_enabled") or 0):
                override_targets = _normalize_rule_target_ids(params.get("signal_override_targets"))
                override_scope = _rule_override_scope_label(params.get("signal_override_scope"))
                cfg += f" override={override_scope} targets={len(override_targets)}"
        elif kind == "bb":
            length = max(2, int(_to_int_opt(params.get("length")) or 20))
            std_mult = max(0.1, float(_to_float_opt(params.get("std_mult")) or 2.0))
            buy_cond = _normalize_bb_condition(params.get("buy_condition"), default="hold")
            sell_cond = _normalize_bb_condition(params.get("sell_condition"), default="hold")
            squeeze = float(_to_float_opt(params.get("squeeze_threshold_pct")) or 5.0)
            pb_buy = float(_to_float_opt(params.get("percent_b_buy_threshold")) or 0.2)
            pb_sell = float(_to_float_opt(params.get("percent_b_sell_threshold")) or 0.8)
            cfg = (
                f"len={length} std={_fmt_market_num(std_mult,2)} "
                f"buy={buy_cond} sell={sell_cond} "
                f"sq<={_fmt_market_num(squeeze,3)}% "
                f"pb_buy={_fmt_market_num(pb_buy,3)} pb_sell={_fmt_market_num(pb_sell,3)}"
            )
        elif kind == "ichimoku":
            conversion, base, leading_b, displacement = _ichimoku_lengths_from_params(params)
            delayed_cross = max(1, int(_to_int_opt(params.get("delayed_cross_lookback")) or 3))
            buy_mode = _normalize_ichi_match_mode(params.get("buy_match_mode"), default="all")
            sell_mode = _normalize_ichi_match_mode(params.get("sell_match_mode"), default="all")
            block_mode = _normalize_ichi_match_mode(params.get("block_match_mode"), default="all")
            buy_conds = _normalize_ichi_conditions(params.get("buy_conditions", params.get("buy_condition")), default="hold")
            sell_conds = _normalize_ichi_conditions(params.get("sell_conditions", params.get("sell_condition")), default="hold")
            block_conds = _normalize_ichi_conditions(params.get("block_conditions", params.get("block_condition")), default="hold")
            buy_active = [c for c in buy_conds if c != "hold"]
            sell_active = [c for c in sell_conds if c != "hold"]
            block_active = [c for c in block_conds if c != "hold"]
            buy_txt = "hold" if not buy_active else "+".join(_ICHI_CONDITION_LABELS.get(c, c) for c in buy_active)
            sell_txt = "hold" if not sell_active else "+".join(_ICHI_CONDITION_LABELS.get(c, c) for c in sell_active)
            block_txt = "hold" if not block_active else "+".join(_ICHI_CONDITION_LABELS.get(c, c) for c in block_active)
            thickness = float(_to_float_opt(params.get("cloud_thickness_threshold_pct")) or 1.0)
            bounce_tol = _ichimoku_base_bounce_tolerance_pct(params)
            cfg = (
                f"conversion/base/leadingB={conversion}/{base}/{leading_b} disp={displacement} "
                f"buy({buy_mode})={buy_txt} sell({sell_mode})={sell_txt} block({block_mode})={block_txt} "
                f"thick>={_fmt_market_num(thickness,3)}% "
                f"base_tol={_fmt_market_num(bounce_tol,3)}% "
                f"delay={delayed_cross}"
            )
        elif kind == "ttm":
            bb_len = max(2, int(_to_int_opt(params.get("bb_length")) or 20))
            bb_mult = max(0.1, float(_to_float_opt(params.get("bb_mult")) or 2.0))
            kc_len = max(2, int(_to_int_opt(params.get("kc_length")) or 20))
            kc_mult = max(0.1, float(_to_float_opt(params.get("kc_mult")) or 1.5))
            mom_len = max(2, int(_to_int_opt(params.get("momentum_length")) or 20))
            buy_cond = _normalize_ttm_condition(params.get("buy_condition"), default="hold")
            sell_cond = _normalize_ttm_condition(params.get("sell_condition"), default="hold")
            cfg = (
                f"BB={bb_len}/{_fmt_market_num(bb_mult,2)} "
                f"KC={kc_len}/{_fmt_market_num(kc_mult,2)} "
                f"MOM={mom_len} buy={buy_cond} sell={sell_cond}"
            )
        elif kind == "sar":
            step = max(0.0001, float(_to_float_opt(params.get("step")) or 0.02))
            max_step = max(step, float(_to_float_opt(params.get("max_step")) or 0.2))
            buy_cond = _normalize_sar_condition(params.get("buy_condition"), default="hold")
            sell_cond = _normalize_sar_condition(params.get("sell_condition"), default="hold")
            cfg = (
                f"step={_fmt_market_num(step,4)} max={_fmt_market_num(max_step,4)} "
                f"buy={buy_cond} sell={sell_cond}"
            )
        elif kind == "donchian":
            lookback = max(1, int(_to_int_opt(params.get("lookback")) or 20))
            default_buy = "high_above_upper" if bool(params.get("use_high_break")) else "close_above_upper"
            buy_cond = _normalize_donchian_condition(params.get("buy_condition"), default=default_buy)
            sell_cond = _normalize_donchian_condition(params.get("sell_condition"), default="close_below_lower")
            cfg = f"lookback={lookback} buy={buy_cond} sell={sell_cond}"
        elif kind == "supertrend":
            atr_length = max(1, int(_to_int_opt(params.get("atr_length")) or 10))
            multiplier = max(0.1, float(_to_float_opt(params.get("multiplier")) or 3.0))
            buy_cond = _normalize_supertrend_condition(params.get("buy_condition"), default="trend_up")
            sell_cond = _normalize_supertrend_condition(params.get("sell_condition"), default="trend_down")
            cfg = (
                f"ST({atr_length},{_fmt_market_num(multiplier,3)}) | "
                f"Buy: {_SUPERTREND_CONDITION_LABELS.get(buy_cond, buy_cond)} | "
                f"Sell: {_SUPERTREND_CONDITION_LABELS.get(sell_cond, sell_cond)}"
            )
        elif kind == "vwap":
            buy_cond = _normalize_vwap_condition(params.get("buy_condition"), default="within_band")
            sell_cond = _normalize_vwap_condition(params.get("sell_condition"), default="exit_below")
            max_extension = _indicator_pct_decimal(params.get("max_extension_pct"), default=0.015)
            max_pullback = _indicator_pct_decimal(params.get("max_pullback_pct"), default=0.010)
            exit_below = _indicator_pct_decimal(params.get("exit_below_pct"), default=0.012)
            cfg = (
                f"buy={buy_cond} sell={sell_cond} "
                f"ext={_fmt_market_num(max_extension * 100.0,3)}% "
                f"pull={_fmt_market_num(max_pullback * 100.0,3)}% "
                f"exit={_fmt_market_num(exit_below * 100.0,3)}%"
            )
        elif kind == "relative_volume":
            length = max(1, int(_to_int_opt(params.get("length")) or 20))
            threshold = max(0.0, float(_to_float_opt(params.get("threshold")) or 1.2))
            buy_cond = _normalize_relative_volume_condition(params.get("buy_condition"), default="above_threshold")
            sell_cond = _normalize_relative_volume_condition(params.get("sell_condition"), default="below_threshold")
            cfg = f"len={length} threshold={_fmt_market_num(threshold,3)} buy={buy_cond} sell={sell_cond}"
        enabled = bool(r.get("enabled"))
        st = "<span class='badge ok'>enabled</span>" if enabled else "<span class='badge warn'>disabled</span>"
        type_txt = html.escape(kind.upper())
        if kind == "ma":
            if _normalize_ma_mode(params.get("mode"), default="single") == "ribbon":
                type_txt = "MA RIBBON"
            else:
                type_txt = html.escape(_normalize_ma_type(params.get("ma_type"), default="sma").upper())
        elif kind == "ema":
            type_txt = "EMA"
        elif kind == "heikin_ashi":
            type_txt = "HEIKIN ASHI"
        elif kind == "bb":
            type_txt = "BOLLINGER"
        elif kind == "ichimoku":
            type_txt = "ICHIMOKU"
        elif kind == "roc":
            type_txt = "ROC"
        elif kind == "ttm":
            type_txt = "TTM"
        elif kind == "sar":
            type_txt = "SAR"
        elif kind == "donchian":
            type_txt = "DONCHIAN"
        elif kind == "supertrend":
            type_txt = "SUPERTREND"
        elif kind == "vwap":
            type_txt = "VWAP"
        elif kind == "relative_volume":
            type_txt = "RELATIVE VOLUME"
        out.append(
            "<tr>"
            f"<td><b>{html.escape(str(r.get('name') or ''))}</b></td>"
            f"<td>{type_txt}</td>"
            f"<td>{tf_html}</td>"
            f"<td class='small'>{html.escape(cfg)}</td>"
            f"<td>{st}</td>"
            "<td>"
            f"<form method='post' action='/markets/rules/{rid}/toggle' style='display:inline-block;margin-right:6px;'>"
            f"<button class='btn' type='submit'>{'Disable' if enabled else 'Enable'}</button>"
            "</form>"
            f"<form method='post' action='/markets/rules/{rid}/delete' style='display:inline-block;' "
            "onsubmit=\"return confirm('Delete this indicator rule?');\">"
            "<button class='btn danger' type='submit'>Delete</button>"
            "</form>"
            "</td>"
            "</tr>"
        )
    out.append("</tbody></table>")
    return "".join(out)


def _markets_optimizer_fmt_money(value: Any) -> str:
    fv = _to_float_opt(value)
    if fv is None:
        return "—"
    amt = float(fv)
    sign = "-" if amt < 0 else ""
    return f"{sign}${abs(amt):,.2f}"


def _markets_optimizer_default_config(symbols_csv: str = "") -> dict[str, Any]:
    return {
        "symbols": str(symbols_csv or "").strip(),
        "timeframe": "1h",
        "lookback_candles": 2400,
        "population_size": 128,
        "elite_count": 12,
        "immigrant_count": 20,
        "max_generations": 200,
        "stagnation_patience": 18,
        "max_rules": 4,
        "top_k_per_generation": 10,
    }


def _markets_optimizer_clean_symbols(raw: Any) -> list[str]:
    return _clean_symbol_list(raw)


def _markets_optimizer_sanitize_config(raw: dict[str, Any]) -> dict[str, Any]:
    defaults = _markets_optimizer_default_config(str(raw.get("symbols") or ""))
    cfg = dict(defaults)
    cfg.update(raw or {})
    symbols = _markets_optimizer_clean_symbols(cfg.get("symbols"))
    cfg["symbols"] = ", ".join(symbols)
    timeframe = str(cfg.get("timeframe") or "1h").strip().lower()
    if timeframe not in ("5m", "10m", "30m", "1h", "1d"):
        timeframe = "1h"
    cfg["timeframe"] = timeframe
    population_size = max(16, min(int(_to_int_opt(cfg.get("population_size")) or 128), 512))
    max_rules = max(1, min(int(_to_int_opt(cfg.get("max_rules")) or 4), 8))
    elite_count = max(1, min(int(_to_int_opt(cfg.get("elite_count")) or 12), max(1, population_size // 2)))
    immigrant_count = max(0, min(int(_to_int_opt(cfg.get("immigrant_count")) or 20), max(0, population_size // 2)))
    if elite_count + immigrant_count >= population_size:
        immigrant_count = max(0, population_size - elite_count - 1)
    cfg["population_size"] = population_size
    cfg["elite_count"] = elite_count
    cfg["immigrant_count"] = immigrant_count
    cfg["max_generations"] = max(1, min(int(_to_int_opt(cfg.get("max_generations")) or 200), 5000))
    cfg["stagnation_patience"] = max(1, min(int(_to_int_opt(cfg.get("stagnation_patience")) or 18), 500))
    cfg["lookback_candles"] = max(120, min(int(_to_int_opt(cfg.get("lookback_candles")) or 2400), 5000))
    cfg["max_rules"] = max_rules
    cfg["top_k_per_generation"] = max(3, min(int(_to_int_opt(cfg.get("top_k_per_generation")) or 10), 25))
    return cfg


def _markets_optimizer_fetch_run(run_id: int) -> Optional[dict[str, Any]]:
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, status, config_json, summary_json, best_rules_json, generation,
               best_realized_gain_total, best_avg_trade_profit, best_sell_executions,
               stop_requested, created_ts, started_ts, updated_ts, ended_ts
        FROM markets_optimizer_runs
        WHERE id=?
        """,
        (int(run_id),),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return {
        "id": int(row["id"]),
        "status": str(row["status"] or ""),
        "config": _markets_optimizer_sanitize_config(_safe_json(str(row["config_json"] or "{}"), default={})),
        "summary": _safe_json(str(row["summary_json"] or "{}"), default={}),
        "best_rules": _normalize_indicator_rules_payload(str(row["best_rules_json"] or "[]")),
        "generation": int(row["generation"] or 0),
        "best_realized_gain_total": float(row["best_realized_gain_total"] or 0.0),
        "best_avg_trade_profit": float(row["best_avg_trade_profit"] or 0.0),
        "best_sell_executions": int(row["best_sell_executions"] or 0),
        "stop_requested": bool(int(row["stop_requested"] or 0)),
        "created_ts": int(row["created_ts"] or 0),
        "started_ts": _to_int_opt(row["started_ts"]),
        "updated_ts": int(row["updated_ts"] or 0),
        "ended_ts": _to_int_opt(row["ended_ts"]),
    }


def _markets_optimizer_latest_run() -> Optional[dict[str, Any]]:
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM markets_optimizer_runs ORDER BY id DESC LIMIT 1")
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return _markets_optimizer_fetch_run(int(row["id"]))


def _markets_optimizer_latest_active_run() -> Optional[dict[str, Any]]:
    conn = db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM markets_optimizer_runs WHERE status IN ('queued','running','stopping') ORDER BY id DESC LIMIT 1"
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return _markets_optimizer_fetch_run(int(row["id"]))


def _markets_optimizer_update_run(
    run_id: int,
    *,
    status: Optional[str] = None,
    config: Optional[dict[str, Any]] = None,
    summary: Optional[dict[str, Any]] = None,
    best_rules: Optional[list[dict[str, Any]]] = None,
    generation: Optional[int] = None,
    best_realized_gain_total: Optional[float] = None,
    best_avg_trade_profit: Optional[float] = None,
    best_sell_executions: Optional[int] = None,
    stop_requested: Optional[bool] = None,
    started_ts: Optional[int] = None,
    ended_ts: Optional[int] = None,
) -> None:
    conn = db()
    cur = conn.cursor()
    sets: list[str] = ["updated_ts=?"]
    vals: list[Any] = [_utc_ts()]
    if status is not None:
        sets.append("status=?")
        vals.append(str(status))
    if config is not None:
        sets.append("config_json=?")
        vals.append(json.dumps(_markets_optimizer_sanitize_config(config)))
    if summary is not None:
        sets.append("summary_json=?")
        vals.append(json.dumps(summary))
    if best_rules is not None:
        sets.append("best_rules_json=?")
        vals.append(json.dumps(best_rules))
    if generation is not None:
        sets.append("generation=?")
        vals.append(int(generation))
    if best_realized_gain_total is not None:
        sets.append("best_realized_gain_total=?")
        vals.append(float(best_realized_gain_total))
    if best_avg_trade_profit is not None:
        sets.append("best_avg_trade_profit=?")
        vals.append(float(best_avg_trade_profit))
    if best_sell_executions is not None:
        sets.append("best_sell_executions=?")
        vals.append(int(best_sell_executions))
    if stop_requested is not None:
        sets.append("stop_requested=?")
        vals.append(1 if stop_requested else 0)
    if started_ts is not None:
        sets.append("started_ts=?")
        vals.append(int(started_ts))
    if ended_ts is not None:
        sets.append("ended_ts=?")
        vals.append(int(ended_ts))
    vals.append(int(run_id))
    cur.execute(f"UPDATE markets_optimizer_runs SET {', '.join(sets)} WHERE id=?", tuple(vals))
    conn.commit()
    conn.close()


def _markets_optimizer_create_run(config: dict[str, Any]) -> int:
    cfg = _markets_optimizer_sanitize_config(config)
    now = _utc_ts()
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO markets_optimizer_runs
        (status, config_json, summary_json, best_rules_json, generation,
         best_realized_gain_total, best_avg_trade_profit, best_sell_executions,
         stop_requested, created_ts, started_ts, updated_ts, ended_ts)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            "queued",
            json.dumps(cfg),
            json.dumps({"phase": "queued", "message": "Queued for optimization"}),
            "[]",
            0,
            0.0,
            0.0,
            0,
            0,
            now,
            None,
            now,
            None,
        ),
    )
    run_id = int(cur.lastrowid)
    conn.commit()
    conn.close()
    return run_id


def _markets_optimizer_store_generation_candidates(
    run_id: int,
    generation: int,
    results: list[dict[str, Any]],
    *,
    limit: int,
) -> None:
    conn = db()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM markets_optimizer_candidates WHERE run_id=? AND generation=?",
        (int(run_id), int(generation)),
    )
    now = _utc_ts()
    for idx, result in enumerate(results[: max(1, int(limit))], start=1):
        cur.execute(
            """
            INSERT INTO markets_optimizer_candidates
            (run_id, generation, rank_idx, realized_gain_total, avg_trade_profit,
             sell_executions, open_units, stats_json, rules_json, created_ts)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                int(run_id),
                int(generation),
                int(idx),
                float(result.get("realized_gain_total") or 0.0),
                float(result.get("avg_trade_profit") or 0.0),
                int(result.get("sell_executions") or 0),
                int(result.get("open_units") or 0),
                json.dumps(result.get("stats") or {}),
                json.dumps(result.get("rules") or []),
                now,
            ),
        )
    conn.commit()
    conn.close()


def _markets_optimizer_list_generation_candidates(
    run_id: int,
    generation: int,
    *,
    limit: int = 10,
) -> list[dict[str, Any]]:
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT rank_idx, realized_gain_total, avg_trade_profit, sell_executions,
               open_units, stats_json, rules_json
        FROM markets_optimizer_candidates
        WHERE run_id=? AND generation=?
        ORDER BY rank_idx ASC
        LIMIT ?
        """,
        (int(run_id), int(generation), max(1, int(limit))),
    )
    rows = cur.fetchall()
    conn.close()
    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "rank_idx": int(row["rank_idx"] or 0),
                "realized_gain_total": float(row["realized_gain_total"] or 0.0),
                "avg_trade_profit": float(row["avg_trade_profit"] or 0.0),
                "sell_executions": int(row["sell_executions"] or 0),
                "open_units": int(row["open_units"] or 0),
                "stats": _safe_json(str(row["stats_json"] or "{}"), default={}),
                "rules": _normalize_indicator_rules_payload(str(row["rules_json"] or "[]")),
            }
        )
    return out


def _markets_optimizer_mark_stop_requested(run_id: int) -> None:
    _markets_optimizer_update_run(run_id, stop_requested=True, status="stopping")


def _markets_optimizer_cleanup_orphan_run() -> None:
    latest = _markets_optimizer_latest_active_run()
    if latest is None:
        return
    with MARKETS_OPTIMIZER_LOCK:
        thread_alive = bool(MARKETS_OPTIMIZER_THREAD and MARKETS_OPTIMIZER_THREAD.is_alive())
    if thread_alive:
        return
    _markets_optimizer_update_run(
        int(latest["id"]),
        status="stopped",
        summary={"phase": "stopped", "message": "Stopped because optimizer worker was no longer active."},
        ended_ts=_utc_ts(),
    )


_MARKETS_OPTIMIZER_RULE_KINDS = (
    "ma",
    "rsi",
    "macd",
    "rsi_d",
    "heikin_ashi",
    "bb",
    "ichimoku",
    "ttm",
    "roc",
    "sar",
)
_MARKETS_OPTIMIZER_BB_CONDITIONS = (
    "touch_upper",
    "touch_lower",
    "close_outside_upper",
    "close_outside_lower",
    "reenter_from_above",
    "reenter_from_below",
    "width_increasing",
    "width_decreasing",
    "width_below_threshold",
    "width_expanding_from_squeeze",
    "above_middle",
    "below_middle",
    "percent_b_above",
    "percent_b_below",
    "hold",
)
_MARKETS_OPTIMIZER_ROC_BUY_CONDITIONS = (
    "momentum_long",
    "roc_cross_up_zero",
    "roc_above_threshold",
    "roc_cross_up_threshold",
    "roc_increasing",
    "roc_positive",
    "hold",
)
_MARKETS_OPTIMIZER_ROC_SELL_CONDITIONS = (
    "momentum_short",
    "roc_cross_down_zero",
    "roc_below_threshold",
    "roc_cross_down_threshold",
    "roc_decreasing",
    "roc_negative",
    "hold",
)
_MARKETS_OPTIMIZER_TTM_CONDITIONS = (
    "long_release",
    "short_release",
    "long_trend",
    "short_trend",
    "squeeze_fired",
    "squeeze_on",
    "squeeze_off",
    "momentum_above_zero",
    "momentum_below_zero",
    "momentum_increasing",
    "momentum_decreasing",
    "momentum_cross_up",
    "momentum_cross_down",
    "hold",
)
_MARKETS_OPTIMIZER_SAR_BUY_CONDITIONS = (
    "sar_cross_up",
    "price_above_sar",
    "sar_rising",
    "trend_long",
    "hold",
)
_MARKETS_OPTIMIZER_SAR_SELL_CONDITIONS = (
    "sar_cross_down",
    "price_below_sar",
    "sar_falling",
    "trend_short",
    "hold",
)
_MARKETS_OPTIMIZER_ICHI_BUY_CONDITIONS = (
    "strong_long_confirm",
    "full_bullish_stack",
    "partial_bullish_stack",
    "price_above_cloud",
    "price_inside_cloud",
    "cloud_bullish",
    "tenkan_cross_above",
    "tenkan_above_kijun",
    "future_twist_bullish",
    "approaching_future_twist_bullish",
    "delayed_bullish_cross_valid",
    "bullish_cross_strong_above_cloud",
    "bullish_cross_medium_at_cloud",
    "bullish_cross_weak_below_cloud",
    "chikou_above_price",
    "chikou_clears_past_cloud_bullish",
    "cloud_breakout_bullish",
    "bullish_breakout_retest_hold",
    "cloud_exit_up_with_momentum",
    "kijun_bounce_bullish",
    "kijun_flat",
    "kijun_rising",
    "tenkan_accelerating_up",
    "cloud_rejection_bullish",
    "bullish_to_neutral_transition",
    "cloud_expanding",
    "cloud_contracting",
    "cloud_thickness_above",
    "cloud_thickness_below",
    "hold",
)
_MARKETS_OPTIMIZER_ICHI_SELL_CONDITIONS = (
    "strong_short_confirm",
    "full_bearish_stack",
    "partial_bearish_stack",
    "price_below_cloud",
    "price_inside_cloud",
    "cloud_bearish",
    "tenkan_cross_below",
    "tenkan_below_kijun",
    "future_twist_bearish",
    "approaching_future_twist_bearish",
    "delayed_bearish_cross_valid",
    "bearish_cross_strong_below_cloud",
    "bearish_cross_medium_at_cloud",
    "bearish_cross_weak_above_cloud",
    "chikou_below_price",
    "chikou_clears_past_cloud_bearish",
    "cloud_breakout_bearish",
    "bearish_breakdown_retest_fail",
    "cloud_exit_down_with_momentum",
    "kijun_reject_bearish",
    "kijun_flat",
    "kijun_falling",
    "tenkan_accelerating_down",
    "cloud_rejection_bearish",
    "bearish_to_neutral_transition",
    "cloud_expanding",
    "cloud_contracting",
    "weak_cross_inside_cloud",
    "cloud_thickness_above",
    "cloud_thickness_below",
    "hold",
)
_MARKETS_OPTIMIZER_ICHI_BLOCK_CONDITIONS = (
    "hold",
    "price_inside_cloud",
    "deep_inside_cloud",
    "cloud_thickness_below",
    "weak_cross_inside_cloud",
    "chikou_in_congestion_zone",
    "cloud_contracting",
)


def _markets_optimizer_clone_rules(rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return copy.deepcopy(rules)


def _markets_optimizer_random_float(min_val: float, max_val: float, digits: int = 2) -> float:
    return round(random.uniform(float(min_val), float(max_val)), int(digits))


def _markets_optimizer_random_choice(values: tuple[str, ...]) -> str:
    return str(random.choice(list(values)))


def _markets_optimizer_rule_name(kind: str, params: dict[str, Any]) -> str:
    kd = str(kind or "").strip().lower()
    if kd == "ma":
        if _normalize_ma_mode(params.get("mode"), default="single") == "ribbon":
            return "MA Ribbon"
        ma_type = _normalize_ma_type(params.get("ma_type"), default="sma").upper()
        ln = int(_to_int_opt(params.get("length")) or 30)
        return f"{ma_type}{ln}"
    if kd == "rsi":
        return f"RSI {int(_to_float_opt(params.get('oversold')) or 30)}/{int(_to_float_opt(params.get('overbought')) or 70)}"
    if kd == "macd":
        return "MACD"
    if kd == "rsi_d":
        return "dRSI"
    if kd == "heikin_ashi":
        return "Heikin Ashi"
    if kd == "bb":
        return "Bollinger"
    if kd == "ichimoku":
        return "Ichimoku"
    if kd == "ttm":
        return "TTM Squeeze"
    if kd == "roc":
        return "ROC"
    if kd == "sar":
        return "Parabolic SAR"
    return kd.upper()


def _markets_optimizer_random_rule(kind: Optional[str] = None) -> dict[str, Any]:
    kd = str(kind or random.choice(list(_MARKETS_OPTIMIZER_RULE_KINDS))).strip().lower()
    params: dict[str, Any] = {}
    if kd == "ma":
        if random.random() < 0.22:
            params = {
                "mode": "ribbon",
                "levels": [
                    {
                        "slot": "short",
                        "label": "Short",
                        "ma_type": random.choice(["sma", "ema"]),
                        "length": random.randint(5, 45),
                        "above_action": random.choice(["buy", "sell", "hold"]),
                        "below_action": random.choice(["buy", "sell", "hold"]),
                    },
                    {
                        "slot": "medium",
                        "label": "Medium",
                        "ma_type": random.choice(["sma", "ema"]),
                        "length": random.randint(30, 110),
                        "above_action": random.choice(["buy", "sell", "hold"]),
                        "below_action": random.choice(["buy", "sell", "hold"]),
                    },
                    {
                        "slot": "long",
                        "label": "Long",
                        "ma_type": random.choice(["sma", "ema"]),
                        "length": random.randint(90, 220),
                        "above_action": random.choice(["buy", "sell", "hold"]),
                        "below_action": random.choice(["buy", "sell", "hold"]),
                    },
                ],
            }
        else:
            track_d = random.random() < 0.35
            unless_enabled = random.random() < 0.2
            params = {
                "mode": "single",
                "length": random.randint(4, 220),
                "ma_type": random.choice(["sma", "ema"]),
                "buy_relation": random.choice(["above", "below", "hold"]),
                "sell_relation": random.choice(["above", "below", "hold"]),
                "track_derivative": 1 if track_d else 0,
                "buy_derivative_min": _markets_optimizer_random_float(-2.0, 2.0, 4) if track_d and random.random() < 0.7 else None,
                "sell_derivative_max": _markets_optimizer_random_float(-2.0, 2.0, 4) if track_d and random.random() < 0.7 else None,
                "unless_enabled": 1 if unless_enabled else 0,
                "unless_relation": random.choice(["above", "below"]),
                "unless_type": random.choice(["sma", "ema"]),
                "unless_length": random.randint(10, 220),
                "unless_action": random.choice(["buy", "sell"]),
            }
    elif kd == "rsi":
        oversold = _markets_optimizer_random_float(15.0, 45.0, 1)
        overbought = _markets_optimizer_random_float(max(oversold + 8.0, 55.0), 85.0, 1)
        params = {
            "oversold": oversold,
            "overbought": overbought,
            "oversold_relation": random.choice(["below", "above"]),
            "oversold_action": random.choice(["buy", "sell", "hold"]),
            "overbought_relation": random.choice(["above", "below"]),
            "overbought_action": random.choice(["sell", "buy", "hold"]),
            "signal_override_enabled": 0,
            "signal_override_scope": "both",
            "signal_override_targets": [],
        }
    elif kd == "macd":
        mode = random.choice(
            ["signal_cross", "cross_regime", "hist_momentum", "zero_reclaim_loss", "macd_derivative_sign"]
        )
        params = {
            "mode": mode,
            "fast_length": random.randint(4, 18),
            "slow_length": random.randint(16, 42),
            "signal_length": random.randint(3, 15),
            "derivative_buy_above": _markets_optimizer_random_float(-1.0, 1.0, 4),
            "derivative_sell_below": _markets_optimizer_random_float(-1.0, 1.0, 4),
            "derivative_signal_scope": random.choice(["both", "buy", "sell"]),
            "signal_override_enabled": 0,
            "signal_override_scope": "both",
            "signal_override_targets": [],
        }
        if int(params["slow_length"]) <= int(params["fast_length"]):
            params["slow_length"] = int(params["fast_length"]) + random.randint(4, 20)
    elif kd == "rsi_d":
        params = {
            "buy_above": _markets_optimizer_random_float(-5.0, 5.0, 4),
            "sell_below": _markets_optimizer_random_float(-5.0, 5.0, 4),
        }
    elif kd == "heikin_ashi":
        use_tol = random.random() < 0.4
        params = {
            "mode": random.choice(["transition", "state"]),
            "signal_override_enabled": 0,
            "signal_override_scope": "both",
            "signal_override_targets": [],
        }
        if use_tol:
            params["doji_tolerance_pct"] = _markets_optimizer_random_float(0.0, 0.25, 3)
    elif kd == "bb":
        params = {
            "length": random.randint(8, 40),
            "std_mult": _markets_optimizer_random_float(1.0, 3.2, 2),
            "buy_condition": _markets_optimizer_random_choice(_MARKETS_OPTIMIZER_BB_CONDITIONS),
            "sell_condition": _markets_optimizer_random_choice(_MARKETS_OPTIMIZER_BB_CONDITIONS),
            "squeeze_threshold_pct": _markets_optimizer_random_float(1.0, 12.0, 2),
            "percent_b_buy_threshold": _markets_optimizer_random_float(0.05, 0.55, 3),
            "percent_b_sell_threshold": _markets_optimizer_random_float(0.45, 0.95, 3),
        }
    elif kd == "ichimoku":
        buy_conds = random.sample(list(_MARKETS_OPTIMIZER_ICHI_BUY_CONDITIONS[:-1]), k=random.randint(1, 3))
        sell_conds = random.sample(list(_MARKETS_OPTIMIZER_ICHI_SELL_CONDITIONS[:-1]), k=random.randint(1, 3))
        block_conds = random.sample(list(_MARKETS_OPTIMIZER_ICHI_BLOCK_CONDITIONS), k=random.randint(1, 2))
        params = {
            "tenkan_length": random.randint(5, 15),
            "kijun_length": random.randint(18, 40),
            "senkou_b_length": random.randint(40, 80),
            "displacement": random.randint(15, 40),
            "buy_conditions": buy_conds,
            "sell_conditions": sell_conds,
            "block_conditions": block_conds,
            "buy_condition": buy_conds[0] if buy_conds else "hold",
            "sell_condition": sell_conds[0] if sell_conds else "hold",
            "block_condition": block_conds[0] if block_conds else "hold",
            "buy_match_mode": random.choice(["all", "any"]),
            "sell_match_mode": random.choice(["all", "any"]),
            "block_match_mode": random.choice(["all", "any"]),
            "cloud_thickness_threshold_pct": _markets_optimizer_random_float(0.2, 4.0, 2),
            "kijun_bounce_tolerance_pct": _markets_optimizer_random_float(0.1, 1.2, 2),
            "delayed_cross_lookback": random.randint(1, 6),
        }
        if int(params["kijun_length"]) <= int(params["tenkan_length"]):
            params["kijun_length"] = int(params["tenkan_length"]) + random.randint(4, 15)
        if int(params["senkou_b_length"]) <= int(params["kijun_length"]):
            params["senkou_b_length"] = int(params["kijun_length"]) + random.randint(8, 40)
    elif kd == "ttm":
        params = {
            "bb_length": random.randint(8, 36),
            "bb_mult": _markets_optimizer_random_float(1.0, 3.0, 2),
            "kc_length": random.randint(8, 36),
            "kc_mult": _markets_optimizer_random_float(1.0, 2.5, 2),
            "momentum_length": random.randint(8, 36),
            "buy_condition": _markets_optimizer_random_choice(_MARKETS_OPTIMIZER_TTM_CONDITIONS),
            "sell_condition": _markets_optimizer_random_choice(_MARKETS_OPTIMIZER_TTM_CONDITIONS),
        }
    elif kd == "roc":
        params = {
            "length": random.randint(2, 30),
            "buy_condition": _markets_optimizer_random_choice(_MARKETS_OPTIMIZER_ROC_BUY_CONDITIONS),
            "sell_condition": _markets_optimizer_random_choice(_MARKETS_OPTIMIZER_ROC_SELL_CONDITIONS),
            "buy_threshold_pct": _markets_optimizer_random_float(-15.0, 15.0, 2),
            "sell_threshold_pct": _markets_optimizer_random_float(-15.0, 15.0, 2),
        }
    elif kd == "sar":
        step = _markets_optimizer_random_float(0.005, 0.06, 4)
        max_step = _markets_optimizer_random_float(max(step, 0.06), 0.4, 4)
        params = {
            "step": step,
            "max_step": max_step,
            "buy_condition": _markets_optimizer_random_choice(_MARKETS_OPTIMIZER_SAR_BUY_CONDITIONS),
            "sell_condition": _markets_optimizer_random_choice(_MARKETS_OPTIMIZER_SAR_SELL_CONDITIONS),
        }
    else:
        params = {}
    return {
        "name": _markets_optimizer_rule_name(kd, params),
        "kind": kd,
        "params": params,
    }


def _markets_optimizer_mutate_rule(rule: dict[str, Any]) -> dict[str, Any]:
    kd = str(rule.get("kind") or "").strip().lower()
    if kd not in _MARKETS_OPTIMIZER_RULE_KINDS:
        return _markets_optimizer_random_rule()
    if random.random() < 0.25:
        return _markets_optimizer_random_rule(kd)
    mutated = _markets_optimizer_clone_rules([rule])[0]
    params = mutated.get("params") if isinstance(mutated.get("params"), dict) else {}
    mutated["params"] = params
    if kd == "ma":
        if _normalize_ma_mode(params.get("mode"), default="single") == "ribbon":
            levels = params.get("levels") if isinstance(params.get("levels"), list) else []
            if not levels:
                return _markets_optimizer_random_rule("ma")
            idx = random.randrange(len(levels))
            level = levels[idx]
            if not isinstance(level, dict):
                return _markets_optimizer_random_rule("ma")
            field = random.choice(["ma_type", "length", "above_action", "below_action"])
            if field == "ma_type":
                level["ma_type"] = random.choice(["sma", "ema"])
            elif field == "length":
                slot = str(level.get("slot") or "")
                if slot == "short":
                    level["length"] = random.randint(5, 45)
                elif slot == "medium":
                    level["length"] = random.randint(30, 110)
                else:
                    level["length"] = random.randint(90, 220)
            elif field == "above_action":
                level["above_action"] = random.choice(["buy", "sell", "hold"])
            else:
                level["below_action"] = random.choice(["buy", "sell", "hold"])
        else:
            field = random.choice(
                [
                    "length",
                    "ma_type",
                    "buy_relation",
                    "sell_relation",
                    "track_derivative",
                    "buy_derivative_min",
                    "sell_derivative_max",
                    "unless_enabled",
                    "unless_relation",
                    "unless_type",
                    "unless_length",
                    "unless_action",
                ]
            )
            if field == "length":
                params["length"] = random.randint(4, 220)
            elif field == "ma_type":
                params["ma_type"] = random.choice(["sma", "ema"])
            elif field == "buy_relation":
                params["buy_relation"] = random.choice(["above", "below", "hold"])
            elif field == "sell_relation":
                params["sell_relation"] = random.choice(["above", "below", "hold"])
            elif field == "track_derivative":
                params["track_derivative"] = 0 if int(params.get("track_derivative") or 0) else 1
            elif field == "buy_derivative_min":
                params["buy_derivative_min"] = _markets_optimizer_random_float(-2.0, 2.0, 4) if random.random() < 0.8 else None
            elif field == "sell_derivative_max":
                params["sell_derivative_max"] = _markets_optimizer_random_float(-2.0, 2.0, 4) if random.random() < 0.8 else None
            elif field == "unless_enabled":
                params["unless_enabled"] = 0 if int(params.get("unless_enabled") or 0) else 1
            elif field == "unless_relation":
                params["unless_relation"] = random.choice(["above", "below"])
            elif field == "unless_type":
                params["unless_type"] = random.choice(["sma", "ema"])
            elif field == "unless_length":
                params["unless_length"] = random.randint(10, 220)
            elif field == "unless_action":
                params["unless_action"] = random.choice(["buy", "sell"])
    elif kd == "rsi":
        field = random.choice(
            [
                "oversold",
                "overbought",
                "oversold_relation",
                "oversold_action",
                "overbought_relation",
                "overbought_action",
            ]
        )
        if field == "oversold":
            params["oversold"] = _markets_optimizer_random_float(15.0, 45.0, 1)
        elif field == "overbought":
            params["overbought"] = _markets_optimizer_random_float(55.0, 85.0, 1)
        elif field == "oversold_relation":
            params["oversold_relation"] = random.choice(["below", "above"])
        elif field == "oversold_action":
            params["oversold_action"] = random.choice(["buy", "sell", "hold"])
        elif field == "overbought_relation":
            params["overbought_relation"] = random.choice(["above", "below"])
        else:
            params["overbought_action"] = random.choice(["sell", "buy", "hold"])
        if float(_to_float_opt(params.get("overbought")) or 70.0) <= float(_to_float_opt(params.get("oversold")) or 30.0):
            params["overbought"] = float(_to_float_opt(params.get("oversold")) or 30.0) + 10.0
    elif kd == "macd":
        field = random.choice(
            [
                "mode",
                "fast_length",
                "slow_length",
                "signal_length",
                "derivative_buy_above",
                "derivative_sell_below",
                "derivative_signal_scope",
            ]
        )
        if field == "mode":
            params["mode"] = random.choice(["signal_cross", "cross_regime", "hist_momentum", "zero_reclaim_loss", "macd_derivative_sign"])
        elif field == "fast_length":
            params["fast_length"] = random.randint(4, 18)
        elif field == "slow_length":
            params["slow_length"] = random.randint(16, 42)
        elif field == "signal_length":
            params["signal_length"] = random.randint(3, 15)
        elif field == "derivative_buy_above":
            params["derivative_buy_above"] = _markets_optimizer_random_float(-1.0, 1.0, 4)
        elif field == "derivative_sell_below":
            params["derivative_sell_below"] = _markets_optimizer_random_float(-1.0, 1.0, 4)
        else:
            params["derivative_signal_scope"] = random.choice(["both", "buy", "sell"])
        if int(_to_int_opt(params.get("slow_length")) or 26) <= int(_to_int_opt(params.get("fast_length")) or 12):
            params["slow_length"] = int(_to_int_opt(params.get("fast_length")) or 12) + random.randint(4, 20)
    elif kd == "rsi_d":
        if random.random() < 0.5:
            params["buy_above"] = _markets_optimizer_random_float(-5.0, 5.0, 4)
        else:
            params["sell_below"] = _markets_optimizer_random_float(-5.0, 5.0, 4)
    elif kd == "heikin_ashi":
        if random.random() < 0.5:
            params["mode"] = random.choice(["transition", "state"])
        else:
            if random.random() < 0.75:
                params["doji_tolerance_pct"] = _markets_optimizer_random_float(0.0, 0.25, 3)
            else:
                params.pop("doji_tolerance_pct", None)
    elif kd == "bb":
        field = random.choice(
            [
                "length",
                "std_mult",
                "buy_condition",
                "sell_condition",
                "squeeze_threshold_pct",
                "percent_b_buy_threshold",
                "percent_b_sell_threshold",
            ]
        )
        if field == "length":
            params["length"] = random.randint(8, 40)
        elif field == "std_mult":
            params["std_mult"] = _markets_optimizer_random_float(1.0, 3.2, 2)
        elif field == "buy_condition":
            params["buy_condition"] = _markets_optimizer_random_choice(_MARKETS_OPTIMIZER_BB_CONDITIONS)
        elif field == "sell_condition":
            params["sell_condition"] = _markets_optimizer_random_choice(_MARKETS_OPTIMIZER_BB_CONDITIONS)
        elif field == "squeeze_threshold_pct":
            params["squeeze_threshold_pct"] = _markets_optimizer_random_float(1.0, 12.0, 2)
        elif field == "percent_b_buy_threshold":
            params["percent_b_buy_threshold"] = _markets_optimizer_random_float(0.05, 0.55, 3)
        else:
            params["percent_b_sell_threshold"] = _markets_optimizer_random_float(0.45, 0.95, 3)
    elif kd == "ichimoku":
        field = random.choice(
            [
                "tenkan_length",
                "kijun_length",
                "senkou_b_length",
                "displacement",
                "buy_conditions",
                "sell_conditions",
                "block_conditions",
                "buy_match_mode",
                "sell_match_mode",
                "block_match_mode",
                "cloud_thickness_threshold_pct",
                "kijun_bounce_tolerance_pct",
                "delayed_cross_lookback",
            ]
        )
        if field == "tenkan_length":
            params["tenkan_length"] = random.randint(5, 15)
        elif field == "kijun_length":
            params["kijun_length"] = random.randint(18, 40)
        elif field == "senkou_b_length":
            params["senkou_b_length"] = random.randint(40, 80)
        elif field == "displacement":
            params["displacement"] = random.randint(15, 40)
        elif field == "buy_conditions":
            vals = random.sample(list(_MARKETS_OPTIMIZER_ICHI_BUY_CONDITIONS[:-1]), k=random.randint(1, 3))
            params["buy_conditions"] = vals
            params["buy_condition"] = vals[0]
        elif field == "sell_conditions":
            vals = random.sample(list(_MARKETS_OPTIMIZER_ICHI_SELL_CONDITIONS[:-1]), k=random.randint(1, 3))
            params["sell_conditions"] = vals
            params["sell_condition"] = vals[0]
        elif field == "block_conditions":
            vals = random.sample(list(_MARKETS_OPTIMIZER_ICHI_BLOCK_CONDITIONS), k=random.randint(1, 2))
            params["block_conditions"] = vals
            params["block_condition"] = vals[0]
        elif field == "buy_match_mode":
            params["buy_match_mode"] = random.choice(["all", "any"])
        elif field == "sell_match_mode":
            params["sell_match_mode"] = random.choice(["all", "any"])
        elif field == "block_match_mode":
            params["block_match_mode"] = random.choice(["all", "any"])
        elif field == "cloud_thickness_threshold_pct":
            params["cloud_thickness_threshold_pct"] = _markets_optimizer_random_float(0.2, 4.0, 2)
        elif field == "kijun_bounce_tolerance_pct":
            params["kijun_bounce_tolerance_pct"] = _markets_optimizer_random_float(0.1, 1.2, 2)
        else:
            params["delayed_cross_lookback"] = random.randint(1, 6)
        if int(_to_int_opt(params.get("kijun_length")) or 26) <= int(_to_int_opt(params.get("tenkan_length")) or 9):
            params["kijun_length"] = int(_to_int_opt(params.get("tenkan_length")) or 9) + random.randint(4, 15)
        if int(_to_int_opt(params.get("senkou_b_length")) or 52) <= int(_to_int_opt(params.get("kijun_length")) or 26):
            params["senkou_b_length"] = int(_to_int_opt(params.get("kijun_length")) or 26) + random.randint(8, 40)
    elif kd == "ttm":
        field = random.choice(["bb_length", "bb_mult", "kc_length", "kc_mult", "momentum_length", "buy_condition", "sell_condition"])
        if field == "bb_length":
            params["bb_length"] = random.randint(8, 36)
        elif field == "bb_mult":
            params["bb_mult"] = _markets_optimizer_random_float(1.0, 3.0, 2)
        elif field == "kc_length":
            params["kc_length"] = random.randint(8, 36)
        elif field == "kc_mult":
            params["kc_mult"] = _markets_optimizer_random_float(1.0, 2.5, 2)
        elif field == "momentum_length":
            params["momentum_length"] = random.randint(8, 36)
        elif field == "buy_condition":
            params["buy_condition"] = _markets_optimizer_random_choice(_MARKETS_OPTIMIZER_TTM_CONDITIONS)
        else:
            params["sell_condition"] = _markets_optimizer_random_choice(_MARKETS_OPTIMIZER_TTM_CONDITIONS)
    elif kd == "roc":
        field = random.choice(["length", "buy_condition", "sell_condition", "buy_threshold_pct", "sell_threshold_pct"])
        if field == "length":
            params["length"] = random.randint(2, 30)
        elif field == "buy_condition":
            params["buy_condition"] = _markets_optimizer_random_choice(_MARKETS_OPTIMIZER_ROC_BUY_CONDITIONS)
        elif field == "sell_condition":
            params["sell_condition"] = _markets_optimizer_random_choice(_MARKETS_OPTIMIZER_ROC_SELL_CONDITIONS)
        elif field == "buy_threshold_pct":
            params["buy_threshold_pct"] = _markets_optimizer_random_float(-15.0, 15.0, 2)
        else:
            params["sell_threshold_pct"] = _markets_optimizer_random_float(-15.0, 15.0, 2)
    elif kd == "sar":
        field = random.choice(["step", "max_step", "buy_condition", "sell_condition"])
        if field == "step":
            params["step"] = _markets_optimizer_random_float(0.005, 0.06, 4)
            params["max_step"] = max(float(_to_float_opt(params.get("max_step")) or 0.2), float(params["step"]))
        elif field == "max_step":
            step = float(_to_float_opt(params.get("step")) or 0.02)
            params["max_step"] = _markets_optimizer_random_float(max(step, 0.06), 0.4, 4)
        elif field == "buy_condition":
            params["buy_condition"] = _markets_optimizer_random_choice(_MARKETS_OPTIMIZER_SAR_BUY_CONDITIONS)
        else:
            params["sell_condition"] = _markets_optimizer_random_choice(_MARKETS_OPTIMIZER_SAR_SELL_CONDITIONS)
    mutated["name"] = _markets_optimizer_rule_name(kd, params)
    return mutated


def _markets_optimizer_random_genome(max_rules: int) -> list[dict[str, Any]]:
    rule_count = random.randint(1, max(1, int(max_rules)))
    return [_markets_optimizer_random_rule() for _ in range(rule_count)]


def _markets_optimizer_crossover(
    parent_a: list[dict[str, Any]],
    parent_b: list[dict[str, Any]],
    *,
    max_rules: int,
) -> list[dict[str, Any]]:
    pool = _markets_optimizer_clone_rules(parent_a) + _markets_optimizer_clone_rules(parent_b)
    random.shuffle(pool)
    child: list[dict[str, Any]] = []
    target = random.randint(1, max(1, int(max_rules)))
    for rule in pool:
        if len(child) >= target:
            break
        if random.random() < 0.5 or not child:
            child.append(rule)
    if not child:
        seed_parent = parent_a if parent_a and random.random() < 0.5 else parent_b
        if seed_parent:
            child.append(_markets_optimizer_clone_rules(seed_parent[:1])[0])
    return child[: max(1, int(max_rules))] or [_markets_optimizer_random_rule()]


def _markets_optimizer_mutate_genome(genome: list[dict[str, Any]], *, max_rules: int) -> list[dict[str, Any]]:
    child = _markets_optimizer_clone_rules(genome)
    if not child:
        return _markets_optimizer_random_genome(max_rules)
    roll = random.random()
    if roll < 0.18 and len(child) < max_rules:
        child.append(_markets_optimizer_random_rule())
    elif roll < 0.30 and len(child) > 1:
        del child[random.randrange(len(child))]
    elif roll < 0.42:
        child[random.randrange(len(child))] = _markets_optimizer_random_rule()
    else:
        idx = random.randrange(len(child))
        child[idx] = _markets_optimizer_mutate_rule(child[idx])
    if not child:
        child.append(_markets_optimizer_random_rule())
    return child[: max(1, int(max_rules))]


def _markets_optimizer_rules_signature(rules: list[dict[str, Any]]) -> str:
    normalized = _normalize_indicator_rules_payload(rules)
    for item in normalized:
        if isinstance(item.get("params"), dict):
            item["params"].pop("rule_id", None)
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"))


def _markets_optimizer_result_key(result: dict[str, Any]) -> tuple[float, float, int, int, int]:
    return (
        round(float(result.get("realized_gain_total") or 0.0), 8),
        round(float(result.get("avg_trade_profit") or 0.0), 8),
        int(result.get("sell_executions") or 0),
        -int(result.get("open_units") or 0),
        -int(result.get("rule_count") or 0),
    )


def _markets_optimizer_result_better(candidate: dict[str, Any], incumbent: Optional[dict[str, Any]]) -> bool:
    if incumbent is None:
        return True
    return _markets_optimizer_result_key(candidate) > _markets_optimizer_result_key(incumbent)


def _markets_optimizer_should_stop(run_id: int, stop_event: Optional[threading.Event]) -> bool:
    if stop_event is not None and stop_event.is_set():
        return True
    run = _markets_optimizer_fetch_run(int(run_id))
    return bool(run and run.get("stop_requested"))


def _markets_optimizer_eval_rules_on_closes(
    rules: list[dict[str, Any]],
    closes: list[float],
    *,
    lookback_candles: int,
) -> dict[str, Any]:
    normalized = _normalize_indicator_rules_payload(rules)
    if not normalized:
        return {"ok": False, "reason": "no valid rules"}
    chart_cfg = _indicator_rules_chart_config(normalized)
    min_required = int(chart_cfg.get("min_required") or 30)
    if len(closes) < (min_required + 2):
        return {"ok": False, "reason": f"need >= {min_required + 2} candles, got {len(closes)}"}
    start_idx = max(int(min_required), int(len(closes) - max(40, int(lookback_candles))))
    end_idx = len(closes) - 2
    if end_idx <= start_idx:
        return {"ok": False, "reason": "not enough evaluation bars after warmup"}
    signals: dict[int, str] = {}
    for i in range(start_idx, end_idx + 1):
        window = closes[: i + 1]
        checks = _build_indicator_rule_checks(normalized, window, float(window[-1]))
        signals[i] = _indicator_signal_from_checks_for_backtest(checks)
    return _simulate_signal_series_backtest(
        closes,
        signals_by_index=signals,
        start_idx=start_idx,
        end_idx=end_idx,
        stoploss_enabled=False,
    )


def _markets_optimizer_evaluate_genome(
    rules: list[dict[str, Any]],
    closes_by_symbol: dict[str, list[float]],
    *,
    lookback_candles: int,
) -> dict[str, Any]:
    genome = _normalize_indicator_rules_payload(rules)
    total_realized_gain = 0.0
    total_sale_count = 0
    total_open_units = 0
    total_buy_cost = 0.0
    total_open_unrealized = 0.0
    usable_symbols = 0
    bars_tested = 0
    per_symbol: list[dict[str, Any]] = []
    for sym, closes in closes_by_symbol.items():
        stats = _markets_optimizer_eval_rules_on_closes(genome, closes, lookback_candles=lookback_candles)
        if not bool(stats.get("ok")):
            per_symbol.append({"symbol": sym, "ok": False, "reason": str(stats.get("reason") or "invalid")})
            continue
        usable_symbols += 1
        realized_gain = float(_to_float_opt(stats.get("realized_gain_total")) or 0.0)
        sells = int(stats.get("sell_executions") or 0)
        open_units = int(stats.get("open_units") or 0)
        bars_tested += int(stats.get("bars_tested") or 0)
        total_realized_gain += realized_gain
        total_sale_count += sells
        total_open_units += open_units
        total_buy_cost += float(_to_float_opt(stats.get("total_buy_cost")) or 0.0)
        total_open_unrealized += float(_to_float_opt(stats.get("open_unrealized_gain_total")) or 0.0)
        per_symbol.append(
            {
                "symbol": sym,
                "ok": True,
                "realized_gain_total": realized_gain,
                "sell_executions": sells,
                "open_units": open_units,
                "net_cumulative_gain_pct": _to_float_opt(stats.get("net_cumulative_gain_pct")),
                "avg_win_pct": _to_float_opt(stats.get("avg_win_pct")),
            }
        )
    avg_trade_profit = (float(total_realized_gain) / float(total_sale_count)) if total_sale_count > 0 else 0.0
    aggregate_net_gain_pct: Optional[float] = None
    if total_buy_cost > 0.0:
        aggregate_net_gain_pct = ((float(total_realized_gain) + float(total_open_unrealized)) * 100.0) / float(total_buy_cost)
    stats_summary = {
        "usable_symbols": usable_symbols,
        "symbols_requested": len(closes_by_symbol),
        "bars_tested": bars_tested,
        "realized_gain_total": total_realized_gain,
        "avg_trade_profit": avg_trade_profit,
        "sell_executions": total_sale_count,
        "open_units": total_open_units,
        "total_buy_cost": total_buy_cost,
        "aggregate_net_cumulative_gain_pct": aggregate_net_gain_pct,
        "per_symbol": per_symbol,
    }
    return {
        "rules": genome,
        "rule_count": len(genome),
        "realized_gain_total": total_realized_gain,
        "avg_trade_profit": avg_trade_profit,
        "sell_executions": total_sale_count,
        "open_units": total_open_units,
        "usable_symbols": usable_symbols,
        "stats": stats_summary,
    }


def _markets_optimizer_pick_parent(results: list[dict[str, Any]]) -> dict[str, Any]:
    if not results:
        return {"rules": _markets_optimizer_random_genome(4)}
    sample = random.sample(results, k=min(len(results), 5))
    sample.sort(key=_markets_optimizer_result_key, reverse=True)
    return sample[0]


def _markets_optimizer_rule_summary(rule: dict[str, Any]) -> str:
    kind = str(rule.get("kind") or "").strip().lower()
    params = rule.get("params") if isinstance(rule.get("params"), dict) else {}
    name = html.escape(str(rule.get("name") or kind.upper()))
    if kind == "ma":
        if _normalize_ma_mode(params.get("mode"), default="single") == "ribbon":
            return f"{name}: MA ribbon"
        ma_type = _normalize_ma_type(params.get("ma_type"), default="sma").upper()
        ln = int(_to_int_opt(params.get("length")) or 30)
        buy_rel = _normalize_relation_mode(params.get("buy_relation"), default="hold")
        sell_rel = _normalize_relation_mode(params.get("sell_relation"), default="hold")
        return f"{name}: {ma_type}{ln} buy={buy_rel} sell={sell_rel}"
    if kind == "rsi":
        return (
            f"{name}: RSI os={_fmt_market_num(params.get('oversold'),1)}->{html.escape(str(params.get('oversold_action') or 'buy'))} "
            f"ob={_fmt_market_num(params.get('overbought'),1)}->{html.escape(str(params.get('overbought_action') or 'sell'))}"
        )
    if kind == "macd":
        return (
            f"{name}: MACD {html.escape(str(params.get('mode') or 'signal_cross'))} "
            f"{int(_to_int_opt(params.get('fast_length')) or 12)}/"
            f"{int(_to_int_opt(params.get('slow_length')) or 26)}/"
            f"{int(_to_int_opt(params.get('signal_length')) or 9)}"
        )
    if kind == "rsi_d":
        return f"{name}: dRSI buy>={_fmt_market_num(params.get('buy_above'),4)} sell<={_fmt_market_num(params.get('sell_below'),4)}"
    if kind == "heikin_ashi":
        return f"{name}: HA mode={html.escape(str(params.get('mode') or 'transition'))}"
    if kind == "bb":
        return (
            f"{name}: BB len={int(_to_int_opt(params.get('length')) or 20)} "
            f"std={_fmt_market_num(params.get('std_mult'),2)} "
            f"buy={html.escape(str(params.get('buy_condition') or 'hold'))} "
            f"sell={html.escape(str(params.get('sell_condition') or 'hold'))}"
        )
    if kind == "ichimoku":
        conversion, base, leading_b, displacement = _ichimoku_lengths_from_params(params)
        return (
            f"{name}: ICHI conversion/base/leadingB={conversion}/{base}/{leading_b} "
            f"disp={displacement}"
        )
    if kind == "ttm":
        return (
            f"{name}: TTM bb={int(_to_int_opt(params.get('bb_length')) or 20)}/"
            f"{_fmt_market_num(params.get('bb_mult'),2)} kc={int(_to_int_opt(params.get('kc_length')) or 20)}/"
            f"{_fmt_market_num(params.get('kc_mult'),2)}"
        )
    if kind == "roc":
        return (
            f"{name}: ROC len={int(_to_int_opt(params.get('length')) or 12)} "
            f"buy={html.escape(str(params.get('buy_condition') or 'hold'))} "
            f"sell={html.escape(str(params.get('sell_condition') or 'hold'))}"
        )
    if kind == "sar":
        return (
            f"{name}: SAR step={_fmt_market_num(params.get('step'),4)} "
            f"max={_fmt_market_num(params.get('max_step'),4)}"
        )
    return name


def _markets_optimizer_rules_html(rules: list[dict[str, Any]]) -> str:
    if not rules:
        return "<div class='small'>No rules.</div>"
    return (
        "<div class='small' style='display:flex; flex-direction:column; gap:4px;'>"
        + "".join(f"<div>{_markets_optimizer_rule_summary(rule)}</div>" for rule in rules)
        + "</div>"
    )


def _markets_optimizer_worker(run_id: int) -> None:
    global MARKETS_OPTIMIZER_ACTIVE_RUN_ID, MARKETS_OPTIMIZER_STOP_EVENT, MARKETS_OPTIMIZER_THREAD
    stop_event: Optional[threading.Event]
    with MARKETS_OPTIMIZER_LOCK:
        MARKETS_OPTIMIZER_ACTIVE_RUN_ID = int(run_id)
        if MARKETS_OPTIMIZER_STOP_EVENT is None:
            MARKETS_OPTIMIZER_STOP_EVENT = threading.Event()
        stop_event = MARKETS_OPTIMIZER_STOP_EVENT
    started_ts = _utc_ts()
    run = _markets_optimizer_fetch_run(int(run_id))
    if run is None:
        return
    cfg = _markets_optimizer_sanitize_config(run.get("config") or {})
    try:
        _markets_optimizer_update_run(
            int(run_id),
            status="running",
            config=cfg,
            summary={"phase": "starting", "message": "Preparing candle cache"},
            started_ts=started_ts,
        )
        ok, msg = _ensure_robinhood_markets_session()
        if not ok:
            raise RuntimeError(msg)
        symbols = _markets_optimizer_clean_symbols(cfg.get("symbols"))
        if not symbols:
            symbols = _list_markets_watchlist()
        if not symbols:
            raise RuntimeError("No symbols configured for optimizer")
        lookback = int(cfg.get("lookback_candles") or 2400)
        prefetch_target = max(lookback + 420, 800)
        closes_by_symbol: dict[str, list[float]] = {}
        usable_counts: dict[str, int] = {}
        for sym in symbols:
            closes = _market_fetch_closes(sym, str(cfg.get("timeframe") or "1h"), min_candles=prefetch_target)
            cleaned = [float(v) for v in closes if _to_float_opt(v) is not None]
            if len(cleaned) < 120:
                continue
            closes_by_symbol[sym] = cleaned
            usable_counts[sym] = len(cleaned)
        if not closes_by_symbol:
            raise RuntimeError("Unable to load enough market history for any selected symbol")

        population_size = int(cfg.get("population_size") or 128)
        elite_count = int(cfg.get("elite_count") or 12)
        immigrant_count = int(cfg.get("immigrant_count") or 20)
        max_generations = int(cfg.get("max_generations") or 200)
        stagnation_patience = int(cfg.get("stagnation_patience") or 18)
        max_rules = int(cfg.get("max_rules") or 4)
        top_k_per_generation = int(cfg.get("top_k_per_generation") or 10)

        current_enabled = _normalize_indicator_rules_payload(_list_indicator_rules(enabled_only=True))
        population: list[list[dict[str, Any]]] = []
        population_seen: set[str] = set()

        def _push_population(genome: list[dict[str, Any]]) -> bool:
            normalized = _normalize_indicator_rules_payload(genome)
            if not normalized:
                return False
            if len(normalized) > max_rules:
                normalized = normalized[:max_rules]
            sig = _markets_optimizer_rules_signature(normalized)
            if sig in population_seen:
                return False
            population_seen.add(sig)
            population.append(normalized)
            return True

        if current_enabled:
            seed = current_enabled[:max_rules]
            _push_population(seed)
            for _ in range(min(8, population_size // 6)):
                _push_population(_markets_optimizer_mutate_genome(seed, max_rules=max_rules))
        while len(population) < population_size:
            _push_population(_markets_optimizer_random_genome(max_rules))

        best_overall: Optional[dict[str, Any]] = None
        plateau_generations = 0
        stop_reason = "completed"
        stop_message = "Optimization completed."

        for generation in range(1, max_generations + 1):
            if _markets_optimizer_should_stop(int(run_id), stop_event):
                stop_reason = "stopped"
                stop_message = "Stop requested."
                break
            _markets_optimizer_update_run(
                int(run_id),
                generation=generation,
                summary={
                    "phase": "evaluating",
                    "message": f"Evaluating generation {generation}",
                    "plateau_generations": plateau_generations,
                    "population_size": population_size,
                    "evaluated_in_generation": 0,
                    "symbols": symbols,
                    "usable_symbols": list(closes_by_symbol.keys()),
                    "timeframe": str(cfg.get("timeframe") or "1h"),
                    "lookback_candles": lookback,
                },
            )
            results: list[dict[str, Any]] = []
            progress_step = max(1, population_size // 8)
            for idx, genome in enumerate(population, start=1):
                if _markets_optimizer_should_stop(int(run_id), stop_event):
                    stop_reason = "stopped"
                    stop_message = "Stop requested."
                    break
                result = _markets_optimizer_evaluate_genome(genome, closes_by_symbol, lookback_candles=lookback)
                results.append(result)
                if idx == 1 or idx % progress_step == 0 or idx == population_size:
                    _markets_optimizer_update_run(
                        int(run_id),
                        summary={
                            "phase": "evaluating",
                            "message": f"Evaluating generation {generation}",
                            "plateau_generations": plateau_generations,
                            "population_size": population_size,
                            "evaluated_in_generation": idx,
                            "symbols": symbols,
                            "usable_symbols": list(closes_by_symbol.keys()),
                            "timeframe": str(cfg.get("timeframe") or "1h"),
                            "lookback_candles": lookback,
                        },
                    )
            if stop_reason == "stopped":
                break
            if not results:
                raise RuntimeError("Population evaluation produced no results")
            results.sort(key=_markets_optimizer_result_key, reverse=True)
            generation_best = results[0]
            _markets_optimizer_store_generation_candidates(
                int(run_id),
                generation,
                results,
                limit=top_k_per_generation,
            )

            if _markets_optimizer_result_better(generation_best, best_overall):
                best_overall = {
                    "rules": _markets_optimizer_clone_rules(generation_best.get("rules") or []),
                    "realized_gain_total": float(generation_best.get("realized_gain_total") or 0.0),
                    "avg_trade_profit": float(generation_best.get("avg_trade_profit") or 0.0),
                    "sell_executions": int(generation_best.get("sell_executions") or 0),
                }
                plateau_generations = 0
            else:
                plateau_generations += 1

            _markets_optimizer_update_run(
                int(run_id),
                generation=generation,
                summary={
                    "phase": "running",
                    "message": f"Completed generation {generation}",
                    "plateau_generations": plateau_generations,
                    "population_size": population_size,
                    "elite_count": elite_count,
                    "immigrant_count": immigrant_count,
                    "symbols": symbols,
                    "usable_symbols": list(closes_by_symbol.keys()),
                    "symbol_candle_counts": usable_counts,
                    "timeframe": str(cfg.get("timeframe") or "1h"),
                    "lookback_candles": lookback,
                    "generation_best_realized_gain_total": float(generation_best.get("realized_gain_total") or 0.0),
                    "generation_best_avg_trade_profit": float(generation_best.get("avg_trade_profit") or 0.0),
                    "generation_best_sell_executions": int(generation_best.get("sell_executions") or 0),
                },
                best_rules=best_overall.get("rules") if best_overall else [],
                best_realized_gain_total=best_overall.get("realized_gain_total") if best_overall else 0.0,
                best_avg_trade_profit=best_overall.get("avg_trade_profit") if best_overall else 0.0,
                best_sell_executions=best_overall.get("sell_executions") if best_overall else 0,
            )

            if plateau_generations >= stagnation_patience:
                stop_reason = "completed"
                stop_message = f"Stopped after {plateau_generations} stagnant generations."
                break
            if generation >= max_generations:
                stop_reason = "completed"
                stop_message = f"Reached generation limit ({max_generations})."
                break

            next_population: list[list[dict[str, Any]]] = []
            next_seen: set[str] = set()

            def _push_next(genome: list[dict[str, Any]]) -> bool:
                normalized = _normalize_indicator_rules_payload(genome)
                if not normalized:
                    return False
                if len(normalized) > max_rules:
                    normalized = normalized[:max_rules]
                sig = _markets_optimizer_rules_signature(normalized)
                if sig in next_seen:
                    return False
                next_seen.add(sig)
                next_population.append(normalized)
                return True

            for elite in results[: max(1, elite_count)]:
                _push_next(_markets_optimizer_clone_rules(elite.get("rules") or []))

            attempts = 0
            target_children = max(0, population_size - immigrant_count)
            while len(next_population) < target_children and attempts < (population_size * 40):
                attempts += 1
                parent_a = _markets_optimizer_pick_parent(results)
                parent_b = _markets_optimizer_pick_parent(results)
                if random.random() < 0.68:
                    child = _markets_optimizer_crossover(
                        parent_a.get("rules") or [],
                        parent_b.get("rules") or [],
                        max_rules=max_rules,
                    )
                else:
                    child = _markets_optimizer_clone_rules(parent_a.get("rules") or [])
                if random.random() < 0.85:
                    child = _markets_optimizer_mutate_genome(child, max_rules=max_rules)
                _push_next(child)

            while len(next_population) < population_size:
                if len(next_population) < target_children:
                    _push_next(_markets_optimizer_mutate_genome(results[0].get("rules") or [], max_rules=max_rules))
                else:
                    _push_next(_markets_optimizer_random_genome(max_rules))
                if len(next_population) >= population_size:
                    break

            population = next_population[:population_size]

        end_status = "completed" if stop_reason == "completed" else "stopped"
        _markets_optimizer_update_run(
            int(run_id),
            status=end_status,
            summary={
                "phase": end_status,
                "message": stop_message,
                "plateau_generations": plateau_generations,
                "symbols": symbols,
                "usable_symbols": list(closes_by_symbol.keys()),
                "timeframe": str(cfg.get("timeframe") or "1h"),
                "lookback_candles": lookback,
            },
            best_rules=best_overall.get("rules") if best_overall else [],
            best_realized_gain_total=best_overall.get("realized_gain_total") if best_overall else 0.0,
            best_avg_trade_profit=best_overall.get("avg_trade_profit") if best_overall else 0.0,
            best_sell_executions=best_overall.get("sell_executions") if best_overall else 0,
            ended_ts=_utc_ts(),
        )
    except Exception as exc:
        _markets_optimizer_update_run(
            int(run_id),
            status="error",
            summary={"phase": "error", "message": str(exc)},
            ended_ts=_utc_ts(),
        )
    finally:
        with MARKETS_OPTIMIZER_LOCK:
            if MARKETS_OPTIMIZER_ACTIVE_RUN_ID == int(run_id):
                MARKETS_OPTIMIZER_ACTIVE_RUN_ID = None
            if MARKETS_OPTIMIZER_STOP_EVENT is not None:
                MARKETS_OPTIMIZER_STOP_EVENT.clear()
            MARKETS_OPTIMIZER_THREAD = None


def _render_markets_optimizer_html() -> str:
    _markets_optimizer_cleanup_orphan_run()
    run = _markets_optimizer_latest_active_run()
    if run is None:
        run = _markets_optimizer_latest_run()
    if run is None:
        return "<div class='small'>No optimizer run yet.</div>"
    status = str(run.get("status") or "unknown").strip().lower()
    badge = "ok" if status == "running" else ("warn" if status in ("queued", "stopping", "completed") else "bad")
    summary = run.get("summary") if isinstance(run.get("summary"), dict) else {}
    cfg = run.get("config") if isinstance(run.get("config"), dict) else {}
    generation = int(run.get("generation") or 0)
    top_candidates = _markets_optimizer_list_generation_candidates(int(run["id"]), generation, limit=int(cfg.get("top_k_per_generation") or 10)) if generation > 0 else []
    out: list[str] = [
        "<div style='display:flex; flex-direction:column; gap:10px;'>",
        "<div class='row' style='justify-content:space-between; align-items:flex-start;'>",
        "<div>",
        f"<div><b>Run #{int(run['id'])}</b> <span class='badge {badge}'>{html.escape(status)}</span></div>",
        f"<div class='small'>{html.escape(str(summary.get('message') or ''))}</div>",
        "</div>",
        "<div class='small' style='text-align:right;'>"
        f"<div>Generation: {generation}</div>"
        f"<div>Plateau: {int(summary.get('plateau_generations') or 0)} / {int(cfg.get('stagnation_patience') or 0)}</div>"
        "</div>",
        "</div>",
        "<div class='small'>"
        f"Universe: {html.escape(str(cfg.get('symbols') or '')) or '—'} · "
        f"Timeframe: {html.escape(str(cfg.get('timeframe') or '1h'))} · "
        f"Lookback: {int(cfg.get('lookback_candles') or 0)} candles · "
        f"Population: {int(cfg.get('population_size') or 0)} · "
        f"Max rules/genome: {int(cfg.get('max_rules') or 0)}"
        "</div>",
    ]
    usable_symbols = summary.get("usable_symbols")
    if isinstance(usable_symbols, list) and usable_symbols:
        out.append(
            "<div class='small'>Usable symbols: "
            + html.escape(", ".join(str(x) for x in usable_symbols if str(x).strip()))
            + "</div>"
        )
    out.append(
        "<div class='card' style='padding:10px;'>"
        "<div class='small'><b>Champion</b></div>"
        f"<div class='small' style='margin-top:6px;'>Realized Profit: {_markets_optimizer_fmt_money(run.get('best_realized_gain_total'))}</div>"
        f"<div class='small'>Avg Realized Profit / Sale: {_markets_optimizer_fmt_money(run.get('best_avg_trade_profit'))}</div>"
        f"<div class='small'>Completed Sales: {int(run.get('best_sell_executions') or 0)}</div>"
        "<div style='margin-top:8px;'>"
        + _markets_optimizer_rules_html(run.get("best_rules") if isinstance(run.get("best_rules"), list) else [])
        + "</div>"
        "</div>"
    )
    if top_candidates:
        out.append(
            "<div class='status-table-wrap'><table>"
            "<thead><tr>"
            "<th>Rank</th><th>Realized Profit</th><th>Avg / Sale</th><th>Sells</th><th>Held End</th><th>Rules</th>"
            "</tr></thead><tbody>"
        )
        for row in top_candidates:
            out.append(
                "<tr>"
                f"<td>{int(row.get('rank_idx') or 0)}</td>"
                f"<td>{_markets_optimizer_fmt_money(row.get('realized_gain_total'))}</td>"
                f"<td>{_markets_optimizer_fmt_money(row.get('avg_trade_profit'))}</td>"
                f"<td>{int(row.get('sell_executions') or 0)}</td>"
                f"<td>{int(row.get('open_units') or 0)}</td>"
                f"<td>{_markets_optimizer_rules_html(row.get('rules') if isinstance(row.get('rules'), list) else [])}</td>"
                "</tr>"
            )
        out.append("</tbody></table></div>")
    else:
        out.append("<div class='small'>No completed generation leaderboard yet.</div>")
    out.append("</div>")
    return "".join(out)


# =========================
# Pages
# =========================
@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    return render("dashboard.html", title="Dashboard", path="/", request=request)


@app.get("/markets", response_class=HTMLResponse)
def markets_page(request: Request):
    _markets_optimizer_cleanup_orphan_run()
    _default_indicator_rules_if_empty()
    _ensure_saved_indicator_rules_have_ids()
    all_rules = _list_indicator_rules(enabled_only=False)
    enabled_rules = [r for r in all_rules if bool(r.get("enabled"))]
    target_entries = _saved_indicator_rule_target_entries(all_rules)
    override_target_entries = [
        e
        for e in target_entries
        if str(e.get("kind") or "").strip().lower() not in ("ichimoku", "ichimoku_cloud", "ichi")
    ]
    watchlist_symbols = _list_markets_watchlist()
    optimizer_defaults = _markets_optimizer_default_config(", ".join(watchlist_symbols))
    return render(
        "markets.html",
        title="Markets",
        path="/markets",
        request=request,
        enabled_indicator_rules_json=json.dumps(enabled_rules),
        markets_watchlist_json=json.dumps(watchlist_symbols),
        markets_watchlist_symbols_csv=", ".join(watchlist_symbols),
        rsi_override_target_entries=[
            e for e in override_target_entries if str(e.get("kind") or "").strip().lower() != "rsi"
        ],
        macd_override_target_entries=[
            e for e in override_target_entries if str(e.get("kind") or "").strip().lower() != "macd"
        ],
        ha_override_target_entries=[
            e
            for e in override_target_entries
            if str(e.get("kind") or "").strip().lower() not in ("heikin_ashi", "ha")
        ],
        markets_optimizer_defaults=optimizer_defaults,
    )


@app.get("/partials/markets_watchlist", response_class=HTMLResponse)
def partial_markets_watchlist():
    return HTMLResponse(_render_markets_watchlist_html())


@app.post("/markets/watchlist/add")
def markets_watchlist_add(symbol: str = Form(...)):
    _add_markets_symbol(symbol)
    return RedirectResponse("/markets", status_code=303)


@app.post("/markets/watchlist/{symbol}/remove")
def markets_watchlist_remove(symbol: str):
    _remove_markets_symbol(symbol)
    return RedirectResponse("/markets", status_code=303)


@app.get("/partials/markets_rules", response_class=HTMLResponse)
def partial_markets_rules():
    return HTMLResponse(_render_indicator_rules_html())


@app.get("/partials/markets_optimizer", response_class=HTMLResponse)
def partial_markets_optimizer():
    return HTMLResponse(_render_markets_optimizer_html())


@app.post("/markets/optimizer/start")
async def markets_optimizer_start(request: Request):
    global MARKETS_OPTIMIZER_THREAD, MARKETS_OPTIMIZER_STOP_EVENT
    _markets_optimizer_cleanup_orphan_run()
    active = _markets_optimizer_latest_active_run()
    with MARKETS_OPTIMIZER_LOCK:
        thread_alive = bool(MARKETS_OPTIMIZER_THREAD and MARKETS_OPTIMIZER_THREAD.is_alive())
    if active is not None or thread_alive:
        return RedirectResponse("/markets", status_code=303)
    form = await request.form()
    cfg = _markets_optimizer_sanitize_config(
        {
            "symbols": str(form.get("symbols") or ""),
            "timeframe": str(form.get("timeframe") or "1h"),
            "lookback_candles": _to_int_opt(form.get("lookback_candles")),
            "population_size": _to_int_opt(form.get("population_size")),
            "elite_count": _to_int_opt(form.get("elite_count")),
            "immigrant_count": _to_int_opt(form.get("immigrant_count")),
            "max_generations": _to_int_opt(form.get("max_generations")),
            "stagnation_patience": _to_int_opt(form.get("stagnation_patience")),
            "max_rules": _to_int_opt(form.get("max_rules")),
            "top_k_per_generation": _to_int_opt(form.get("top_k_per_generation")),
        }
    )
    if not _markets_optimizer_clean_symbols(cfg.get("symbols")):
        cfg["symbols"] = ", ".join(_list_markets_watchlist())
    run_id = _markets_optimizer_create_run(cfg)
    with MARKETS_OPTIMIZER_LOCK:
        MARKETS_OPTIMIZER_STOP_EVENT = threading.Event()
        MARKETS_OPTIMIZER_THREAD = threading.Thread(
            target=_markets_optimizer_worker,
            args=(int(run_id),),
            daemon=True,
            name=f"markets-optimizer-{run_id}",
        )
        MARKETS_OPTIMIZER_THREAD.start()
    return RedirectResponse("/markets", status_code=303)


@app.post("/markets/optimizer/stop")
def markets_optimizer_stop():
    active = _markets_optimizer_latest_active_run()
    if active is not None:
        _markets_optimizer_mark_stop_requested(int(active["id"]))
    with MARKETS_OPTIMIZER_LOCK:
        if MARKETS_OPTIMIZER_STOP_EVENT is not None:
            MARKETS_OPTIMIZER_STOP_EVENT.set()
    return RedirectResponse("/markets", status_code=303)


@app.post("/markets/rules/add")
async def markets_rules_add(request: Request):
    form = await request.form()

    def _f(key: str, default: str = "") -> str:
        return str(form.get(key) or default)

    def _flag(key: str) -> bool:
        return _coerce_bool(form.get(key), default=False)

    def _list(key: str) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for raw in form.getlist(key):
            txt = str(raw or "").strip()
            if not txt or txt in seen:
                continue
            seen.add(txt)
            out.append(txt)
        return out

    rule_timeframe = _normalize_indicator_rule_timeframe(form.get("timeframe"), default="1h")

    def _save_rule(rule_name: str, rule_kind: str, rule_params: dict[str, Any]) -> None:
        _add_indicator_rule(rule_name, rule_kind, rule_params, timeframe=rule_timeframe)

    k = str(form.get("kind") or "").strip().lower()
    name = str(form.get("name") or "").strip()
    if k in ("ma", "ema"):
        ma_mode = _normalize_ma_mode(form.get("ma_mode"), default="single")
        if ma_mode == "ribbon":
            levels: list[dict[str, Any]] = []
            for slot in _MA_RIBBON_LEVEL_ORDER:
                label = str(_MA_RIBBON_LEVEL_LABELS.get(slot, slot.title())).strip() or "Level"
                default_len = int(_MA_RIBBON_DEFAULT_LENGTHS.get(slot, 30))
                levels.append(
                    {
                        "slot": slot,
                        "label": label,
                        "ma_type": _normalize_ma_type(form.get(f"ma_ribbon_{slot}_type"), default="sma"),
                        "length": max(2, int(_to_int_opt(form.get(f"ma_ribbon_{slot}_length")) or default_len)),
                        "above_action": _normalize_signal_action_mode(
                            form.get(f"ma_ribbon_{slot}_above"),
                            default="hold",
                        ),
                        "below_action": _normalize_signal_action_mode(
                            form.get(f"ma_ribbon_{slot}_below"),
                            default="hold",
                        ),
                    }
                )
            _save_rule(
                name or "MA Ribbon",
                "ma",
                {
                    "mode": "ribbon",
                    "levels": levels,
                },
            )
        else:
            selected_type = _normalize_ma_type(form.get("ma_type"), default=("ema" if k == "ema" else "sma"))
            params = {
                "mode": "single",
                "length": max(2, int(_to_int_opt(form.get("ma_length")) or 30)),
                "ma_type": selected_type,
                "buy_relation": _normalize_relation_mode(form.get("ma_buy_relation"), default="hold"),
                "sell_relation": _normalize_relation_mode(form.get("ma_sell_relation"), default="hold"),
                "track_derivative": 1 if _flag("ma_track_derivative") else 0,
                "buy_derivative_min": _to_float_opt(form.get("ma_buy_derivative_min")),
                "sell_derivative_max": _to_float_opt(form.get("ma_sell_derivative_max")),
                "unless_enabled": 1 if _flag("ma_unless_enabled") else 0,
                "unless_relation": str(form.get("ma_unless_relation") or "above").strip().lower(),
                "unless_type": str(form.get("ma_unless_type") or "sma").strip().lower(),
                "unless_length": max(2, int(_to_int_opt(form.get("ma_unless_length")) or 30)),
                "unless_action": str(form.get("ma_unless_action") or "sell").strip().lower(),
            }
            default_name = f"{'EMA' if selected_type == 'ema' else 'MA'}{params['length']}"
            _save_rule(name or default_name, "ma", params)
    elif k == "macd":
        mode = str(form.get("macd_mode") or "signal_cross").strip().lower()
        if mode not in (
            "signal_cross",
            "cross_regime",
            "hist_momentum",
            "zero_reclaim_loss",
            "macd_derivative_sign",
        ):
            mode = "signal_cross"
        override_enabled = _flag("macd_signal_override_enabled")
        params = {
            "mode": mode,
            "fast_length": max(2, int(_to_int_opt(form.get("macd_fast_length")) or 12)),
            "slow_length": max(2, int(_to_int_opt(form.get("macd_slow_length")) or 26)),
            "signal_length": max(2, int(_to_int_opt(form.get("macd_signal_length")) or 9)),
            "derivative_buy_above": _to_float_opt(form.get("macd_derivative_buy_above")),
            "derivative_sell_below": _to_float_opt(form.get("macd_derivative_sell_below")),
            "derivative_signal_scope": _normalize_dual_signal_scope(
                form.get("macd_derivative_signal_scope"),
                default="both",
            ),
            "signal_override_enabled": 1 if override_enabled else 0,
            "signal_override_scope": _normalize_dual_signal_scope(
                form.get("macd_signal_override_scope"),
                default="both",
            ),
            "signal_override_targets": _list("macd_signal_override_targets") if override_enabled else [],
        }
        _save_rule(name or "MACD", "macd", params)
    elif k == "rsi":
        override_enabled = _flag("rsi_signal_override_enabled")
        params = {
            "overbought": _to_float_opt(form.get("rsi_overbought")),
            "oversold": _to_float_opt(form.get("rsi_oversold")),
            "overbought_relation": str(form.get("rsi_overbought_relation") or "above").strip().lower(),
            "overbought_action": _normalize_signal_action_mode(form.get("rsi_overbought_action"), default="sell"),
            "oversold_relation": str(form.get("rsi_oversold_relation") or "below").strip().lower(),
            "oversold_action": _normalize_signal_action_mode(form.get("rsi_oversold_action"), default="buy"),
            "signal_override_enabled": 1 if override_enabled else 0,
            "signal_override_scope": _normalize_dual_signal_scope(
                form.get("rsi_signal_override_scope"),
                default="both",
            ),
            "signal_override_targets": _list("rsi_signal_override_targets") if override_enabled else [],
        }
        _save_rule(name or "RSI", "rsi", params)
    elif k == "rsi_d":
        params = {
            "buy_above": _to_float_opt(form.get("rsi_d_buy_above")),
            "sell_below": _to_float_opt(form.get("rsi_d_sell_below")),
        }
        _save_rule(name or "RSI Derivative", "rsi_d", params)
    elif k in ("heikin_ashi", "ha"):
        mode = str(form.get("ha_mode") or "transition").strip().lower()
        if mode not in ("transition", "state"):
            mode = "transition"
        override_enabled = _flag("ha_signal_override_enabled")
        params: dict[str, Any] = {
            "mode": mode,
            "signal_override_enabled": 1 if override_enabled else 0,
            "signal_override_scope": _normalize_dual_signal_scope(
                form.get("ha_signal_override_scope"),
                default="both",
            ),
            "signal_override_targets": _list("ha_signal_override_targets") if override_enabled else [],
        }
        doji_tol = _to_float_opt(form.get("ha_doji_tolerance_pct"))
        if doji_tol is not None:
            params["doji_tolerance_pct"] = max(0.0, float(doji_tol))
        _save_rule(name or "Heikin Ashi", "heikin_ashi", params)
    elif k in ("bb", "bollinger", "bollinger_bands"):
        params = {
            "length": max(2, int(_to_int_opt(form.get("bb_length")) or 20)),
            "std_mult": max(0.1, float(_to_float_opt(form.get("bb_std_mult")) or 2.0)),
            "buy_condition": _normalize_bb_condition(form.get("bb_buy_condition"), default="hold"),
            "sell_condition": _normalize_bb_condition(form.get("bb_sell_condition"), default="hold"),
            "squeeze_threshold_pct": float(_to_float_opt(form.get("bb_squeeze_threshold_pct")) or 5.0),
            "percent_b_buy_threshold": float(_to_float_opt(form.get("bb_percent_b_buy_threshold")) or 0.2),
            "percent_b_sell_threshold": float(_to_float_opt(form.get("bb_percent_b_sell_threshold")) or 0.8),
        }
        _save_rule(name or "Bollinger Bands", "bb", params)
    elif k in ("ichimoku", "ichimoku_cloud", "ichi"):
        buy_conditions = _normalize_ichi_conditions(_list("ichi_buy_condition"), default="hold")
        sell_conditions = _normalize_ichi_conditions(_list("ichi_sell_condition"), default="hold")
        block_conditions = _normalize_ichi_conditions(_list("ichi_block_condition"), default="hold")
        stored_buy_conditions = _english_ichi_conditions(buy_conditions)
        stored_sell_conditions = _english_ichi_conditions(sell_conditions)
        stored_block_conditions = _english_ichi_conditions(block_conditions)
        params = {
            "conversion_line_length": max(1, int(_to_int_opt(form.get("ichi_tenkan_length")) or 9)),
            "base_line_length": max(1, int(_to_int_opt(form.get("ichi_kijun_length")) or 26)),
            "leading_span_b_length": max(2, int(_to_int_opt(form.get("ichi_senkou_b_length")) or 52)),
            "lagging_line_displacement": max(1, int(_to_int_opt(form.get("ichi_displacement")) or 26)),
            "buy_condition": (stored_buy_conditions[0] if stored_buy_conditions else "hold"),
            "sell_condition": (stored_sell_conditions[0] if stored_sell_conditions else "hold"),
            "block_condition": (stored_block_conditions[0] if stored_block_conditions else "hold"),
            "buy_conditions": stored_buy_conditions,
            "sell_conditions": stored_sell_conditions,
            "block_conditions": stored_block_conditions,
            "buy_match_mode": _normalize_ichi_match_mode(form.get("ichi_buy_match_mode"), default="all"),
            "sell_match_mode": _normalize_ichi_match_mode(form.get("ichi_sell_match_mode"), default="all"),
            "block_match_mode": _normalize_ichi_match_mode(form.get("ichi_block_match_mode"), default="all"),
            "cloud_thickness_threshold_pct": float(_to_float_opt(form.get("ichi_cloud_thickness_threshold_pct")) or 1.0),
            "base_line_bounce_tolerance_pct": float(_to_float_opt(form.get("ichi_kijun_bounce_tolerance_pct")) or 0.35),
            "delayed_cross_lookback": max(1, int(_to_int_opt(form.get("ichi_delayed_cross_lookback")) or 3)),
        }
        _save_rule(name or "Ichimoku", "ichimoku", params)
    elif k in ("ttm", "ttm_squeeze", "squeeze_momentum"):
        params = {
            "bb_length": max(2, int(_to_int_opt(form.get("ttm_bb_length")) or 20)),
            "bb_mult": max(0.1, float(_to_float_opt(form.get("ttm_bb_mult")) or 2.0)),
            "kc_length": max(2, int(_to_int_opt(form.get("ttm_kc_length")) or 20)),
            "kc_mult": max(0.1, float(_to_float_opt(form.get("ttm_kc_mult")) or 1.5)),
            "momentum_length": max(2, int(_to_int_opt(form.get("ttm_momentum_length")) or 20)),
            "buy_condition": _normalize_ttm_condition(form.get("ttm_buy_condition"), default="hold"),
            "sell_condition": _normalize_ttm_condition(form.get("ttm_sell_condition"), default="hold"),
        }
        _save_rule(name or "TTM Squeeze", "ttm", params)
    elif k in ("roc", "rate_of_change"):
        params = {
            "length": max(1, int(_to_int_opt(form.get("roc_length")) or 12)),
            "buy_condition": _normalize_roc_condition(form.get("roc_buy_condition"), default="hold"),
            "sell_condition": _normalize_roc_condition(form.get("roc_sell_condition"), default="hold"),
            "buy_threshold_pct": float(_to_float_opt(form.get("roc_buy_threshold_pct")) or 0.0),
            "sell_threshold_pct": float(_to_float_opt(form.get("roc_sell_threshold_pct")) or 0.0),
        }
        _save_rule(name or "ROC", "roc", params)
    elif k in ("sar", "psar", "parabolic_sar", "parabolic"):
        step = max(0.0001, float(_to_float_opt(form.get("sar_step")) or 0.02))
        max_step = max(step, float(_to_float_opt(form.get("sar_max_step")) or 0.2))
        params = {
            "step": step,
            "max_step": max_step,
            "buy_condition": _normalize_sar_condition(form.get("sar_buy_condition"), default="hold"),
            "sell_condition": _normalize_sar_condition(form.get("sar_sell_condition"), default="hold"),
        }
        _save_rule(name or "Parabolic SAR", "sar", params)
    elif k in ("donchian", "donchian_breakout", "donchian_channel", "donchian_channels"):
        params = {
            "lookback": max(1, int(_to_int_opt(form.get("donchian_lookback")) or 20)),
            "buy_condition": _normalize_donchian_condition(
                form.get("donchian_buy_condition"),
                default="close_above_upper",
            ),
            "sell_condition": _normalize_donchian_condition(
                form.get("donchian_sell_condition"),
                default="close_below_lower",
            ),
        }
        _save_rule(name or "Donchian Breakout", "donchian", params)
    elif k in ("pivot", "pivot_points", "pivots"):
        params = {
            "buy_condition": _normalize_pivot_condition(form.get("pivot_buy_condition"), default="above_p"),
            "sell_condition": _normalize_pivot_condition(form.get("pivot_sell_condition"), default="below_p"),
            "tolerance_pct": max(0.0, float(_to_float_opt(form.get("pivot_tolerance_pct")) or 0.25)),
            "include_half_levels": _to_bool(form.get("pivot_include_half_levels"), False),
        }
        _save_rule(name or "Pivot Points", "pivot", params)
    elif k in ("supertrend", "supertrend_trend"):
        params = {
            "atr_length": max(1, int(_to_int_opt(form.get("supertrend_atr_length")) or 10)),
            "multiplier": max(0.1, float(_to_float_opt(form.get("supertrend_multiplier")) or 3.0)),
            "buy_condition": _normalize_supertrend_condition(
                form.get("supertrend_buy_condition"),
                default="trend_up",
            ),
            "sell_condition": _normalize_supertrend_condition(
                form.get("supertrend_sell_condition"),
                default="trend_down",
            ),
        }
        _save_rule(name or "Supertrend", "supertrend", params)
    elif k in ("vwap", "vwap_filter"):
        params = {
            "buy_condition": _normalize_vwap_condition(form.get("vwap_buy_condition"), default="within_band"),
            "sell_condition": _normalize_vwap_condition(form.get("vwap_sell_condition"), default="exit_below"),
            "max_extension_pct": _indicator_pct_decimal(form.get("vwap_max_extension_pct"), default=0.015),
            "max_pullback_pct": _indicator_pct_decimal(form.get("vwap_max_pullback_pct"), default=0.010),
            "exit_below_pct": _indicator_pct_decimal(form.get("vwap_exit_below_pct"), default=0.012),
        }
        _save_rule(name or "VWAP Filter", "vwap", params)
    elif k in ("relative_volume", "rvol", "rel_volume"):
        params = {
            "length": max(1, int(_to_int_opt(form.get("rvol_length")) or 20)),
            "threshold": max(0.0, float(_to_float_opt(form.get("rvol_threshold")) or 1.2)),
            "buy_condition": _normalize_relative_volume_condition(
                form.get("rvol_buy_condition"),
                default="above_threshold",
            ),
            "sell_condition": _normalize_relative_volume_condition(
                form.get("rvol_sell_condition"),
                default="below_threshold",
            ),
        }
        _save_rule(name or "Relative Volume", "relative_volume", params)
    return RedirectResponse("/markets", status_code=303)


@app.post("/markets/rules/{rule_id}/toggle")
def markets_rules_toggle(rule_id: int):
    _toggle_indicator_rule(rule_id)
    return RedirectResponse("/markets", status_code=303)


@app.post("/markets/rules/{rule_id}/delete")
def markets_rules_delete(rule_id: int):
    _delete_indicator_rule(rule_id)
    return RedirectResponse("/markets", status_code=303)


@app.get("/partials/markets_indicators", response_class=HTMLResponse)
def partial_markets_indicators(
    timeframe: str = "1h",
    symbol: str = "",
):
    return HTMLResponse(_render_markets_indicators_html(timeframe=timeframe, symbol=symbol))


@app.get("/partials/markets_backtest", response_class=HTMLResponse)
def partial_markets_backtest(
    timeframe: str = "1h",
    symbols: str = "",
    rules_json: str = "[]",
    backtest_candles: int = INDICATORFORGE_BACKTEST_DEFAULT_CANDLES,
):
    return HTMLResponse(
        _render_indicatorforge_backtest_html(
            timeframe=str(timeframe or "1h"),
            symbols=str(symbols or ""),
            rules_json=str(rules_json or "[]"),
            broker_hint="robinhood",
            include_extended_hours_data=False,
            entangled_mode=False,
            entangled_primary_symbol="",
            entangled_inverse_symbol="",
            backtest_candles=max(40, min(int(backtest_candles or INDICATORFORGE_BACKTEST_DEFAULT_CANDLES), INDICATORFORGE_BACKTEST_MAX_CANDLES)),
            stoploss_enabled=False,
            target_gain_pct=None,
            stop_loss_pct=None,
            include_held_end_column=True,
        )
    )


@app.get("/partials/indicatorforge_preview", response_class=HTMLResponse)
def partial_indicatorforge_preview(
    timeframe: str = "1h",
    symbols: str = "",
    rules_json: str = "[]",
    broker_hint: str = "robinhood",
    include_extended_hours_data: str = "false",
    use_current_candle: str = "true",
    entangled_mode: str = "false",
    entangled_primary_symbol: str = "",
    entangled_inverse_symbol: str = "",
):
    include_extended = str(include_extended_hours_data or "").strip().lower() in ("1", "true", "yes", "on")
    use_entangled_mode = str(entangled_mode or "").strip().lower() in ("1", "true", "yes", "on")
    return HTMLResponse(
        _render_indicatorforge_preview_html(
            timeframe=str(timeframe or "1h"),
            symbols=str(symbols or ""),
            rules_json=str(rules_json or "[]"),
            broker_hint=str(broker_hint or "robinhood"),
            include_extended_hours_data=bool(include_extended),
            use_current_candle=True,
            entangled_mode=bool(use_entangled_mode),
            entangled_primary_symbol=str(entangled_primary_symbol or ""),
            entangled_inverse_symbol=str(entangled_inverse_symbol or ""),
        )
    )


@app.get("/partials/indicatorforge_backtest", response_class=HTMLResponse)
def partial_indicatorforge_backtest(
    timeframe: str = "1h",
    symbols: str = "",
    rules_json: str = "[]",
    broker_hint: str = "robinhood",
    include_extended_hours_data: str = "false",
    entangled_mode: str = "false",
    entangled_primary_symbol: str = "",
    entangled_inverse_symbol: str = "",
    backtest_candles: int = INDICATORFORGE_BACKTEST_DEFAULT_CANDLES,
    stoploss_enabled: str = "false",
    target_gain_pct: str = "",
    stop_loss_pct: str = "",
):
    include_extended = str(include_extended_hours_data or "").strip().lower() in ("1", "true", "yes", "on")
    use_entangled_mode = str(entangled_mode or "").strip().lower() in ("1", "true", "yes", "on")
    use_stoploss = str(stoploss_enabled or "").strip().lower() in ("1", "true", "yes", "on")
    arm_gain = _to_float_opt(target_gain_pct)
    trigger_pct = _to_float_opt(stop_loss_pct)
    return HTMLResponse(
        _render_indicatorforge_backtest_html(
            timeframe=str(timeframe or "1h"),
            symbols=str(symbols or ""),
            rules_json=str(rules_json or "[]"),
            broker_hint=str(broker_hint or "robinhood"),
            include_extended_hours_data=bool(include_extended),
            entangled_mode=bool(use_entangled_mode),
            entangled_primary_symbol=str(entangled_primary_symbol or ""),
            entangled_inverse_symbol=str(entangled_inverse_symbol or ""),
            backtest_candles=max(40, min(int(backtest_candles or INDICATORFORGE_BACKTEST_DEFAULT_CANDLES), INDICATORFORGE_BACKTEST_MAX_CANDLES)),
            stoploss_enabled=bool(use_stoploss),
            target_gain_pct=arm_gain,
            stop_loss_pct=trigger_pct,
        )
    )


STRATEGY_FORGE_QUICK_LOCK = threading.Lock()
STRATEGY_FORGE_QUICK_JOBS: dict[str, dict[str, Any]] = {}
STRATEGY_FORGE_QUICK_MAX_JOBS = 16


def _strategy_forge_quick_fmt_pct(value: Any) -> str:
    try:
        return f"{float(value) * 100.0:.2f}%"
    except Exception:
        return "N/A"


def _strategy_forge_quick_fmt_num(value: Any) -> str:
    try:
        return f"{float(value):.3f}"
    except Exception:
        return "N/A"


def _strategy_forge_quick_fmt_money(value: Any) -> str:
    try:
        numeric = float(value)
    except Exception:
        return "N/A"
    sign = "-" if numeric < 0 else ""
    return f"{sign}${abs(numeric):,.2f}"


def _strategy_forge_quick_sort_key(row: dict[str, Any]) -> tuple[float, float, float, float]:
    metrics = dict(row.get("metrics") or {})
    return (
        float(row.get("score") or 0.0),
        float(metrics.get("total_return") or 0.0),
        float(metrics.get("worst_symbol_return") or 0.0),
        -float(metrics.get("max_drawdown") or 0.0),
    )


def _strategy_forge_quick_candidate_payload(candidate: Any) -> dict[str, Any]:
    if isinstance(candidate, dict):
        return copy.deepcopy(candidate)
    to_dict = getattr(candidate, "to_dict", None)
    if callable(to_dict):
        try:
            payload = to_dict()
            return copy.deepcopy(payload) if isinstance(payload, dict) else {}
        except Exception:
            return {}
    return {}


def _strategy_forge_quick_compact_value(value: Any) -> str:
    if isinstance(value, (dict, list)):
        try:
            return json.dumps(value, sort_keys=True, separators=(",", ":"))
        except Exception:
            return str(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    return str(value)


def _strategy_forge_quick_symbol_returns_html(symbol_returns: Any, symbol_total_profit: Any = None) -> str:
    has_returns = isinstance(symbol_returns, dict) and bool(symbol_returns)
    has_profit = isinstance(symbol_total_profit, dict) and bool(symbol_total_profit)
    if not has_returns and not has_profit:
        return ""
    rows: list[str] = []
    if has_profit:
        profit_bits: list[str] = []
        for symbol, value in list(symbol_total_profit.items())[:12]:
            profit_bits.append(f"{html.escape(str(symbol).upper())} {_strategy_forge_quick_fmt_money(value)}")
        if len(symbol_total_profit) > 12:
            profit_bits.append("...")
        rows.append(f"<strong>Symbol total $ P/L</strong>: {', '.join(profit_bits)}")
    if has_returns:
        return_bits: list[str] = []
        for symbol, value in list(symbol_returns.items())[:12]:
            return_bits.append(f"{html.escape(str(symbol).upper())} {_strategy_forge_quick_fmt_pct(value)}")
        if len(symbol_returns) > 12:
            return_bits.append("...")
        rows.append(f"<strong>Symbol returns</strong>: {', '.join(return_bits)}")
    return "<div style='margin-top:4px;'>" + "<br>".join(rows) + "</div>"


def _strategy_forge_quick_rule_settings_html(candidate: Any) -> str:
    payload = _strategy_forge_quick_candidate_payload(candidate)
    if not payload:
        return ""
    params = payload.get("parameters") if isinstance(payload.get("parameters"), dict) else {}
    risk = payload.get("risk") if isinstance(payload.get("risk"), dict) else {}
    rules = [rule for rule in params.get("rules") or [] if isinstance(rule, dict)]
    rule_count = len(rules)
    entry_threshold = params.get("entry_threshold", rule_count)
    exit_threshold = params.get("exit_threshold", 1)
    execution_tf = str(payload.get("timeframe") or "").strip()
    symbols = payload.get("symbols") if isinstance(payload.get("symbols"), list) else []
    symbol_text = ",".join(str(symbol).upper() for symbol in symbols if str(symbol).strip())
    summary_bits = [
        f"execution_tf={execution_tf or 'N/A'}",
        f"symbols={symbol_text or 'N/A'}",
        f"entry={entry_threshold}/{rule_count}",
        f"exit={exit_threshold}/{rule_count}",
    ]
    for key in ("atr_length", "atr_stop_mult", "atr_trailing"):
        if key in params:
            summary_bits.append(f"{key}={_strategy_forge_quick_compact_value(params.get(key))}")
    rows = [
        "<details style='margin-top:4px;'>",
        "<summary>Exact settings</summary>",
        "<div style='display:flex; flex-direction:column; gap:4px; margin-top:4px;'>",
        f"<div><strong>Combo</strong>: {html.escape(', '.join(summary_bits))}</div>",
    ]
    for idx, rule in enumerate(rules, start=1):
        rule_params = rule.get("params") if isinstance(rule.get("params"), dict) else {}
        tf = str(rule.get("timeframe") or rule_params.get("timeframe") or execution_tf or "").strip()
        kind = str(rule.get("kind") or "rule").strip()
        rule_id = str(rule.get("id") or rule_params.get("rule_id") or "").strip()
        name = str(rule.get("name") or kind).strip()
        param_bits = [
            f"{key}={_strategy_forge_quick_compact_value(rule_params.get(key))}"
            for key in sorted(rule_params.keys())
        ]
        title_bits = [f"#{idx}", f"[{tf or 'N/A'}]", kind]
        if name and name != kind:
            title_bits.append(name)
        if rule_id:
            title_bits.append(f"id={rule_id}")
        rows.append(
            "<div>"
            f"<strong>{html.escape(' '.join(title_bits))}</strong><br>"
            f"<code style='white-space:normal;'>{html.escape(', '.join(param_bits) or 'no params')}</code>"
            "</div>"
        )
    if risk:
        risk_bits = [
            f"{key}={_strategy_forge_quick_compact_value(risk.get(key))}"
            for key in sorted(risk.keys())
        ]
        rows.append(f"<div><strong>Risk</strong>: <code style='white-space:normal;'>{html.escape(', '.join(risk_bits))}</code></div>")
    rows.append("</div></details>")
    return "".join(rows)


def _strategy_forge_quick_public_row(row: dict[str, Any], describe_candidate: Any) -> dict[str, Any]:
    metrics = dict(row.get("metrics") or {})
    candidate = row.get("candidate")
    reasons = list(row.get("reasons") or [])
    timeframes = list(row.get("timeframes") or [])
    symbols = list(row.get("symbols") or [])
    combo_text = str(row.get("combo") or "")
    if not combo_text and candidate is not None:
        try:
            combo_text = str(describe_candidate(candidate))
        except Exception:
            combo_text = "combo unavailable"
    if not timeframes and isinstance(metrics.get("rule_timeframes"), list):
        timeframes = [str(item) for item in metrics.get("rule_timeframes") or []]
    if not symbols and isinstance(metrics.get("tested_symbols"), list):
        symbols = [str(item).upper() for item in metrics.get("tested_symbols") or []]
    if not symbols and candidate is not None:
        symbols = [str(item).upper() for item in getattr(candidate, "symbols", [])]
    if reasons:
        combo_text += " | " + ", ".join(str(item) for item in reasons[:2])
    return {
        "run_id": int(row.get("run_id") or 0),
        "grade": str(row.get("grade") or ""),
        "generation": int(row.get("generation") or 0),
        "evaluation": int(row.get("evaluation") or 0),
        "origin": str(row.get("origin") or ""),
        "symbols": ", ".join(str(item).upper() for item in symbols if str(item).strip()),
        "timeframe": ", ".join(str(item) for item in timeframes) or str(row.get("timeframe") or getattr(candidate, "timeframe", "") or ""),
        "score": float(row.get("score") or 0.0),
        "total_return": metrics.get("total_return"),
        "one_share_net_profit": metrics.get("one_share_net_profit"),
        "worst_symbol_return": metrics.get("worst_symbol_return"),
        "max_drawdown": metrics.get("max_drawdown"),
        "profit_factor": metrics.get("profit_factor"),
        "win_rate": metrics.get("win_rate"),
        "trade_count": int(metrics.get("trade_count") or 0),
        "symbol_returns": dict(metrics.get("symbol_returns") or {}) if isinstance(metrics.get("symbol_returns"), dict) else {},
        "symbol_one_share_net_profit": (
            dict(metrics.get("symbol_one_share_net_profit") or {})
            if isinstance(metrics.get("symbol_one_share_net_profit"), dict)
            else {}
        ),
        "combo": combo_text,
        "settings_html": _strategy_forge_quick_rule_settings_html(candidate),
    }


def _strategy_forge_quick_public_rows(
    rows: list[dict[str, Any]],
    describe_candidate: Any,
    *,
    limit: int = 8,
) -> list[dict[str, Any]]:
    sorted_rows = sorted(rows, key=_strategy_forge_quick_sort_key, reverse=True)
    return [_strategy_forge_quick_public_row(row, describe_candidate) for row in sorted_rows[: max(0, int(limit))]]


def _strategy_forge_quick_seed_candidates(rows: list[dict[str, Any]], *, limit: int = 8) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in sorted(rows, key=_strategy_forge_quick_sort_key, reverse=True)[: max(0, int(limit))]:
        candidate = row.get("candidate")
        if candidate is None:
            continue
        try:
            payload = _strategy_forge_quick_candidate_payload(candidate)
        except Exception:
            continue
        if payload.get("parameters"):
            out.append(payload)
    return out


def _strategy_forge_quick_prune_locked() -> None:
    if len(STRATEGY_FORGE_QUICK_JOBS) <= STRATEGY_FORGE_QUICK_MAX_JOBS:
        return
    ordered = sorted(
        STRATEGY_FORGE_QUICK_JOBS.items(),
        key=lambda item: float(item[1].get("created_ts") or 0.0),
    )
    overflow = max(0, len(ordered) - STRATEGY_FORGE_QUICK_MAX_JOBS)
    removed = 0
    for job_id, job in ordered:
        if removed >= overflow:
            break
        if str(job.get("status") or "") in {"completed", "error"}:
            STRATEGY_FORGE_QUICK_JOBS.pop(job_id, None)
            removed += 1
    for job_id, _job in ordered:
        if removed >= overflow:
            break
        STRATEGY_FORGE_QUICK_JOBS.pop(job_id, None)
        removed += 1


def _strategy_forge_quick_update(job_id: str, **updates: Any) -> None:
    with STRATEGY_FORGE_QUICK_LOCK:
        job = STRATEGY_FORGE_QUICK_JOBS.get(str(job_id))
        if not job:
            return
        job.update(copy.deepcopy(updates))
        job["updated_ts"] = time.time()


def _strategy_forge_quick_snapshot(job_id: str) -> Optional[dict[str, Any]]:
    with STRATEGY_FORGE_QUICK_LOCK:
        job = STRATEGY_FORGE_QUICK_JOBS.get(str(job_id))
        return copy.deepcopy(job) if job else None


def _strategy_forge_quick_event(
    events: list[dict[str, Any]],
    *,
    kind: str,
    generation: int,
    timeframe: str,
    combo: str,
    detail: str = "",
) -> None:
    events.append(
        {
            "kind": str(kind),
            "generation": int(generation),
            "timeframe": str(timeframe),
            "combo": str(combo),
            "detail": str(detail),
        }
    )
    del events[:-12]


def _render_strategy_forge_quick_save_form(
    row: dict[str, Any],
    *,
    broker_hint: str,
    include_extended_hours_data: bool,
    db_path: str,
) -> str:
    run_id = int(row.get("run_id") or 0)
    if run_id <= 0:
        return "<span class='small'>Saved run unavailable.</span>"
    return (
        "<form hx-post='/partials/strategy_forge_save_finalist' hx-target='this' hx-swap='outerHTML' "
        "style='display:flex; flex-direction:column; gap:4px; min-width:120px;'>"
        f"<input type='hidden' name='run_id' value='{run_id}' />"
        f"<input type='hidden' name='broker_hint' value='{html.escape(str(broker_hint or 'robinhood'), quote=True)}' />"
        f"<input type='hidden' name='include_extended_hours_data' value='{'true' if include_extended_hours_data else 'false'}' />"
        f"<input type='hidden' name='db_path' value='{html.escape(str(db_path or ''), quote=True)}' />"
        "<button type='submit' class='btn' style='white-space:nowrap;'>Save as Cryptid</button>"
        "</form>"
    )


def _render_strategy_forge_quick_rows(
    rows: list[dict[str, Any]],
    *,
    final: bool = False,
    broker_hint: str = "robinhood",
    include_extended_hours_data: bool = False,
    db_path: str = "",
) -> str:
    if not rows:
        return "<div class='small'>No evaluated candidates yet.</div>"
    action_head = "<th>Action</th>" if final else ""
    head = (
        "<thead><tr><th>Rank</th><th>Run</th><th>Grade</th><th>Gen</th><th>Eval</th><th>Tickers</th><th>TF</th><th>Origin</th>"
        f"<th>Score</th><th title='Sum of dollar P/L over all completed trades, assuming one share per trade'>Total $ P/L</th><th>Return %</th><th>Worst</th><th>Drawdown</th><th>PF</th><th>Win</th><th>Trades</th><th>Indicators</th>{action_head}</tr></thead>"
    )
    out = ["<div class='status-table-wrap'><table>", head, "<tbody>"]
    for index, row in enumerate(rows, start=1):
        run_id = int(row.get("run_id") or 0)
        run_text = f"#{run_id}" if run_id else ""
        grade = str(row.get("grade") or "")
        if not final and not grade:
            grade = "live"
        settings_html = str(row.get("settings_html") or "")
        symbol_returns_html = _strategy_forge_quick_symbol_returns_html(
            row.get("symbol_returns"),
            row.get("symbol_one_share_net_profit"),
        )
        action_html = (
            _render_strategy_forge_quick_save_form(
                row,
                broker_hint=broker_hint,
                include_extended_hours_data=include_extended_hours_data,
                db_path=db_path,
            )
            if final
            else ""
        )
        out.append(
            "<tr>"
            f"<td>{index}</td>"
            f"<td>{html.escape(run_text)}</td>"
            f"<td>{html.escape(grade)}</td>"
            f"<td>{int(row.get('generation') or 0)}</td>"
            f"<td>{int(row.get('evaluation') or 0)}</td>"
            f"<td class='small'>{html.escape(str(row.get('symbols') or ''))}</td>"
            f"<td>{html.escape(str(row.get('timeframe') or ''))}</td>"
            f"<td>{html.escape(str(row.get('origin') or ''))}</td>"
            f"<td>{_strategy_forge_quick_fmt_num(row.get('score'))}</td>"
            f"<td>{_strategy_forge_quick_fmt_money(row.get('one_share_net_profit'))}</td>"
            f"<td>{_strategy_forge_quick_fmt_pct(row.get('total_return'))}</td>"
            f"<td>{_strategy_forge_quick_fmt_pct(row.get('worst_symbol_return'))}</td>"
            f"<td>{_strategy_forge_quick_fmt_pct(row.get('max_drawdown'))}</td>"
            f"<td>{_strategy_forge_quick_fmt_num(row.get('profit_factor'))}</td>"
            f"<td>{_strategy_forge_quick_fmt_pct(row.get('win_rate'))}</td>"
            f"<td>{int(row.get('trade_count') or 0)}</td>"
            f"<td class='small'><div>{html.escape(str(row.get('combo') or ''))}</div>{symbol_returns_html}{settings_html}</td>"
            + (f"<td>{action_html}</td>" if final else "")
            + "</tr>"
        )
    out.append("</tbody></table></div>")
    return "".join(out)


def _render_strategy_forge_quick_status_html(job_id: str) -> str:
    job = _strategy_forge_quick_snapshot(job_id)
    if not job:
        return "<div class='small' data-forge-status='error'>Strategy Forge run was not found. Start a new run.</div>"

    status = str(job.get("status") or "queued")
    polling = status in {"queued", "running"}
    escaped_job_id = html.escape(str(job_id), quote=True)
    poll_attrs = (
        f' hx-get="/partials/strategy_forge_quick_status?job_id={escaped_job_id}"'
        ' hx-trigger="every 1s" hx-swap="outerHTML"'
        if polling
        else ""
    )
    evaluated = int(job.get("evaluated_count") or 0)
    trial_count = max(1, int(job.get("trial_count") or 1))
    generation = int(job.get("generation_index") or 0)
    population_index = int(job.get("population_index") or 0)
    population_size = int(job.get("population_size") or 0)
    stale_generations = int(job.get("stale_generations") or 0)
    patience = int(job.get("patience") or 0)
    progress = max(0.0, min(100.0, (float(evaluated) / float(trial_count)) * 100.0))
    phase = html.escape(str(job.get("phase") or status))
    symbols_txt = html.escape(", ".join(str(item) for item in list(job.get("active_symbols") or job.get("symbol_list") or [])))
    timeframe_txt = html.escape(str(job.get("timeframe") or ""))
    current_combo = html.escape(str(job.get("current_combo") or "Waiting for first candidate."))
    current_origin = html.escape(str(job.get("current_origin") or ""))
    best_score = job.get("best_score")
    current_score = job.get("current_score")
    timeframe_pool = list(job.get("timeframe_pool") or [])
    min_rules = int(job.get("min_rules") or 2)
    max_rules = int(job.get("max_rules") or min_rules)
    broker_hint = str(job.get("broker_hint") or "robinhood")
    include_extended = bool(job.get("include_extended_hours_data"))
    seed_mode = str(job.get("seed_mode") or "random")
    seed_source_job_id = str(job.get("seed_source_job_id") or job.get("seed_job_id") or "")
    seed_count = int(job.get("seed_count") or 0)
    data_errors = list(job.get("data_errors") or [])
    error = str(job.get("error") or "")
    leaderboard = list(job.get("leaderboard") or [])
    generation_rows = list(job.get("generation_rows") or [])
    events = list(job.get("events") or [])

    out = [
        f"<div class='strategy-forge-live' data-forge-status='{html.escape(status, quote=True)}'{poll_attrs}>",
        "<div class='row' style='align-items:center; gap:10px; margin-bottom:8px;'>",
        "<div><strong>Strategy Forge</strong></div>",
        f"<div><strong>{html.escape(status.title())}</strong></div>",
        f"<div class='small'>Phase: {phase}</div>",
        f"<div class='small'>Symbols: {symbols_txt or 'N/A'}</div>",
        f"<div class='small'>Execution TF: {timeframe_txt or 'N/A'}</div>",
        f"<div class='small'>Rule TF pool: {html.escape(', '.join(str(item) for item in timeframe_pool) if timeframe_pool else (timeframe_txt or 'N/A'))}</div>",
        f"<div class='small'>Indicators/combo: {min_rules}-{max_rules}</div>",
        f"<div class='small'>Seed: {html.escape(f'top combos from {seed_source_job_id[:8]}' if seed_mode == 'leaderboard' and seed_source_job_id else 'random')}</div>",
        f"<div class='small'>Evaluations: {evaluated}/{trial_count}</div>",
        f"<div class='small'>Generation: {generation}</div>",
        "</div>",
        "<div style='height:8px; background:#eceff3; border-radius:999px; overflow:hidden; margin:6px 0 10px 0;'>",
        f"<div style='height:8px; width:{progress:.1f}%; background:#2f7dd3;'></div>",
        "</div>",
        "<div class='row' style='align-items:flex-start; gap:12px; margin-bottom:10px;'>",
        "<div class='small' style='min-width:220px;'>"
        f"Population: {population_index}/{population_size or 'N/A'} &nbsp; "
        f"Stale generations: {stale_generations}/{patience or 'N/A'} &nbsp; "
        f"Best score: {_strategy_forge_quick_fmt_num(best_score)}"
        "</div>",
        "<div class='small' style='min-width:220px;'>"
        f"Current score: {_strategy_forge_quick_fmt_num(current_score)} &nbsp; "
        f"Origin: {current_origin or 'N/A'}"
        "</div>",
        "</div>",
        "<div class='small' style='margin:6px 0 10px 0;'><strong>Current combo</strong>: "
        f"{current_combo}</div>",
    ]
    if events:
        out.append("<div class='small' style='margin:6px 0 10px 0;'><strong>Recent evolution/mutation activity</strong>: ")
        event_bits: list[str] = []
        for event in events[-6:]:
            bit = (
                f"G{int(event.get('generation') or 0)} "
                f"{html.escape(str(event.get('kind') or 'event'))} "
                f"{html.escape(str(event.get('timeframe') or ''))}: "
                f"{html.escape(str(event.get('combo') or ''))}"
            )
            detail = str(event.get("detail") or "")
            if detail:
                bit += f" ({html.escape(detail)})"
            event_bits.append(bit)
        out.append(" | ".join(event_bits))
        out.append("</div>")
    if error:
        out.append(f"<div class='small' style='margin:6px 0 10px 0;'>Error: {html.escape(error)}</div>")
    if data_errors:
        out.append(
            "<div class='small' style='margin:6px 0 10px 0;'>Skipped data: "
            + html.escape("; ".join(str(item) for item in data_errors[:4]))
            + "</div>"
        )
    if status == "completed" and leaderboard:
        out.append(
            "<div class='row' style='align-items:center; gap:8px; margin:8px 0 10px 0;'>"
            f"<button type='button' class='btn primary' data-strategy-forge-action='continue' "
            f"data-strategy-forge-job-id='{escaped_job_id}'>Run Again From Top Combos</button>"
            "<button type='button' class='btn' data-strategy-forge-action='random'>New Random Set</button>"
            f"<div class='small'>Continuation seed candidates: {seed_count or len(leaderboard)}</div>"
            "</div>"
        )
    db_path = str(job.get("db_path") or "")
    leaderboard_title = "Finalist combinations" if status == "completed" else "Live leaderboard"
    out.append(f"<div class='small' style='margin:8px 0 4px 0;'><strong>{leaderboard_title}</strong></div>")
    out.append(
        _render_strategy_forge_quick_rows(
            leaderboard,
            final=status == "completed",
            broker_hint=broker_hint,
            include_extended_hours_data=include_extended,
            db_path=db_path,
        )
    )
    if generation_rows and status in {"queued", "running"}:
        out.append("<div class='small' style='margin:10px 0 4px 0;'><strong>Current generation candidates</strong></div>")
        out.append(_render_strategy_forge_quick_rows(generation_rows[:8], final=False))
    if db_path:
        out.append(f"<div class='small' style='margin-top:6px;'>DB: {html.escape(db_path)}</div>")
    out.append("</div>")
    return "".join(out)


def _strategy_forge_quick_worker(job_id: str, config: dict[str, Any]) -> None:
    try:
        from strategy_forge import DEFAULT_DB_PATH as FORGE_DEFAULT_DB_PATH
        from strategy_forge.backtest_runner import BacktestConfig, BacktestResult, run_backtest
        from strategy_forge.combo_search import (
            COMBO_TIMEFRAMES,
            OpenComboGenerator,
            aggregate_result_metrics,
            build_combo_candidate,
            candidate_signature,
            candidate_rule_timeframes,
            combo_search_score,
            describe_candidate,
            grade_combo_metrics,
            normalize_timeframe,
            normalize_symbols,
        )
        from strategy_forge.data_loader import normalize_rows
        from strategy_forge.result_store import store_backtest_result, store_robustness_test
        from strategy_forge.strategy_templates import StrategyCandidate
    except Exception as exc:
        _strategy_forge_quick_update(
            job_id,
            status="error",
            phase="import failed",
            error=f"Strategy Forge unavailable: {exc}",
        )
        return

    symbol_list = list(config.get("symbol_list") or [])
    tf = normalize_timeframe(config.get("timeframe") or "1h")
    trial_count = max(1, min(int(config.get("trials") or 250), 5000))
    min_trade_count = max(1, min(int(config.get("min_trades") or 100), 10000))
    min_rule_count = max(2, min(int(config.get("min_rules") or 2), 30))
    max_rule_count = max(min_rule_count, min(int(config.get("max_rules") or 5), 30))
    source_hint = _normalize_market_source_hint(str(config.get("broker_hint") or "robinhood"))
    include_extended = True if source_hint == "robinhood_crypto" else bool(config.get("include_extended_hours_data"))
    forge_db_path = Path(config.get("db_path")) if config.get("db_path") is not None else FORGE_DEFAULT_DB_PATH
    seed_mode = str(config.get("seed_mode") or "random").strip().lower()
    if seed_mode not in {"leaderboard"}:
        seed_mode = "random"
    seed_source_job_id = str(config.get("seed_job_id") or "").strip()
    seed_payloads: list[dict[str, Any]] = []
    if seed_mode == "leaderboard" and seed_source_job_id:
        source_job = _strategy_forge_quick_snapshot(seed_source_job_id)
        seed_payloads = [
            dict(item)
            for item in list((source_job or {}).get("seed_candidates") or [])
            if isinstance(item, dict)
        ]
    if seed_mode == "leaderboard" and not seed_payloads:
        seed_mode = "random"
        seed_source_job_id = ""
    events: list[dict[str, Any]] = []

    try:
        _strategy_forge_quick_update(
            job_id,
            status="running",
            phase="checking market data session",
            timeframe=tf,
            trial_count=trial_count,
            min_trades=min_trade_count,
            min_rules=min_rule_count,
            max_rules=max_rule_count,
            broker_hint=source_hint,
            include_extended_hours_data=include_extended,
            seed_mode=seed_mode,
            seed_source_job_id=seed_source_job_id,
            seed_count=len(seed_payloads),
            db_path=str(forge_db_path),
        )

        if source_hint in ("robinhood", "robinhood_crypto"):
            ok, msg = _ensure_robinhood_markets_session()
            if not ok:
                _strategy_forge_quick_update(
                    job_id,
                    status="error",
                    phase="session unavailable",
                    error=f"Strategy Forge unavailable: {msg}",
                )
                return

        candidate_timeframes: list[str] = []
        for item in (COMBO_TIMEFRAMES or ()):
            normalized_tf = normalize_timeframe(item, default=tf)
            if normalized_tf not in candidate_timeframes:
                candidate_timeframes.append(normalized_tf)
        if tf not in candidate_timeframes:
            candidate_timeframes.insert(0, tf)

        def _load_strategy_forge_data(symbol: str, timeframe_key: str) -> Any:
            opens, highs, lows, closes, raw_rows, requested_bounds = _market_fetch_ohlc(
                symbol,
                timeframe_key,
                broker_hint=source_hint,
                min_candles=360,
                include_extended=include_extended,
            )
            if raw_rows:
                return normalize_rows(raw_rows, symbol=symbol, timeframe=timeframe_key, source=f"ui:{requested_bounds}")
            rows = [
                {
                    "timestamp": str(i),
                    "open": opens[i],
                    "high": highs[i],
                    "low": lows[i],
                    "close": closes[i],
                    "volume": 0.0,
                }
                for i in range(min(len(opens), len(highs), len(lows), len(closes)))
            ]
            return normalize_rows(rows, symbol=symbol, timeframe=timeframe_key, source="ui:synthetic_ohlc")

        datasets = []
        datasets_by_symbol: dict[str, dict[str, Any]] = {}
        candle_counts_by_symbol: dict[str, dict[str, int]] = {}
        data_errors: list[str] = []
        for symbol in symbol_list:
            symbol_tf_map: dict[str, Any] = {}
            count_map: dict[str, int] = {}
            for candidate_tf in candidate_timeframes:
                _strategy_forge_quick_update(
                    job_id,
                    phase=f"loading {candidate_tf} candles for {symbol}",
                    data_errors=data_errors,
                    timeframe_pool=candidate_timeframes,
                    symbol_timeframe_counts=candle_counts_by_symbol,
                )
                try:
                    data = _load_strategy_forge_data(symbol, candidate_tf)
                    count_map[candidate_tf] = int(len(data))
                    if len(data) < 80:
                        if candidate_tf == tf:
                            data_errors.append(f"{symbol} {candidate_tf}: only {len(data)} candles")
                        continue
                    symbol_tf_map[candidate_tf] = data
                except Exception as exc:
                    count_map[candidate_tf] = 0
                    if candidate_tf == tf:
                        data_errors.append(f"{symbol} {candidate_tf}: {exc}")
            candle_counts_by_symbol[str(symbol).upper()] = count_map
            execution_data = symbol_tf_map.get(tf)
            if execution_data is None:
                data_errors.append(f"{symbol}: no usable {tf} execution candles")
                continue
            datasets.append(execution_data)
            datasets_by_symbol[str(execution_data.symbol).upper()] = symbol_tf_map

        if not datasets:
            _strategy_forge_quick_update(
                job_id,
                status="error",
                phase="data unavailable",
                data_errors=data_errors,
                error="Strategy Forge needs more usable candles.",
            )
            return

        active_symbols = [str(data.symbol).upper() for data in datasets]
        rule_timeframe_pool = [
            candidate_tf
            for candidate_tf in candidate_timeframes
            if all(candidate_tf in datasets_by_symbol.get(sym, {}) for sym in active_symbols)
        ]
        if tf not in rule_timeframe_pool:
            rule_timeframe_pool.insert(0, tf)
        rule_timeframe_pool = [item for i, item in enumerate(rule_timeframe_pool) if item and item not in rule_timeframe_pool[:i]]
        generator = OpenComboGenerator(
            seed=int(time.time()),
            min_rules=min_rule_count,
            max_rules=max_rule_count,
            timeframes=tuple(rule_timeframe_pool),
            universe_symbols=tuple(active_symbols),
        )
        bt_config = BacktestConfig(
            initial_capital=100000.0,
            commission_pct=0.0005,
            slippage_bps=2.0,
        )

        def _candidate_from_seed_payload(payload: dict[str, Any]) -> Optional[StrategyCandidate]:
            try:
                seeded = StrategyCandidate.from_dict(dict(payload))
                seeded_params = dict(seeded.parameters or {})
                seeded_risk = dict(seeded.risk or {})
                seed_rules = list(copy.deepcopy(seeded_params.get("rules") or []))
                if len(seed_rules) < 2:
                    return None
                for rule in seed_rules:
                    if not isinstance(rule, dict):
                        continue
                    params = rule.get("params") if isinstance(rule.get("params"), dict) else {}
                    rule_tf = normalize_timeframe(rule.get("timeframe") or params.get("timeframe"), default=tf)
                    if rule_tf not in rule_timeframe_pool:
                        rule["timeframe"] = tf
                seed_symbols = [symbol for symbol in normalize_symbols(seeded.symbols) if symbol in active_symbols] or active_symbols
                return build_combo_candidate(
                    symbols=seed_symbols,
                    timeframe=tf,
                    rules=seed_rules,
                    entry_threshold=seeded_params.get("entry_threshold"),
                    exit_threshold=seeded_params.get("exit_threshold"),
                    atr_length=int(seeded_params.get("atr_length") or 14),
                    atr_stop_mult=float(seeded_params.get("atr_stop_mult") or 2.5),
                    atr_trailing=bool(seeded_params.get("atr_trailing", True)),
                    profit_target_atr_mult=seeded_risk.get("profit_target_atr_mult"),
                    max_position_pct=float(seeded_risk.get("max_position_pct") or 0.15),
                    max_daily_loss_pct=float(seeded_risk.get("max_daily_loss_pct") or 0.03),
                    max_trades_per_day=int(seeded_risk.get("max_trades_per_day") or 8),
                )
            except Exception:
                return None

        def _evaluate_candidate(candidate: Any, generation: int, evaluation: int, origin: str) -> Optional[dict[str, Any]]:
            try:
                candidate = copy.deepcopy(candidate)
                candidate_symbols = [
                    symbol for symbol in normalize_symbols(getattr(candidate, "symbols", [])) if symbol in active_symbols
                ] or list(active_symbols)
                candidate.symbols = candidate_symbols
                rule_timeframes = [normalize_timeframe(item, default=tf) for item in candidate_rule_timeframes(candidate)]
                symbol_results = []
                for sym in candidate_symbols:
                    symbol_tf_map = datasets_by_symbol.get(sym, {})
                    data = symbol_tf_map.get(tf)
                    if data is None:
                        raise ValueError(f"{sym} missing execution candles for {tf}")
                    missing = [rule_tf for rule_tf in rule_timeframes if rule_tf not in symbol_tf_map]
                    if missing:
                        raise ValueError(f"{sym} missing candles for rule timeframe(s): {', '.join(missing)}")
                    tf_data_map = {rule_tf: symbol_tf_map[rule_tf] for rule_tf in rule_timeframes}
                    tf_data_map[tf] = data
                    symbol_results.append(
                        run_backtest(
                            data,
                            copy.deepcopy(candidate),
                            bt_config,
                            data_by_timeframe=tf_data_map,
                        )
                    )
                metrics = aggregate_result_metrics(symbol_results)
                metrics["rule_timeframes"] = rule_timeframes
                metrics["tested_symbols"] = candidate_symbols
                metrics["symbol_count"] = len(candidate_symbols)
                score = combo_search_score(metrics, min_trades=min_trade_count)
                return {
                    "candidate": candidate,
                    "generation": generation,
                    "evaluation": evaluation,
                    "origin": origin,
                    "timeframe": tf,
                    "timeframes": rule_timeframes,
                    "score": float(score),
                    "metrics": metrics,
                    "symbols": candidate_symbols,
                    "symbol_results": symbol_results,
                }
            except Exception as exc:
                _strategy_forge_quick_update(job_id, error=f"Last evaluation failed: {exc}")
                return None

        population_size = max(2, min(40, max(2, trial_count // 4)))
        elite_count = max(1, min(8, population_size // 4))
        patience = max(3, min(30, trial_count // max(1, population_size)))
        generation_index = 0
        evaluated_count = 0
        best_score = -999999.0
        stale_generations = 0
        seen: set[str] = set()
        results: list[dict[str, Any]] = []

        seed_candidates: list[StrategyCandidate] = []
        for payload in seed_payloads[: max(population_size, elite_count, 8)]:
            candidate = _candidate_from_seed_payload(payload)
            if candidate is not None:
                seed_candidates.append(candidate)
        unique_seed_candidates: list[StrategyCandidate] = []
        seed_signatures: set[str] = set()
        for candidate in seed_candidates:
            signature = candidate_signature(candidate)
            if signature in seed_signatures:
                continue
            seed_signatures.add(signature)
            unique_seed_candidates.append(candidate)
        seed_candidates = unique_seed_candidates

        population: list[dict[str, Any]] = []
        initial_seen: set[str] = set()

        def _push_initial_candidate(candidate: Any, *, origin: str, detail: str) -> bool:
            signature = candidate_signature(candidate)
            if signature in initial_seen:
                return False
            initial_seen.add(signature)
            population.append({"candidate": candidate, "origin": origin, "detail": detail})
            return True

        for candidate in seed_candidates[: min(population_size, trial_count)]:
            _push_initial_candidate(
                candidate,
                origin="top seed",
                detail=f"from {seed_source_job_id[:8]}" if seed_source_job_id else "leaderboard",
            )

        initial_attempts = 0
        max_initial_attempts = max(20, population_size * 50)
        while len(population) < min(population_size, trial_count) and initial_attempts < max_initial_attempts:
            initial_attempts += 1
            if seed_candidates and generator.random.random() < 0.80:
                if len(seed_candidates) >= 2 and generator.random.random() < 0.55:
                    parents = generator.random.sample(seed_candidates, 2)
                    child = generator.crossover_candidates(parents[0], parents[1])
                    origin = "seed crossover"
                    detail = "top combo parents"
                else:
                    parent = generator.random.choice(seed_candidates)
                    child = generator.mutate_candidate(parent)
                    origin = "seed mutation"
                    detail = "top combo mutation"
            else:
                child = generator.random_candidate(symbols=active_symbols, timeframe=tf)
                origin = "random seed"
                detail = "initial population"
            _push_initial_candidate(child, origin=origin, detail=detail)

        _strategy_forge_quick_update(
            job_id,
            status="running",
            phase="evolving seeded population" if seed_candidates else "evolving population",
            active_symbols=active_symbols,
            data_errors=data_errors,
            population_size=population_size,
            elite_count=elite_count,
            patience=patience,
            timeframe_pool=rule_timeframe_pool,
            symbol_timeframe_counts=candle_counts_by_symbol,
            generation_index=generation_index,
            evaluated_count=evaluated_count,
            stale_generations=stale_generations,
            best_score=None,
            leaderboard=[],
            generation_rows=[],
            seed_mode="leaderboard" if seed_candidates else "random",
            seed_source_job_id=seed_source_job_id if seed_candidates else "",
            seed_count=len(seed_candidates),
        )

        while population and evaluated_count < trial_count and stale_generations < patience:
            generation_best = -999999.0
            generation_results: list[dict[str, Any]] = []
            _strategy_forge_quick_update(
                job_id,
                phase="evaluating generation",
                generation_index=generation_index,
                population_size=len(population),
                population_index=0,
                generation_rows=[],
            )
            for population_index, record in enumerate(population, start=1):
                candidate = record["candidate"]
                signature = candidate_signature(candidate)
                if signature in seen:
                    continue
                seen.add(signature)
                origin = str(record.get("origin") or "candidate")
                combo_text = describe_candidate(candidate)
                _strategy_forge_quick_update(
                    job_id,
                    phase="evaluating candidate",
                    generation_index=generation_index,
                    population_index=population_index,
                    population_size=len(population),
                    evaluated_count=evaluated_count,
                    current_origin=origin,
                    current_combo=combo_text,
                    current_score=None,
                )
                row = _evaluate_candidate(candidate, generation_index, evaluated_count + 1, origin)
                evaluated_count += 1
                if row is not None:
                    results.append(row)
                    generation_results.append(row)
                    generation_best = max(generation_best, float(row["score"]))
                    leaderboard_rows = _strategy_forge_quick_public_rows(results, describe_candidate, limit=8)
                    leaderboard_seeds = _strategy_forge_quick_seed_candidates(results, limit=8)
                    _strategy_forge_quick_update(
                        job_id,
                        evaluated_count=evaluated_count,
                        current_score=float(row["score"]),
                        leaderboard=leaderboard_rows,
                        seed_candidates=leaderboard_seeds,
                        seed_count=len(leaderboard_seeds),
                        generation_rows=_strategy_forge_quick_public_rows(generation_results, describe_candidate, limit=8),
                    )
                else:
                    _strategy_forge_quick_update(job_id, evaluated_count=evaluated_count)
                if evaluated_count >= trial_count:
                    break

            results.sort(key=_strategy_forge_quick_sort_key, reverse=True)
            if generation_best > best_score + 0.000001:
                best_score = generation_best
                stale_generations = 0
            else:
                stale_generations += 1
            generation_index += 1
            display_best_score = None if best_score <= -999998.0 else best_score
            leaderboard_rows = _strategy_forge_quick_public_rows(results, describe_candidate, limit=8)
            leaderboard_seeds = _strategy_forge_quick_seed_candidates(results, limit=8)
            _strategy_forge_quick_update(
                job_id,
                phase="generation complete",
                generation_index=generation_index,
                evaluated_count=evaluated_count,
                stale_generations=stale_generations,
                best_score=display_best_score,
                leaderboard=leaderboard_rows,
                seed_candidates=leaderboard_seeds,
                seed_count=len(leaderboard_seeds),
                generation_rows=_strategy_forge_quick_public_rows(generation_results, describe_candidate, limit=8),
            )
            if evaluated_count >= trial_count or stale_generations >= patience:
                break

            elites = [row["candidate"] for row in results[:elite_count]]
            next_population: list[dict[str, Any]] = []
            next_seen: set[str] = set()
            _strategy_forge_quick_update(job_id, phase="breeding next generation")
            breeding_attempts = 0
            max_breeding_attempts = max(20, population_size * 50)
            while (
                len(next_population) < population_size
                and evaluated_count + len(next_population) < trial_count
                and breeding_attempts < max_breeding_attempts
            ):
                breeding_attempts += 1
                detail = ""
                if len(elites) >= 2 and generator.random.random() < 0.60:
                    parents = generator.random.sample(elites, 2)
                    child = generator.crossover_candidates(parents[0], parents[1])
                    origin = "crossover"
                    detail = "elite parents"
                elif elites and generator.random.random() < 0.85:
                    parent = generator.random.choice(elites)
                    child = generator.mutate_candidate(parent)
                    origin = "mutation"
                    detail = "elite mutation"
                else:
                    child = generator.random_candidate(symbols=active_symbols, timeframe=tf)
                    origin = "random immigrant"
                    detail = "new candidate"
                signature = candidate_signature(child)
                if signature in seen or signature in next_seen:
                    continue
                next_seen.add(signature)
                combo_text = describe_candidate(child)
                _strategy_forge_quick_event(
                    events,
                    kind=origin,
                    generation=generation_index,
                    timeframe=", ".join(candidate_rule_timeframes(child)),
                    combo=combo_text,
                    detail=detail,
                )
                next_population.append({"candidate": child, "origin": origin, "detail": detail})
                _strategy_forge_quick_update(
                    job_id,
                    events=events,
                    current_origin=origin,
                    current_combo=combo_text,
                    population_index=len(next_population),
                    population_size=population_size,
                )
            if not next_population:
                _strategy_forge_quick_update(job_id, phase="candidate pool exhausted")
            population = next_population

        if not results:
            _strategy_forge_quick_update(
                job_id,
                status="error",
                phase="no valid combo",
                error="Strategy Forge did not produce a valid evolved combo. Try more candles or more evaluations.",
            )
            return

        top_rows = results[: min(8, len(results))]
        _strategy_forge_quick_update(job_id, phase="saving top performers")
        for row in top_rows:
            metrics = dict(row["metrics"])
            grade, reasons, robustness_score = grade_combo_metrics(metrics, min_trades=min_trade_count)
            row_candidate = row["candidate"]
            row_symbols = [
                symbol for symbol in normalize_symbols(getattr(row_candidate, "symbols", [])) if symbol in active_symbols
            ] or list(active_symbols)
            aggregate_result = BacktestResult(
                candidate=row_candidate,
                symbol=",".join(row_symbols),
                timeframe=tf,
                metrics=metrics,
                trades=[],
                equity_curve=[],
            )
            try:
                run_id = store_backtest_result(
                    aggregate_result,
                    db_path=forge_db_path,
                    in_sample_score=float(row["score"]),
                    validation_score=0.0,
                    out_of_sample_score=0.0,
                    walk_forward_score=0.0,
                    robustness_score=float(robustness_score),
                    final_grade=grade,
                )
                store_robustness_test(
                    run_id,
                    {
                        "rejected": grade == "Reject",
                        "reasons": reasons,
                        "parameter_stability_score": 0.0,
                        "symbol_stability_score": 1.0 if float(metrics.get("worst_symbol_return") or 0.0) > 0 else 0.5,
                        "time_window_stability_score": 0.0,
                        "regime_score": 0.0,
                        "monte_carlo_score": 0.0,
                        "robustness_score": robustness_score,
                        "final_grade": grade,
                        "instability_penalty": 1.0 - float(robustness_score),
                    },
                    db_path=forge_db_path,
                )
            except Exception:
                run_id = 0
            row["run_id"] = int(run_id)
            row["grade"] = grade
            row["reasons"] = reasons

        final_leaderboard = _strategy_forge_quick_public_rows(top_rows, describe_candidate, limit=8)
        final_seed_candidates = _strategy_forge_quick_seed_candidates(top_rows, limit=8)
        _strategy_forge_quick_update(
            job_id,
            status="completed",
            phase="complete",
            generation_index=generation_index,
            evaluated_count=evaluated_count,
            stale_generations=stale_generations,
            best_score=None if best_score <= -999998.0 else best_score,
            current_origin="winner",
            current_combo=describe_candidate(top_rows[0]["candidate"]),
            current_score=float(top_rows[0]["score"]),
            leaderboard=final_leaderboard,
            seed_candidates=final_seed_candidates,
            seed_count=len(final_seed_candidates),
            generation_rows=[],
            events=events,
            data_errors=data_errors,
        )
    except Exception as exc:
        _strategy_forge_quick_update(
            job_id,
            status="error",
            phase="failed",
            error=str(exc),
        )


def _strategy_forge_quick_indicatorforge_script_path(broker_hint: str) -> str:
    hint = _normalize_market_source_hint(str(broker_hint or "robinhood"))
    if hint == "robinhood_crypto":
        return "scripts/indicatorforge.crypto.robinhood.py"
    if hint == "schwab":
        return "scripts/indicatorforge.schwab.py"
    return "scripts/indicatorforge.robinhood.py"


def _strategy_forge_quick_default_params_for_script(script_path: str) -> dict[str, Any]:
    defs = _base_algo_form_defs().get(_normalize_script_path(script_path), {})
    out: dict[str, Any] = {}
    for item in defs.get("params") or []:
        if not isinstance(item, dict):
            continue
        key = str(item.get("key") or "").strip()
        if not key or "default" not in item:
            continue
        out[key] = copy.deepcopy(item.get("default"))
    return out


def _strategy_forge_quick_saved_name(run_id: int, candidate: dict[str, Any], timeframe: str) -> str:
    symbols = candidate.get("symbols") if isinstance(candidate.get("symbols"), list) else []
    symbol_text = ", ".join(str(symbol).upper() for symbol in symbols[:3] if str(symbol).strip())
    if len(symbols) > 3:
        symbol_text += f" +{len(symbols) - 3}"
    base = f"Forge #{int(run_id)}"
    if symbol_text:
        base += f" {symbol_text}"
    if timeframe:
        base += f" {timeframe}"
    return base


@app.post("/partials/strategy_forge_save_finalist", response_class=HTMLResponse)
def partial_strategy_forge_save_finalist(
    run_id: int = Form(...),
    broker_hint: str = Form("robinhood"),
    include_extended_hours_data: str = Form("false"),
    db_path: str = Form(""),
):
    try:
        from strategy_forge import DEFAULT_DB_PATH as FORGE_DEFAULT_DB_PATH
        from strategy_forge.paper_trade_exporter import indicatorforge_rules
        from strategy_forge.result_store import get_run
    except Exception as exc:
        return HTMLResponse(f"<span class='small'>Strategy Forge unavailable: {html.escape(str(exc))}</span>", status_code=500)

    try:
        forge_db_path = Path(str(db_path).strip()) if str(db_path or "").strip() else FORGE_DEFAULT_DB_PATH
        run = get_run(int(run_id), db_path=forge_db_path)
    except Exception as exc:
        return HTMLResponse(f"<span class='small'>Could not load finalist: {html.escape(str(exc))}</span>", status_code=400)
    if not run:
        return HTMLResponse("<span class='small'>Finalist run not found.</span>", status_code=404)

    candidate = run.get("candidate") if isinstance(run.get("candidate"), dict) else {}
    rules = indicatorforge_rules(candidate)
    if not rules:
        return HTMLResponse("<span class='small'>No IndicatorForge-compatible rules were found for this finalist.</span>", status_code=400)

    script_path = _strategy_forge_quick_indicatorforge_script_path(broker_hint)
    discover_base_scripts()
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM base_scripts WHERE lower(path)=?", (script_path.lower(),))
    base_row = cur.fetchone()
    if not base_row:
        conn.close()
        return HTMLResponse(f"<span class='small'>Base script not found: {html.escape(script_path)}</span>", status_code=400)

    include_extended = str(include_extended_hours_data or "").strip().lower() in ("1", "true", "yes", "on")
    symbols = candidate.get("symbols") if isinstance(candidate.get("symbols"), list) else []
    if not symbols:
        symbols = [s.strip().upper() for s in str(run.get("symbol") or "").split(",") if s.strip()]
    timeframe = str(candidate.get("timeframe") or run.get("timeframe") or "1h").strip().lower() or "1h"
    params = _strategy_forge_quick_default_params_for_script(script_path)
    params.update(
        {
            "symbols": [str(symbol).upper() for symbol in symbols if str(symbol).strip()],
            "timeframe": timeframe,
            "include_extended_hours_data": include_extended,
            "indicator_rules_json": json.dumps(rules, sort_keys=True),
            "strategy_forge_source_run_id": int(run_id),
            "strategy_forge_candidate_json": json.dumps(candidate, sort_keys=True),
        }
    )
    candidate_params = candidate.get("parameters") if isinstance(candidate.get("parameters"), dict) else {}
    if candidate_params:
        combo_rule_count = len(candidate_params.get("rules") or [])
        params["strategy_forge_entry_threshold"] = int(candidate_params.get("entry_threshold") or combo_rule_count or len(rules))
        params["strategy_forge_exit_threshold"] = int(candidate_params.get("exit_threshold") or 1)
        params["strategy_forge_combo_rule_count"] = combo_rule_count
    name = _strategy_forge_quick_saved_name(int(run_id), candidate, timeframe)
    params_json = _sanitize_algorithm_params_for_script(json.dumps(params), script_path)
    cur.execute(
        """INSERT INTO algorithms
        (name, base_script_id, rulesets_json, params_json, max_runtime_min, restart_on_crash, log_level, created_ts)
        VALUES (?,?,?,?,?,?,?,?)""",
        (
            name,
            int(base_row["id"]),
            json.dumps([]),
            params_json,
            0,
            1,
            "INFO",
            _utc_ts(),
        ),
    )
    algo_id = int(cur.lastrowid)
    conn.commit()
    conn.close()
    escaped_name = html.escape(name)
    return HTMLResponse(
        f"<span class='small'>Saved <a href='/algorithms/{algo_id}/edit'>{escaped_name}</a>.</span>"
    )


def _strategy_forge_quick_start_job(
    *,
    timeframe: str,
    symbols: str,
    trials: int,
    min_trades: int,
    min_rules: int = 2,
    max_rules: int = 5,
    broker_hint: str = "robinhood",
    include_extended_hours_data: bool = False,
    db_path: Optional[str | Path] = None,
    template: str = "",
    seed_mode: str = "random",
    seed_job_id: str = "",
) -> str:
    symbol_list = _clean_symbol_list(symbols)
    job_id = uuid4().hex
    tf = str(timeframe or "1h").strip().lower() or "1h"
    trial_count = max(1, min(int(trials or 250), 5000))
    min_trade_count = max(1, min(int(min_trades or 100), 10000))
    min_rule_count = max(2, min(int(min_rules or 2), 30))
    max_rule_count = max(min_rule_count, min(int(max_rules or 5), 30))
    normalized_seed_mode = str(seed_mode or "random").strip().lower()
    if normalized_seed_mode not in {"leaderboard"}:
        normalized_seed_mode = "random"
    normalized_seed_job_id = str(seed_job_id or "").strip() if normalized_seed_mode == "leaderboard" else ""
    now = time.time()
    with STRATEGY_FORGE_QUICK_LOCK:
        STRATEGY_FORGE_QUICK_JOBS[job_id] = {
            "id": job_id,
            "status": "queued" if symbol_list else "error",
            "phase": "queued" if symbol_list else "input needed",
            "error": "" if symbol_list else "Enter at least one symbol before running Strategy Forge.",
            "created_ts": now,
            "updated_ts": now,
            "symbol_list": symbol_list,
            "active_symbols": [],
            "timeframe": tf,
            "trial_count": trial_count,
            "min_trades": min_trade_count,
            "min_rules": min_rule_count,
            "max_rules": max_rule_count,
            "generation_index": 0,
            "evaluated_count": 0,
            "population_index": 0,
            "population_size": 0,
            "stale_generations": 0,
            "patience": 0,
            "best_score": None,
            "current_score": None,
            "current_origin": "",
            "current_combo": "",
            "leaderboard": [],
            "generation_rows": [],
            "seed_candidates": [],
            "seed_mode": normalized_seed_mode,
            "seed_job_id": normalized_seed_job_id,
            "seed_source_job_id": normalized_seed_job_id,
            "seed_count": 0,
            "events": [],
            "data_errors": [],
            "db_path": str(db_path or ""),
            "template": str(template or ""),
        }
        _strategy_forge_quick_prune_locked()

    if symbol_list:
        thread = threading.Thread(
            target=_strategy_forge_quick_worker,
            args=(
                job_id,
                {
                    "symbol_list": symbol_list,
                    "timeframe": tf,
                    "trials": trial_count,
                    "min_trades": min_trade_count,
                    "min_rules": min_rule_count,
                    "max_rules": max_rule_count,
                    "broker_hint": broker_hint,
                    "include_extended_hours_data": include_extended_hours_data,
                    "db_path": str(db_path) if db_path is not None else None,
                    "template": template,
                    "seed_mode": normalized_seed_mode,
                    "seed_job_id": normalized_seed_job_id,
                },
            ),
            daemon=True,
        )
        thread.start()
    return job_id


def _render_strategy_forge_quick_html(
    *,
    timeframe: str,
    symbols: str,
    trials: int,
    min_trades: int,
    min_rules: int = 2,
    max_rules: int = 5,
    broker_hint: str = "robinhood",
    include_extended_hours_data: bool = False,
    db_path: Optional[str | Path] = None,
    template: str = "",
    seed_mode: str = "random",
    seed_job_id: str = "",
) -> str:
    job_id = _strategy_forge_quick_start_job(
        timeframe=timeframe,
        symbols=symbols,
        trials=trials,
        min_trades=min_trades,
        min_rules=min_rules,
        max_rules=max_rules,
        broker_hint=broker_hint,
        include_extended_hours_data=include_extended_hours_data,
        db_path=db_path,
        template=template,
        seed_mode=seed_mode,
        seed_job_id=seed_job_id,
    )
    return _render_strategy_forge_quick_status_html(job_id)


@app.get("/partials/strategy_forge_quick", response_class=HTMLResponse)
def partial_strategy_forge_quick(
    symbols: str = "",
    timeframe: str = "1h",
    broker_hint: str = "robinhood",
    include_extended_hours_data: str = "false",
    template: str = "",
    trials: int = 250,
    min_trades: int = 100,
    min_rules: int = 2,
    max_rules: int = 5,
    seed_mode: str = "random",
    seed_job_id: str = "",
):
    include_extended = str(include_extended_hours_data or "").strip().lower() in ("1", "true", "yes", "on")
    return HTMLResponse(
        _render_strategy_forge_quick_html(
            timeframe=str(timeframe or "1h"),
            symbols=str(symbols or ""),
            template=str(template or ""),
            trials=max(1, min(int(trials or 250), 5000)),
            min_trades=max(1, min(int(min_trades or 100), 10000)),
            min_rules=max(2, min(int(min_rules or 2), 30)),
            max_rules=max(2, min(int(max_rules or 5), 30)),
            broker_hint=str(broker_hint or "robinhood"),
            include_extended_hours_data=include_extended,
            seed_mode=str(seed_mode or "random"),
            seed_job_id=str(seed_job_id or ""),
        )
    )


@app.get("/partials/strategy_forge_quick_status", response_class=HTMLResponse)
def partial_strategy_forge_quick_status(job_id: str = ""):
    return HTMLResponse(_render_strategy_forge_quick_status_html(str(job_id or "")))


@app.get("/partials/markets_news", response_class=HTMLResponse)
def partial_markets_news(limit: int = 5):
    lim = max(1, min(int(limit or 5), 20))
    return HTMLResponse(_render_markets_news_html(limit=lim))


@app.get("/algorithms", response_class=HTMLResponse)
def algorithms(request: Request):
    return render("algorithms.html", title="Algorithms", path="/algorithms", request=request)


@app.get("/algorithms/new", response_class=HTMLResponse)
def algorithms_new(request: Request):
    discover_base_scripts()
    scripts = get_base_scripts()
    base_algo_defs = build_base_algo_form_defs(scripts)
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            a.id,
            a.name,
            a.base_script_id,
            a.restart_on_crash,
            a.params_json,
            COALESCE(bs.name, '(missing base script)') AS base_script_name
        FROM algorithms a
        LEFT JOIN base_scripts bs ON bs.id = a.base_script_id
        ORDER BY a.created_ts DESC, a.id DESC
        """
    )
    clone_rows = cur.fetchall()
    conn.close()

    clonable_algorithms: list[dict[str, Any]] = []
    for row in clone_rows:
        clonable_algorithms.append(
            {
                "id": int(row["id"]),
                "name": str(row["name"] or ""),
                "base_script_id": int(row["base_script_id"]),
                "base_script_name": str(row["base_script_name"] or ""),
                "restart_on_crash": int(row["restart_on_crash"]),
                "params": _safe_json(row["params_json"], default={}),
            }
        )

    return render(
        "algo_new.html",
        title="New Cryptid",
        path="/algorithms",
        scripts=scripts,
        base_algo_defs_json=json.dumps(base_algo_defs),
        clonable_algorithms=clonable_algorithms,
        clonable_algorithms_json=json.dumps(clonable_algorithms),
        request=request,
    )


@app.post("/algorithms/new")
def algorithms_new_post(
    name: str = Form(...),
    base_script_id: int = Form(...),
    restart_on_crash: int = Form(1),
    params_json: str = Form("{}"),
):
    rulesets: list[str] = []
    max_runtime_min = 0
    log_level = "INFO"
    try:
        json.loads(params_json)
    except Exception:
        return PlainTextResponse("params_json must be valid JSON", status_code=400)

    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT id, path FROM base_scripts WHERE id=?", (int(base_script_id),))
    bs = cur.fetchone()
    if not bs:
        conn.close()
        return PlainTextResponse("Base script not found. Refresh and try again.", status_code=400)
    params_json = _sanitize_algorithm_params_for_script(params_json, str(bs["path"] or ""))

    cur.execute(
        """INSERT INTO algorithms
        (name, base_script_id, rulesets_json, params_json, max_runtime_min, restart_on_crash, log_level, created_ts)
        VALUES (?,?,?,?,?,?,?,?)""",
        (
            name,
            int(base_script_id),
            json.dumps(rulesets),
            params_json,
            int(max_runtime_min),
            int(restart_on_crash),
            log_level,
            _utc_ts(),
        ),
    )
    conn.commit()
    conn.close()
    return RedirectResponse("/algorithms", status_code=303)


@app.get("/algorithms/{algo_id}/edit", response_class=HTMLResponse)
def algorithms_edit(algo_id: int, request: Request):
    discover_base_scripts()
    scripts = get_base_scripts()
    base_algo_defs = build_base_algo_form_defs(scripts)

    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM algorithms WHERE id=?", (int(algo_id),))
    row = cur.fetchone()
    conn.close()
    if not row:
        return PlainTextResponse("Cryptid not found.", status_code=404)

    params_obj = _safe_json(row["params_json"], default={})
    return render(
        "algo_edit.html",
        title="Edit Cryptid",
        path="/algorithms",
        algo_id=int(row["id"]),
        algo_name=str(row["name"]),
        base_script_id=int(row["base_script_id"]),
        restart_on_crash=int(row["restart_on_crash"]),
        params_json=json.dumps(params_obj),
        scripts=scripts,
        base_algo_defs_json=json.dumps(base_algo_defs),
        request=request,
    )


@app.post("/algorithms/{algo_id}/edit")
def algorithms_edit_post(
    algo_id: int,
    name: str = Form(...),
    base_script_id: int = Form(...),
    restart_on_crash: int = Form(1),
    params_json: str = Form("{}"),
):
    try:
        json.loads(params_json)
    except Exception:
        return PlainTextResponse("params_json must be valid JSON", status_code=400)

    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM algorithms WHERE id=?", (int(algo_id),))
    row = cur.fetchone()
    if not row:
        conn.close()
        return PlainTextResponse("Cryptid not found.", status_code=404)
    cur.execute("SELECT id, path FROM base_scripts WHERE id=?", (int(base_script_id),))
    bs = cur.fetchone()
    if not bs:
        conn.close()
        return PlainTextResponse("Base script not found. Refresh and try again.", status_code=400)
    params_json = _sanitize_algorithm_params_for_script(params_json, str(bs["path"] or ""))

    cur.execute(
        """
        UPDATE algorithms
        SET name=?, base_script_id=?, params_json=?, restart_on_crash=?
        WHERE id=?
        """,
        (name, int(base_script_id), params_json, int(restart_on_crash), int(algo_id)),
    )
    conn.commit()
    conn.close()
    return RedirectResponse("/algorithms", status_code=303)


@app.post("/algorithms/{algo_id}/delete")
def algorithms_delete(algo_id: int):
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT id, pid, status, run_dir FROM runs WHERE algorithm_id=?", (int(algo_id),))
    runs = cur.fetchall()

    run_dirs_to_remove: list[Path] = []
    for r in runs:
        if r["status"] in ("running", "stopping") and r["pid"]:
            _terminate_pid(int(r["pid"]), grace_sec=1.0)
        pid = int(r["pid"]) if r["pid"] else None
        if pid and _pid_is_alive(pid):
            conn.close()
            return PlainTextResponse("Cryptid still has a live run process. Stop it before deleting.", status_code=409)
        safe_run_dir = _safe_run_dir(r["run_dir"])
        if safe_run_dir is not None:
            run_dirs_to_remove.append(safe_run_dir)

    for run_dir in run_dirs_to_remove:
        try:
            if run_dir.exists():
                shutil.rmtree(run_dir)
        except Exception:
            pass

    cur.execute("DELETE FROM runs WHERE algorithm_id=?", (int(algo_id),))
    cur.execute("DELETE FROM algorithms WHERE id=?", (int(algo_id),))
    conn.commit()
    conn.close()
    return RedirectResponse("/algorithms", status_code=303)


def _pid_is_alive(pid: Optional[int]) -> bool:
    if not pid:
        return False
    pid = int(pid)
    stat_path = Path(f"/proc/{pid}/stat")
    try:
        stat_parts = stat_path.read_text().split()
        if len(stat_parts) >= 3 and stat_parts[2] == "Z":
            return False
    except Exception:
        pass
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        pass
    except Exception:
        return False
    try:
        stat_parts = stat_path.read_text().split()
        if len(stat_parts) >= 3 and stat_parts[2] == "Z":
            return False
    except Exception:
        pass
    return True


def _remember_algorithm_process(proc: subprocess.Popen[Any]) -> None:
    try:
        pid = int(proc.pid)
    except Exception:
        return
    with ALGORITHM_PROCESSES_LOCK:
        ALGORITHM_PROCESSES[pid] = proc


def _forget_algorithm_process(pid: int) -> Optional[subprocess.Popen[Any]]:
    with ALGORITHM_PROCESSES_LOCK:
        return ALGORITHM_PROCESSES.pop(int(pid), None)


def _terminate_pid(pid: int, *, grace_sec: float = 2.0) -> None:
    pid = int(pid)
    tracked_proc = _forget_algorithm_process(pid)
    use_process_group = False
    child_pids: list[int] = []
    if psutil is not None:
        try:
            proc = psutil.Process(pid)
            child_pids = [int(child.pid) for child in proc.children(recursive=True)]
        except Exception:
            child_pids = []
    try:
        use_process_group = os.getpgid(pid) == pid
    except Exception:
        use_process_group = False

    try:
        if use_process_group:
            os.killpg(pid, signal.SIGTERM)
        else:
            os.kill(pid, signal.SIGTERM)
    except Exception:
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:
            return
    for child_pid in child_pids:
        try:
            os.kill(child_pid, signal.SIGTERM)
        except Exception:
            pass
    deadline = time.time() + float(grace_sec)
    while time.time() < deadline:
        live_children = [child_pid for child_pid in child_pids if _pid_is_alive(child_pid)]
        if not _pid_is_alive(pid) and not live_children:
            break
        time.sleep(0.1)
    if _pid_is_alive(pid):
        try:
            if use_process_group:
                os.killpg(pid, signal.SIGKILL)
            else:
                os.kill(pid, signal.SIGKILL)
        except Exception:
            try:
                os.kill(pid, signal.SIGKILL)
            except Exception:
                pass
    for child_pid in child_pids:
        if _pid_is_alive(child_pid):
            try:
                os.kill(child_pid, signal.SIGKILL)
            except Exception:
                pass
    if tracked_proc is not None:
        try:
            tracked_proc.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            try:
                tracked_proc.kill()
                tracked_proc.wait(timeout=0.5)
            except Exception:
                pass
        except Exception:
            pass
    else:
        try:
            os.waitpid(pid, os.WNOHANG)
        except Exception:
            pass


def _proc_cmdline(pid: int) -> list[str]:
    try:
        raw = Path(f"/proc/{int(pid)}/cmdline").read_bytes()
    except Exception:
        return []
    parts = []
    for part in raw.split(b"\0"):
        if not part:
            continue
        try:
            parts.append(part.decode("utf-8", errors="replace"))
        except Exception:
            continue
    return parts


def _safe_resolved_path(value: str) -> Optional[Path]:
    try:
        return Path(value).expanduser().resolve()
    except Exception:
        return None


def _cmdline_script_path(cmdline: list[str]) -> Optional[Path]:
    scripts_root = SCRIPTS_DIR.resolve()
    for part in cmdline:
        if not str(part or "").endswith(".py"):
            continue
        path = _safe_resolved_path(part)
        if path is None:
            continue
        try:
            path.relative_to(scripts_root)
        except ValueError:
            continue
        return path
    return None


def _cmdline_arg_value(cmdline: list[str], name: str) -> Optional[str]:
    for i, part in enumerate(cmdline):
        if part == name and i + 1 < len(cmdline):
            return cmdline[i + 1]
        if part.startswith(f"{name}="):
            return part.split("=", 1)[1]
    return None


def _find_live_cryptid_processes() -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    current_pid = os.getpid()
    for proc_dir in Path("/proc").iterdir():
        if not proc_dir.name.isdigit():
            continue
        pid = int(proc_dir.name)
        if pid == current_pid:
            continue
        cmdline = _proc_cmdline(pid)
        if not cmdline:
            continue
        script_path = _cmdline_script_path(cmdline)
        if script_path is None:
            continue
        if not _pid_is_alive(pid):
            continue
        found.append(
            {
                "pid": pid,
                "cmdline": cmdline,
                "script_path": str(script_path),
                "run_dir": _cmdline_arg_value(cmdline, "--run-dir"),
            }
        )
    return found


def _mark_run_stopped_for_process(
    cur: sqlite3.Cursor,
    *,
    pid: int,
    run_dir: Optional[str],
    ts_now: Optional[int] = None,
) -> None:
    ts = int(ts_now or _utc_ts())
    if run_dir:
        cur.execute(
            "UPDATE runs SET status=?, end_ts=?, pid=? WHERE run_dir=?",
            ("stopped", ts, None, str(run_dir)),
        )
        if cur.rowcount:
            return
    cur.execute(
        "UPDATE runs SET status=?, end_ts=?, pid=? WHERE pid=?",
        ("stopped", ts, None, int(pid)),
    )


def _parse_heartbeat_ts(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return int(value)
        except Exception:
            return None
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return int(dt.timestamp())
        except Exception:
            return None
    return None


def _heartbeat_age_seconds(run_dir: Path) -> Optional[int]:
    status_path = run_dir / "status.json"
    if not status_path.exists():
        return None
    now = _utc_ts()
    try:
        payload = json.loads(status_path.read_text())
        hb = payload.get("ts") or payload.get("heartbeat") or payload.get("last_heartbeat")
        hb_ts = _parse_heartbeat_ts(hb)
        if hb_ts is not None:
            return now - hb_ts
    except Exception:
        pass
    try:
        return now - int(status_path.stat().st_mtime)
    except Exception:
        return None


def _run_hang_threshold_seconds(params_json: str) -> int:
    if RUN_HANG_TIMEOUT_SEC > 0:
        return RUN_HANG_TIMEOUT_SEC
    params = _safe_json(params_json or "{}", default={})
    candidates: list[float] = []
    for key in ("watch_interval_seconds", "sleep_duration", "poll_interval_seconds"):
        try:
            val = float(params.get(key))
            if val > 0:
                candidates.append(val)
        except Exception:
            continue
    if not candidates:
        try:
            val = float(params.get("max_silent_seconds"))
            if val > 0:
                candidates.append(val)
        except Exception:
            pass
    base = max(candidates) if candidates else 30.0
    threshold = int(base * max(1.0, RUN_HANG_MULTIPLIER))
    threshold = max(int(threshold), int(RUN_HANG_MIN_SEC))
    if RUN_HANG_MAX_SEC > 0:
        threshold = min(int(threshold), int(RUN_HANG_MAX_SEC))
    return int(threshold)


def _row_get(row: Any, key: str, default: Any = None) -> Any:
    try:
        if hasattr(row, "keys") and key not in row.keys():
            return default
        value = row[key]
    except Exception:
        return default
    return default if value is None else value


def _run_owned_by_current_server(row: Any) -> bool:
    try:
        supervisor_pid = int(_row_get(row, "supervisor_pid", 0) or 0)
        supervisor_started_ts = int(_row_get(row, "supervisor_started_ts", 0) or 0)
    except Exception:
        return False
    return supervisor_pid == os.getpid() and supervisor_started_ts == SERVER_STARTED_TS


def _run_restart_count(row: Any) -> int:
    try:
        return max(0, int(_row_get(row, "restart_count", 0) or 0))
    except Exception:
        return 0


def _auto_restart_decision(row: Any, *, allow_auto_restart: bool) -> tuple[bool, str]:
    if not allow_auto_restart:
        return False, "passive_recovery_disabled"
    if not AUTO_RESTART_RUNS:
        return False, "auto_restart_disabled"
    if not _run_owned_by_current_server(row):
        return False, "not_owned_by_current_server"
    if RUN_MAX_AUTO_RESTARTS >= 0 and _run_restart_count(row) >= RUN_MAX_AUTO_RESTARTS:
        return False, "restart_limit_reached"
    return True, "restart_allowed"


def _mark_run_terminal_after_dead_pid(cur: sqlite3.Cursor, row: Any, *, reason: str) -> None:
    restart_enabled = int(_row_get(row, "restart_on_crash", 0) or 0) == 1
    status = "crashed" if restart_enabled else "exited"
    cur.execute(
        """
        UPDATE runs
        SET status=?, end_ts=?, pid=?, restart_reason=?
        WHERE id=?
        """,
        (status, _utc_ts(), None, str(reason), int(row["id"])),
    )


def _ensure_params_file(run_dir: Path, params_json: str) -> Path:
    params_path = run_dir / "params.json"
    if params_path.exists():
        return params_path
    try:
        params_path.write_text(params_json, encoding="utf-8")
    except Exception:
        pass
    return params_path


def _algorithm_subprocess_env(broker_hint: Optional[str]) -> dict[str, str]:
    env_map = os.environ.copy()
    algo_thread_limit = str(os.getenv("CRYPTID_ALGO_NUM_THREADS", "") or "").strip()
    if algo_thread_limit:
        for key in ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
            env_map.setdefault(key, algo_thread_limit)
    if str(broker_hint or "").strip().lower() != "schwab":
        return env_map
    cfg = _schwab_config()
    mappings = {
        "SCHWAB_CLIENT_ID": cfg.get("client_id"),
        "SCHWAB_CLIENT_SECRET": cfg.get("client_secret"),
        "SCHWAB_REDIRECT_URI": cfg.get("redirect_uri"),
        "SCHWAB_SCOPE": cfg.get("scope"),
        "SCHWAB_TRADER_API_BASE": cfg.get("trader_api_base"),
        "SCHWAB_MARKET_DATA_BASE": cfg.get("market_data_base"),
        "SCHWAB_ACCOUNT_HASH": cfg.get("account_hash"),
    }
    for key, value in mappings.items():
        val = str(value or "").strip()
        if val:
            env_map[key] = val
    return env_map


def _broker_hint_from_script(script_path: str) -> Optional[str]:
    script_lower = str(script_path or "").lower()
    if "robinhood" in script_lower:
        return "robinhood"
    if "schwab" in script_lower:
        return "schwab"
    if script_lower.endswith("scripts/foxscry.py"):
        return "robinhood"
    return None


def _resolve_connection_id(params_json: str, broker_hint: Optional[str], conn: sqlite3.Connection) -> Optional[int]:
    params_obj = _safe_json(params_json or "{}", default={})
    connection_id: Optional[int] = None
    if isinstance(params_obj, dict):
        for key in ("connection_id", "broker_connection_id", "robinhood_connection_id", "schwab_connection_id"):
            if key in params_obj:
                try:
                    connection_id = int(params_obj.get(key))
                    break
                except Exception:
                    connection_id = None

    if broker_hint and connection_id is None:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id
            FROM broker_connections
            WHERE broker=? AND status IN ('connected','ok','')
            ORDER BY id DESC
            LIMIT 1
            """,
            (broker_hint,),
        )
        row = cur.fetchone()
        if row:
            connection_id = int(row["id"])

    return connection_id


def _trim_log_file(log_path: Path, *, max_bytes: int = LOG_MAX_BYTES, keep_bytes: int = LOG_TRIM_KEEP_BYTES) -> None:
    if max_bytes <= 0:
        return
    try:
        if not log_path.exists():
            return
        size = log_path.stat().st_size
        if size <= max_bytes:
            return
        keep_bytes = max(0, min(int(keep_bytes), int(max_bytes)))
        with log_path.open("rb+") as f:
            if keep_bytes <= 0:
                f.truncate(0)
                return
            if size > keep_bytes:
                f.seek(-keep_bytes, os.SEEK_END)
            else:
                f.seek(0)
            data = f.read()
            nl = data.find(b"\n")
            if 0 <= nl < len(data) - 1:
                data = data[nl + 1 :]
            f.seek(0)
            f.write(data)
            f.truncate()
    except Exception:
        pass


def _safe_child_path(path: Any, root: Path) -> Optional[Path]:
    """Return a resolved child path, or None if path escapes root."""
    try:
        root_resolved = root.resolve()
        candidate = Path(str(path)).expanduser().resolve()
    except Exception:
        return None
    try:
        if candidate == root_resolved or not candidate.is_relative_to(root_resolved):
            return None
    except Exception:
        return None
    return candidate


def _safe_run_dir(path: Any) -> Optional[Path]:
    candidate = _safe_child_path(path, RUNS_DIR)
    if candidate is None:
        return None
    try:
        if not candidate.name.startswith("run_"):
            return None
    except Exception:
        return None
    return candidate


def _safe_assistant_news_dir(path: Any) -> Optional[Path]:
    return _safe_child_path(path, ASSISTANT_NEWS_RUNS_DIR)


def _dir_size_bytes(path: Path) -> int:
    total = 0
    try:
        for item in path.rglob("*"):
            try:
                if item.is_file():
                    total += int(item.stat().st_size)
            except Exception:
                continue
    except Exception:
        return 0
    return total


def _remove_tree_if_safe(path: Path, root: Path) -> bool:
    safe_path = _safe_child_path(path, root)
    if safe_path is None:
        return False
    try:
        if safe_path.exists():
            shutil.rmtree(safe_path)
        return True
    except Exception:
        return False


def _terminal_run_status(status: Any) -> bool:
    return str(status or "").strip().lower() in {"stopped", "crashed", "failed", "error"}


def _run_dir_file_names(path: Path) -> list[str]:
    try:
        return sorted(str(item.relative_to(path)) for item in path.rglob("*") if item.is_file())
    except Exception:
        return []


def _run_cleanup_item(row: sqlite3.Row, run_dir: Optional[Path], *, reason: str) -> dict[str, Any]:
    return {
        "id": int(row["id"]),
        "algorithm_id": int(row["algorithm_id"]) if row["algorithm_id"] is not None else None,
        "algorithm_name": str(row["algorithm_name"] or ""),
        "status": str(row["status"] or ""),
        "start_ts": int(row["start_ts"] or 0),
        "end_ts": int(row["end_ts"] or 0) if row["end_ts"] is not None else None,
        "run_dir": str(run_dir or row["run_dir"] or ""),
        "bytes": _dir_size_bytes(run_dir) if run_dir is not None and run_dir.exists() else 0,
        "reason": reason,
    }


def build_storage_cleanup_report(
    *,
    run_retention_days: int = CLEANUP_RUN_RETENTION_DAYS,
    keep_per_algorithm: int = CLEANUP_KEEP_PER_ALGORITHM,
    assistant_news_retention_days: int = CLEANUP_ASSISTANT_NEWS_RETENTION_DAYS,
    log_max_bytes: int = LOG_MAX_BYTES,
    log_keep_bytes: int = LOG_TRIM_KEEP_BYTES,
) -> dict[str, Any]:
    ensure_dirs()
    run_retention_days = max(0, int(run_retention_days))
    keep_per_algorithm = max(0, int(keep_per_algorithm))
    assistant_news_retention_days = max(0, int(assistant_news_retention_days))
    log_max_bytes = max(0, int(log_max_bytes))
    log_keep_bytes = max(0, int(log_keep_bytes))

    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, algorithm_id, algorithm_name, status, pid, start_ts, end_ts, run_dir
        FROM runs
        ORDER BY start_ts DESC, id DESC
        """
    )
    rows = cur.fetchall()
    conn.close()

    status_counts: dict[str, int] = {}
    referenced_dirs: set[Path] = set()
    safe_run_dirs_by_id: dict[int, Optional[Path]] = {}
    active_ids: set[int] = set()
    terminal_rows: list[sqlite3.Row] = []

    for row in rows:
        run_id = int(row["id"])
        status = str(row["status"] or "").strip().lower()
        status_counts[status or "unknown"] = status_counts.get(status or "unknown", 0) + 1
        run_dir = _safe_run_dir(row["run_dir"])
        safe_run_dirs_by_id[run_id] = run_dir
        if run_dir is not None:
            referenced_dirs.add(run_dir)
        pid = int(row["pid"]) if row["pid"] else None
        if status in {"running", "stopping"} or _pid_is_alive(pid):
            active_ids.add(run_id)
        if _terminal_run_status(status):
            terminal_rows.append(row)

    try:
        actual_run_dirs = sorted(p.resolve() for p in RUNS_DIR.iterdir() if p.is_dir() and p.name.startswith("run_"))
    except Exception:
        actual_run_dirs = []

    orphan_run_dirs = [
        {
            "run_dir": str(path),
            "bytes": _dir_size_bytes(path),
            "reason": "directory is not referenced by runs table",
        }
        for path in actual_run_dirs
        if path not in referenced_dirs
    ]

    keep_ids: set[int] = set()
    if keep_per_algorithm > 0:
        by_algorithm: dict[int, list[sqlite3.Row]] = {}
        for row in terminal_rows:
            try:
                algo_id = int(row["algorithm_id"])
            except Exception:
                continue
            by_algorithm.setdefault(algo_id, []).append(row)
        for algo_rows in by_algorithm.values():
            ordered = sorted(algo_rows, key=lambda r: (int(r["start_ts"] or 0), int(r["id"])), reverse=True)
            keep_ids.update(int(r["id"]) for r in ordered[:keep_per_algorithm])

    now_ts = _utc_ts()
    run_cutoff_ts = now_ts - (run_retention_days * 86400)
    expired_runs: list[dict[str, Any]] = []
    stale_starting_runs: list[dict[str, Any]] = []
    oversized_logs: list[dict[str, Any]] = []

    for row in rows:
        run_id = int(row["id"])
        status = str(row["status"] or "").strip().lower()
        run_dir = safe_run_dirs_by_id.get(run_id)
        start_ts = int(row["start_ts"] or 0)
        if run_id in active_ids:
            continue

        if status == "starting":
            files = _run_dir_file_names(run_dir) if run_dir is not None and run_dir.exists() else []
            if not files or set(files).issubset({"params.json"}):
                stale_starting_runs.append(_run_cleanup_item(row, run_dir, reason="starting row has no live pid"))
            continue

        if _terminal_run_status(status):
            if run_dir is not None and run_dir.exists():
                log_path = run_dir / "algo.log"
                try:
                    log_size = int(log_path.stat().st_size) if log_path.exists() else 0
                except Exception:
                    log_size = 0
                if log_max_bytes > 0 and log_size > log_max_bytes:
                    oversized_logs.append(
                        {
                            "id": run_id,
                            "algorithm_id": int(row["algorithm_id"]) if row["algorithm_id"] is not None else None,
                            "algorithm_name": str(row["algorithm_name"] or ""),
                            "status": status,
                            "log_path": str(log_path),
                            "bytes": log_size,
                            "max_bytes": log_max_bytes,
                            "keep_bytes": log_keep_bytes,
                            "reason": "terminal run log exceeds configured max bytes",
                        }
                    )
            if run_retention_days > 0 and start_ts < run_cutoff_ts and run_id not in keep_ids:
                expired_runs.append(
                    _run_cleanup_item(
                        row,
                        run_dir,
                        reason=f"terminal run older than {run_retention_days} days and not in latest {keep_per_algorithm} per algorithm",
                    )
                )

    assistant_cutoff_ts = now_ts - (assistant_news_retention_days * 86400)
    assistant_news_dirs: list[dict[str, Any]] = []
    if assistant_news_retention_days > 0:
        try:
            news_dirs = sorted(p.resolve() for p in ASSISTANT_NEWS_RUNS_DIR.iterdir() if p.is_dir())
        except Exception:
            news_dirs = []
        for path in news_dirs:
            safe_path = _safe_assistant_news_dir(path)
            if safe_path is None:
                continue
            try:
                mtime = int(safe_path.stat().st_mtime)
            except Exception:
                mtime = now_ts
            if mtime < assistant_cutoff_ts:
                assistant_news_dirs.append(
                    {
                        "run_dir": str(safe_path),
                        "bytes": _dir_size_bytes(safe_path),
                        "mtime": mtime,
                        "reason": f"assistant news output older than {assistant_news_retention_days} days",
                    }
                )

    planned_reclaimed = (
        sum(int(item.get("bytes") or 0) for item in orphan_run_dirs)
        + sum(int(item.get("bytes") or 0) for item in stale_starting_runs)
        + sum(int(item.get("bytes") or 0) for item in expired_runs)
        + sum(max(0, int(item.get("bytes") or 0) - log_keep_bytes) for item in oversized_logs)
        + sum(int(item.get("bytes") or 0) for item in assistant_news_dirs)
    )

    return {
        "settings": {
            "run_retention_days": run_retention_days,
            "keep_per_algorithm": keep_per_algorithm,
            "assistant_news_retention_days": assistant_news_retention_days,
            "log_max_bytes": log_max_bytes,
            "log_keep_bytes": log_keep_bytes,
        },
        "summary": {
            "run_rows": len(rows),
            "actual_run_dirs": len(actual_run_dirs),
            "status_counts": status_counts,
            "orphan_run_dirs": len(orphan_run_dirs),
            "stale_starting_runs": len(stale_starting_runs),
            "expired_terminal_runs": len(expired_runs),
            "oversized_terminal_logs": len(oversized_logs),
            "assistant_news_dirs": len(assistant_news_dirs),
            "planned_reclaimed_bytes": planned_reclaimed,
        },
        "orphan_run_dirs": orphan_run_dirs,
        "stale_starting_runs": stale_starting_runs,
        "expired_terminal_runs": expired_runs,
        "oversized_terminal_logs": oversized_logs,
        "assistant_news_dirs": assistant_news_dirs,
    }


def apply_storage_cleanup(report: dict[str, Any]) -> dict[str, Any]:
    applied = {
        "orphan_run_dirs_removed": 0,
        "stale_starting_runs_removed": 0,
        "expired_terminal_runs_removed": 0,
        "oversized_logs_trimmed": 0,
        "assistant_news_dirs_removed": 0,
        "run_rows_deleted": 0,
        "errors": [],
    }

    run_ids_to_delete: set[int] = set()

    for item in report.get("orphan_run_dirs", []):
        path = _safe_run_dir(item.get("run_dir"))
        if path is None:
            applied["errors"].append(f"unsafe orphan run dir skipped: {item.get('run_dir')}")
            continue
        try:
            if path.exists():
                shutil.rmtree(path)
            applied["orphan_run_dirs_removed"] += 1
        except Exception as e:
            applied["errors"].append(f"failed removing orphan run dir {path}: {e}")

    for key, counter_name in (
        ("stale_starting_runs", "stale_starting_runs_removed"),
        ("expired_terminal_runs", "expired_terminal_runs_removed"),
    ):
        for item in report.get(key, []):
            path = _safe_run_dir(item.get("run_dir"))
            if path is None:
                applied["errors"].append(f"unsafe run dir skipped: {item.get('run_dir')}")
                continue
            try:
                if path.exists():
                    shutil.rmtree(path)
                run_id = item.get("id")
                if isinstance(run_id, int):
                    run_ids_to_delete.add(run_id)
                applied[counter_name] += 1
            except Exception as e:
                applied["errors"].append(f"failed removing run dir {path}: {e}")

    if run_ids_to_delete:
        conn = db()
        cur = conn.cursor()
        safe_ids: list[int] = []
        for run_id in sorted(run_ids_to_delete):
            cur.execute("SELECT status, pid FROM runs WHERE id=?", (int(run_id),))
            row = cur.fetchone()
            if not row:
                continue
            pid = int(row["pid"]) if row["pid"] else None
            if str(row["status"] or "").lower() in {"running", "stopping"} or _pid_is_alive(pid):
                applied["errors"].append(f"run row {run_id} skipped because it is active")
                continue
            safe_ids.append(int(run_id))
        if safe_ids:
            qmarks = ",".join("?" for _ in safe_ids)
            cur.execute(f"DELETE FROM runs WHERE id IN ({qmarks})", safe_ids)
            applied["run_rows_deleted"] = int(cur.rowcount if cur.rowcount is not None else 0)
            conn.commit()
        conn.close()

    deleted_run_ids = run_ids_to_delete
    for item in report.get("oversized_terminal_logs", []):
        if item.get("id") in deleted_run_ids:
            continue
        log_path = Path(str(item.get("log_path") or ""))
        safe_log_dir = _safe_run_dir(log_path.parent)
        if safe_log_dir is None or safe_log_dir != log_path.parent.resolve():
            applied["errors"].append(f"unsafe log path skipped: {item.get('log_path')}")
            continue
        try:
            _trim_log_file(
                log_path,
                max_bytes=int(item.get("max_bytes") or LOG_MAX_BYTES),
                keep_bytes=int(item.get("keep_bytes") or LOG_TRIM_KEEP_BYTES),
            )
            applied["oversized_logs_trimmed"] += 1
        except Exception as e:
            applied["errors"].append(f"failed trimming log {log_path}: {e}")

    for item in report.get("assistant_news_dirs", []):
        path = _safe_assistant_news_dir(item.get("run_dir"))
        if path is None:
            applied["errors"].append(f"unsafe assistant news dir skipped: {item.get('run_dir')}")
            continue
        try:
            if path.exists():
                shutil.rmtree(path)
            applied["assistant_news_dirs_removed"] += 1
        except Exception as e:
            applied["errors"].append(f"failed removing assistant news dir {path}: {e}")

    return applied


def run_storage_cleanup(
    *,
    apply: bool = False,
    run_retention_days: int = CLEANUP_RUN_RETENTION_DAYS,
    keep_per_algorithm: int = CLEANUP_KEEP_PER_ALGORITHM,
    assistant_news_retention_days: int = CLEANUP_ASSISTANT_NEWS_RETENTION_DAYS,
    log_max_bytes: int = LOG_MAX_BYTES,
    log_keep_bytes: int = LOG_TRIM_KEEP_BYTES,
) -> dict[str, Any]:
    report = build_storage_cleanup_report(
        run_retention_days=run_retention_days,
        keep_per_algorithm=keep_per_algorithm,
        assistant_news_retention_days=assistant_news_retention_days,
        log_max_bytes=log_max_bytes,
        log_keep_bytes=log_keep_bytes,
    )
    payload: dict[str, Any] = {
        "mode": "apply" if apply else "dry-run",
        "report": report,
    }
    if apply:
        payload["applied"] = apply_storage_cleanup(report)
    return payload


def _spawn_run_process(
    *,
    entrypoint: str,
    run_dir: Path,
    params_json: str,
    broker_hint: Optional[str] = None,
    connection_id: Optional[int] = None,
) -> int:
    run_dir.mkdir(parents=True, exist_ok=True)
    params_path = _ensure_params_file(run_dir, params_json)
    cmd = [
        sys.executable,
        entrypoint,
        "--run-dir",
        str(run_dir),
        "--params-json",
        str(params_path),
    ]
    if broker_hint:
        if connection_id is None:
            raise RuntimeError(f"Missing connection_id for {broker_hint} run restart.")
        cmd.extend(["--db-path", str(DB_PATH), "--connection-id", str(connection_id)])
    log_path = run_dir / "algo.log"
    _trim_log_file(log_path)
    log_fp = None
    try:
        log_fp = open(log_path, "ab", buffering=0)
        proc = subprocess.Popen(
            cmd,
            stdout=log_fp,
            stderr=log_fp,
            start_new_session=(os.name != "nt"),
            env=_algorithm_subprocess_env(broker_hint),
        )
        _remember_algorithm_process(proc)
        return int(proc.pid)
    finally:
        if log_fp is not None:
            try:
                log_fp.close()
            except Exception:
                pass


def _refresh_run_processes(
    conn: sqlite3.Connection,
    *,
    run_id: Optional[int] = None,
    allow_auto_restart: bool = True,
) -> None:
    cur = conn.cursor()
    if run_id is None:
        cur.execute(
            """
            SELECT r.*, a.restart_on_crash, b.path AS script_path
            FROM runs r
            JOIN algorithms a ON a.id = r.algorithm_id
            JOIN base_scripts b ON b.id = a.base_script_id
            WHERE r.status IN ('running', 'stopping')
            ORDER BY r.id DESC
            LIMIT 200
            """
        )
    else:
        cur.execute(
            """
            SELECT r.*, a.restart_on_crash, b.path AS script_path
            FROM runs r
            JOIN algorithms a ON a.id = r.algorithm_id
            JOIN base_scripts b ON b.id = a.base_script_id
            WHERE r.id=? AND r.status IN ('running', 'stopping')
            """,
            (int(run_id),),
        )
    rows = cur.fetchall()
    if not rows:
        return

    for r in rows:
        run_id_val = int(r["id"])
        status = str(r["status"] or "")
        pid_val = int(r["pid"]) if r["pid"] else None
        try:
            _trim_log_file(Path(r["run_dir"]) / "algo.log")
        except Exception:
            pass
        alive = _pid_is_alive(pid_val)

        if status == "stopping":
            if alive and pid_val:
                _terminate_pid(pid_val, grace_sec=0.5)
                alive = _pid_is_alive(pid_val)
            if not alive:
                cur.execute(
                    "UPDATE runs SET status=?, end_ts=?, pid=?, restart_reason=? WHERE id=?",
                    ("stopped", _utc_ts(), None, "stop_requested", run_id_val),
                )
            continue

        if status != "running":
            continue

        if alive:
            try:
                threshold = _run_hang_threshold_seconds(str(r["params_json"] or "{}"))
            except Exception:
                threshold = 0
            if threshold > 0:
                now = _utc_ts()
                start_ts = int(r["start_ts"] or 0)
                if not start_ts or (now - start_ts) >= threshold:
                    age = _heartbeat_age_seconds(Path(r["run_dir"]))
                    if age is None and start_ts:
                        age = now - start_ts
                    if age is not None and age > threshold:
                        if int(r["restart_on_crash"] or 0) == 1:
                            restart_allowed, restart_reason = _auto_restart_decision(
                                r,
                                allow_auto_restart=allow_auto_restart,
                            )
                            if not restart_allowed:
                                continue
                            entrypoint = str(APP_ROOT / r["script_path"])
                            try:
                                if pid_val:
                                    _terminate_pid(pid_val)
                                broker_hint = _broker_hint_from_script(r["script_path"])
                                connection_id = _resolve_connection_id(str(r["params_json"] or "{}"), broker_hint, conn)
                                if broker_hint and connection_id is None:
                                    raise RuntimeError(f"No connected {broker_hint} broker available for restart.")
                                new_pid = _spawn_run_process(
                                    entrypoint=entrypoint,
                                    run_dir=Path(r["run_dir"]),
                                    params_json=str(r["params_json"] or "{}"),
                                    broker_hint=broker_hint,
                                    connection_id=connection_id,
                                )
                                cur.execute(
                                    """
                                    UPDATE runs
                                    SET pid=?, status=?, start_ts=?, end_ts=?,
                                        supervisor_pid=?, supervisor_started_ts=?,
                                        restart_count=?, last_restart_ts=?, restart_reason=?
                                    WHERE id=?
                                    """,
                                    (
                                        new_pid,
                                        "running",
                                        _utc_ts(),
                                        None,
                                        os.getpid(),
                                        SERVER_STARTED_TS,
                                        _run_restart_count(r) + 1,
                                        _utc_ts(),
                                        restart_reason,
                                        run_id_val,
                                    ),
                                )
                            except Exception:
                                _mark_run_terminal_after_dead_pid(cur, r, reason="restart_failed")
                        continue
            continue

        if int(r["restart_on_crash"] or 0) == 1:
            restart_allowed, restart_reason = _auto_restart_decision(
                r,
                allow_auto_restart=allow_auto_restart,
            )
            if not restart_allowed:
                _mark_run_terminal_after_dead_pid(cur, r, reason=restart_reason)
                continue
            entrypoint = str(APP_ROOT / r["script_path"])
            try:
                broker_hint = _broker_hint_from_script(r["script_path"])
                connection_id = _resolve_connection_id(str(r["params_json"] or "{}"), broker_hint, conn)
                if broker_hint and connection_id is None:
                    raise RuntimeError(f"No connected {broker_hint} broker available for restart.")
                new_pid = _spawn_run_process(
                    entrypoint=entrypoint,
                    run_dir=Path(r["run_dir"]),
                    params_json=str(r["params_json"] or "{}"),
                    broker_hint=broker_hint,
                    connection_id=connection_id,
                )
                cur.execute(
                    """
                    UPDATE runs
                    SET pid=?, status=?, start_ts=?, end_ts=?,
                        supervisor_pid=?, supervisor_started_ts=?,
                        restart_count=?, last_restart_ts=?, restart_reason=?
                    WHERE id=?
                    """,
                    (
                        new_pid,
                        "running",
                        _utc_ts(),
                        None,
                        os.getpid(),
                        SERVER_STARTED_TS,
                        _run_restart_count(r) + 1,
                        _utc_ts(),
                        restart_reason,
                        run_id_val,
                    ),
                )
            except Exception:
                _mark_run_terminal_after_dead_pid(cur, r, reason="restart_failed")
        else:
            cur.execute(
                "UPDATE runs SET status=?, end_ts=?, pid=?, restart_reason=? WHERE id=?",
                ("exited", _utc_ts(), None, "process_exited", run_id_val),
            )

    conn.commit()


@app.on_event("startup")
def _startup_recover_runs() -> None:
    init_db()
    discover_base_scripts()
    conn = db()
    try:
        _refresh_run_processes(conn, allow_auto_restart=False)
    finally:
        conn.close()


def _shutdown_markets_optimizer(timeout_sec: float = 5.0) -> dict[str, Any]:
    with MARKETS_OPTIMIZER_LOCK:
        thread = MARKETS_OPTIMIZER_THREAD
        stop_event = MARKETS_OPTIMIZER_STOP_EVENT
        run_id = MARKETS_OPTIMIZER_ACTIVE_RUN_ID
    if stop_event is not None:
        stop_event.set()
    if run_id is not None:
        try:
            _markets_optimizer_mark_stop_requested(int(run_id))
        except Exception:
            pass
    if thread is not None and thread.is_alive():
        thread.join(timeout=max(0.1, float(timeout_sec)))
    return {
        "active_run_id": run_id,
        "thread_alive": bool(thread and thread.is_alive()),
    }


def _shutdown_assistant_news_jobs(timeout_sec: float = 5.0) -> dict[str, Any]:
    with ASSISTANT_NEWS_JOBS_LOCK:
        stop_events = list(ASSISTANT_NEWS_STOP_EVENTS.items())
        threads = list(ASSISTANT_NEWS_THREADS.items())
        for job_id, event in stop_events:
            event.set()
            job = ASSISTANT_NEWS_JOBS.get(job_id)
            if isinstance(job, dict) and str(job.get("status") or "").lower() not in {"complete", "error", "stopped"}:
                job["status"] = "stopping"
                job["stage"] = "stopping"
                job["message"] = "Server shutdown requested."
                job["cancel_requested"] = True
                job["updated_at"] = _utc_now_iso()

    deadline = time.time() + max(0.1, float(timeout_sec))
    for _job_id, thread in threads:
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        if thread.is_alive():
            thread.join(timeout=remaining)
    return {
        "jobs_signaled": len(stop_events),
        "threads_alive": sum(1 for _job_id, thread in threads if thread.is_alive()),
    }


def _shutdown_owned_run_processes(timeout_sec: float = 2.0) -> dict[str, Any]:
    if not STOP_RUNS_ON_SHUTDOWN:
        return {"enabled": False, "runs_signaled": 0, "alive_after_shutdown": 0}

    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, pid, run_dir
        FROM runs
        WHERE status IN ('running', 'stopping')
          AND supervisor_pid=?
          AND supervisor_started_ts=?
        """,
        (os.getpid(), SERVER_STARTED_TS),
    )
    rows = cur.fetchall()
    ts_now = _utc_ts()
    for row in rows:
        cur.execute(
            "UPDATE runs SET status=?, end_ts=?, restart_reason=? WHERE id=?",
            ("stopping", ts_now, "server_shutdown", int(row["id"])),
        )
    conn.commit()

    for row in rows:
        pid = int(row["pid"]) if row["pid"] else None
        if pid:
            _terminate_pid(pid, grace_sec=max(0.1, float(timeout_sec)))

    alive_after = 0
    for row in rows:
        pid = int(row["pid"]) if row["pid"] else None
        if pid and _pid_is_alive(pid):
            alive_after += 1
            cur.execute(
                "UPDATE runs SET status=?, end_ts=?, pid=?, restart_reason=? WHERE id=?",
                ("stopping", _utc_ts(), pid, "server_shutdown_timeout", int(row["id"])),
            )
        else:
            cur.execute(
                "UPDATE runs SET status=?, end_ts=?, pid=?, restart_reason=? WHERE id=?",
                ("stopped", _utc_ts(), None, "server_shutdown", int(row["id"])),
            )
    conn.commit()
    conn.close()
    return {"enabled": True, "runs_signaled": len(rows), "alive_after_shutdown": alive_after}


@app.on_event("shutdown")
def _shutdown_runtime_services() -> None:
    markets = _shutdown_markets_optimizer()
    assistant_news = _shutdown_assistant_news_jobs()
    runs = _shutdown_owned_run_processes()
    _log_runtime_event(
        "shutdown",
        markets_optimizer=markets,
        assistant_news=assistant_news,
        managed_runs=runs,
    )


@app.get("/runs", response_class=HTMLResponse)
def runs_page(request: Request):
    return render("runs.html", title="Runs", path="/runs", request=request)


@app.get("/runs/{run_id}", response_class=HTMLResponse)
def run_detail(run_id: int, request: Request):
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM runs WHERE id=?", (run_id,))
    r = cur.fetchone()
    conn.close()
    if not r:
        return PlainTextResponse("Run not found", status_code=404)
    run_obj = dict(r)
    algo_id = int(run_obj.get("algorithm_id")) if run_obj.get("algorithm_id") is not None else None
    return render(
        "run_detail.html",
        title=f"Run {run_id}",
        path="/runs",
        run=run_obj,
        run_algo_id=algo_id,
        request=request,
    )


@app.post("/runs/{run_id}/stop")
def run_stop(run_id: int, request: Request):
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM runs WHERE id=?", (run_id,))
    r = cur.fetchone()
    if not r:
        conn.close()
        return PlainTextResponse("Run not found", status_code=404)

    if r["status"] not in ("running", "stopping"):
        run_dir = str(r["run_dir"] or "")
        stale_proc = next(
            (
                proc
                for proc in _find_live_cryptid_processes()
                if run_dir and str(proc.get("run_dir") or "") == run_dir
            ),
            None,
        )
        if stale_proc is not None:
            pid = int(stale_proc["pid"])
            cur.execute("UPDATE runs SET status=?, end_ts=?, pid=? WHERE id=?", ("stopping", _utc_ts(), pid, run_id))
            conn.commit()
            _terminate_pid(pid)
            _mark_run_stopped_for_process(cur, pid=pid, run_dir=run_dir)
            conn.commit()
        conn.close()
        next_url = request.headers.get("HX-Current-URL") or request.headers.get("Referer") or f"/runs/{run_id}"
        return RedirectResponse(next_url, status_code=303)

    if not r["pid"]:
        cur.execute("UPDATE runs SET status=?, end_ts=?, pid=? WHERE id=?", ("stopped", _utc_ts(), None, run_id))
        conn.commit()
        conn.close()
        next_url = request.headers.get("HX-Current-URL") or request.headers.get("Referer") or f"/runs/{run_id}"
        return RedirectResponse(next_url, status_code=303)

    pid = int(r["pid"])
    cur.execute("UPDATE runs SET status=?, end_ts=? WHERE id=?", ("stopping", _utc_ts(), run_id))
    conn.commit()
    _terminate_pid(pid)
    alive = _pid_is_alive(pid)
    if alive:
        cur.execute("UPDATE runs SET status=?, end_ts=?, pid=? WHERE id=?", ("stopping", _utc_ts(), pid, run_id))
    else:
        cur.execute("UPDATE runs SET status=?, end_ts=?, pid=? WHERE id=?", ("stopped", _utc_ts(), None, run_id))
    conn.commit()
    conn.close()
    next_url = request.headers.get("HX-Current-URL") or request.headers.get("Referer") or f"/runs/{run_id}"
    return RedirectResponse(next_url, status_code=303)


@app.post("/runs/{run_id}/rerun")
def run_rerun(run_id: int, request: Request):
    conn = db()
    _refresh_run_processes(conn, run_id=run_id, allow_auto_restart=False)
    cur = conn.cursor()
    cur.execute("SELECT algorithm_id, status FROM runs WHERE id=?", (run_id,))
    r = cur.fetchone()
    conn.close()
    if not r:
        return PlainTextResponse("Run not found", status_code=404)

    status = str(r["status"] or "")
    if status == "running":
        next_url = request.headers.get("HX-Current-URL") or request.headers.get("Referer") or f"/runs/{run_id}"
        return RedirectResponse(next_url, status_code=303)

    algo_id = int(r["algorithm_id"])
    return run_algorithm(algo_id)


@app.post("/runs/stop_all")
def runs_stop_all(request: Request):
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT id, pid, status, run_dir FROM runs WHERE status IN ('running', 'stopping')")
    runs = cur.fetchall()

    # Phase 1: immediately mark targets as stopping/stopped in DB so concurrent
    # dashboard refreshes do not treat them as recoverable running jobs.
    ts_now = _utc_ts()
    pids_to_stop: list[dict[str, Any]] = []
    seen_pids: set[int] = set()
    for r in runs:
        rid = int(r["id"])
        pid = int(r["pid"]) if r["pid"] else None
        if pid:
            pids_to_stop.append({"run_id": rid, "pid": pid, "run_dir": str(r["run_dir"] or "")})
            seen_pids.add(pid)
            cur.execute(
                "UPDATE runs SET status=?, end_ts=? WHERE id=?",
                ("stopping", ts_now, rid),
            )
        else:
            cur.execute(
                "UPDATE runs SET status=?, end_ts=?, pid=? WHERE id=?",
                ("stopped", ts_now, None, rid),
            )
    conn.commit()

    for proc in _find_live_cryptid_processes():
        pid = int(proc["pid"])
        if pid in seen_pids:
            continue
        seen_pids.add(pid)
        run_dir = str(proc.get("run_dir") or "")
        pids_to_stop.append({"run_id": None, "pid": pid, "run_dir": run_dir})
        if run_dir:
            cur.execute(
                "UPDATE runs SET status=?, end_ts=?, pid=? WHERE run_dir=?",
                ("stopping", ts_now, pid, run_dir),
            )
    conn.commit()

    # Phase 2: terminate processes and finalize state.
    for proc in pids_to_stop:
        rid = proc.get("run_id")
        pid = int(proc["pid"])
        run_dir = str(proc.get("run_dir") or "")
        _terminate_pid(pid)
        alive = _pid_is_alive(pid)
        if alive:
            if rid is not None:
                cur.execute(
                    "UPDATE runs SET status=?, end_ts=?, pid=? WHERE id=?",
                    ("stopping", _utc_ts(), pid, int(rid)),
                )
            elif run_dir:
                cur.execute(
                    "UPDATE runs SET status=?, end_ts=?, pid=? WHERE run_dir=?",
                    ("stopping", _utc_ts(), pid, run_dir),
                )
        else:
            if rid is not None:
                cur.execute(
                    "UPDATE runs SET status=?, end_ts=?, pid=? WHERE id=?",
                    ("stopped", _utc_ts(), None, int(rid)),
                )
            else:
                _mark_run_stopped_for_process(cur, pid=pid, run_dir=run_dir)
    conn.commit()
    conn.close()
    next_url = request.headers.get("HX-Current-URL") or request.headers.get("Referer") or "/"
    return RedirectResponse(next_url, status_code=303)


@app.post("/runs/{run_id}/clear")
def run_clear(run_id: int):
    conn = db()
    _refresh_run_processes(conn, run_id=run_id, allow_auto_restart=False)
    cur = conn.cursor()
    cur.execute("SELECT run_dir, status, pid, params_json FROM runs WHERE id=?", (run_id,))
    r = cur.fetchone()
    if not r:
        conn.close()
        return PlainTextResponse("Run not found", status_code=404)

    status = str(r["status"] or "")
    pid = int(r["pid"]) if r["pid"] else None
    if status in ("running", "stopping") or _pid_is_alive(pid):
        conn.close()
        return RedirectResponse(f"/runs/{run_id}", status_code=303)

    run_dir = _safe_run_dir(r["run_dir"])
    try:
        if run_dir is not None and run_dir.exists():
            shutil.rmtree(run_dir)
    except Exception:
        pass

    cur.execute("DELETE FROM runs WHERE id=?", (run_id,))
    conn.commit()
    conn.close()
    return RedirectResponse("/runs", status_code=303)


_ANSI_SEQ_RE = re.compile(r"\x1b\[([0-9;]+)m")


def _ansi_line_to_html(line: str) -> str:
    if not line:
        return ""
    out: list[str] = []
    pos = 0
    span_open = False
    for m in _ANSI_SEQ_RE.finditer(line):
        out.append(html.escape(line[pos : m.start()]))
        codes = [c for c in str(m.group(1) or "").split(";") if c]
        if "0" in codes:
            if span_open:
                out.append("</span>")
                span_open = False
            codes = [c for c in codes if c != "0"]

        style_parts: list[str] = []
        for c in codes:
            if c == "1":
                style_parts.append("font-weight:700")
            elif c in ("31", "91"):
                style_parts.append("color:#ff5f5f")
            elif c in ("32", "92"):
                style_parts.append("color:#5fff87")
            elif c in ("33", "93"):
                style_parts.append("color:#ffd75f")
            elif c in ("36", "96"):
                style_parts.append("color:#5fd7ff")
            elif c in ("90",):
                style_parts.append("color:#9aa4b2")

        if style_parts:
            if span_open:
                out.append("</span>")
            style = "; ".join(style_parts)
            out.append(f"<span style='{style}'>")
            span_open = True
        pos = m.end()

    out.append(html.escape(line[pos:]))
    if span_open:
        out.append("</span>")
    return "".join(out)


def _ansi_block_to_html(lines: list[str]) -> str:
    rendered = [_ansi_line_to_html(ln) for ln in lines]
    return "<pre>" + "\n".join(rendered) + "</pre>"


_BROKER_ERROR_RE = re.compile(
    r"(error|exception|traceback|failed|rejected|forbidden|unauthorized|denied|invalid|timeout|unavailable|rate[ -]?limit|status(?:_code)?\s*[:=]?\s*(?:4\d\d|5\d\d)|\b4\d\d\b|\b5\d\d\b)",
    re.IGNORECASE,
)


def _tail_text_lines(log_path: Path, *, max_lines: int = 600, max_bytes: int = 240000) -> list[str]:
    if max_lines <= 0 or max_bytes <= 0:
        return []
    try:
        if not log_path.exists():
            return []
        with log_path.open("rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            read_size = min(int(max_bytes), int(size))
            if read_size > 0:
                f.seek(-read_size, os.SEEK_END)
            else:
                f.seek(0)
            raw = f.read()
        text = raw.decode("utf-8", errors="ignore")
        return text.splitlines()[-int(max_lines) :]
    except Exception:
        return []


def _is_broker_error_line(line: str) -> bool:
    txt = str(line or "").strip()
    if not txt:
        return False
    has_error_hint = bool(_BROKER_ERROR_RE.search(txt))
    if has_error_hint:
        return True
    low = txt.lower()
    if "[error]" in low or "[warn]" in low:
        return True
    return False


def _collect_broker_connection_errors(*, max_connections: int = 30, max_lines_per_item: int = 6) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in list_broker_connections()[: max(1, int(max_connections))]:
        status = str(row.get("status") or "").strip().lower()
        metadata = row.get("metadata")
        metadata = metadata if isinstance(metadata, dict) else {}

        err = str(metadata.get("error") or "").strip()
        status_flagged = status in ("error", "needs_auth", "needs_attention")
        if not err and not status_flagged:
            continue

        lines: list[str] = []
        if err:
            lines.append(err)
        elif status_flagged:
            lines.append(f"Connection status is '{status}'.")

        status_code = metadata.get("status_code")
        if status_code not in (None, ""):
            lines.append(f"status_code={status_code}")

        debug = metadata.get("debug")
        if debug not in (None, "", {}, []):
            try:
                dbg = json.dumps(debug, sort_keys=True, separators=(",", ":"))
            except Exception:
                dbg = str(debug)
            dbg = str(dbg).strip()
            if dbg:
                if len(dbg) > 280:
                    dbg = dbg[:277] + "..."
                lines.append(f"debug={dbg}")

        out.append(
            {
                "connection_id": int(row.get("id") or 0),
                "broker": str(row.get("broker") or ""),
                "label": str(row.get("label") or ""),
                "status": status,
                "lines": lines[-max(1, int(max_lines_per_item)) :],
                "updated_ts": int(row.get("updated_ts") or 0),
            }
        )
    return out


def _collect_broker_log_errors(*, max_runs: int = 40, max_lines_per_run: int = 8) -> list[dict[str, Any]]:
    conn = db()
    try:
        _refresh_run_processes(conn)
        cur = conn.cursor()
        cur.execute(
            "SELECT id, algorithm_name, status, run_dir, start_ts, end_ts FROM runs ORDER BY id DESC LIMIT ?",
            (max(1, int(max_runs)),),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    out: list[dict[str, Any]] = []
    for r in rows:
        run_dir = Path(str(r["run_dir"]))
        log_path = run_dir / "algo.log"
        lines = _tail_text_lines(log_path, max_lines=900, max_bytes=280000)
        if not lines:
            continue
        matches = [ln.rstrip() for ln in lines if _is_broker_error_line(ln)]
        if not matches:
            continue
        try:
            log_mtime = int(log_path.stat().st_mtime)
        except Exception:
            log_mtime = None
        out.append(
            {
                "run_id": int(r["id"]),
                "algorithm_name": str(r["algorithm_name"] or ""),
                "status": str(r["status"] or ""),
                "match_count": int(len(matches)),
                "lines": matches[-max(1, int(max_lines_per_run)) :],
                "log_mtime": log_mtime,
            }
        )
    return out


@app.get("/runs/{run_id}/logs", response_class=HTMLResponse)
def run_logs(run_id: int):
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT run_dir FROM runs WHERE id=?", (run_id,))
    r = cur.fetchone()
    conn.close()
    if not r:
        return HTMLResponse("<pre>Run not found</pre>", status_code=404)

    log_path = Path(r["run_dir"]) / "algo.log"
    if not log_path.exists():
        return HTMLResponse("<pre>No logs yet.</pre>")

    try:
        _trim_log_file(log_path)
        text = log_path.read_text(errors="ignore")
        lines = text.splitlines()[-400:]
        return HTMLResponse(_ansi_block_to_html(lines))
    except Exception as e:
        return HTMLResponse("<pre>Error reading logs: %s</pre>" % e)


@app.get("/runs/{run_id}/status", response_class=HTMLResponse)
def run_status(run_id: int):
    conn = db()
    _refresh_run_processes(conn, run_id=run_id)
    cur = conn.cursor()
    cur.execute("SELECT run_dir, status, pid, params_json FROM runs WHERE id=?", (run_id,))
    r = cur.fetchone()
    conn.close()
    if not r:
        return HTMLResponse("Not found", status_code=404)

    status_path = Path(r["run_dir"]) / "status.json"
    payload = None
    if status_path.exists():
        try:
            payload = json.loads(status_path.read_text())
        except Exception:
            payload = None

    params_obj: dict[str, Any] = {}
    timeframe = None
    try:
        params_obj = _safe_json(r["params_json"], default={})
        if isinstance(params_obj, dict):
            tf = params_obj.get("timeframe")
            if tf is not None:
                timeframe = str(tf).strip()
        else:
            params_obj = {}
    except Exception:
        params_obj = {}
        timeframe = None

    badge_class = "ok" if r["status"] == "running" else "warn"
    tf_badge = f"<span class='badge'>TF {html.escape(timeframe)}</span>" if timeframe else ""
    html_out = (
        "<div class='row'>"
        f"<span class='badge {badge_class}'>{r['status']}</span>"
        f"<span class='small'>PID {r['pid'] or '—'}</span>"
        f"{tf_badge}"
        "</div>"
    )

    if payload:
        hb = payload.get("ts") or payload.get("heartbeat") or payload.get("last_heartbeat") or "—"
        pnl = payload.get("pnl", "—")
        trades = payload.get("trades", "—")
        tf_payload = payload.get("timeframe")
        tf_display = str(tf_payload).strip() if tf_payload is not None else timeframe
        tf_badge_row = f"<span class='badge'>TF {html.escape(tf_display)}</span>" if tf_display else ""
        html_out += f"<div class='small'>Last heartbeat: {hb}</div>"
        html_out += (
            "<div class='row'>"
            f"<span class='badge'>PNL {pnl}</span>"
            f"<span class='badge'>Trades {trades}</span>"
            f"{tf_badge_row}"
            "</div>"
        )
        if payload.get("reason"):
            html_out += f"<div class='small'>Last reason: {html.escape(str(payload.get('reason')))}</div>"

        tickers = payload.get("tickers")
        if isinstance(tickers, list) and tickers:
            def _fmt_pct(val: Any) -> str:
                try:
                    return f"{float(val):+.2f}%"
                except Exception:
                    return "—"

            def _fmt_num(val: Any, digits: int = 2) -> str:
                try:
                    return f"{float(val):.{digits}f}"
                except Exception:
                    return "—"

            def _to_float(val: Any) -> Optional[float]:
                try:
                    return float(val)
                except Exception:
                    return None

            def _fmt_avg_buy(val: Any) -> str:
                v = _to_float(val)
                if v is None or v <= 0:
                    return "—"
                if abs(v) >= 1:
                    return f"{v:.2f}"
                txt = f"{v:.6f}".rstrip("0").rstrip(".")
                return txt if txt else "0"

            def _fmt_gain_dollar(val: Any) -> str:
                v = _to_float(val)
                if v is None:
                    return "—"
                sign = "+" if v >= 0 else "-"
                return f"{sign}${abs(v):,.2f}"

            def _fmt_money_plain(val: Any) -> str:
                v = _to_float(val)
                if v is None:
                    return "—"
                return f"${v:,.2f}"

            def _level_gap_pct(level: Any, current: Any, existing_gap_pct: Any = None) -> Optional[float]:
                gap = _to_float(existing_gap_pct)
                if gap is not None:
                    return gap
                lv = _to_float(level)
                cp = _to_float(current)
                if lv is None or lv <= 0 or cp is None:
                    return None
                return ((cp - lv) / lv) * 100.0

            def _fmt_level_with_gap(level: Any, gap_pct: Any) -> str:
                lv = _to_float(level)
                if lv is None or lv <= 0:
                    return "—"
                gap = _to_float(gap_pct)
                if gap is None:
                    return _fmt_num(lv)
                return f"{_fmt_num(lv)} ({_fmt_pct(gap)})"

            script_tag = str(payload.get("script") or "")
            indicator_mode = script_tag in (
                "DreadFox.Stock.Robinhood",
                "DreadFox.Stock.Schwab",
                "Rokurokubi.Options.Robinhood",
                "Rokurokubi.Options.Schwab",
            )
            options_mode = script_tag in ("Rokurokubi.Options.Robinhood", "Rokurokubi.Options.Schwab")
            superhex_mode = script_tag in ("Superhexagon.Robinhood", "Superhexagon.Schwab", "FoxScry.Robinhood")
            crypto_chart_mode = script_tag in ("Dreadfox.Crypto.Robinhood", "IndicatorForge.Crypto.Robinhood")
            indicatorforge_mode = script_tag in (
                "IndicatorForge.Robinhood",
                "IndicatorForge.Crypto.Robinhood",
                "IndicatorForge.Schwab",
                "EntangledTickers.Robinhood",
                "EntangledTickers.Schwab",
            )

            def _indicator_classes(t: dict[str, Any]) -> dict[str, str]:
                if not indicator_mode:
                    return {}
                price = _to_float(t.get("price"))
                ma20 = _to_float(t.get("ma20"))
                ma78 = _to_float(t.get("ma78"))
                ma150 = _to_float(t.get("ma150"))
                rsi = _to_float(t.get("rsi"))
                rsi_d = _to_float(t.get("rsi_d"))

                classes: dict[str, str] = {}
                if price is not None and ma20 is not None:
                    if ma78 is not None and ma150 is not None and price > ma20 and price > ma78 and price > ma150:
                        classes["ma20"] = "indicator-sell"
                    elif ma78 is not None and price > ma20 and price < ma78:
                        classes["ma20"] = "indicator-buy"
                if price is not None and ma78 is not None:
                    if price > ma78:
                        classes["ma78"] = "indicator-sell"
                    elif price < ma78:
                        classes["ma78"] = "indicator-buy"
                if price is not None and ma150 is not None:
                    if price > ma150:
                        classes["ma150"] = "indicator-sell"
                    elif price < ma150:
                        classes["ma150"] = "indicator-buy"
                if rsi is not None:
                    if rsi > 69:
                        classes["rsi"] = "indicator-sell"
                    elif 30 < rsi < 55:
                        classes["rsi"] = "indicator-buy"
                if rsi_d is not None:
                    if rsi_d < 1:
                        classes["rsi_d"] = "indicator-sell"
                    elif rsi_d > 1:
                        classes["rsi_d"] = "indicator-buy"
                return classes

            def _superhex_indicator_classes(t: dict[str, Any]) -> dict[str, str]:
                if not superhex_mode:
                    return {}
                price = _to_float(t.get("price"))
                ma20 = _to_float(t.get("ma20"))
                ma78 = _to_float(t.get("ma78"))
                ma150 = _to_float(t.get("ma150"))
                rsi = _to_float(t.get("rsi"))
                rsi_d = _to_float(t.get("rsi_d"))

                classes: dict[str, str] = {}
                if price is not None and ma20 is not None:
                    if ma78 is not None and ma150 is not None and price > ma20 and price > ma78 and price > ma150:
                        classes["ma20"] = "indicator-sell"
                    elif (
                        ma78 is not None
                        and ma150 is not None
                        and price > ma20
                        and price < ma78
                        and price < ma150
                    ):
                        classes["ma20"] = "indicator-buy"
                if price is not None and ma78 is not None:
                    if price > ma78:
                        classes["ma78"] = "indicator-sell"
                    elif price < ma78:
                        classes["ma78"] = "indicator-buy"
                if price is not None and ma150 is not None:
                    if price > ma150:
                        classes["ma150"] = "indicator-sell"
                    elif price < ma150:
                        classes["ma150"] = "indicator-buy"
                if rsi is not None:
                    if rsi > 69:
                        classes["rsi"] = "indicator-sell"
                    elif 30 < rsi < 55:
                        classes["rsi"] = "indicator-buy"
                if rsi_d is not None:
                    if rsi_d < 0.5:
                        classes["rsi_d"] = "indicator-sell"
                    elif rsi_d > 0.25:
                        classes["rsi_d"] = "indicator-buy"
                return classes

            def _foxbalance_indicator_classes(t: dict[str, Any], role_hint: str) -> dict[str, str]:
                if role_hint not in ("LIQ", "ACQ"):
                    return {}

                price = _to_float(t.get("price"))
                ma20 = _to_float(t.get("ma20"))
                ma78 = _to_float(t.get("ma78"))
                ma150 = _to_float(t.get("ma150"))
                rsi = _to_float(t.get("rsi"))
                rsi_d = _to_float(t.get("rsi_d"))

                classes: dict[str, str] = {}
                if role_hint == "LIQ":
                    if price is not None and ma20 is not None and price > ma20:
                        classes["ma20"] = "indicator-sell"
                    if price is not None and ma78 is not None and price > ma78:
                        classes["ma78"] = "indicator-sell"
                    if price is not None and ma150 is not None and price > ma150:
                        classes["ma150"] = "indicator-sell"
                    if rsi is not None and rsi > 70:
                        classes["rsi"] = "indicator-sell"
                    if rsi_d is not None and rsi_d < 0.5:
                        classes["rsi_d"] = "indicator-sell"
                else:
                    if price is not None and ma20 is not None and price > ma20:
                        classes["ma20"] = "indicator-buy"
                    if price is not None and ma78 is not None and price < ma78:
                        classes["ma78"] = "indicator-buy"
                    if price is not None and ma150 is not None and price < ma150:
                        classes["ma150"] = "indicator-buy"
                    if rsi is not None and 30 < rsi < 55:
                        classes["rsi"] = "indicator-buy"
                    if rsi_d is not None and rsi_d > 1:
                        classes["rsi_d"] = "indicator-buy"
                return classes

            def _chart_svg(chart: Any) -> str:
                if not isinstance(chart, dict):
                    return "<span class='small'>—</span>"
                series = {
                    "price": chart.get("price"),
                    "open": chart.get("open"),
                    "high": chart.get("high"),
                    "low": chart.get("low"),
                    "ma20": chart.get("ma20"),
                    "ma78": chart.get("ma78"),
                    "ma150": chart.get("ma150"),
                }
                price = series["price"]
                if not isinstance(price, list) or len(price) < 2:
                    return "<span class='small'>—</span>"

                numeric_values: list[float] = []
                for vals in series.values():
                    if isinstance(vals, list):
                        for v in vals:
                            if isinstance(v, (int, float)):
                                fv = float(v)
                                if math.isfinite(fv) and fv > 0:
                                    numeric_values.append(fv)
                if not numeric_values:
                    return "<span class='small'>—</span>"

                min_v = min(numeric_values)
                max_v = max(numeric_values)
                rng = max(max_v - min_v, 1e-9)
                width = 240.0
                height = 80.0
                display_width = 700
                display_height = 220

                def _path(values: Any) -> str:
                    if not isinstance(values, list) or len(values) < 2:
                        return ""
                    n = len(values)
                    d: list[str] = []
                    for i, v in enumerate(values):
                        if not isinstance(v, (int, float)):
                            continue
                        fv = float(v)
                        if (not math.isfinite(fv)) or fv <= 0:
                            continue
                        x = (i / (n - 1)) * width
                        y = height - ((fv - min_v) / rng) * height
                        cmd = "M" if not d else "L"
                        d.append(f"{cmd}{x:.2f},{y:.2f}")
                    return " ".join(d)

                def _candles() -> str:
                    prices = series["price"]
                    opens = series.get("open")
                    highs = series.get("high")
                    lows = series.get("low")
                    if not (
                        isinstance(prices, list)
                        and isinstance(opens, list)
                        and isinstance(highs, list)
                        and isinstance(lows, list)
                    ):
                        return ""
                    n = min(len(prices), len(opens), len(highs), len(lows))
                    if n < 2:
                        return ""
                    slot_w = width / max(1, n - 1)
                    body_w = max(1.0, min(4.8, slot_w * 0.56))
                    parts: list[str] = ["<g class='chart-price-candles'>"]
                    for i in range(n):
                        try:
                            o = float(opens[i])
                            h = float(highs[i])
                            l = float(lows[i])
                            c = float(prices[i])
                        except Exception:
                            continue
                        if not all(math.isfinite(v) and v > 0.0 for v in (o, h, l, c)):
                            continue
                        x = (i / max(1, n - 1)) * width
                        yo = height - ((o - min_v) / rng) * height
                        yh = height - ((max(h, o, c) - min_v) / rng) * height
                        yl = height - ((min(l, o, c) - min_v) / rng) * height
                        yc = height - ((c - min_v) / rng) * height
                        y_top = min(yo, yc)
                        body_h = max(1.0, abs(yc - yo))
                        color = "#22c55e" if c >= o else "#ef4444"
                        parts.append(
                            f"<line x1='{x:.2f}' y1='{yh:.2f}' x2='{x:.2f}' y2='{yl:.2f}' "
                            f"stroke='{color}' stroke-width='0.8' opacity='0.86'/>"
                        )
                        parts.append(
                            f"<rect x='{(x - (body_w / 2.0)):.2f}' y='{y_top:.2f}' width='{body_w:.2f}' height='{body_h:.2f}' "
                            f"fill='{color}' stroke='{color}' stroke-width='0.45' opacity='0.86'/>"
                        )
                    parts.append("</g>")
                    return "".join(parts)

                paths: list[str] = []
                candle_markup = _candles()
                if candle_markup:
                    paths.append(candle_markup)
                price_path = _path(series["price"])
                if price_path:
                    paths.append(f"<path class='chart-price chart-price-line' d='{price_path}'/>")
                ma20_path = _path(series["ma20"])
                if ma20_path:
                    paths.append(f"<path class='chart-ma20' d='{ma20_path}'/>")
                ma78_path = _path(series["ma78"])
                if ma78_path:
                    paths.append(f"<path class='chart-ma78' d='{ma78_path}'/>")
                ma150_path = _path(series["ma150"])
                if ma150_path:
                    paths.append(f"<path class='chart-ma150' d='{ma150_path}'/>")

                if not paths:
                    return "<span class='small'>—</span>"

                return (
                    f"<svg class='chart-spark run-status-chart' viewBox='0 0 {int(width)} {int(height)}' "
                    f"preserveAspectRatio='none' width='{display_width}' height='{display_height}' "
                    f"style='width:{display_width}px;height:{display_height}px;display:block;max-width:none;'>"
                    + "".join(paths)
                    + "</svg>"
                )

            def _render_ticker_table(
                title: str,
                rows: list[dict[str, Any]],
                *,
                indicator_mode: bool,
                show_chart: bool,
                superhex_mode: bool = False,
                options_mode: bool = False,
                role_hint: str = "",
            ) -> str:
                if not rows:
                    return f"<div class='small' style='margin-top:8px'>{title}: none</div>"
                foxscry_mode = script_tag == "FoxScry.Robinhood"
                ma_short_label = "MA30" if script_tag == "FoxScry.Robinhood" else "MA20"
                chart_head = "<th class='chart-col'>Chart</th>" if show_chart else ""
                ma30_d_head = "<th>d30MA</th>" if foxscry_mode else ""
                alloc_label = "Alloc%"
                delta_label = "Delta%"
                if superhex_mode:
                    alloc_label = "Held%"
                    delta_label = "Cap Δ%"
                show_stoploss_cols = superhex_mode or any(
                    isinstance(row, dict)
                    and any(
                        k in row
                        for k in (
                            "stoploss_armed",
                            "stoploss_trigger",
                            "stoploss_arm_price",
                            "stoploss_peak_pct",
                            "stoploss_arm_gap_pct",
                            "stoploss_trigger_gap_pct",
                        )
                    )
                    for row in rows
                )
                sl_head = ""
                if superhex_mode:
                    sl_head = "<th>SL Armed</th><th>SL Peak%</th><th>SL Trigger</th>"
                elif show_stoploss_cols:
                    sl_head = "<th>SL Armed</th><th>SL Arm @</th><th>SL Trigger</th>"
                opt_head = ""
                if options_mode:
                    opt_head = (
                        "<th>CC Target</th><th>BE</th><th>Cap Gain $</th><th>Cap Gain %</th>"
                        "<th>CC B/A</th><th>Scan Q/C</th><th>Top Reject</th><th>CC Shortlist</th>"
                    )
                out = [
                    f"<div class='small' style='margin-top:8px'>{title}</div>",
                    "<div class='status-table-wrap'><table>",
                    "<thead><tr>"
                    "<th>Symbol</th><th>Avg Buy</th><th>Gain %</th><th>Gain $</th><th>Role</th><th>Signal</th><th>Price</th><th>Qty</th>"
                    f"<th>P/L%</th><th>{alloc_label}</th><th>{delta_label}</th>"
                    f"<th>RSI</th><th>dRSI</th>{ma30_d_head}<th>{ma_short_label}</th><th>MA78</th><th>MA190</th><th>ATR</th>"
                    f"{sl_head}{opt_head}{chart_head}"
                    "</tr></thead><tbody>",
                ]
                for t in rows:
                    sym = html.escape(str(t.get("symbol") or "—"))
                    role = html.escape(str(t.get("role") or ""))
                    signal = str(t.get("signal") or "HOLD").upper()
                    signal_class = "signal-hold"
                    if signal == "BUY":
                        signal_class = "signal-buy"
                    elif signal == "SELL":
                        signal_class = "signal-sell"
                    price = _fmt_num(t.get("price"))
                    qty = _fmt_num(t.get("qty"), 4)
                    pnl_pct = _fmt_pct(t.get("pnl_pct"))
                    avg_buy_val = _to_float(t.get("avg_buy"))
                    price_val = _to_float(t.get("price"))
                    qty_val = _to_float(t.get("qty"))
                    gain_pct_val = _to_float(t.get("pnl_pct"))
                    if gain_pct_val is None and price_val is not None and avg_buy_val is not None and avg_buy_val > 0:
                        gain_pct_val = ((price_val - avg_buy_val) / avg_buy_val) * 100.0
                    gain_dollar_val: Optional[float] = None
                    if qty_val is not None and price_val is not None and avg_buy_val is not None and avg_buy_val > 0:
                        gain_dollar_val = (price_val - avg_buy_val) * qty_val
                    avg_buy_txt = _fmt_avg_buy(avg_buy_val)
                    gain_pct_txt = _fmt_pct(gain_pct_val)
                    gain_dollar_txt = _fmt_gain_dollar(gain_dollar_val)
                    alloc_val = t.get("alloc_pct")
                    delta_val = t.get("delta_pct")
                    if superhex_mode:
                        if t.get("held_pct") is not None:
                            alloc_val = t.get("held_pct")
                        if t.get("cap_delta_pct") is not None:
                            delta_val = t.get("cap_delta_pct")
                    alloc_pct = _fmt_pct(alloc_val)
                    delta_pct = _fmt_pct(delta_val)
                    rsi = _fmt_num(t.get("rsi"))
                    rsi_d = _fmt_num(t.get("rsi_d"))
                    ma30_d = _fmt_num(t.get("ma30_d")) if foxscry_mode else ""
                    ma20 = _fmt_num(t.get("ma20"))
                    ma78 = _fmt_num(t.get("ma78"))
                    ma150 = _fmt_num(t.get("ma150"))
                    atr = _fmt_num(t.get("atr"))
                    if indicator_mode:
                        ind_classes = _indicator_classes(t)
                    elif superhex_mode:
                        ind_classes = _superhex_indicator_classes(t)
                    elif role_hint:
                        ind_classes = _foxbalance_indicator_classes(t, role_hint)
                    else:
                        ind_classes = {}
                    rsi_cls = ind_classes.get("rsi", "")
                    rsi_d_cls = ind_classes.get("rsi_d", "")
                    ma20_cls = ind_classes.get("ma20", "")
                    ma78_cls = ind_classes.get("ma78", "")
                    ma150_cls = ind_classes.get("ma150", "")
                    rsi_td = f"<td class='{rsi_cls}'>{rsi}</td>" if rsi_cls else f"<td>{rsi}</td>"
                    rsi_d_td = f"<td class='{rsi_d_cls}'>{rsi_d}</td>" if rsi_d_cls else f"<td>{rsi_d}</td>"
                    ma30_d_td = ""
                    if foxscry_mode:
                        ma30_d_num = _to_float(t.get("ma30_d"))
                        ma30_d_cls = ""
                        if ma30_d_num is not None:
                            if ma30_d_num > 0:
                                ma30_d_cls = "indicator-buy"
                            elif ma30_d_num < 0:
                                ma30_d_cls = "indicator-sell"
                        ma30_d_td = f"<td class='{ma30_d_cls}'>{ma30_d}</td>" if ma30_d_cls else f"<td>{ma30_d}</td>"
                    ma20_td = f"<td class='{ma20_cls}'>{ma20}</td>" if ma20_cls else f"<td>{ma20}</td>"
                    ma78_td = f"<td class='{ma78_cls}'>{ma78}</td>" if ma78_cls else f"<td>{ma78}</td>"
                    ma150_td = f"<td class='{ma150_cls}'>{ma150}</td>" if ma150_cls else f"<td>{ma150}</td>"
                    delta_cls = ""
                    if superhex_mode:
                        delta_num = _to_float(delta_val)
                        if delta_num is not None:
                            if delta_num > 0:
                                delta_cls = "cap-over"
                            elif delta_num < 0:
                                delta_cls = "cap-under"
                    delta_td = f"<td class='{delta_cls}'>{delta_pct}</td>" if delta_cls else f"<td>{delta_pct}</td>"
                    chart_td = f"<td class='chart-cell'>{_chart_svg(t.get('chart'))}</td>" if show_chart else ""
                    sl_td = ""
                    if superhex_mode:
                        sl_armed = "Yes" if t.get("stoploss_armed") else "No"
                        sl_peak = _fmt_pct(t.get("stoploss_peak_pct")) if t.get("stoploss_armed") else "—"
                        sl_trigger = _fmt_num(t.get("stoploss_trigger")) if t.get("stoploss_armed") else "—"
                        sl_td = f"<td>{sl_armed}</td><td>{sl_peak}</td><td>{sl_trigger}</td>"
                    elif show_stoploss_cols:
                        sl_armed_raw = t.get("stoploss_armed")
                        if sl_armed_raw is None:
                            sl_armed = "—"
                        else:
                            sl_armed = "Yes" if bool(sl_armed_raw) else "No"
                        arm_level = t.get("stoploss_arm_price")
                        trigger_level = t.get("stoploss_trigger")
                        arm_gap = _level_gap_pct(arm_level, t.get("price"), t.get("stoploss_arm_gap_pct"))
                        trigger_gap = _level_gap_pct(trigger_level, t.get("price"), t.get("stoploss_trigger_gap_pct"))
                        arm_txt = _fmt_level_with_gap(arm_level, arm_gap)
                        trigger_txt = _fmt_level_with_gap(trigger_level, trigger_gap)
                        sl_td = f"<td>{sl_armed}</td><td>{arm_txt}</td><td>{trigger_txt}</td>"
                    opt_td = ""
                    if options_mode:
                        exp = t.get("cc_best_exp")
                        strike = t.get("cc_best_strike")
                        if exp and strike is not None:
                            opt_target = f"{exp} {float(strike):.2f}C"
                        else:
                            opt_target = "—"
                        opt_be = _fmt_num(t.get("cc_breakeven"))
                        opt_cap = _fmt_num(t.get("cc_capped_gain_contract"))
                        opt_cap_pct = _fmt_pct(t.get("cc_capped_gain_pct"))
                        bid_txt = _fmt_num(t.get("cc_best_bid"))
                        ask_txt = _fmt_num(t.get("cc_best_ask"))
                        opt_ba = "—"
                        if bid_txt != "—" or ask_txt != "—":
                            opt_ba = f"{bid_txt}/{ask_txt}"
                        q = t.get("cc_qualified_count")
                        c = t.get("cc_candidates_checked")
                        scan_txt = "—"
                        if q is not None or c is not None:
                            scan_txt = f"{int(q or 0)}/{int(c or 0)}"
                        rej_reason = t.get("cc_top_reject_reason")
                        rej_count = t.get("cc_top_reject_count")
                        rej_txt = "—"
                        if rej_reason:
                            rej_txt = f"{rej_reason} ({int(rej_count or 0)})"
                        shortlist_txt = t.get("cc_shortlist_text")
                        shortlist_cell = html.escape(str(shortlist_txt)) if shortlist_txt else "—"
                        opt_td = (
                            f"<td>{opt_target}</td><td>{opt_be}</td><td>{opt_cap}</td><td>{opt_cap_pct}</td>"
                            f"<td>{opt_ba}</td><td>{scan_txt}</td><td>{html.escape(str(rej_txt))}</td><td class='small'>{shortlist_cell}</td>"
                        )

                    out.append(
                        "<tr>"
                        f"<td><b>{sym}</b></td>"
                        f"<td>{avg_buy_txt}</td>"
                        f"<td>{gain_pct_txt}</td>"
                        f"<td>{gain_dollar_txt}</td>"
                        f"<td class='small'>{role or '—'}</td>"
                        f"<td class='{signal_class}'>{html.escape(signal)}</td>"
                        f"<td>{price}</td>"
                        f"<td>{qty}</td>"
                        f"<td>{pnl_pct}</td>"
                        f"<td>{alloc_pct}</td>"
                        f"{delta_td}"
                        f"{rsi_td}"
                        f"{rsi_d_td}"
                        f"{ma30_d_td}"
                        f"{ma20_td}"
                        f"{ma78_td}"
                        f"{ma150_td}"
                        f"<td>{atr}</td>"
                        f"{sl_td}{opt_td}{chart_td}"
                        "</tr>"
                    )
                out.append("</tbody></table></div>")
                return "".join(out)

            def _render_indicatorforge_runtime_table(rows: list[dict[str, Any]]) -> str:
                if not rows:
                    return "<div class='small' style='margin-top:8px'>Ticker Status: none</div>"

                runtime_rules_raw = params_obj.get("indicator_rules_json") if isinstance(params_obj, dict) else []
                source_hint = (
                    "robinhood_crypto"
                    if script_tag == "IndicatorForge.Crypto.Robinhood"
                    else
                    "schwab"
                    if script_tag in ("IndicatorForge.Schwab", "EntangledTickers.Schwab")
                    else "robinhood"
                )
                include_history_extended = source_hint == "robinhood_crypto"
                if source_hint in ("robinhood", "schwab") and isinstance(params_obj, dict):
                    raw_ext = params_obj.get("include_extended_hours_data")
                    if isinstance(raw_ext, bool):
                        include_history_extended = raw_ext
                    elif isinstance(raw_ext, (int, float)):
                        include_history_extended = float(raw_ext) != 0.0
                    else:
                        include_history_extended = str(raw_ext or "").strip().lower() in ("1", "true", "yes", "on", "y")
                market_tf_raw = tf_display if tf_display else timeframe
                market_tf = str(market_tf_raw or "").strip()
                if not market_tf and isinstance(params_obj, dict):
                    market_tf = str(params_obj.get("timeframe") or "").strip()
                if not market_tf:
                    market_tf = "1h"
                runtime_rules = _rules_with_default_timeframe(
                    _normalize_indicator_rules_payload(runtime_rules_raw, default_timeframe=market_tf),
                    market_tf,
                )
                runtime_rules_by_tf = _rules_by_timeframe(runtime_rules, market_tf)
                runtime_chart_cfg_by_tf = {
                    tf: _indicator_rules_chart_config(tf_rules) for tf, tf_rules in runtime_rules_by_tf.items()
                }
                runtime_candle_target_by_tf: dict[str, int] = {}
                for tf, cfg in runtime_chart_cfg_by_tf.items():
                    min_required = max(2, int(cfg["min_required"]))
                    target = max(min_required, 600) if source_hint == "schwab" else min_required
                    if source_hint == "schwab" and str(tf).strip().lower() == "1h":
                        target = max(min_required + 80, min_required)
                    runtime_candle_target_by_tf[tf] = target
                summary_html = _render_indicator_rule_summary_panel(runtime_rules, title="Configured Indicator Rules")
                robinhood_markets_ok = True
                if source_hint in ("robinhood", "robinhood_crypto"):
                    ok_rh, _msg_rh = _ensure_robinhood_markets_session()
                    robinhood_markets_ok = bool(ok_rh)
                fetched_ohlcv_cache: dict[str, tuple[list[float], ...]] = {}
                chart_closes_cache: dict[str, list[float]] = {}
                chart_ohlc_cache: dict[str, tuple[list[float], ...]] = {}

                def _indicatorforge_runtime_chart(*, symbol: str, timeframe_key: str, chart: Any, price_hint: Any) -> str:
                    tf = _normalize_indicator_rule_timeframe(timeframe_key, default=market_tf)
                    runtime_chart_cfg = runtime_chart_cfg_by_tf.get(tf) or _indicator_rules_chart_config([])
                    runtime_candle_target = int(runtime_candle_target_by_tf.get(tf) or max(2, int(runtime_chart_cfg.get("min_required") or 30)))

                    def _trim_tail_outlier(values: list[float], expected_last: Optional[float]) -> list[float]:
                        # Drop a likely bad terminal quote if it is far outside recent candle volatility.
                        n = len(values)
                        if n < 25:
                            return values
                        prev = float(values[-2])
                        last = float(values[-1])
                        if expected_last is not None and expected_last > 0:
                            if abs(last - expected_last) / max(abs(expected_last), 1e-9) <= 0.005:
                                return values
                        if prev <= 0.0:
                            return values
                        tail_move = abs(last - prev) / prev
                        recent_moves: list[float] = []
                        start_idx = max(1, n - 25)
                        for i in range(start_idx, n - 1):
                            base = float(values[i - 1])
                            cur = float(values[i])
                            if base <= 0.0:
                                continue
                            recent_moves.append(abs(cur - base) / base)
                        baseline = (sum(recent_moves) / float(len(recent_moves))) if recent_moves else 0.0
                        limit = max(0.08, baseline * 8.0)
                        if tail_move > limit:
                            return values[:-1]
                        return values

                    chart_obj = chart if isinstance(chart, dict) else {}
                    raw_prices = chart_obj.get("price")
                    if not isinstance(raw_prices, list):
                        raw_prices = []
                    symbol_key = str(symbol or "").strip().upper()
                    cache_key = f"{symbol_key}|{tf}"

                    def _chart_float_series(raw: Any) -> list[float]:
                        vals: list[float] = []
                        if not isinstance(raw, list):
                            return vals
                        for v in raw:
                            try:
                                fv = float(v)
                            except Exception:
                                continue
                            if (not math.isfinite(fv)) or fv <= 0.0:
                                continue
                            vals.append(fv)
                        return vals

                    closes: list[float] = []
                    for v in raw_prices:
                        try:
                            fv = float(v)
                        except Exception:
                            continue
                        if (not math.isfinite(fv)) or fv <= 0.0:
                            continue
                        closes.append(fv)
                    ohlc: Optional[tuple[list[float], ...]] = None
                    chart_opens = _chart_float_series(chart_obj.get("open"))
                    chart_highs = _chart_float_series(chart_obj.get("high"))
                    chart_lows = _chart_float_series(chart_obj.get("low"))
                    chart_volumes = _chart_float_series(chart_obj.get("volume"))
                    raw_timestamps = chart_obj.get("timestamp") or chart_obj.get("time") or chart_obj.get("begins_at")
                    chart_timestamps = [str(v or "") for v in raw_timestamps] if isinstance(raw_timestamps, list) else []
                    if (
                        len(chart_opens) == len(closes)
                        and len(chart_highs) == len(closes)
                        and len(chart_lows) == len(closes)
                    ):
                        if len(chart_volumes) != len(closes):
                            chart_volumes = [0.0] * len(closes)
                        if len(chart_timestamps) != len(closes):
                            chart_timestamps = [""] * len(closes)
                        ohlc = (chart_opens, chart_highs, chart_lows, list(closes), chart_volumes, chart_timestamps)
                    if len(closes) < runtime_candle_target:
                        fetched_ohlcv = fetched_ohlcv_cache.get(cache_key)
                        if fetched_ohlcv is None and (source_hint not in ("robinhood", "robinhood_crypto") or robinhood_markets_ok):
                            try:
                                fo, fh, fl, fc, fv, ft, _fr, _fb = _market_fetch_ohlcv(
                                    symbol_key,
                                    tf,
                                    broker_hint=source_hint,
                                    min_candles=runtime_candle_target,
                                    include_extended=include_history_extended,
                                )
                            except Exception:
                                fetched_ohlcv = ([], [], [], [], [], [])
                            else:
                                fetched_ohlcv = (fo, fh, fl, fc, fv, ft)
                            fetched_ohlcv_cache[cache_key] = fetched_ohlcv
                        if (
                            isinstance(fetched_ohlcv, tuple)
                            and len(fetched_ohlcv) >= 4
                            and len(fetched_ohlcv[3]) >= 2
                        ):
                            n_fetch = min(len(fetched_ohlcv[0]), len(fetched_ohlcv[1]), len(fetched_ohlcv[2]), len(fetched_ohlcv[3]))
                            fc = [
                                float(x)
                                for x in list(fetched_ohlcv[3])[:n_fetch]
                                if isinstance(x, (int, float)) and math.isfinite(float(x)) and float(x) > 0.0
                            ]
                            closes = fc
                            fo = list(fetched_ohlcv[0])[: len(fc)]
                            fh = list(fetched_ohlcv[1])[: len(fc)]
                            fl = list(fetched_ohlcv[2])[: len(fc)]
                            fv = list(fetched_ohlcv[4])[: len(fc)] if len(fetched_ohlcv) >= 5 else [0.0] * len(fc)
                            ft = list(fetched_ohlcv[5])[: len(fc)] if len(fetched_ohlcv) >= 6 else [""] * len(fc)
                            ohlc = (fo, fh, fl, fc, fv, ft)
                    expected_last = _to_float(price_hint)
                    if expected_last is not None and expected_last > 0.0 and math.isfinite(expected_last):
                        if not closes:
                            closes = [float(expected_last)]
                        elif abs(closes[-1] - float(expected_last)) > 1e-9:
                            prev_close = float(closes[-1])
                            closes.append(float(expected_last))
                            if ohlc is not None:
                                o, h, l, c = ohlc[:4]
                                v = list(ohlc[4]) if len(ohlc) >= 5 else []
                                t = list(ohlc[5]) if len(ohlc) >= 6 else []
                                o.append(prev_close)
                                h.append(max(prev_close, float(expected_last)))
                                l.append(min(prev_close, float(expected_last)))
                                c.append(float(expected_last))
                                v.append(0.0)
                                t.append(datetime.now(timezone.utc).isoformat())
                                ohlc = (o, h, l, c, v, t)
                        else:
                            closes[-1] = float(expected_last)
                            if ohlc is not None:
                                o, h, l, c = ohlc[:4]
                                c[-1] = float(expected_last)
                                h[-1] = max(float(h[-1]), float(expected_last))
                                l[-1] = min(float(l[-1]), float(expected_last))
                    closes = _trim_tail_outlier(closes, expected_last)
                    if ohlc is not None:
                        trim_len = len(closes)
                        o, h, l, c = ohlc[:4]
                        v = list(ohlc[4]) if len(ohlc) >= 5 else [0.0] * len(c)
                        t = list(ohlc[5]) if len(ohlc) >= 6 else [""] * len(c)
                        if len(c) != trim_len:
                            ohlc = (o[:trim_len], h[:trim_len], l[:trim_len], c[:trim_len], v[:trim_len], t[:trim_len])
                    if len(closes) < 2:
                        return "<span class='small'>—</span>"
                    if cache_key:
                        chart_closes_cache[cache_key] = list(closes)
                        if ohlc is not None:
                            chart_ohlc_cache[cache_key] = ohlc
                    chart_opens_arg: Optional[list[float]] = None
                    chart_highs_arg: Optional[list[float]] = None
                    chart_lows_arg: Optional[list[float]] = None
                    chart_volumes_arg: Optional[list[float]] = None
                    chart_timestamps_arg: Optional[list[str]] = None
                    if ohlc is not None:
                        chart_opens_arg, chart_highs_arg, chart_lows_arg, closes = ohlc[:4]
                        chart_volumes_arg = list(ohlc[4]) if len(ohlc) >= 5 else None
                        chart_timestamps_arg = list(ohlc[5]) if len(ohlc) >= 6 else None
                    return _market_chart_svg(
                        closes=closes,
                        opens=chart_opens_arg,
                        highs=chart_highs_arg,
                        lows=chart_lows_arg,
                        ma_lengths=runtime_chart_cfg["ma_lengths"],
                        ema_lengths=runtime_chart_cfg["ema_lengths"],
                        macd_configs=runtime_chart_cfg["macd_configs"],
                        bb_configs=runtime_chart_cfg["bb_configs"],
                        ttm_configs=runtime_chart_cfg["ttm_configs"],
                        roc_lengths=runtime_chart_cfg["roc_lengths"],
                        sar_configs=runtime_chart_cfg["sar_configs"],
                        heikin_ashi_mode=bool(runtime_chart_cfg["has_heikin_ashi"]),
                        required_points=runtime_candle_target,
                        show_price=True,
                        show_rsi=bool(runtime_chart_cfg["has_rsi"]),
                        show_drsi=bool(runtime_chart_cfg["has_drsi"]),
                        d_ma_lengths=runtime_chart_cfg["d_ma_lengths"],
                        d_ema_lengths=runtime_chart_cfg["d_ema_lengths"],
                        ichimoku_configs=runtime_chart_cfg["ichimoku_configs"],
                        css_class="chart-spark run-status-chart indicatorforge-chart",
                        display_width=700,
                        display_height=220,
                        show_price_markers=True,
                        volumes=chart_volumes_arg,
                        timestamps=chart_timestamps_arg,
                        donchian_lookbacks=runtime_chart_cfg.get("donchian_lookbacks") or [],
                        supertrend_configs=runtime_chart_cfg.get("supertrend_configs") or [],
                        pivot_enabled=bool(runtime_chart_cfg.get("has_pivot")),
                        pivot_include_half_levels=bool(runtime_chart_cfg.get("pivot_include_half_levels")),
                        vwap_enabled=bool(runtime_chart_cfg.get("has_vwap")),
                        rvol_lengths=runtime_chart_cfg.get("rvol_lengths") or [],
                    )

                def _indicatorforge_runtime_chart_row(*, symbol: str, chart: Any, charts_by_timeframe: Any, price_hint: Any) -> str:
                    parts: list[str] = []
                    chart_map = charts_by_timeframe if isinstance(charts_by_timeframe, dict) else {}
                    for tf in runtime_rules_by_tf:
                        tf_chart = chart_map.get(tf) or chart_map.get(str(tf).upper())
                        if tf_chart is None and tf == _normalize_indicator_rule_timeframe(market_tf, default="1h"):
                            tf_chart = chart
                        rendered = _indicatorforge_runtime_chart(
                            symbol=symbol,
                            timeframe_key=tf,
                            chart=tf_chart if isinstance(tf_chart, dict) else {},
                            price_hint=price_hint,
                        )
                        parts.append(
                            "<div class='indicatorforge-tf-chart'>"
                            f"<div class='small'><b>TF {html.escape(tf)}</b></div>"
                            f"{rendered}"
                            "</div>"
                        )
                    if not parts:
                        return "<span class='small'>—</span>"
                    return "<div class='indicatorforge-chart-row'>" + "".join(parts) + "</div>"

                def _refresh_runtime_rule_item(
                    *,
                    symbol: str,
                    rule_key: str,
                    item: dict[str, Any],
                    price_hint: Any,
                ) -> dict[str, Any]:
                    val_txt = str(item.get("value") or "").strip()
                    detail_txt = str(item.get("detail") or "").strip().lower()
                    needs_refresh = (not val_txt) or (val_txt == "—") or ("unavailable" in detail_txt) or ("warmup incomplete" in detail_txt)
                    if not needs_refresh:
                        return item

                    entry = runtime_rule_lookup.get(rule_key)
                    if not isinstance(entry, dict):
                        return item
                    rule = entry.get("rule") if isinstance(entry.get("rule"), dict) else None
                    if not isinstance(rule, dict):
                        return item
                    if not str(rule.get("kind") or "").strip():
                        return item

                    tf = _rule_timeframe(rule, market_tf)
                    cache_key = f"{str(symbol or '').strip().upper()}|{tf}"
                    closes = chart_closes_cache.get(cache_key)
                    if not isinstance(closes, list) or len(closes) < 2:
                        return item

                    px = _to_float(price_hint)
                    if px is None and closes:
                        px = float(closes[-1])
                    if px is None:
                        return item

                    try:
                        ohlc = chart_ohlc_cache.get(cache_key)
                        if ohlc is not None:
                            ohlc_volumes = ohlc[4] if len(ohlc) >= 5 and isinstance(ohlc[4], list) else None
                            ohlc_timestamps = ohlc[5] if len(ohlc) >= 6 and isinstance(ohlc[5], list) else None
                            refreshed = _eval_indicator_rule(
                                rule,
                                ohlc[3],
                                float(px),
                                opens=ohlc[0],
                                highs=ohlc[1],
                                lows=ohlc[2],
                                volumes=ohlc_volumes,
                                timestamps=ohlc_timestamps,
                            )
                        else:
                            refreshed = _eval_indicator_rule(rule, closes, float(px))
                    except Exception:
                        return item

                    ribbon_slot = str(entry.get("ribbon_slot") or "").strip().lower()
                    if ribbon_slot:
                        level_checks = refreshed.get("_ribbon_level_checks")
                        if not isinstance(level_checks, list):
                            return item
                        matched: Optional[dict[str, Any]] = None
                        for child in level_checks:
                            if not isinstance(child, dict):
                                continue
                            if str(child.get("_ribbon_slot") or child.get("slot") or "").strip().lower() == ribbon_slot:
                                matched = child
                                break
                        if matched is None:
                            return item
                        refreshed = matched

                    merged = dict(item)
                    for k in (
                        "buy_ok",
                        "sell_ok",
                        "buy_ignored",
                        "sell_ignored",
                        "value",
                        "detail",
                        "rsi_buy_signal",
                        "rsi_sell_signal",
                        "macd_buy_signal",
                        "macd_sell_signal",
                    ):
                        if k in refreshed:
                            merged[k] = refreshed.get(k)
                    return merged

                runtime_rule_columns = _indicator_runtime_rule_entries(runtime_rules)
                runtime_rule_lookup: dict[str, dict[str, Any]] = {
                    str(col.get("key") or ""): col for col in runtime_rule_columns if str(col.get("key") or "")
                }

                max_summary_len = 0
                for t in rows:
                    summaries = t.get("rule_summary")
                    if isinstance(summaries, list):
                        max_summary_len = max(max_summary_len, len(summaries))

                if max_summary_len > len(runtime_rule_columns):
                    for idx in range(len(runtime_rule_columns), max_summary_len):
                        nm = ""
                        for t in rows:
                            summaries = t.get("rule_summary")
                            if not isinstance(summaries, list) or idx >= len(summaries):
                                continue
                            item = summaries[idx]
                            if not isinstance(item, dict):
                                continue
                            nm = str(item.get("name") or "").strip()
                            if nm:
                                break
                        if not nm:
                            nm = f"RULE {idx + 1}"
                        key = f"idx:{idx}"
                        placeholder_rule = {"name": nm, "kind": "", "params": {}}
                        runtime_rule_lookup[key] = {
                            "key": key,
                            "index": idx,
                            "name": nm,
                            "display_kind": "",
                            "kind": "",
                            "rule": placeholder_rule,
                            "rule_id": "",
                            "ribbon_slot": "",
                            "color": "#e8ecff",
                        }
                        runtime_rule_columns.append(
                            {
                                "key": key,
                                "index": idx,
                                "name": nm,
                                "display_kind": "",
                                "kind": "",
                                "rule": placeholder_rule,
                                "rule_id": "",
                                "ribbon_slot": "",
                                "color": "#e8ecff",
                            }
                        )

                if not runtime_rule_columns:
                    table_html = _render_ticker_table(
                        "Ticker Status",
                        rows,
                        indicator_mode=False,
                        show_chart=True,
                        superhex_mode=False,
                        options_mode=False,
                    )
                    return summary_html + table_html

                def _row_has_cap_fields(row: Any) -> bool:
                    if not isinstance(row, dict):
                        return False
                    if bool(row.get("buy_cap_rule_enabled")):
                        return True
                    for k in (
                        "portfolio_cap_divisor",
                        "portfolio_cap_mode",
                        "portfolio_cap_percent",
                        "cap_pct",
                        "held_pct",
                        "alloc_pct",
                        "cap_delta_pct",
                        "delta_pct",
                        "cash_pct",
                        "cash_target_value",
                        "available_cash",
                        "buying_power",
                        "buy_order_cost",
                        "buy_power_blocked",
                        "cash_slice_blocked",
                    ):
                        if row.get(k) is not None:
                            return True
                    return False

                def _indicatorforge_cap_values(row: dict[str, Any]) -> dict[str, Any]:
                    held_pct = row.get("held_pct")
                    if held_pct is None:
                        held_pct = row.get("alloc_pct")
                    cap_delta_pct = row.get("cap_delta_pct")
                    if cap_delta_pct is None:
                        cap_delta_pct = row.get("delta_pct")
                    cap_divisor = _to_int_opt(row.get("portfolio_cap_divisor"))
                    cap_pct = row.get("cap_pct")
                    if cap_pct is None and cap_divisor is not None and cap_divisor > 0:
                        cap_pct = 100.0 / float(cap_divisor)
                    return {
                        "enabled": bool(row.get("buy_cap_rule_enabled")),
                        "held_pct": held_pct,
                        "cap_delta_pct": cap_delta_pct,
                        "cap_divisor": cap_divisor,
                        "cap_pct": cap_pct,
                        "cap_mode": str(row.get("portfolio_cap_mode") or "divisor_cash_slice"),
                        "cap_percent": row.get("portfolio_cap_percent"),
                        "buy_cap_blocked": bool(row.get("buy_cap_blocked")),
                        "cash_pct": row.get("cash_pct"),
                        "cash_target_value": row.get("cash_target_value"),
                        "available_cash": row.get("available_cash"),
                        "buying_power": row.get("buying_power"),
                        "buy_order_cost": row.get("buy_order_cost"),
                        "buy_power_blocked": bool(row.get("buy_power_blocked")),
                        "cash_slice_blocked": bool(row.get("cash_slice_blocked")),
                    }

                show_cap_cols = any(_row_has_cap_fields(row) for row in rows)
                show_stoploss_cols = any(
                    isinstance(row, dict)
                    and any(
                        k in row
                        for k in (
                            "stoploss_armed",
                            "stoploss_trigger",
                            "stoploss_arm_price",
                            "stoploss_arm_gap_pct",
                            "stoploss_trigger_gap_pct",
                        )
                    )
                    for row in rows
                )
                pivot_preorder_param_enabled = False
                if isinstance(params_obj, dict):
                    raw_pivot_preorder = params_obj.get("pivot_preorder_enabled")
                    raw_profit_preorder = params_obj.get("pivot_preorder_profit_enabled")
                    if isinstance(raw_pivot_preorder, bool):
                        pivot_preorder_param_enabled = raw_pivot_preorder
                    elif isinstance(raw_pivot_preorder, (int, float)):
                        pivot_preorder_param_enabled = float(raw_pivot_preorder) != 0.0
                    else:
                        pivot_preorder_param_enabled = str(raw_pivot_preorder or "").strip().lower() in ("1", "true", "yes", "on", "y")
                    if isinstance(raw_profit_preorder, bool):
                        pivot_preorder_param_enabled = pivot_preorder_param_enabled or raw_profit_preorder
                    elif isinstance(raw_profit_preorder, (int, float)):
                        pivot_preorder_param_enabled = pivot_preorder_param_enabled or float(raw_profit_preorder) != 0.0
                    else:
                        pivot_preorder_param_enabled = pivot_preorder_param_enabled or str(raw_profit_preorder or "").strip().lower() in ("1", "true", "yes", "on", "y")
                show_pivot_preorder_cols = pivot_preorder_param_enabled or any(
                    isinstance(row, dict)
                    and (
                        bool(row.get("pivot_preorder_enabled"))
                        or bool(row.get("pivot_preorder_profit_enabled"))
                        or row.get("pivot_preorder_target_price") is not None
                        or row.get("pivot_preorder_order_status") is not None
                    )
                    for row in rows
                )
                headers = [
                    "<th>Symbol</th>",
                    "<th>Avg Buy</th>",
                    "<th>Gain %</th>",
                    "<th>Gain $</th>",
                    "<th>Signal</th>",
                    "<th>Price</th>",
                ]
                if show_pivot_preorder_cols:
                    headers.extend(["<th>Pivot Target</th>", "<th>Pivot Margin</th>", "<th>Pivot Order</th>"])
                if show_cap_cols:
                    headers.extend([
                        "<th>Held %</th>",
                        "<th>Cap Δ%</th>",
                        "<th>Cash %</th>",
                        "<th>Cash Target</th>",
                        "<th>Buying Power</th>",
                        "<th>Order Cost</th>",
                        "<th>Cap Divisor</th>",
                    ])
                if show_stoploss_cols:
                    headers.extend(["<th>SL Armed</th>", "<th>SL Arm @</th>", "<th>SL Trigger</th>"])
                for col in runtime_rule_columns:
                    nm = str(col.get("name") or "").strip() or "RULE"
                    idx = _to_int_opt(col.get("index"))
                    idx_prefix = f"#{int(idx) + 1} " if idx is not None and idx >= 0 else ""
                    kind_txt = str(col.get("display_kind") or col.get("kind") or "").strip().upper()
                    tf_txt = _rule_timeframe(col.get("rule") if isinstance(col.get("rule"), dict) else {}, market_tf)
                    kind_detail = " · ".join([part for part in (kind_txt, f"TF {tf_txt}" if tf_txt else "") if part])
                    kind_html = f"<div class='small'>{html.escape(kind_detail)}</div>" if kind_detail else ""
                    headers.append(f"<th>{html.escape(idx_prefix + nm)}{kind_html}</th>")
                headers.append("<th class='chart-col'>Charts</th>")

                out: list[str] = []
                if summary_html:
                    out.append(summary_html)
                out.extend([
                    "<div class='small' style='margin-top:8px'>Indicator Scanner</div>",
                    "<div class='status-table-wrap'><table>",
                    "<thead><tr>" + "".join(headers) + "</tr></thead><tbody>",
                ])

                for t in rows:
                    sym_raw = str(t.get("symbol") or "").strip()
                    sym = html.escape(sym_raw or "—")
                    stale_quote = bool(t.get("quote_stale"))
                    chart_price_hint = None if stale_quote else t.get("price")
                    chart_html = _indicatorforge_runtime_chart_row(
                        symbol=sym_raw,
                        chart=t.get("chart"),
                        charts_by_timeframe=t.get("charts_by_timeframe"),
                        price_hint=chart_price_hint,
                    )
                    action_signal = str(t.get("signal") or "HOLD").upper()
                    if action_signal not in ("BUY", "SELL", "HOLD"):
                        action_signal = "HOLD"
                    rule_signal = str(t.get("rule_signal") or action_signal).upper()
                    if rule_signal not in ("BUY", "SELL", "HOLD"):
                        rule_signal = action_signal
                    signal_class = "signal-hold"
                    if rule_signal == "BUY":
                        signal_class = "signal-buy"
                    elif rule_signal == "SELL":
                        signal_class = "signal-sell"
                    signal_html = html.escape(rule_signal)
                    if action_signal != rule_signal:
                        action_class = "signal-hold"
                        if action_signal == "BUY":
                            action_class = "signal-buy"
                        elif action_signal == "SELL":
                            action_class = "signal-sell"
                        hold_reason = str(t.get("execution_hold_reason") or "").strip()
                        exec_line = f"Exec {action_signal}"
                        if hold_reason:
                            exec_line += f" ({hold_reason})"
                        signal_html += f"<div class='small {action_class}'>{html.escape(exec_line)}</div>"
                    price = _fmt_num(t.get("price"))
                    price_html = html.escape(price)
                    if stale_quote:
                        age_seconds = _to_float(t.get("quote_age_seconds"))
                        age_txt = ""
                        if age_seconds is not None:
                            if age_seconds >= 3600:
                                age_txt = f" {age_seconds / 3600.0:.1f}h"
                            elif age_seconds >= 60:
                                age_txt = f" {age_seconds / 60.0:.0f}m"
                            else:
                                age_txt = f" {age_seconds:.0f}s"
                        price_html += f"<div class='small signal-hold'>stale quote{html.escape(age_txt)}</div>"
                    avg_buy_val = _to_float(t.get("avg_buy"))
                    price_val = _to_float(t.get("price"))
                    qty_val = _to_float(t.get("qty"))
                    gain_pct_val = _to_float(t.get("pnl_pct"))
                    if gain_pct_val is None and price_val is not None and avg_buy_val is not None and avg_buy_val > 0:
                        gain_pct_val = ((price_val - avg_buy_val) / avg_buy_val) * 100.0
                    gain_dollar_val: Optional[float] = None
                    if qty_val is not None and price_val is not None and avg_buy_val is not None and avg_buy_val > 0:
                        gain_dollar_val = (price_val - avg_buy_val) * qty_val
                    avg_buy_txt = _fmt_avg_buy(avg_buy_val)
                    gain_pct_txt = _fmt_pct(gain_pct_val)
                    gain_dollar_txt = _fmt_gain_dollar(gain_dollar_val)

                    summaries_raw = t.get("rule_summary")
                    summaries_list = summaries_raw if isinstance(summaries_raw, list) else []
                    row_rule_items: list[dict[str, Any]] = []
                    for col in runtime_rule_columns:
                        idx = _to_int_opt(col.get("index"))
                        item: dict[str, Any] = {}
                        if idx is not None and idx >= 0 and idx < len(summaries_list):
                            raw_item = summaries_list[idx]
                            if isinstance(raw_item, dict):
                                item = raw_item
                        key = str(col.get("key") or "")
                        item = _refresh_runtime_rule_item(symbol=sym_raw, rule_key=key, item=item, price_hint=chart_price_hint)
                        item = dict(item)
                        entry = col if isinstance(col, dict) else runtime_rule_lookup.get(key, {})
                        rule = entry.get("rule") if isinstance(entry.get("rule"), dict) else {
                            "name": str(col.get("name") or "RULE"),
                            "kind": "",
                            "params": {},
                        }
                        params = rule.get("params") if isinstance(rule.get("params"), dict) else {}
                        item["name"] = str(item.get("name") or entry.get("name") or _rule_name(rule))
                        item["_rule_kind"] = str(entry.get("kind") or rule.get("kind") or "").strip().lower()
                        item["_rule_params"] = params
                        item["_rule_id"] = str(entry.get("rule_id") or params.get("rule_id") or "").strip()
                        row_rule_items.append(item)
                    _apply_indicator_signal_overrides(row_rule_items)

                    cells = [
                        f"<td><b>{sym}</b></td>",
                        f"<td>{avg_buy_txt}</td>",
                        f"<td>{gain_pct_txt}</td>",
                        f"<td>{gain_dollar_txt}</td>",
                        f"<td class='{signal_class}'>{signal_html}</td>",
                        f"<td>{price_html}</td>",
                    ]
                    if show_pivot_preorder_cols:
                        pivot_enabled = bool(t.get("pivot_preorder_enabled")) or bool(t.get("pivot_preorder_profit_enabled"))
                        pivot_label = str(t.get("pivot_preorder_target_label") or "").strip()
                        pivot_price = _to_float(t.get("pivot_preorder_target_price"))
                        pivot_margin_pct = _to_float(t.get("pivot_preorder_margin_pct"))
                        pivot_margin_per_share = _to_float(t.get("pivot_preorder_margin_per_share"))
                        pivot_margin_total = _to_float(t.get("pivot_preorder_margin_total"))
                        pivot_shares = _to_float(t.get("pivot_preorder_shares"))
                        pivot_status = str(t.get("pivot_preorder_order_status") or "").strip()
                        pivot_reason = str(t.get("pivot_preorder_order_reason") or "").strip()
                        if not pivot_enabled:
                            target_cell = "Off"
                            margin_cell = "—"
                            order_cell = "—"
                        elif pivot_price is None:
                            target_cell = "No target"
                            margin_cell = "—"
                            order_cell = html.escape(pivot_status or "no target")
                        else:
                            target_title = html.escape(pivot_label or "Pivot")
                            target_cell = f"<b>{target_title}</b><div>{_fmt_money_plain(pivot_price)}</div>"
                            if pivot_shares is not None and pivot_shares > 0:
                                target_cell += f"<div class='small'>{_fmt_num(pivot_shares)} sh</div>"
                            margin_bits = []
                            margin_bits.append(_fmt_pct(pivot_margin_pct))
                            if pivot_margin_per_share is not None:
                                margin_bits.append(f"{_fmt_money_plain(pivot_margin_per_share)}/sh")
                            margin_cell = "<div>".join(html.escape(bit) for bit in margin_bits if bit and bit != "—")
                            if margin_cell:
                                margin_cell = margin_cell.replace("<div>", "<br>")
                            else:
                                margin_cell = "—"
                            if pivot_margin_total is not None:
                                margin_cell += f"<div class='small'>est total {html.escape(_fmt_money_plain(pivot_margin_total))}</div>"
                            order_cls = "signal-hold"
                            status_lower = pivot_status.lower()
                            if status_lower == "accepted":
                                order_cls = "signal-buy"
                            elif status_lower == "rejected":
                                order_cls = "signal-sell"
                            order_cell = f"<span class='{order_cls}'>{html.escape(pivot_status or 'preview')}</span>"
                            if pivot_reason:
                                order_cell += f"<div class='small'>{html.escape(pivot_reason)}</div>"
                        cells.extend([f"<td>{target_cell}</td>", f"<td>{margin_cell}</td>", f"<td>{order_cell}</td>"])
                    if show_cap_cols:
                        cap_vals = _indicatorforge_cap_values(t)
                        held_txt = _fmt_pct(cap_vals.get("held_pct"))
                        delta_txt = _fmt_pct(cap_vals.get("cap_delta_pct"))
                        delta_num = _to_float(cap_vals.get("cap_delta_pct"))
                        delta_cls = ""
                        if delta_num is not None:
                            if delta_num > 0:
                                delta_cls = "cap-over"
                            elif delta_num < 0:
                                delta_cls = "cap-under"
                        delta_cell = f"<td class='{delta_cls}'>{delta_txt}</td>" if delta_cls else f"<td>{delta_txt}</td>"
                        divisor_txt = "Off"
                        cap_mode = str(cap_vals.get("cap_mode") or "divisor_cash_slice")
                        cap_divisor = cap_vals.get("cap_divisor")
                        cap_pct_txt = _fmt_pct(cap_vals.get("cap_pct"))
                        if cap_mode == "percent":
                            pct_txt = _fmt_pct(cap_vals.get("cap_percent"))
                            divisor_txt = f"Ticker % {pct_txt}" if pct_txt != "—" else "Ticker %"
                        elif isinstance(cap_divisor, int) and cap_divisor >= 2:
                            divisor_txt = f"1/{cap_divisor}"
                            if cap_pct_txt != "—":
                                divisor_txt += f" ({cap_pct_txt})"
                        elif cap_vals.get("enabled"):
                            divisor_txt = cap_pct_txt if cap_pct_txt != "—" else "On"
                        show_buy_blockers = rule_signal == "BUY"
                        if show_buy_blockers and cap_vals.get("enabled") and cap_vals.get("buy_cap_blocked"):
                            divisor_txt += "<div class='small signal-hold'>BUY blocked</div>"
                        if show_buy_blockers and cap_vals.get("buy_power_blocked"):
                            divisor_txt += "<div class='small signal-hold'>Power blocked</div>"
                        if show_buy_blockers and cap_vals.get("cash_slice_blocked"):
                            divisor_txt += "<div class='small signal-hold'>Cash target blocked</div>"
                        cash_txt = _fmt_pct(cap_vals.get("cash_pct"))
                        cash_target_txt = _fmt_money_plain(cap_vals.get("cash_target_value"))
                        buying_power_txt = _fmt_money_plain(cap_vals.get("buying_power"))
                        buy_order_cost_txt = _fmt_money_plain(cap_vals.get("buy_order_cost"))
                        cells.extend([
                            f"<td>{held_txt}</td>",
                            delta_cell,
                            f"<td>{cash_txt}</td>",
                            f"<td>{cash_target_txt}</td>",
                            f"<td>{buying_power_txt}</td>",
                            f"<td>{buy_order_cost_txt}</td>",
                            f"<td>{divisor_txt}</td>",
                        ])
                    if show_stoploss_cols:
                        sl_armed_raw = t.get("stoploss_armed")
                        if sl_armed_raw is None:
                            sl_armed = "—"
                        else:
                            sl_armed = "Yes" if bool(sl_armed_raw) else "No"
                        arm_level = t.get("stoploss_arm_price")
                        trigger_level = t.get("stoploss_trigger")
                        arm_gap = _level_gap_pct(arm_level, t.get("price"), t.get("stoploss_arm_gap_pct"))
                        trigger_gap = _level_gap_pct(trigger_level, t.get("price"), t.get("stoploss_trigger_gap_pct"))
                        arm_txt = _fmt_level_with_gap(arm_level, arm_gap)
                        trigger_txt = _fmt_level_with_gap(trigger_level, trigger_gap)
                        cells.extend([f"<td>{sl_armed}</td>", f"<td>{arm_txt}</td>", f"<td>{trigger_txt}</td>"])

                    for col_idx, col in enumerate(runtime_rule_columns):
                        item = row_rule_items[col_idx] if col_idx < len(row_rule_items) else {}
                        v_txt = html.escape(str(item.get("value") or "—"))
                        d_txt = html.escape(str(item.get("detail") or ""))
                        state_txt = _indicator_rule_state_html(item)
                        key = str(col.get("key") or "")
                        rule = col.get("rule") if isinstance(col.get("rule"), dict) else runtime_rule_lookup.get(
                            key, {"name": str(col.get("name") or "RULE"), "kind": "", "params": {}}
                        )
                        rule_color = _rule_line_color(rule)
                        cell_inner = (
                            f"<div style='color:{html.escape(rule_color)}'>{v_txt}{state_txt}</div>"
                            f"<div class='small'>{d_txt}</div>"
                        )
                        cells.append(f"<td>{cell_inner}</td>")

                    cells.append(f"<td class='chart-cell'>{chart_html}</td>")
                    out.append("<tr>" + "".join(cells) + "</tr>")

                out.append("</tbody></table></div>")
                src_txt = html.escape(", ".join(runtime_rules_by_tf.keys()) or str(tf_display if tf_display else timeframe) or "n/a")
                src_label = _market_source_label(source_hint, include_extended=include_history_extended)
                out.append(
                    f"<div class='small' style='margin-top:6px;'>"
                    f"Source: Runtime payload + {html.escape(src_label)} refresh · timeframes {src_txt}"
                    "</div>"
                )
                return "".join(out)

            is_fox_balance = (not superhex_mode) and any(
                isinstance(t, dict) and ("alloc_pct" in t or "delta_pct" in t) for t in tickers
            )
            if indicatorforge_mode:
                html_out += _render_indicatorforge_runtime_table(tickers)
            elif is_fox_balance:
                liq_rows: list[dict[str, Any]] = []
                acq_rows: list[dict[str, Any]] = []
                for t in tickers:
                    pnl = _to_float(t.get("pnl_pct"))
                    if pnl is None:
                        pnl = 0.0
                    if pnl > 0.0:
                        liq_rows.append(t)
                    else:
                        acq_rows.append(t)

                liq_rows.sort(
                    key=lambda t: (
                        _to_float(t.get("pnl_pct")) or float("-inf"),
                        _to_float(t.get("delta_pct")) or float("-inf"),
                    ),
                    reverse=True,
                )

                def _acq_score(t: dict[str, Any]) -> float:
                    window_pos = _to_float(t.get("window_pos"))
                    delta = _to_float(t.get("delta_pct"))
                    if window_pos is None or delta is None:
                        return float("-inf")
                    return (1.0 - window_pos) + (abs(delta) / 100.0)

                acq_rows.sort(key=_acq_score, reverse=True)

                html_out += _render_ticker_table(
                    "LIQ Targets (ranked by PNL% then DELTA%)",
                    liq_rows,
                    indicator_mode=False,
                    show_chart=True,
                    superhex_mode=False,
                    role_hint="LIQ",
                )
                html_out += _render_ticker_table(
                    "ACQ Targets (ranked by 52-low bias)",
                    acq_rows,
                    indicator_mode=False,
                    show_chart=True,
                    superhex_mode=False,
                    role_hint="ACQ",
                )
            else:
                html_out += _render_ticker_table(
                    "Ticker Status",
                    tickers,
                    indicator_mode=indicator_mode,
                    show_chart=indicator_mode or crypto_chart_mode or superhex_mode,
                    superhex_mode=superhex_mode,
                    options_mode=options_mode,
                )
    else:
        html_out += "<div class='small'>No status yet.</div>"

    return HTMLResponse(html_out)


@app.get("/runs/{run_id}/assistant_summary", response_class=HTMLResponse)
def run_assistant_summary(run_id: int):
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT run_dir, status FROM runs WHERE id=?", (run_id,))
    r = cur.fetchone()
    conn.close()
    if not r:
        return HTMLResponse("<div class='small'>Run not found</div>", status_code=404)

    log_path = Path(r["run_dir"]) / "algo.log"
    advice: list[str] = []
    if log_path.exists():
        tail = log_path.read_text(errors="ignore").splitlines()[-120:]
        for line in tail:
            low = line.lower()
            if "token" in low and "fail" in low:
                advice.append("Broker auth looks unhealthy (token failure). Re-auth may be needed.")
            if "price_feed_stale" in low:
                advice.append("Price feed staleness detected — consider pausing or lowering risk until stable.")
            if "error" in low:
                advice.append("Errors present in logs — inspect before letting it run unattended.")
    if not advice:
        advice.append("No major issues detected in recent logs.")
    html = "<ul class='small'>" + "".join(f"<li>{a}</li>" for a in advice[:4]) + "</ul>"
    return HTMLResponse(html)


# =========================
# Partials (HTMX)
# =========================
@app.get("/partials/algorithms_table", response_class=HTMLResponse)
def partial_algorithms_table():
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT a.*, b.name AS base_name, b.path AS base_path
        FROM algorithms a
        JOIN base_scripts b ON b.id = a.base_script_id
        ORDER BY a.id DESC
        LIMIT 50
        """
    )
    rows = cur.fetchall()

    cur.execute(
        """
        SELECT id, algorithm_id
        FROM runs
        WHERE status='running'
        ORDER BY id DESC
        """
    )
    running_rows = cur.fetchall()
    conn.close()

    if not rows:
        return HTMLResponse("<div class='small'>No algorithms yet. Create one.</div>")

    running_by_algo: dict[int, int] = {}
    for r in running_rows:
        algo_id = int(r["algorithm_id"])
        if algo_id not in running_by_algo:
            running_by_algo[algo_id] = int(r["id"])

    rows_html = [
        "<table><thead><tr>"
        "<th>Name</th><th>Base Script</th><th>Params</th><th></th>"
        "</tr></thead><tbody>"
    ]
    for r in rows:
        params_obj = _safe_json(r["params_json"], default={})
        params_html = _format_params_table(params_obj)
        run_id = running_by_algo.get(int(r["id"]))
        if run_id:
            action_html = (
                f"<form method='post' action='/runs/{run_id}/stop'>"
                f"<button class='btn danger'>Stop Cryptid</button></form>"
            )
            log_link = f"<a class='btn' href='/runs/{run_id}'>Logs</a>"
        else:
            action_html = (
                f"<form method='post' action='/algorithms/{r['id']}/run'>"
                f"<button class='btn primary'>Run Cryptid</button></form>"
            )
            log_link = ""
        actions_html = (
            "<div class='row'>"
            f"{action_html}"
            f"{log_link}"
            f"<a class='btn' href='/algorithms/{r['id']}/edit'>Edit</a>"
            f"<form method='post' action='/algorithms/{r['id']}/delete' "
            f"onsubmit=\"return confirm('Delete this cryptid?');\">"
            f"<button class='btn danger'>Delete</button></form>"
            "</div>"
        )
        rows_html.append(
            "<tr>"
            f"<td><b>{r['name']}</b><div class='small'>id {r['id']}</div></td>"
            f"<td><span class='small'>{r['base_name']}</span></td>"
            f"<td>{params_html}</td>"
            f"<td>{actions_html}</td>"
            "</tr>"
        )
    rows_html.append("</tbody></table>")
    return HTMLResponse("".join(rows_html))


@app.get("/partials/base_scripts_table", response_class=HTMLResponse)
def partial_base_scripts_table():
    discover_base_scripts()
    scripts = get_base_scripts()
    if not scripts:
        return HTMLResponse("<div class='small'>No base scripts discovered yet.</div>")

    rows_html = [
        "<table><thead><tr>"
        "<th>Name</th><th>Description</th>"
        "</tr></thead><tbody>"
    ]
    for s in scripts:
        desc = s.get("description") or "—"
        rows_html.append(
            "<tr>"
            f"<td><b>{html.escape(str(s['name']))}</b></td>"
            f"<td class='small'>{html.escape(str(desc))}</td>"
            "</tr>"
        )
    rows_html.append("</tbody></table>")
    return HTMLResponse("".join(rows_html))


@app.post("/algorithms/{algo_id}/run")
def run_algorithm(algo_id: int):
    conn = db()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT a.*, b.path AS script_path
        FROM algorithms a
        JOIN base_scripts b ON b.id = a.base_script_id
        WHERE a.id=?
        """,
        (algo_id,),
    )
    a = cur.fetchone()
    if not a:
        conn.close()
        return PlainTextResponse("Algorithm not found", status_code=404)

    params_json = a["params_json"]
    script_path = str(a["script_path"] or "")
    broker_hint: Optional[str] = _broker_hint_from_script(script_path)
    connection_id = _resolve_connection_id(params_json, broker_hint, conn)

    entrypoint = str(APP_ROOT / a["script_path"])
    if not Path(entrypoint).exists():
        conn.close()
        return PlainTextResponse("Base script file is missing. Refresh base scripts and try again.", status_code=400)

    if broker_hint and connection_id is None:
        conn.close()
        return PlainTextResponse(
            f"No connected {broker_hint} broker found for this cryptid. Link a broker or set connection_id in params.",
            status_code=400,
        )

    run_dir = RUNS_DIR / f"run_{_utc_ts()}_{algo_id}_{uuid4().hex[:8]}"
    try:
        run_dir.mkdir(parents=True, exist_ok=False)
    except Exception as e:
        conn.close()
        return PlainTextResponse(f"Unable to create run directory: {e}", status_code=500)

    # Write params.json for the algorithm process to load (preferred),
    # instead of passing a large JSON string via argv.
    params_path = run_dir / "params.json"
    try:
        params_path.write_text(params_json, encoding="utf-8")
    except Exception as e:
        conn.close()
        shutil.rmtree(run_dir, ignore_errors=True)
        return PlainTextResponse(f"Unable to write run parameters: {e}", status_code=500)

    cmd = [
        sys.executable,
        entrypoint,
        "--run-dir",
        str(run_dir),
        "--params-json",
        str(params_path),
    ]

    if broker_hint:
        cmd.extend(["--db-path", str(DB_PATH), "--connection-id", str(connection_id)])

    log_path = run_dir / "algo.log"
    _trim_log_file(log_path)
    log_fp = None
    proc: Optional[subprocess.Popen[Any]] = None
    try:
        log_fp = open(log_path, "ab", buffering=0)
        proc = subprocess.Popen(
            cmd,
            stdout=log_fp,
            stderr=log_fp,
            start_new_session=(os.name != "nt"),
            env=_algorithm_subprocess_env(broker_hint),
        )
    except Exception as e:
        if log_fp is not None:
            try:
                log_fp.close()
            except Exception:
                pass
        conn.close()
        shutil.rmtree(run_dir, ignore_errors=True)
        return PlainTextResponse(f"Unable to start cryptid process: {e}", status_code=500)
    finally:
        if log_fp is not None:
            try:
                log_fp.close()
            except Exception:
                pass

    try:
        cur.execute(
            """
            INSERT INTO runs
            (algorithm_id, algorithm_name, params_json, run_dir, pid, status, start_ts,
             supervisor_pid, supervisor_started_ts, restart_count, last_restart_ts, restart_reason)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                algo_id,
                a["name"],
                params_json,
                str(run_dir),
                int(proc.pid),
                "running",
                _utc_ts(),
                os.getpid(),
                SERVER_STARTED_TS,
                0,
                None,
                "manual_start",
            ),
        )
        run_id = int(cur.lastrowid)
        conn.commit()
    except Exception as e:
        try:
            _terminate_pid(int(proc.pid), grace_sec=1.0)
        except Exception:
            pass
        conn.close()
        shutil.rmtree(run_dir, ignore_errors=True)
        return PlainTextResponse(f"Unable to record run in database: {e}", status_code=500)
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return RedirectResponse(f"/runs/{run_id}", status_code=303)


@app.get("/partials/runs_table", response_class=HTMLResponse)
def partial_runs_table():
    conn = db()
    _refresh_run_processes(conn)
    cur = conn.cursor()
    cur.execute("SELECT * FROM runs ORDER BY id DESC LIMIT 100")
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return HTMLResponse("<div class='small'>No runs yet.</div>")

    html = [
        "<table><thead><tr>"
        "<th>Run</th><th>Algorithm</th><th>Status</th><th>Started</th><th></th>"
        "</tr></thead><tbody>"
    ]
    for r in rows:
        started = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r["start_ts"]))
        badge = "ok" if r["status"] == "running" else "warn"
        actions: list[str] = []
        if r["status"] == "running":
            actions.append(
                f"<form method='post' action='/runs/{r['id']}/stop'>"
                f"<button class='btn danger'>Stop</button></form>"
            )
            actions.append(f"<a class='btn' href='/runs/{r['id']}'>Logs</a>")
        elif r["status"] not in ("stopping",):
            actions.append(f"<a class='btn' href='/runs/{r['id']}'>Review</a>")
            actions.append(
                f"<form method='post' action='/runs/{r['id']}/clear'>"
                f"<button class='btn'>Clear</button></form>"
            )
        actions_html = f"<div class='row'>{''.join(actions)}</div>" if actions else ""
        html.append(
            "<tr>"
            f"<td><a href='/runs/{r['id']}'><b>#{r['id']}</b></a><div class='small'>PID {r['pid'] or '—'}</div></td>"
            f"<td>{r['algorithm_name']}</td>"
            f"<td><span class='badge {badge}'>{r['status']}</span></td>"
            f"<td><span class='small'>{started}</span></td>"
            f"<td>{actions_html}</td>"
            "</tr>"
        )
    html.append("</tbody></table>")
    return HTMLResponse("".join(html))


@app.get("/partials/live_runs", response_class=HTMLResponse)
def partial_live_runs():
    conn = db()
    _refresh_run_processes(conn)
    cur = conn.cursor()
    cur.execute(
        "SELECT id, algorithm_name, pid, status, run_dir, start_ts FROM runs "
        "WHERE status='running' ORDER BY id DESC LIMIT 20"
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return HTMLResponse("<div class='small'>No live runs.</div>")

    def _fmt_duration(seconds: Optional[int]) -> str:
        if seconds is None:
            return "—"
        seconds = max(0, int(seconds))
        hrs = seconds // 3600
        mins = (seconds % 3600) // 60
        secs = seconds % 60
        if hrs > 0:
            return f"{hrs}h {mins:02d}m"
        if mins > 0:
            return f"{mins}m {secs:02d}s"
        return f"{secs}s"

    now_ts = _utc_ts()

    cards: list[str] = []
    for r in rows:
        status_path = Path(r["run_dir"]) / "status.json"
        pnl = trades = "—"
        tf_display = None
        hb = "—"
        if status_path.exists():
            try:
                payload = json.loads(status_path.read_text())
                pnl = payload.get("pnl", "—")
                trades = payload.get("trades", "—")
                tf_payload = payload.get("timeframe")
                if tf_payload is not None:
                    tf_display = str(tf_payload).strip()
                hb = payload.get("ts") or payload.get("heartbeat") or payload.get("last_heartbeat") or "—"
            except Exception:
                pass

        start_ts = r["start_ts"] if "start_ts" in r.keys() else None
        uptime = _fmt_duration((now_ts - int(start_ts)) if start_ts else None)
        tf_badge_card = f"<span class='badge'>TF {html.escape(tf_display)}</span>" if tf_display else ""

        cards.append(
            "<div class='card' style='margin-top:10px'>"
            "<div class='row' style='justify-content:space-between'>"
            f"<div><b>#{r['id']}</b> {r['algorithm_name']}<div class='small'>PID {r['pid']}</div></div>"
            "<span class='badge ok'>running</span>"
            "</div>"
            f"<div class='small'>Heartbeat: {hb}</div>"
            f"<div class='small'>Uptime: {uptime}</div>"
            "<div class='row' style='margin-top:8px'>"
            f"<span class='badge'>PNL {pnl}</span>"
            f"<span class='badge'>Trades {trades}</span>"
            f"{tf_badge_card}"
            f"<a class='btn' href='/runs/{r['id']}'>Open</a>"
            "</div>"
            "</div>"
        )
    return HTMLResponse("".join(cards))


@app.get("/partials/broker_errors", response_class=HTMLResponse)
def partial_broker_errors():
    run_rows = _collect_broker_log_errors(max_runs=50, max_lines_per_run=6)
    conn_rows = _collect_broker_connection_errors(max_connections=30, max_lines_per_item=6)
    if not run_rows and not conn_rows:
        return HTMLResponse("<div class='small'>No errors detected in recent broker connections or run logs.</div>")

    cards: list[str] = []
    events: list[dict[str, Any]] = []
    for row in run_rows:
        events.append(
            {
                "source": "run_log",
                "ts": int(row.get("log_mtime") or 0),
                "payload": row,
            }
        )
    for row in conn_rows:
        events.append(
            {
                "source": "connection",
                "ts": int(row.get("updated_ts") or 0),
                "payload": row,
            }
        )

    events.sort(key=lambda item: int(item.get("ts") or 0), reverse=True)
    for event in events[:24]:
        source = str(event.get("source") or "")
        payload = event.get("payload")
        payload = payload if isinstance(payload, dict) else {}
        ts_raw = event.get("ts")
        updated = (
            time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(ts_raw)))
            if isinstance(ts_raw, int)
            else "—"
        )

        if source == "connection":
            status_txt = str(payload.get("status") or "")
            if status_txt == "connected":
                badge = "ok"
            elif status_txt in ("needs_auth", "needs_attention"):
                badge = "warn"
            else:
                badge = "bad"

            cid = int(payload.get("connection_id") or 0)
            broker = html.escape(str(payload.get("broker") or "broker"))
            label = html.escape(str(payload.get("label") or broker))
            lines = payload.get("lines") if isinstance(payload.get("lines"), list) else []
            rendered = [html.escape(str(ln)) for ln in lines]
            lines_html = (
                "<pre style='max-height:180px; overflow:auto; white-space:pre-wrap;'>"
                + "\n".join(rendered)
                + "</pre>"
            )
            cards.append(
                "<div class='card' style='margin-top:10px'>"
                "<div class='row' style='justify-content:space-between; align-items:center;'>"
                f"<div><b>{label}</b><div class='small'>{broker} · connection #{cid}</div></div>"
                f"<span class='badge {badge}'>{html.escape(status_txt or 'unknown')}</span>"
                "</div>"
                f"<div class='small'>Source: broker connection metadata | Last update: {updated}</div>"
                "<div class='row' style='margin-top:8px; justify-content:space-between;'>"
                f"<a class='btn' href='/broker?connection_id={cid}'>Open broker</a>"
                "</div>"
                f"{lines_html}"
                "</div>"
            )
            continue

        status_txt = str(payload.get("status") or "")
        badge = "ok" if status_txt == "running" else "warn"
        run_id = int(payload.get("run_id") or 0)
        algo_name = html.escape(str(payload.get("algorithm_name") or ""))
        match_count = int(payload.get("match_count") or 0)
        lines = payload.get("lines") if isinstance(payload.get("lines"), list) else []
        rendered = [_ansi_line_to_html(str(ln)) for ln in lines]
        lines_html = (
            "<pre style='max-height:180px; overflow:auto; white-space:pre-wrap;'>"
            + "\n".join(rendered)
            + "</pre>"
        )
        cards.append(
            "<div class='card' style='margin-top:10px'>"
            "<div class='row' style='justify-content:space-between; align-items:center;'>"
            f"<div><b>#{run_id}</b> {algo_name}</div>"
            f"<span class='badge {badge}'>{html.escape(status_txt or 'unknown')}</span>"
            "</div>"
            f"<div class='small'>Source: run log | Matches: {match_count} | Last log write: {updated}</div>"
            "<div class='row' style='margin-top:8px; justify-content:space-between;'>"
            f"<a class='btn' href='/runs/{run_id}'>Open full logs</a>"
            "</div>"
            f"{lines_html}"
            "</div>"
        )
    return HTMLResponse("".join(cards))


# =========================
# Assistant Page
# =========================
def _assistant_news_default_system_prompt() -> str:
    if callable(default_news_system_prompt):
        return str(default_news_system_prompt())
    return """You are a local market-news triage assistant. Use only provided news and market data. Identify primary drivers, positive-return setups, downtrend warnings, and confidence. Do not execute trades or guarantee returns."""


def _assistant_news_config_summary(config: Any) -> dict[str, Any]:
    return {
        "provider": "openai",
        "model": getattr(config, "model", DEFAULT_OPENAI_MODEL),
        "openai_base_url": getattr(config, "openai_base_url", OPENAI_BASE_URL),
        "articles_per_ticker": getattr(config, "articles_per_ticker", None),
        "include_article_text": getattr(config, "include_article_text", None),
        "include_market_data": getattr(config, "include_market_data", None),
        "max_article_chars": getattr(config, "max_article_chars", None),
        "openai_timeout": getattr(config, "openai_timeout", None),
        "max_input_chars": getattr(config, "max_input_chars", None),
        "max_output_tokens": getattr(config, "max_output_tokens", None),
    }


def _assistant_news_trim_result(result: dict[str, Any]) -> dict[str, Any]:
    impressions_in = result.get("impressions") if isinstance(result.get("impressions"), list) else []
    impressions: list[dict[str, Any]] = []
    for item in impressions_in:
        if not isinstance(item, dict):
            continue
        articles = item.get("articles") if isinstance(item.get("articles"), list) else []
        impressions.append(
            {
                "ticker": item.get("ticker"),
                "status": item.get("status", "ok"),
                "market": item.get("market") if isinstance(item.get("market"), dict) else {},
                "article_count": len(articles),
                "articles": [
                    {
                        "title": a.get("title"),
                        "source": a.get("source") or a.get("provider"),
                        "published_at": a.get("published_at"),
                        "url": a.get("url"),
                        "summary": a.get("summary"),
                    }
                    for a in articles[:5]
                    if isinstance(a, dict)
                ],
                "impression": item.get("impression"),
                "source_note": item.get("source_note"),
            }
        )
    return {
        "generated_at": result.get("generated_at"),
        "elapsed_sec": result.get("elapsed_sec"),
        "model": result.get("model"),
        "tickers": result.get("tickers"),
        "articles_per_ticker": result.get("articles_per_ticker"),
        "final_status": result.get("final_status"),
        "final_summary": result.get("final_summary"),
        "files": result.get("files") if isinstance(result.get("files"), dict) else {},
        "impressions": impressions,
    }


def _assistant_news_job_snapshot(job_id: str) -> dict[str, Any]:
    with ASSISTANT_NEWS_JOBS_LOCK:
        job = copy.deepcopy(ASSISTANT_NEWS_JOBS.get(job_id) or {})
    if not job:
        return {}
    return job


def _assistant_news_active_job_snapshot_locked() -> dict[str, Any]:
    active_jobs = [
        job
        for job in ASSISTANT_NEWS_JOBS.values()
        if str(job.get("status") or "").lower() in {"queued", "running", "stopping"}
    ]
    if not active_jobs:
        return {}
    active_jobs.sort(key=lambda job: str(job.get("started_at") or job.get("updated_at") or ""), reverse=True)
    return copy.deepcopy(active_jobs[0])


def _assistant_news_conflict_payload(active_job: dict[str, Any]) -> dict[str, Any]:
    return {
        "error": "Assistant news workflow is already running. Wait for it to finish before starting another OpenAI run.",
        "active_job_id": active_job.get("job_id"),
        "active_job": active_job,
    }


def _assistant_news_update_job(job_id: str, **updates: Any) -> None:
    with ASSISTANT_NEWS_JOBS_LOCK:
        job = ASSISTANT_NEWS_JOBS.get(job_id)
        if not job:
            return
        job.update(updates)
        job["updated_at"] = _utc_now_iso()


def _assistant_news_append_event(job_id: str, event: dict[str, Any]) -> None:
    with ASSISTANT_NEWS_JOBS_LOCK:
        job = ASSISTANT_NEWS_JOBS.get(job_id)
        if not job:
            return
        events = job.setdefault("events", [])
        if isinstance(events, list):
            events.append(event)
            del events[:-360]
        if event.get("stage") in {"ticker_collected", "ticker_ready", "ticker_ai", "ticker_done"} and event.get("prompt_index"):
            responses = job.setdefault("ticker_responses", [])
            if isinstance(responses, list):
                prompt_index = int(event.get("prompt_index") or 0)
                existing = None
                for response in responses:
                    if isinstance(response, dict) and int(response.get("prompt_index") or 0) == prompt_index:
                        existing = response
                        break
                if existing is not None:
                    existing.update(event)
                else:
                    responses.append(dict(event))
                    responses.sort(key=lambda item: int(item.get("prompt_index") or 0) if isinstance(item, dict) else 0)
                del responses[:-120]
        job["stage"] = event.get("stage", job.get("stage"))
        job["message"] = event.get("message", job.get("message"))
        job["current_ticker"] = event.get("current_ticker", job.get("current_ticker"))
        job["completed"] = event.get("completed", job.get("completed", 0))
        job["total"] = event.get("total", job.get("total", 0))
        job["updated_at"] = _utc_now_iso()


def _assistant_news_cleanup_jobs() -> None:
    with ASSISTANT_NEWS_JOBS_LOCK:
        if len(ASSISTANT_NEWS_JOBS) <= 20:
            return
        ordered = sorted(
            ASSISTANT_NEWS_JOBS.items(),
            key=lambda kv: str(kv[1].get("updated_at") or kv[1].get("started_at") or ""),
        )
        for stale_id, stale_job in ordered[:-20]:
            if stale_job.get("status") in ("complete", "error", "stopped"):
                ASSISTANT_NEWS_JOBS.pop(stale_id, None)
                ASSISTANT_NEWS_STOP_EVENTS.pop(stale_id, None)
                ASSISTANT_NEWS_THREADS.pop(stale_id, None)


def _assistant_news_worker(
    *,
    job_id: str,
    tickers: list[str],
    config: Any,
    output_dir: Path,
    stop_event: threading.Event,
) -> None:
    _assistant_news_update_job(job_id, status="running", stage="starting", message="Starting workflow")

    def on_progress(event: dict[str, Any]) -> None:
        _assistant_news_append_event(job_id, event)

    try:
        if not callable(run_news_workflow):
            raise RuntimeError("Assistant news workflow module is unavailable.")
        result = run_news_workflow(
            tickers=tickers,
            config=config,
            output_dir=output_dir,
            progress_callback=on_progress,
            stop_event=stop_event,
        )
        if stop_event.is_set():
            raise RuntimeError("Assistant news workflow stopped.")
        _assistant_news_update_job(
            job_id,
            status="complete",
            stage="complete",
            message="Workflow complete",
            completed=len(tickers),
            total=len(tickers),
            result=_assistant_news_trim_result(result),
        )
    except Exception as e:
        if stop_event.is_set():
            snapshot = _assistant_news_job_snapshot(job_id)
            completed = int(snapshot.get("completed") or 0) if snapshot else 0
            total = int(snapshot.get("total") or len(tickers)) if snapshot else len(tickers)
            _assistant_news_update_job(
                job_id,
                status="stopped",
                stage="stopped",
                message="Workflow stopped by user.",
                error="",
                completed=completed,
                total=total,
            )
            _assistant_news_append_event(
                job_id,
                {
                    "ts": _utc_now_iso(),
                    "stage": "stopped",
                    "message": "Workflow stopped by user.",
                    "completed": completed,
                    "total": total,
                },
            )
        else:
            _assistant_news_update_job(
                job_id,
                status="error",
                stage="error",
                message=str(e),
                error=str(e),
            )
    finally:
        with ASSISTANT_NEWS_JOBS_LOCK:
            ASSISTANT_NEWS_STOP_EVENTS.pop(job_id, None)
            ASSISTANT_NEWS_THREADS.pop(job_id, None)


@app.get("/assistant", response_class=HTMLResponse)
def assistant_page(request: Request):
    openai_status = _assistant_openai_config_status()
    return render(
        "assistant.html",
        title="Assistant",
        path="/assistant",
        default_model=DEFAULT_OPENAI_MODEL,
        default_system=_assistant_news_default_system_prompt(),
        assistant_model_runs_enabled=ASSISTANT_MODEL_RUNS_ENABLED,
        model_runs_enabled=ASSISTANT_MODEL_RUNS_ENABLED and bool(openai_status["configured"]),
        openai_configured=bool(openai_status["configured"]),
        openai_status=openai_status,
        openai_base_url=OPENAI_BASE_URL,
        default_num_ctx=_assistant_news_default_num_ctx(),
        request=request,
    )


@app.get("/assistant/nasdaq100", response_class=JSONResponse)
def assistant_nasdaq100():
    if not callable(fetch_nasdaq100_tickers):
        return JSONResponse({"tickers": [], "error": "Nasdaq-100 loader is unavailable."}, status_code=503)
    try:
        tickers = fetch_nasdaq100_tickers()
    except Exception as e:
        return JSONResponse({"tickers": [], "error": str(e)}, status_code=502)
    if not tickers:
        return JSONResponse({"tickers": [], "error": "No Nasdaq-100 tickers found."}, status_code=502)
    return JSONResponse(
        {
            "tickers": tickers[:100],
            "count": len(tickers[:100]),
            "source": "Wikipedia Nasdaq-100 current components table",
            "loaded_at": _utc_now_iso(),
        }
    )


@app.get("/assistant/openai-key", response_class=JSONResponse)
def assistant_openai_key_status():
    return JSONResponse(_assistant_openai_config_status())


@app.post("/assistant/openai-key", response_class=JSONResponse)
async def assistant_openai_key_save(request: Request):
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    api_key = str(payload.get("api_key") or "").strip()
    try:
        _assistant_openai_save_api_key(api_key)
    except ValueError as e:
        return JSONResponse({**_assistant_openai_config_status(), "error": str(e)}, status_code=400)
    except Exception as e:
        return JSONResponse({**_assistant_openai_config_status(), "error": str(e)}, status_code=500)
    return JSONResponse(_assistant_openai_config_status())


@app.delete("/assistant/openai-key", response_class=JSONResponse)
def assistant_openai_key_delete():
    _assistant_openai_delete_saved_key()
    return JSONResponse(_assistant_openai_config_status())


@app.post("/assistant/news-workflow", response_class=JSONResponse)
async def assistant_news_workflow_start(request: Request):
    if AssistantNewsWorkflowConfig is None or not callable(parse_ticker_symbols):
        return JSONResponse({"error": "Assistant news workflow module is unavailable."}, status_code=503)
    if not ASSISTANT_MODEL_RUNS_ENABLED:
        return JSONResponse(
            {"error": "Assistant OpenAI runs are disabled. Set ASSISTANT_MODEL_RUNS_ENABLED=1 to enable them."},
            status_code=503,
        )
    openai_api_key = _assistant_openai_effective_api_key()
    if not openai_api_key:
        return JSONResponse(
            {"error": "OpenAI API key is not configured. Save an API key on the Assistant page or set OPENAI_API_KEY in .env."},
            status_code=503,
        )

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    tickers = parse_ticker_symbols(payload.get("tickers") or payload.get("ticker_text") or "", max_symbols=100)
    if not tickers:
        return JSONResponse({"error": "Enter at least one valid ticker."}, status_code=400)
    if len(tickers) > 100:
        return JSONResponse({"error": "Ticker limit is 100."}, status_code=400)

    def _int_setting(key: str, default: int, low: int, high: int) -> int:
        try:
            value = int(payload.get(key, default))
        except Exception:
            value = default
        return max(low, min(high, value))

    def _float_setting(key: str, default: float, low: float, high: float) -> float:
        try:
            value = float(payload.get(key, default))
        except Exception:
            value = default
        return max(low, min(high, value))

    articles_per_ticker = _int_setting("articles_per_ticker", 4, 1, 10)
    max_article_chars = _int_setting("max_article_chars", 1800, 300, 6000)
    request_timeout = _float_setting("request_timeout", 15.0, 5.0, 45.0)
    try:
        openai_timeout_default = float(os.getenv("ASSISTANT_NEWS_OPENAI_TIMEOUT", "180") or "180")
    except Exception:
        openai_timeout_default = 180.0
    openai_timeout = _float_setting("openai_timeout", openai_timeout_default, 30.0, 1200.0)
    try:
        max_input_default = int(os.getenv("ASSISTANT_NEWS_MAX_INPUT_CHARS", "180000") or "180000")
    except Exception:
        max_input_default = 180000
    max_input_chars = _int_setting("max_input_chars", max_input_default, 6000, 400000)
    try:
        max_output_default = int(os.getenv("ASSISTANT_NEWS_MAX_OUTPUT_TOKENS", "6000") or "6000")
    except Exception:
        max_output_default = 6000
    max_output_tokens = _int_setting("max_output_tokens", max_output_default, 512, 20000)
    allow_model_run = _coerce_bool(payload.get("allow_model_run", False), False)
    if not allow_model_run:
        return JSONResponse({"error": "Confirm OpenAI API usage before starting the workflow."}, status_code=400)

    model = str(payload.get("model") or DEFAULT_OPENAI_MODEL).strip() or DEFAULT_OPENAI_MODEL
    system_prompt = str(payload.get("system_prompt") or _assistant_news_default_system_prompt()).strip()
    include_article_text = _coerce_bool(payload.get("include_article_text", True), True)
    include_market_data = _coerce_bool(payload.get("include_market_data", True), True)
    config = AssistantNewsWorkflowConfig(
        openai_api_key=openai_api_key,
        openai_base_url=OPENAI_BASE_URL,
        openai_organization=OPENAI_ORGANIZATION,
        openai_project=OPENAI_PROJECT,
        model=model,
        system_prompt=system_prompt,
        articles_per_ticker=articles_per_ticker,
        include_article_text=include_article_text,
        include_market_data=include_market_data,
        max_article_chars=max_article_chars,
        request_timeout=request_timeout,
        openai_timeout=openai_timeout,
        max_input_chars=max_input_chars,
        max_output_tokens=max_output_tokens,
    )

    ensure_dirs()
    _assistant_news_cleanup_jobs()
    job_id = uuid4().hex[:12]
    output_dir = ASSISTANT_NEWS_RUNS_DIR / job_id
    now = _utc_now_iso()
    stop_event = threading.Event()
    with ASSISTANT_NEWS_JOBS_LOCK:
        active_job = _assistant_news_active_job_snapshot_locked()
        if active_job:
            return JSONResponse(_assistant_news_conflict_payload(active_job), status_code=409)
        ASSISTANT_NEWS_STOP_EVENTS[job_id] = stop_event
        ASSISTANT_NEWS_JOBS[job_id] = {
            "job_id": job_id,
            "status": "queued",
            "stage": "queued",
            "message": "Queued",
            "started_at": now,
            "updated_at": now,
            "tickers": tickers,
            "completed": 0,
            "total": len(tickers),
            "current_ticker": "",
            "events": [{"ts": now, "stage": "queued", "message": "Queued", "completed": 0, "total": len(tickers)}],
            "ticker_responses": [],
            "cancel_requested": False,
            "config": _assistant_news_config_summary(config),
        }

    thread = threading.Thread(
        target=_assistant_news_worker,
        kwargs={"job_id": job_id, "tickers": tickers, "config": config, "output_dir": output_dir, "stop_event": stop_event},
        daemon=True,
        name=f"assistant-news-{job_id}",
    )
    try:
        with ASSISTANT_NEWS_JOBS_LOCK:
            ASSISTANT_NEWS_THREADS[job_id] = thread
        thread.start()
    except Exception as e:
        _assistant_news_update_job(job_id, status="error", stage="error", message=str(e), error=str(e))
        with ASSISTANT_NEWS_JOBS_LOCK:
            ASSISTANT_NEWS_STOP_EVENTS.pop(job_id, None)
            ASSISTANT_NEWS_THREADS.pop(job_id, None)
        return JSONResponse({"error": f"Unable to start assistant workflow: {e}"}, status_code=500)
    return JSONResponse(_assistant_news_job_snapshot(job_id))


@app.post("/assistant/news-workflow/{job_id}/stop", response_class=JSONResponse)
def assistant_news_workflow_stop(job_id: str):
    clean_job_id = re.sub(r"[^a-fA-F0-9]", "", str(job_id or ""))[:32]
    now = _utc_now_iso()
    with ASSISTANT_NEWS_JOBS_LOCK:
        job = ASSISTANT_NEWS_JOBS.get(clean_job_id)
        if not job:
            return JSONResponse({"error": "Workflow job not found."}, status_code=404)

        status = str(job.get("status") or "").lower()
        if status in {"complete", "error", "stopped"}:
            return JSONResponse(copy.deepcopy(job))

        stop_event = ASSISTANT_NEWS_STOP_EVENTS.get(clean_job_id)
        if stop_event:
            stop_event.set()

        job["status"] = "stopping"
        job["stage"] = "stopping"
        job["message"] = "Stop requested; waiting for the active OpenAI request to halt."
        job["cancel_requested"] = True
        job["updated_at"] = now
        event = {
            "ts": now,
            "stage": "stopping",
            "message": "Stop requested; waiting for the active OpenAI request to halt.",
            "completed": job.get("completed", 0),
            "total": job.get("total", 0),
        }
        events = job.setdefault("events", [])
        if isinstance(events, list):
            events.append(event)
            del events[:-360]
        snapshot = copy.deepcopy(job)
    return JSONResponse(snapshot)


@app.get("/assistant/news-workflow/{job_id}", response_class=JSONResponse)
def assistant_news_workflow_status(job_id: str):
    clean_job_id = re.sub(r"[^a-fA-F0-9]", "", str(job_id or ""))[:32]
    job = _assistant_news_job_snapshot(clean_job_id)
    if not job:
        return JSONResponse({"error": "Workflow job not found."}, status_code=404)
    return JSONResponse(job)


@app.get("/assistant/news-workflow/{job_id}/files/{file_name}", response_class=PlainTextResponse)
def assistant_news_workflow_file(job_id: str, file_name: str):
    clean_job_id = re.sub(r"[^a-fA-F0-9]", "", str(job_id or ""))[:32]
    safe_file = str(file_name or "").strip()
    if safe_file not in {"impressions.md", "impressions.json", "news_packet.md", "news_packet.json", "final_summary.md"}:
        return PlainTextResponse("File not allowed.", status_code=400)
    path = ASSISTANT_NEWS_RUNS_DIR / clean_job_id / safe_file
    if not path.exists() or not path.is_file():
        return PlainTextResponse("File not found.", status_code=404)
    media_type = "application/json" if safe_file.endswith(".json") else "text/plain"
    return PlainTextResponse(path.read_text(encoding="utf-8", errors="ignore"), media_type=media_type)


@app.get("/partials/assistant_overview", response_class=HTMLResponse)
def assistant_overview():
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT id, algorithm_name, status, run_dir FROM runs ORDER BY id DESC LIMIT 5")
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return HTMLResponse("<div class='small'>No runs yet.</div>")

    html = ["<div class='small'>Recent runs</div>", "<ul class='small'>"]
    for r in rows:
        hint = "Looks normal."
        log_path = Path(r["run_dir"]) / "algo.log"
        if log_path.exists():
            tail = log_path.read_text(errors="ignore").lower()
            if "warn price_feed_stale" in tail:
                hint = "Stale data warnings."
            if "error" in tail:
                hint = "Errors detected."
        html.append(f"<li><a href='/runs/{r['id']}'>Run #{r['id']}</a> — {hint}</li>")
    html.append("</ul>")
    return HTMLResponse("".join(html))


@app.get("/assistant/models", response_class=JSONResponse)
def assistant_models():
    openai_status = _assistant_openai_config_status()
    choices_raw = os.getenv("OPENAI_MODEL_CHOICES", "")
    choices = [part.strip() for part in re.split(r"[\s,;|]+", choices_raw) if part.strip()]
    models: list[str] = []
    seen: set[str] = set()
    for name in [DEFAULT_OPENAI_MODEL, *choices]:
        if name and name not in seen:
            models.append(name)
            seen.add(name)
    return JSONResponse(
        {
            "provider": "openai",
            "models": models,
            "default_model": DEFAULT_OPENAI_MODEL,
            "openai_base_url": OPENAI_BASE_URL,
            "configured": bool(openai_status["configured"]),
            "source": openai_status["source"],
            "masked_key": openai_status["masked_key"],
            "env_key_present": openai_status["env_key_present"],
            "saved_key_present": openai_status["saved_key_present"],
            "error": "" if openai_status["configured"] else openai_status["error"],
        }
    )


@app.get("/assistant/context", response_class=JSONResponse)
def assistant_context(
    include_portfolio: int = 1,
    include_runs: int = 1,
    include_logs: int = 0,
    include_indicators: int = 0,
    log_lines: int = 120,
):
    ctx = _assistant_context_data(
        include_portfolio=bool(include_portfolio),
        include_runs=bool(include_runs),
        include_logs=bool(include_logs),
        include_indicators=bool(include_indicators),
        log_lines=int(log_lines),
    )
    return JSONResponse(ctx)


@app.post("/assistant/chat", response_class=JSONResponse)
async def assistant_chat(request: Request):
    """Enhanced chat endpoint using new strategic agent if available"""
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    prompt = str(payload.get("prompt") or "").strip()
    if not prompt:
        return JSONResponse({"error": "Prompt is required."}, status_code=400)

    # Try to use new strategic agent if available
    global ASSISTANT_MONITOR_MANAGER
    if ASSISTANT_MONITOR_MANAGER:
        try:
            agent = ASSISTANT_MONITOR_MANAGER.get_agent()
            if agent:
                result = agent.chat(
                    user_message=prompt,
                    include_portfolio=bool(payload.get("include_portfolio", True)),
                    include_cryptids=bool(payload.get("include_runs", True)),
                    include_indicators=bool(payload.get("include_indicators", False)),
                    query_type="balanced"
                )
                return JSONResponse({
                    "reply": result.get("response"),
                    "model": result.get("model_used"),
                    "context_summary": result.get("context_summary"),
                    "duration_sec": result.get("duration_sec")
                })
        except Exception as e:
            print(f"[Assistant] New agent failed, falling back to legacy: {e}")
            import traceback
            traceback.print_exc()

    # Legacy fallback
    model = str(payload.get("model") or DEFAULT_OLLAMA_MODEL).strip() or DEFAULT_OLLAMA_MODEL
    system_prompt = str(payload.get("system") or _assistant_default_system_prompt()).strip()

    include_portfolio = bool(payload.get("include_portfolio", True))
    include_runs = bool(payload.get("include_runs", True))
    include_logs = bool(payload.get("include_logs", False))
    include_indicators = bool(payload.get("include_indicators", False))
    log_lines = int(payload.get("log_lines", 120))

    history_in = payload.get("history", [])
    history: list[dict[str, str]] = []
    if isinstance(history_in, list):
        for item in history_in[-12:]:
            if not isinstance(item, dict):
                continue
            role = str(item.get("role") or "").strip().lower()
            content = str(item.get("content") or "").strip()
            if role in ("user", "assistant") and content:
                history.append({"role": role, "content": content})

    context_data = _assistant_context_data(
        include_portfolio=include_portfolio,
        include_runs=include_runs,
        include_logs=include_logs,
        include_indicators=include_indicators,
        log_lines=log_lines,
    )
    context_text = json.dumps(context_data, indent=2)
    system_content = system_prompt + "\n\nContext (JSON):\n" + context_text

    messages: list[dict[str, str]] = [{"role": "system", "content": system_content}]
    messages.extend(history)
    messages.append({"role": "user", "content": prompt})

    try:
        ollama_resp = httpx.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": model,
                "messages": messages,
                "stream": False,
                "keep_alive": ASSISTANT_OLLAMA_KEEP_ALIVE,
                "options": _assistant_ollama_options(),
            },
            timeout=60.0,
        )
        ollama_resp.raise_for_status()
        data = ollama_resp.json()
        reply = (data.get("message") or {}).get("content") or ""
        return JSONResponse({"reply": reply, "model": model})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


# =========================
# New Enhanced Assistant Endpoints
# =========================
@app.get("/assistant/health", response_class=JSONResponse)
def assistant_health():
    """Get health stats for monitoring system"""
    global ASSISTANT_MONITOR_MANAGER, ASSISTANT_MONITORS_STARTING, ASSISTANT_MONITORS_LAST_ERROR

    env_enabled = _assistant_monitors_env_enabled()
    config_enabled = _assistant_monitors_config_enabled()

    with ASSISTANT_MONITORS_LOCK:
        manager = ASSISTANT_MONITOR_MANAGER
        starting = bool(ASSISTANT_MONITORS_STARTING)
        last_error = str(ASSISTANT_MONITORS_LAST_ERROR or "").strip()

    base_payload = {
        "env_enabled": env_enabled,
        "config_enabled": config_enabled,
        "can_enable": bool(env_enabled and not manager and not starting),
        "can_disable": bool(manager or starting or config_enabled),
    }

    if starting:
        return JSONResponse(
            {
                "status": "initializing",
                "message": "Monitor system is starting",
                **base_payload,
            }
        )

    if not manager:
        if not env_enabled:
            return JSONResponse(
                {
                    "status": "disabled",
                    "reason": "env",
                    "message": "Monitor system disabled via ASSISTANT_MONITORS_ENABLED",
                    **base_payload,
                }
            )
        if not config_enabled:
            return JSONResponse(
                {
                    "status": "disabled",
                    "reason": "config",
                    "message": "Monitor system disabled in configuration",
                    **base_payload,
                }
            )
        if last_error:
            return JSONResponse(
                {
                    "status": "error",
                    "error": last_error,
                    "message": "Monitor system failed to start",
                    **base_payload,
                },
                status_code=500,
            )
        return JSONResponse({"status": "disabled", "message": "Monitor system not running", **base_payload})

    try:
        stats = manager.get_health_stats()
        return JSONResponse({"status": "ok", "stats": stats, **base_payload})
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e), **base_payload}, status_code=500)


@app.post("/assistant/monitor-system", response_class=JSONResponse)
async def assistant_monitor_system(request: Request):
    """Enable or disable assistant monitor system at runtime"""
    try:
        payload = await request.json()
    except Exception:
        payload = {}

    enabled = _parse_optional_bool((payload or {}).get("enabled"))
    if enabled is None:
        return JSONResponse({"error": "Boolean field 'enabled' is required."}, status_code=400)

    if enabled:
        if not _assistant_monitors_env_enabled():
            return JSONResponse(
                {
                    "status": "disabled",
                    "reason": "env",
                    "message": "Cannot enable while ASSISTANT_MONITORS_ENABLED is off",
                },
                status_code=409,
            )
        try:
            _set_assistant_monitors_config_enabled(True)
        except Exception as e:
            return JSONResponse({"error": f"Failed to persist monitor config: {e}"}, status_code=500)

        started, reason = _request_assistant_monitors_start()
        if not started and reason == "disabled_env":
            return JSONResponse(
                {
                    "status": "disabled",
                    "reason": "env",
                    "message": "Cannot enable while ASSISTANT_MONITORS_ENABLED is off",
                },
                status_code=409,
            )
        if started and reason == "already_running":
            return JSONResponse({"status": "ok", "message": "Monitor system already running"})
        if started and reason in ("starting", "already_starting"):
            return JSONResponse({"status": "initializing", "message": "Monitor system starting"})
        return JSONResponse({"status": "ok", "message": "Monitor system enabled"})

    try:
        _set_assistant_monitors_config_enabled(False)
    except Exception as e:
        return JSONResponse({"error": f"Failed to persist monitor config: {e}"}, status_code=500)

    stopped, reason = _stop_assistant_monitors_runtime()
    if stopped or reason == "not_running":
        return JSONResponse({"status": "disabled", "message": "Monitor system disabled"})
    return JSONResponse({"status": "disabled", "message": f"Disabled with stop error: {reason}"})


@app.get("/assistant/events/recent", response_class=JSONResponse)
def assistant_recent_events(hours: int = 24, limit: int = 50):
    """Get recent events detected by monitors"""
    global ASSISTANT_MONITOR_MANAGER
    hours_i = max(1, int(hours or 24))
    limit_i = max(1, min(200, int(limit or 50)))

    if ASSISTANT_MONITOR_MANAGER:
        try:
            events = ASSISTANT_MONITOR_MANAGER.event_processor.storage.get_recent_events(hours_i, limit_i)
            return JSONResponse({"events": events, "status": "ok"})
        except Exception as e:
            return JSONResponse({"events": [], "status": "error", "error": str(e)}, status_code=500)

    try:
        try:
            from .assistant.events.event_processor import EventStorage
        except Exception:
            from app.assistant.events.event_processor import EventStorage  # type: ignore
        try:
            from .assistant.vector_store import VectorStore
        except Exception:
            from app.assistant.vector_store import VectorStore  # type: ignore
        # Ensure assistant tables exist even when monitor manager is disabled.
        _ = VectorStore(DB_PATH)
        storage = EventStorage(DB_PATH)
        events = storage.get_recent_events(hours_i, limit_i)
        return JSONResponse({"events": events, "status": "disabled"})
    except Exception as e:
        return JSONResponse({"events": [], "status": "disabled", "error": str(e)})


@app.post("/assistant/analyze/ticker", response_class=JSONResponse)
async def assistant_analyze_ticker(request: Request):
    """Explain what's happening with a specific ticker"""
    global ASSISTANT_MONITOR_MANAGER
    if not ASSISTANT_MONITOR_MANAGER:
        return JSONResponse({"error": "Monitor system not running"}, status_code=503)

    try:
        payload = await request.json()
        ticker = str(payload.get("ticker", "")).strip().upper()
        if not ticker:
            return JSONResponse({"error": "Ticker required"}, status_code=400)

        agent = ASSISTANT_MONITOR_MANAGER.get_agent()
        result = agent.explain_ticker(ticker)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/assistant/review/portfolio", response_class=JSONResponse)
async def assistant_review_portfolio():
    """Generate comprehensive portfolio review"""
    global ASSISTANT_MONITOR_MANAGER
    if not ASSISTANT_MONITOR_MANAGER:
        return JSONResponse({"error": "Monitor system not running"}, status_code=503)

    try:
        agent = ASSISTANT_MONITOR_MANAGER.get_agent()
        result = agent.portfolio_review()
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/assistant/memory/search", response_class=JSONResponse)
def assistant_memory_search(q: str, limit: int = 10):
    """Search assistant memory with semantic similarity"""
    global ASSISTANT_MONITOR_MANAGER
    if not ASSISTANT_MONITOR_MANAGER:
        return JSONResponse({"query": q, "results": [], "status": "disabled"})

    try:
        agent = ASSISTANT_MONITOR_MANAGER.get_agent()
        results = agent.search_memory(q, limit)
        return JSONResponse({"query": q, "results": results, "status": "ok"})
    except Exception as e:
        return JSONResponse({"query": q, "results": [], "status": "error", "error": str(e)}, status_code=500)


@app.get("/assistant/memory/stats", response_class=JSONResponse)
def assistant_memory_stats():
    """Get memory system statistics"""
    global ASSISTANT_MONITOR_MANAGER
    try:
        if ASSISTANT_MONITOR_MANAGER:
            agent = ASSISTANT_MONITOR_MANAGER.get_agent()
            stats = agent.get_memory_stats()
            if isinstance(stats, dict):
                stats = dict(stats)
                stats.setdefault("status", "ok")
            return JSONResponse(stats)

        try:
            from .assistant.vector_store import VectorStore
        except Exception:
            from app.assistant.vector_store import VectorStore  # type: ignore

        store = VectorStore(DB_PATH)
        return JSONResponse(
            {
                "total_items": store.count(),
                "conversations": store.count("conversation"),
                "events": store.count("event"),
                "analyses": store.count("analysis"),
                "status": "disabled",
            }
        )
    except Exception as e:
        return JSONResponse(
            {
                "total_items": 0,
                "conversations": 0,
                "events": 0,
                "analyses": 0,
                "status": "error",
                "error": str(e),
            },
            status_code=500,
        )


@app.get("/assistant/memory/recent", response_class=JSONResponse)
def assistant_memory_recent(hours: int = 168, limit: int = 20, content_type: str = ""):
    """Get recent memory items directly from assistant_memory_vectors"""
    hours_i = max(1, int(hours or 168))
    limit_i = max(1, min(200, int(limit or 20)))
    ctype = str(content_type or "").strip().lower()
    if ctype in ("all", "*"):
        ctype = ""

    cutoff = int(time.time()) - (hours_i * 3600)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    try:
        if ctype:
            cur.execute(
                """
                SELECT id, ts, content_type, content_text, metadata_json
                FROM assistant_memory_vectors
                WHERE ts >= ? AND LOWER(content_type)=?
                ORDER BY ts DESC
                LIMIT ?
                """,
                (cutoff, ctype, limit_i),
            )
        else:
            cur.execute(
                """
                SELECT id, ts, content_type, content_text, metadata_json
                FROM assistant_memory_vectors
                WHERE ts >= ?
                ORDER BY ts DESC
                LIMIT ?
                """,
                (cutoff, limit_i),
            )
        rows = cur.fetchall()
    except Exception as e:
        conn.close()
        return JSONResponse({"items": [], "status": "error", "error": str(e)}, status_code=500)
    conn.close()

    items: list[dict[str, Any]] = []
    for row in rows:
        metadata: dict[str, Any] = {}
        try:
            raw_meta = row["metadata_json"]
            if isinstance(raw_meta, str) and raw_meta:
                parsed = json.loads(raw_meta)
                if isinstance(parsed, dict):
                    metadata = parsed
        except Exception:
            metadata = {}
        items.append(
            {
                "id": int(row["id"] or 0),
                "ts": int(row["ts"] or 0),
                "content_type": str(row["content_type"] or ""),
                "content_text": str(row["content_text"] or ""),
                "metadata": metadata,
            }
        )
    return JSONResponse({"items": items, "status": "ok"})


@app.get("/assistant/analyses/recent", response_class=JSONResponse)
def assistant_analyses_recent(hours: int = 168, limit: int = 20):
    """Get recent analyses; fallback to analysis items in memory vectors if table is empty"""
    hours_i = max(1, int(hours or 168))
    limit_i = max(1, min(200, int(limit or 20)))
    cutoff = int(time.time()) - (hours_i * 3600)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                a.id,
                a.ts,
                a.event_id,
                a.model_used,
                a.prompt_type,
                a.analysis_text,
                e.event_type,
                e.severity,
                e.description
            FROM assistant_analyses a
            LEFT JOIN assistant_events e ON e.id = a.event_id
            WHERE a.ts >= ?
            ORDER BY a.ts DESC
            LIMIT ?
            """,
            (cutoff, limit_i),
        )
        rows = cur.fetchall()
    except Exception as e:
        conn.close()
        return JSONResponse({"items": [], "status": "error", "error": str(e)}, status_code=500)

    items: list[dict[str, Any]] = []
    for row in rows:
        items.append(
            {
                "id": int(row["id"] or 0),
                "ts": int(row["ts"] or 0),
                "event_id": int(row["event_id"]) if row["event_id"] is not None else None,
                "model_used": str(row["model_used"] or ""),
                "prompt_type": str(row["prompt_type"] or ""),
                "analysis_text": str(row["analysis_text"] or ""),
                "event_type": str(row["event_type"] or ""),
                "severity": str(row["severity"] or ""),
                "description": str(row["description"] or ""),
                "source": "assistant_analyses",
            }
        )

    if not items:
        try:
            cur.execute(
                """
                SELECT id, ts, content_text, metadata_json
                FROM assistant_memory_vectors
                WHERE ts >= ? AND LOWER(content_type)='analysis'
                ORDER BY ts DESC
                LIMIT ?
                """,
                (cutoff, limit_i),
            )
            mem_rows = cur.fetchall()
            for row in mem_rows:
                metadata: dict[str, Any] = {}
                try:
                    raw_meta = row["metadata_json"]
                    if isinstance(raw_meta, str) and raw_meta:
                        parsed = json.loads(raw_meta)
                        if isinstance(parsed, dict):
                            metadata = parsed
                except Exception:
                    metadata = {}
                items.append(
                    {
                        "id": int(row["id"] or 0),
                        "ts": int(row["ts"] or 0),
                        "event_id": None,
                        "model_used": str(metadata.get("model") or metadata.get("model_used") or ""),
                        "prompt_type": str(metadata.get("prompt_type") or ""),
                        "analysis_text": str(row["content_text"] or ""),
                        "event_type": str(metadata.get("event_type") or ""),
                        "severity": str(metadata.get("severity") or ""),
                        "description": str(metadata.get("description") or ""),
                        "source": "assistant_memory_vectors",
                    }
                )
        except Exception:
            pass
    conn.close()
    return JSONResponse({"items": items, "status": "ok"})


# =========================
# Broker Hub Page (multi-broker)
# =========================
@app.get("/broker", response_class=HTMLResponse)
def broker_page(request: Request):
    _ensure_legacy_schwab_connection()
    schwab_cfg = _schwab_config()
    return render("broker.html", title="Broker", path="/broker", request=request, schwab_config=schwab_cfg)


@app.get("/partials/broker_connections", response_class=HTMLResponse)
def partial_broker_connections():
    conns = list_broker_connections()
    if not conns:
        return HTMLResponse("<div class='small'>No broker connections yet.</div>")

    blocks: list[str] = []
    for c in conns:
        status = str(c["status"])
        badge = "ok" if status == "connected" else ("warn" if status in ("needs_auth", "needs_attention") else "bad")
        metadata = c.get("metadata") or {}
        err = metadata.get("error")
        debug = metadata.get("debug")
        debug_html = ""
        if err or debug:
            parts: list[str] = []
            if err:
                parts.append(f"<div class='small'><b>Error:</b> {html.escape(str(err))}</div>")
            if debug:
                try:
                    dbg_txt = json.dumps(debug, indent=2, sort_keys=True)
                except Exception:
                    dbg_txt = str(debug)
                parts.append(
                    "<pre class='small' style='white-space:pre-wrap; margin-top:6px;'>"
                    f"{html.escape(dbg_txt)}"
                    "</pre>"
                )
            debug_html = "<details style='margin-top:8px'><summary class='small'>Debug</summary>" + "".join(parts) + "</details>"
        blocks.append(
            "<div class='card' style='margin-top:10px'>"
            "<div class='row' style='justify-content:space-between'>"
            f"<div><b>{c['label']}</b><div class='small'>{c['broker']} · id {c['id']}</div></div>"
            f"<span class='badge {badge}'>{c['status']}</span>"
            "</div>"
            "<div class='row' style='margin-top:10px'>"
            f"<a class='btn' href='/broker?connection_id={c['id']}'>Open</a>"
            f"<form method='post' action='/broker/connections/{c['id']}/remove'>"
            f"<button class='btn danger' type='submit'>Remove</button>"
            f"</form>"
            "</div>"
            f"{debug_html}"
            "</div>"
        )
    return HTMLResponse("".join(blocks))


@app.post("/broker/connections/{connection_id}/remove")
def broker_remove_connection(connection_id: int):
    try:
        unlink_connection(connection_id=connection_id, db_path=str(DB_PATH))
    except Exception:
        delete_broker_connection(connection_id)
    return RedirectResponse("/broker", status_code=303)


# =========================
# Schwab OAuth (legacy routes preserved)
# =========================
@app.post("/broker/schwab/configure")
def schwab_configure(
    label: str = Form("Schwab"),
    client_id: str = Form(""),
    client_secret: str = Form(""),
    redirect_uri: str = Form("https://127.0.0.1:8000/callback"),
    scope: str = Form("readonly"),
    trader_api_base: str = Form("https://api.schwabapi.com/trader/v1"),
    market_data_base: str = Form(""),
    account_hash: str = Form(""),
):
    _save_schwab_config(
        label=label,
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        scope=scope,
        trader_api_base=trader_api_base,
        market_data_base=market_data_base,
        account_hash=account_hash,
    )
    return RedirectResponse("/broker", status_code=303)


@app.get("/broker/connect")
def broker_connect():
    cfg = _schwab_config()
    client_id = str(cfg.get("client_id") or "").strip()
    redirect_uri = str(cfg.get("redirect_uri") or "https://127.0.0.1:8000/callback").strip()
    scope = str(cfg.get("scope") or "readonly").strip()
    if not client_id:
        return PlainTextResponse("Configure Schwab Client ID on the Broker page first.", status_code=400)

    state = str(_utc_ts())
    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": scope,
        "state": state,
    }
    url = httpx.URL(SCHWAB_AUTHORIZE_URL, params=params)
    return RedirectResponse(str(url), status_code=302)


@app.get("/callback")
def broker_callback(code: Optional[str] = None, state: Optional[str] = None, session: Optional[str] = None):
    if not code:
        return PlainTextResponse("Missing code parameter on callback", status_code=400)

    cfg = _schwab_config()
    client_id = str(cfg.get("client_id") or "").strip()
    client_secret = str(cfg.get("client_secret") or "").strip()
    redirect_uri = str(cfg.get("redirect_uri") or "https://127.0.0.1:8000/callback").strip()
    if not client_id or not client_secret:
        return PlainTextResponse("Configure Schwab Client ID and Client Secret on the Broker page first.", status_code=400)

    # Official OAuth token exchange:
    #   POST https://api.schwabapi.com/v1/oauth/token
    #   Authorization: Basic base64(client_id:client_secret)
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    headers = {"Authorization": f"Basic {basic}", "Content-Type": "application/x-www-form-urlencoded"}
    data = {"grant_type": "authorization_code", "code": code, "redirect_uri": redirect_uri}

    with httpx.Client(timeout=30.0) as client:
        resp = client.post(SCHWAB_TOKEN_URL, headers=headers, data=data)

    if resp.status_code >= 400:
        return PlainTextResponse(f"Token exchange failed: {resp.status_code}\n{resp.text}", status_code=500)

    tok = resp.json()

    # IMPORTANT: connector expects ISO-8601 string (not epoch int)
    # to support datetime.fromisoformat parsing.
    tok["obtained_at"] = _utc_now_iso()

    save_token(tok)
    row = _latest_schwab_config_row()
    if row is not None and _db_update_broker_connection is not None:
        meta = _safe_json(str(row["metadata_json"] or "{}"), default={})
        meta["storage"] = "file"
        meta["token_path"] = str(TOKEN_PATH)
        meta["obtained_at"] = tok.get("obtained_at")
        secrets = _db_read_connection_secrets(row, default={})
        _db_update_broker_connection(
            db_path=str(DB_PATH),
            connection_id=int(row["id"]),
            broker="schwab",
            label=str(row["label"] or "Schwab"),
            status="connected",
            metadata=meta,
            secrets=secrets,
            allow_plaintext=True,
        )
    else:
        _ensure_legacy_schwab_connection()
    return RedirectResponse("/broker", status_code=303)


@app.post("/broker/disconnect")
def broker_disconnect():
    try:
        if TOKEN_PATH.exists():
            TOKEN_PATH.unlink()
    except Exception:
        pass

    conn = db()
    cur = conn.cursor()
    cur.execute(
        "UPDATE broker_connections SET status=?, updated_ts=? WHERE broker='schwab'",
        ("needs_auth", _utc_ts()),
    )
    conn.commit()
    conn.close()
    return RedirectResponse("/broker", status_code=303)


@app.get("/broker/test")
def broker_test():
    tok = load_token()
    if not tok:
        return PlainTextResponse("No token. Connect first.", status_code=400)

    age = _token_age_seconds(tok)
    age_txt = f"{age}s" if age is not None else "unknown"

    return PlainTextResponse(
        f"Token present. Age: {age_txt}. obtained_at={tok.get('obtained_at')!r}. "
        f"access_token length={len(tok.get('access_token',''))}"
    )


# =========================
# Robinhood Connect
# =========================
@app.post("/broker/robinhood/link")
def robinhood_link(
    label: str = Form("Robinhood"),
    username: str = Form(...),
    password: str = Form(...),
    mfa_code: str = Form(""),
):
    try:
        ok, msg = link_robinhood_connection(
            db_path=str(DB_PATH),
            label=label,
            username=username,
            password=password,
            mfa_code=mfa_code or None,
        )
        if not ok:
            return PlainTextResponse(msg, status_code=400)
    except Exception as e:
        return PlainTextResponse(f"Robinhood link failed: {e}", status_code=500)
    return RedirectResponse("/broker", status_code=303)


SOLID_FACE_LEVELS = (4, 6, 8, 12, 20)


def _choose_face_level_for_positions(position_count: int) -> int:
    count = max(0, int(position_count or 0))
    for level in SOLID_FACE_LEVELS:
        if count <= level:
            return level
    return SOLID_FACE_LEVELS[-1]


def _aggregate_portfolio_positions(
    *,
    max_positions_per_account: int = 250,
    force_refresh: bool = False,
) -> list[dict[str, Any]]:
    context = get_portfolio_context_data(
        db_path=str(DB_PATH),
        max_positions=max(20, int(max_positions_per_account or 20)),
        force_refresh=bool(force_refresh),
    )
    aggregated: dict[str, dict[str, Any]] = {}

    for portfolio in context:
        accounts = portfolio.get("accounts") if isinstance(portfolio, dict) else None
        if not isinstance(accounts, list):
            continue
        for account in accounts:
            positions = account.get("positions") if isinstance(account, dict) else None
            if not isinstance(positions, list):
                continue
            for raw_pos in positions:
                if not isinstance(raw_pos, dict):
                    continue
                symbol = str(raw_pos.get("symbol") or "").strip().upper()
                if not symbol or symbol == "-":
                    continue
                entry = aggregated.setdefault(
                    symbol,
                    {
                        "symbol": symbol,
                        "market_value_abs": 0.0,
                        "market_value_signed": 0.0,
                        "quantity_abs": 0.0,
                        "quantity_signed": 0.0,
                        "cost_basis_abs": 0.0,
                        "cost_basis_signed": 0.0,
                        "gain_loss_dollar": 0.0,
                        "day_pl_dollar": 0.0,
                        "day_pct_weighted_sum": 0.0,
                        "day_pct_weight_total": 0.0,
                        "last_price_weighted_sum": 0.0,
                        "last_price_weight_total": 0.0,
                        "has_market_value": False,
                        "has_quantity": False,
                        "has_cost_basis": False,
                        "has_gain_loss": False,
                        "has_day_pl": False,
                        "has_day_pct": False,
                        "asset_types": set(),
                    },
                )

                quantity = _to_float_opt(raw_pos.get("quantity"))
                average_price = _to_float_opt(raw_pos.get("average_price"))
                market_price = _to_float_opt(raw_pos.get("market_price"))
                market_value = _to_float_opt(raw_pos.get("market_value"))
                if market_value is None:
                    multiplier = _to_float_opt(raw_pos.get("price_multiplier"))
                    if multiplier is None or multiplier <= 0:
                        multiplier = 1.0
                    if quantity is not None and market_price is not None:
                        market_value = float(quantity) * float(market_price) * float(multiplier)

                if market_value is not None:
                    mv = float(market_value)
                    entry["market_value_abs"] += abs(mv)
                    entry["market_value_signed"] += mv
                    entry["has_market_value"] = True
                if quantity is not None:
                    qty = float(quantity)
                    entry["quantity_abs"] += abs(qty)
                    entry["quantity_signed"] += qty
                    entry["has_quantity"] = True

                multiplier = _to_float_opt(raw_pos.get("price_multiplier"))
                if multiplier is None or multiplier <= 0:
                    multiplier = 1.0

                cost_basis = None
                if quantity is not None and average_price is not None:
                    cost_basis = float(quantity) * float(average_price) * float(multiplier)

                gain_loss = _to_float_opt(raw_pos.get("unrealized_pl"))
                if gain_loss is None and market_value is not None and cost_basis is not None:
                    gain_loss = float(market_value) - float(cost_basis)
                if cost_basis is None and market_value is not None and gain_loss is not None:
                    cost_basis = float(market_value) - float(gain_loss)

                if cost_basis is not None:
                    cb = float(cost_basis)
                    entry["cost_basis_abs"] += abs(cb)
                    entry["cost_basis_signed"] += cb
                    entry["has_cost_basis"] = True

                if gain_loss is not None:
                    gl = float(gain_loss)
                    entry["gain_loss_dollar"] += gl
                    entry["has_gain_loss"] = True

                day_pl = _to_float_opt(raw_pos.get("day_pl"))
                if day_pl is not None:
                    entry["day_pl_dollar"] += float(day_pl)
                    entry["has_day_pl"] = True

                if market_price is not None:
                    px_weight = 0.0
                    if quantity is not None:
                        px_weight = abs(float(quantity) * float(multiplier))
                    if px_weight <= 0:
                        px_weight = abs(float(market_value)) if market_value is not None else 0.0
                    if px_weight <= 0:
                        px_weight = 1.0
                    entry["last_price_weighted_sum"] += float(market_price) * float(px_weight)
                    entry["last_price_weight_total"] += float(px_weight)

                day_pct = None
                previous_close = _to_float_opt(raw_pos.get("previous_close"))
                if previous_close is not None and previous_close != 0 and market_price is not None:
                    day_pct = (float(market_price) - float(previous_close)) / float(previous_close)
                elif day_pl is not None and market_value is not None:
                    prior_value = float(market_value) - float(day_pl)
                    if abs(prior_value) > 1e-9:
                        day_pct = float(day_pl) / prior_value

                if day_pct is not None:
                    weight_base = abs(float(market_value)) if market_value is not None else 0.0
                    if weight_base <= 0 and quantity is not None and market_price is not None:
                        weight_base = abs(float(quantity) * float(market_price) * float(multiplier))
                    if weight_base <= 0 and day_pl is not None:
                        weight_base = abs(float(day_pl))
                    if weight_base <= 0:
                        weight_base = 1.0
                    entry["day_pct_weighted_sum"] += float(day_pct) * float(weight_base)
                    entry["day_pct_weight_total"] += float(weight_base)
                    entry["has_day_pct"] = True

                asset_type = str(raw_pos.get("asset_type") or "").strip()
                if asset_type:
                    entry["asset_types"].add(asset_type)

    ranked = sorted(
        aggregated.values(),
        key=lambda item: (
            0 if item.get("has_market_value") else 1,
            -float(item.get("market_value_abs") if item.get("has_market_value") else item.get("quantity_abs") or 0.0),
            str(item.get("symbol") or ""),
        ),
    )

    total_market_value_abs = sum(
        float(item.get("market_value_abs") or 0.0)
        for item in ranked
        if bool(item.get("has_market_value"))
    )

    output: list[dict[str, Any]] = []
    for item in ranked:
        has_mv = bool(item.get("has_market_value"))
        metric = float(item.get("market_value_abs") if has_mv else item.get("quantity_abs") or 0.0)
        weight = (metric / total_market_value_abs) if has_mv and total_market_value_abs > 0 else None

        gain_loss_dollar = float(item.get("gain_loss_dollar") or 0.0) if bool(item.get("has_gain_loss")) else None
        gain_loss_percent = None
        if bool(item.get("has_gain_loss")) and bool(item.get("has_cost_basis")):
            basis = float(item.get("cost_basis_signed") or 0.0)
            if abs(basis) > 1e-9:
                gain_loss_percent = float(item.get("gain_loss_dollar") or 0.0) / abs(basis)

        day_change_percent = None
        if bool(item.get("has_day_pct")):
            pct_w = float(item.get("day_pct_weight_total") or 0.0)
            if pct_w > 0:
                day_change_percent = float(item.get("day_pct_weighted_sum") or 0.0) / pct_w

        day_pl_dollar = float(item.get("day_pl_dollar") or 0.0) if bool(item.get("has_day_pl")) else None
        last_price = None
        px_weight = float(item.get("last_price_weight_total") or 0.0)
        if px_weight > 0:
            last_price = float(item.get("last_price_weighted_sum") or 0.0) / px_weight

        output.append(
            {
                "symbol": str(item.get("symbol") or ""),
                "market_value_abs": float(item.get("market_value_abs") or 0.0),
                "market_value_signed": float(item.get("market_value_signed") or 0.0),
                "quantity_abs": float(item.get("quantity_abs") or 0.0),
                "quantity_signed": float(item.get("quantity_signed") or 0.0),
                "cost_basis_abs": float(item.get("cost_basis_abs") or 0.0),
                "cost_basis_signed": float(item.get("cost_basis_signed") or 0.0),
                "gain_loss_dollar": gain_loss_dollar,
                "gain_loss_percent": gain_loss_percent,
                "day_change_percent": day_change_percent,
                "day_pl_dollar": day_pl_dollar,
                "last_price": last_price,
                "weight": weight,
                "rank_metric": metric,
                "rank_basis": "market_value_abs" if has_mv else "quantity_abs",
                "asset_types": sorted(str(v) for v in item.get("asset_types") or []),
            }
        )

    return output


@app.get("/api/portfolio/positions/top", response_class=JSONResponse)
def api_portfolio_positions_top(limit: int = 80):
    safe_limit = max(1, min(200, int(limit or 80)))
    try:
        positions = _aggregate_portfolio_positions(
            max_positions_per_account=300,
            force_refresh=True,
        )
    except Exception as e:
        return JSONResponse(
            {
                "error": str(e),
                "positions": [],
                "total_positions": 0,
                "auto_face_count": _choose_face_level_for_positions(0),
                "face_levels": list(SOLID_FACE_LEVELS),
                "generated_at": int(time.time()),
            },
            status_code=500,
        )

    total_positions = len(positions)
    return JSONResponse(
        {
            "positions": positions[:safe_limit],
            "total_positions": total_positions,
            "auto_face_count": _choose_face_level_for_positions(total_positions),
            "face_levels": list(SOLID_FACE_LEVELS),
            "generated_at": int(time.time()),
        }
    )


# =========================
# Portfolio Partials
# =========================
@app.get("/partials/portfolio_summary", response_class=HTMLResponse)
def partial_portfolio_summary():
    """
    Preferred path: use brokers.registry (multi-broker).
    Fallback: show legacy Schwab link status if registry is not available.
    """
    try:
        html = get_portfolio_summary_html(db_path=str(DB_PATH))
        if html:
            return HTMLResponse(html)
    except Exception:
        pass

    # Minimal fallback (legacy Schwab token presence)
    tok = load_token()
    if not tok:
        return HTMLResponse(
            "<div class='row'><span class='small'>Broker:</span> <span class='badge warn'>Not linked</span> "
            "<a class='btn' href='/broker'>Link Schwab</a></div>"
        )

    return HTMLResponse(
        "<div class='row'><span class='small'>Broker:</span> <span class='badge ok'>Linked</span> "
        "<a class='btn' href='/broker'>Open</a></div>"
    )


@app.get("/partials/portfolio_bubbles", response_class=HTMLResponse)
def partial_portfolio_bubbles(pl_mode: str = "dollar"):
    try:
        html = get_portfolio_bubbles_html(db_path=str(DB_PATH), pl_mode=pl_mode)
        if html:
            return HTMLResponse(html)
    except Exception as e:
        return HTMLResponse(f"<div class='small'>Error loading portfolio bubbles: {e}</div>", status_code=500)

    return HTMLResponse("<div class='small'>Portfolio bubbles not available (registry not installed).</div>")


@app.get("/partials/portfolio_dashboard", response_class=HTMLResponse)
def partial_portfolio_dashboard(connection_id: int = 0):
    """
    Preferred path: use brokers.registry so all brokers can render a normalized dashboard.
    """
    try:
        html = get_portfolio_dashboard_html(db_path=str(DB_PATH), connection_id=int(connection_id or 0))
        if html:
            return HTMLResponse(html)
    except Exception as e:
        # If registry exists but errors, show a useful message.
        return HTMLResponse(f"<div class='small'>Error loading portfolio: {e}</div>", status_code=500)

    return HTMLResponse("<div class='small'>Portfolio dashboard not available (registry not installed).</div>")


@app.get("/partials/network_usage", response_class=HTMLResponse)
def partial_network_usage():
    try:
        snap = _get_network_usage_snapshot()
    except Exception as e:
        return HTMLResponse(
            "<div class='small'>Network stats unavailable.</div>"
            f"<div class='small'>{html.escape(str(e))}</div>",
            status_code=500,
        )
    return _render_network_usage_html(snap)


@app.post("/partials/network_usage/reset", response_class=HTMLResponse)
def partial_network_usage_reset():
    try:
        snap = _reset_network_usage_baseline()
    except Exception as e:
        return HTMLResponse(
            "<div class='small'>Network stats reset failed.</div>"
            f"<div class='small'>{html.escape(str(e))}</div>",
            status_code=500,
        )
    return _render_network_usage_html(snap)


# =========================
# Main
# =========================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--init-db", action="store_true")
    ap.add_argument("--setup-new-user", action="store_true", help="Run the fresh-machine setup helper and exit")
    ap.add_argument("--storage-report", action="store_true", help="Print a JSON storage cleanup report and exit")
    ap.add_argument("--cleanup-data", action="store_true", help="Plan storage cleanup and exit; dry-run unless --cleanup-apply is set")
    ap.add_argument("--cleanup-apply", action="store_true", help="Actually remove cleanup candidates planned by --cleanup-data")
    ap.add_argument("--cleanup-run-retention-days", type=int, default=CLEANUP_RUN_RETENTION_DAYS)
    ap.add_argument("--cleanup-keep-per-algo", type=int, default=CLEANUP_KEEP_PER_ALGORITHM)
    ap.add_argument("--cleanup-assistant-news-days", type=int, default=CLEANUP_ASSISTANT_NEWS_RETENTION_DAYS)
    ap.add_argument("--cleanup-log-max-bytes", type=int, default=LOG_MAX_BYTES)
    ap.add_argument("--cleanup-log-keep-bytes", type=int, default=LOG_TRIM_KEEP_BYTES)
    ap.add_argument("--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)")
    ap.add_argument("--port", type=int, default=8000, help="Bind port (default: 8000)")
    ap.add_argument("--http", action="store_true", help="Force HTTP even if CERT_FILE/KEY_FILE are set")
    ap.add_argument("--workers", type=int, default=_env_int("CRYPTID_SERVER_WORKERS", 1, minimum=1), help="Server workers (default: 1)")
    ap.add_argument("--reload", action="store_true", default=_env_flag("CRYPTID_SERVER_RELOAD", False), help="Enable development reload explicitly")
    ap.add_argument("--reload-dir", action="append", default=None, help="Directory to watch when --reload is enabled")
    ap.add_argument("--reload-exclude", action="append", default=None, help="Additional reload exclude pattern")
    args = ap.parse_args()

    if args.cleanup_apply and not args.cleanup_data:
        ap.error("--cleanup-apply requires --cleanup-data")

    if args.setup_new_user:
        setup_script = APP_ROOT.parent / "setup_new_user.py"
        if not setup_script.exists():
            print(f"[cryptid_exchange] setup helper not found: {setup_script}")
            return
        rc = subprocess.call([sys.executable, str(setup_script)], cwd=str(APP_ROOT.parent))
        if rc:
            raise SystemExit(rc)
        return

    if args.init_db:
        init_db()
        discover_base_scripts()
        print("DB initialized at", DB_PATH)
        return

    if args.storage_report or args.cleanup_data:
        init_db()
        payload = run_storage_cleanup(
            apply=bool(args.cleanup_data and args.cleanup_apply),
            run_retention_days=int(args.cleanup_run_retention_days),
            keep_per_algorithm=int(args.cleanup_keep_per_algo),
            assistant_news_retention_days=int(args.cleanup_assistant_news_days),
            log_max_bytes=int(args.cleanup_log_max_bytes),
            log_keep_bytes=int(args.cleanup_log_keep_bytes),
        )
        print(json.dumps(payload, indent=2))
        return

    init_db()
    discover_base_scripts()

    import uvicorn

    cert = str(os.getenv("CERT_FILE") or "").strip()
    key = str(os.getenv("KEY_FILE") or "").strip()

    workers = max(1, int(args.workers or 1))
    reload_enabled = bool(args.reload)
    if reload_enabled and workers != 1:
        print("[cryptid_exchange] NOTE: --reload uses one worker; ignoring --workers > 1 for this launch")
        workers = 1

    os.environ["CRYPTID_SERVER_WORKERS"] = str(workers)
    os.environ["CRYPTID_SERVER_RELOAD"] = "1" if reload_enabled else "0"
    os.environ.setdefault("CRYPTID_PROCESS_ROLE", "server")

    reload_excludes = [
        ".git/*",
        ".idea/*",
        ".venv/*",
        "venv/*",
        "__pycache__/*",
        "*.py[cod]",
        "app/data/*",
        "app/data/**/*",
        "*.sqlite3",
        "*.log",
    ]
    reload_excludes.extend(args.reload_exclude or [])

    uvicorn_kwargs: dict[str, Any] = {
        "host": args.host,
        "port": args.port,
        "reload": reload_enabled,
        "workers": workers,
    }
    if reload_enabled:
        uvicorn_kwargs["reload_dirs"] = args.reload_dir or [str(APP_ROOT)]
        uvicorn_kwargs["reload_excludes"] = reload_excludes

    cert_exists = bool(cert and Path(cert).expanduser().exists())
    key_exists = bool(key and Path(key).expanduser().exists())
    use_https = (not args.http) and bool(cert and key and cert_exists and key_exists)
    if use_https:
        uvicorn_kwargs["ssl_certfile"] = cert
        uvicorn_kwargs["ssl_keyfile"] = key
        print(f"[cryptid_exchange] HTTPS enabled on https://{args.host}:{args.port}")
    else:
        print(f"[cryptid_exchange] HTTP enabled on  http://{args.host}:{args.port}")
        if not args.http and (cert or key):
            if not (cert and key):
                print("[cryptid_exchange] NOTE: CERT_FILE/KEY_FILE not both set; falling back to HTTP")
            elif not cert_exists or not key_exists:
                print("[cryptid_exchange] NOTE: CERT_FILE/KEY_FILE path missing on this machine; falling back to HTTP")

    uvicorn_target: Any = "app.main:app" if reload_enabled or workers != 1 else app
    uvicorn.run(uvicorn_target, **uvicorn_kwargs)


if __name__ == "__main__":
    main()
