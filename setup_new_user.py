#!/usr/bin/env python3
"""
Fresh-machine setup helper for Cryptid Exchange.

Safe default behavior:
  - creates/updates .env with a machine-local APP_SECRET_KEY
  - creates runtime directories
  - reports dependency/database status

Optional behavior:
  - --install-deps installs requirements.txt with the current Python
  - --init-db initializes the app database through app.main
  - --clean-runtime removes copied user-specific runtime files, only with --yes
"""

from __future__ import annotations

import argparse
import os
import secrets
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
ENV_PATH = ROOT / ".env"
ENV_EXAMPLE_PATH = ROOT / ".env.example"
DATA_DIR = ROOT / "app" / "data"
RUNS_DIR = DATA_DIR / "runs"
ROBINHOOD_SESSIONS_DIR = DATA_DIR / "robinhood_sessions"
ASSISTANT_MEMORY_DIR = DATA_DIR / "assistant_memory"
ASSISTANT_NEWS_RUNS_DIR = DATA_DIR / "assistant_news_runs"
STRATEGY_FORGE_DATA_DIR = DATA_DIR / "strategy_forge"
REQUIREMENTS_PATH = ROOT / "requirements.txt"


DEPENDENCY_IMPORTS = {
    "fastapi": "fastapi",
    "uvicorn": "uvicorn",
    "jinja2": "jinja2",
    "python-multipart": "multipart",
    "python-dotenv": "dotenv",
    "httpx": "httpx",
    "robin-stocks": "robin_stocks",
    "requests": "requests",
    "sentence-transformers": "sentence_transformers",
    "numpy": "numpy",
    "cryptography": "cryptography",
}


RUNTIME_PATHS_TO_CLEAN = [
    DATA_DIR / "cryptid_exchange.sqlite3",
    DATA_DIR / "cryptid_exchange.sqlite3-shm",
    DATA_DIR / "cryptid_exchange.sqlite3-wal",
    DATA_DIR / "schwab_token.json",
    DATA_DIR / "trader.db",
    DATA_DIR / "test_vector_store.sqlite3",
    DATA_DIR / "app_server.log",
    DATA_DIR / "assistant_loop.json",
    DATA_DIR / "assistant_loop_config.json",
    DATA_DIR / "assistant_monitors_config.json",
    DATA_DIR / "assistant_openai_config.json",
    DATA_DIR / "_cleanup_archive",
    ROOT / "data.db",
    RUNS_DIR,
    ROBINHOOD_SESSIONS_DIR,
    ASSISTANT_MEMORY_DIR,
    ASSISTANT_NEWS_RUNS_DIR,
    STRATEGY_FORGE_DATA_DIR,
]


def _print(msg: str) -> None:
    print(f"[setup_new_user] {msg}")


def _run(cmd: list[str]) -> int:
    _print("running: " + " ".join(cmd))
    return subprocess.call(cmd, cwd=str(ROOT))


def _read_env_lines() -> list[str]:
    if ENV_PATH.exists():
        return ENV_PATH.read_text(encoding="utf-8").splitlines()
    if ENV_EXAMPLE_PATH.exists():
        return ENV_EXAMPLE_PATH.read_text(encoding="utf-8").splitlines()
    return []


def _write_env_lines(lines: list[str]) -> None:
    text = "\n".join(lines).rstrip() + "\n"
    ENV_PATH.write_text(text, encoding="utf-8")


def ensure_env() -> None:
    lines = _read_env_lines()
    if not ENV_PATH.exists():
        _print("created .env from .env.example" if ENV_EXAMPLE_PATH.exists() else "created .env")

    found_secret = False
    changed = False
    new_secret = secrets.token_urlsafe(48)
    updated: list[str] = []

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("APP_SECRET_KEY=") or stripped.startswith("CRYPTID_SECRET_KEY="):
            found_secret = True
            key, _, value = line.partition("=")
            if key == "APP_SECRET_KEY" and not value.strip():
                updated.append(f"APP_SECRET_KEY={new_secret}")
                changed = True
            else:
                updated.append(line)
        else:
            updated.append(line)

    if not found_secret:
        if updated and updated[-1].strip():
            updated.append("")
        updated.extend(
            [
                "# Required for encrypted local broker secret storage.",
                f"APP_SECRET_KEY={new_secret}",
            ]
        )
        changed = True

    if changed or not ENV_PATH.exists():
        _write_env_lines(updated)
        _print("ensured .env has a machine-local APP_SECRET_KEY")
    else:
        _print(".env already has APP_SECRET_KEY/CRYPTID_SECRET_KEY")


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    ROBINHOOD_SESSIONS_DIR.mkdir(parents=True, exist_ok=True)
    _print("ensured app/data runtime directories exist")


def clean_runtime(*, yes: bool) -> None:
    if not yes:
        _print("refusing runtime cleanup without --yes")
        _print("rerun with: python setup_new_user.py --clean-runtime --yes")
        return
    for path in RUNTIME_PATHS_TO_CLEAN:
        if path.is_dir():
            shutil.rmtree(path)
            _print(f"removed directory {path.relative_to(ROOT)}")
        elif path.exists():
            path.unlink()
            _print(f"removed file {path.relative_to(ROOT)}")
    ensure_dirs()


def install_deps() -> int:
    if not REQUIREMENTS_PATH.exists():
        _print("requirements.txt not found")
        return 1
    return _run([sys.executable, "-m", "pip", "install", "-r", str(REQUIREMENTS_PATH)])


def init_db() -> int:
    return _run([sys.executable, "-m", "app.main", "--init-db"])


def check_deps() -> bool:
    missing: list[str] = []
    for package, import_name in DEPENDENCY_IMPORTS.items():
        try:
            __import__(import_name)
        except Exception:
            missing.append(package)
    if missing:
        _print("missing Python dependencies: " + ", ".join(missing))
        _print("fix with: python setup_new_user.py --install-deps")
        return False
    _print("Python dependency import check passed")
    return True


def check_runtime_state() -> None:
    copied_state = [p for p in RUNTIME_PATHS_TO_CLEAN if p.exists()]
    if copied_state:
        _print("runtime/user-state files currently exist:")
        for path in copied_state:
            _print(f"  - {path.relative_to(ROOT)}")
        _print("for a different Robinhood user, clean these with --clean-runtime --yes before linking accounts")
    else:
        _print("no copied runtime/user-state files detected")


def check_env_state() -> None:
    if not ENV_PATH.exists():
        _print(".env is missing")
        _print("fix with: python setup_new_user.py")
        return
    secret_present = False
    for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("APP_SECRET_KEY=") or stripped.startswith("CRYPTID_SECRET_KEY="):
            _, _, value = stripped.partition("=")
            if value.strip():
                secret_present = True
                break
    if secret_present:
        _print(".env exists and has APP_SECRET_KEY/CRYPTID_SECRET_KEY")
    else:
        _print(".env exists but is missing APP_SECRET_KEY/CRYPTID_SECRET_KEY")
        _print("fix with: python setup_new_user.py")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Set up a fresh Cryptid Exchange copy for a new local user.")
    parser.add_argument("--install-deps", action="store_true", help="Install requirements.txt with the current Python.")
    parser.add_argument("--init-db", action="store_true", help="Initialize the app database after setup.")
    parser.add_argument("--clean-runtime", action="store_true", help="Remove copied app/data state, run logs, DBs, and Robinhood session pickles.")
    parser.add_argument("--yes", action="store_true", help="Confirm destructive actions such as --clean-runtime.")
    parser.add_argument("--check-only", action="store_true", help="Only report setup status; do not change files.")
    return parser.parse_args()


def main() -> int:
    os.chdir(ROOT)
    args = parse_args()

    _print(f"project root: {ROOT}")

    if args.check_only:
        check_deps()
        check_runtime_state()
        check_env_state()
        return 0

    if args.clean_runtime:
        clean_runtime(yes=bool(args.yes))

    ensure_env()
    ensure_dirs()

    if args.install_deps:
        rc = install_deps()
        if rc != 0:
            return rc

    deps_ok = check_deps()
    check_runtime_state()

    if args.init_db:
        rc = init_db()
        if rc != 0:
            return rc

    if deps_ok:
        _print("setup complete")
        _print("start with: python -m app.main --http")
    else:
        _print("setup incomplete until dependencies are installed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
