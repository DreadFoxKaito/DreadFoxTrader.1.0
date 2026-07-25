#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
VENV_DIR=${VENV_DIR:-"$ROOT/.venv"}
PYTHON_BIN=${PYTHON_BIN:-"$VENV_DIR/bin/python"}
HOST=${CRYPTID_HOST:-0.0.0.0}
PORT=${CRYPTID_PORT:-8000}

if [ ! -x "$PYTHON_BIN" ]; then
    echo "[run_app] Python executable not found: $PYTHON_BIN" >&2
    echo "[run_app] Run ./install.sh first." >&2
    exit 1
fi

cd "$ROOT"
exec "$PYTHON_BIN" -m app.main --http --host "$HOST" --port "$PORT"
