#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
VENV_DIR=${VENV_DIR:-"$ROOT/.venv"}
PYTHON_BIN=${PYTHON_BIN:-"$VENV_DIR/bin/python"}
HOST=${CRYPTID_HOST:-0.0.0.0}
PORT=${CRYPTID_PORT:-8000}
WORKERS=${CRYPTID_SERVER_WORKERS:-1}
RELOAD=${CRYPTID_RELOAD:-0}

if [ ! -x "$PYTHON_BIN" ]; then
    echo "[run_app] Python executable not found: $PYTHON_BIN" >&2
    echo "[run_app] Run ./install.sh first." >&2
    exit 1
fi

cd "$ROOT"
if [ "$RELOAD" = "1" ] || [ "$RELOAD" = "true" ] || [ "$RELOAD" = "yes" ]; then
    export CRYPTID_SERVER_WORKERS=1
    export CRYPTID_SERVER_RELOAD=1
    exec "$PYTHON_BIN" -m uvicorn app.main:app \
        --host "$HOST" \
        --port "$PORT" \
        --workers 1 \
        --reload \
        --reload-dir "$ROOT/app" \
        --reload-exclude "$ROOT/app/data/*" \
        --reload-exclude "$ROOT/app/data/**/*" \
        --reload-exclude "$ROOT/.git/*" \
        --reload-exclude "$ROOT/.idea/*" \
        --reload-exclude "$ROOT/.venv/*" \
        --reload-exclude "$ROOT/venv/*" \
        --reload-exclude "*/__pycache__/*" \
        --reload-exclude "*.sqlite3" \
        --reload-exclude "*.log"
fi

export CRYPTID_SERVER_WORKERS="$WORKERS"
export CRYPTID_SERVER_RELOAD=0
exec "$PYTHON_BIN" -m app.main --http --host "$HOST" --port "$PORT" --workers "$WORKERS"
