#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
VENV_DIR=${VENV_DIR:-"$ROOT/.venv"}
PYTHON_BIN=${PYTHON_BIN:-python3}
HOST=${CRYPTID_HOST:-0.0.0.0}
PORT=${CRYPTID_PORT:-8000}

INSTALL_SYSTEM_PACKAGES=1
INIT_DB=1
CLEAN_RUNTIME=0
WITH_AI=0
WITH_SOUND=0
WITH_STRATEGY=0
INSTALL_SYSTEMD_USER=0
START_AFTER_INSTALL=0

usage() {
    cat <<'EOF'
Usage: ./install.sh [options]

Installs DreadFox Trader into a local Python virtual environment.

Options:
  --host HOST              App bind host for run_app.sh/systemd output (default: 0.0.0.0)
  --port PORT              App port for run_app.sh/systemd output (default: 8000)
  --no-system-packages     Skip OS package installation.
  --no-init-db             Skip database initialization.
  --clean-runtime          Remove copied runtime/user state before setup.
  --with-ai                Install optional requirements-ai.txt.
  --with-sound             Install optional requirements-sound.txt.
  --with-strategy          Install optional requirements-strategy.txt.
  --systemd-user           Create and enable a user systemd service.
  --start                  Start the app after installation.
  -h, --help               Show this help.

Environment:
  VENV_DIR                 Override virtualenv path (default: ./.venv)
  PYTHON_BIN               Override Python executable (default: python3)
  CRYPTID_HOST             Default app host.
  CRYPTID_PORT             Default app port.
EOF
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --host)
            HOST=${2:?--host requires a value}
            shift 2
            ;;
        --port)
            PORT=${2:?--port requires a value}
            shift 2
            ;;
        --no-system-packages)
            INSTALL_SYSTEM_PACKAGES=0
            shift
            ;;
        --no-init-db)
            INIT_DB=0
            shift
            ;;
        --clean-runtime)
            CLEAN_RUNTIME=1
            shift
            ;;
        --with-ai)
            WITH_AI=1
            shift
            ;;
        --with-sound)
            WITH_SOUND=1
            shift
            ;;
        --with-strategy)
            WITH_STRATEGY=1
            shift
            ;;
        --systemd-user)
            INSTALL_SYSTEMD_USER=1
            shift
            ;;
        --start)
            START_AFTER_INSTALL=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

log() {
    printf '[install] %s\n' "$*"
}

sudo_cmd() {
    if [ "$(id -u)" -eq 0 ]; then
        "$@"
    else
        sudo "$@"
    fi
}

have_cmd() {
    command -v "$1" >/dev/null 2>&1
}

python_has_venv() {
    "$PYTHON_BIN" -m venv --help >/dev/null 2>&1
}

install_system_packages() {
    [ "$INSTALL_SYSTEM_PACKAGES" -eq 1 ] || return 0

    if have_cmd apt-get; then
        log "Installing system packages with apt."
        sudo_cmd apt-get update
        sudo_cmd apt-get install -y \
            git \
            python3 \
            python3-venv \
            python3-pip \
            build-essential \
            libffi-dev \
            libssl-dev
        return 0
    fi

    if have_cmd dnf && [ ! -d /run/ostree-booted ]; then
        log "Installing system packages with dnf."
        sudo_cmd dnf install -y \
            git \
            python3 \
            python3-pip \
            python3-devel \
            gcc \
            gcc-c++ \
            make \
            libffi-devel \
            openssl-devel
        return 0
    fi

    if have_cmd pacman; then
        log "Installing system packages with pacman."
        sudo_cmd pacman -Sy --needed --noconfirm \
            git \
            python \
            python-pip \
            base-devel \
            libffi \
            openssl
        return 0
    fi

    if have_cmd rpm-ostree; then
        if have_cmd "$PYTHON_BIN" && python_has_venv; then
            log "rpm-ostree system detected; Python venv support is already available."
            return 0
        fi
        cat >&2 <<'EOF'
[install] rpm-ostree/Bazzite-style system detected.
[install] Install system packages on the host, reboot, then rerun:

  sudo rpm-ostree install git python3 python3-pip python3-devel gcc gcc-c++ make libffi-devel openssl-devel
  systemctl reboot
  ./install.sh --no-system-packages
EOF
        exit 1
    fi

    log "No supported OS package manager found. Continuing without OS package installation."
}

install_python_packages() {
    if ! have_cmd "$PYTHON_BIN"; then
        echo "[install] $PYTHON_BIN not found. Install Python 3 first." >&2
        exit 1
    fi

    if ! python_has_venv; then
        echo "[install] Python venv support is missing. Install python3-venv or equivalent." >&2
        exit 1
    fi

    if [ ! -x "$VENV_DIR/bin/python" ]; then
        log "Creating virtual environment at $VENV_DIR."
        "$PYTHON_BIN" -m venv "$VENV_DIR"
    else
        log "Using existing virtual environment at $VENV_DIR."
    fi

    "$VENV_DIR/bin/python" -m pip install --upgrade pip setuptools wheel
    "$VENV_DIR/bin/python" -m pip install -r "$ROOT/requirements.txt"

    [ "$WITH_AI" -eq 0 ] || "$VENV_DIR/bin/python" -m pip install -r "$ROOT/requirements-ai.txt"
    [ "$WITH_SOUND" -eq 0 ] || "$VENV_DIR/bin/python" -m pip install -r "$ROOT/requirements-sound.txt"
    [ "$WITH_STRATEGY" -eq 0 ] || "$VENV_DIR/bin/python" -m pip install -r "$ROOT/requirements-strategy.txt"
}

initialize_project() {
    if [ "$CLEAN_RUNTIME" -eq 1 ]; then
        log "Cleaning copied runtime/user state."
        "$VENV_DIR/bin/python" "$ROOT/setup_new_user.py" --clean-runtime --yes
    fi

    if [ "$INIT_DB" -eq 1 ]; then
        log "Initializing local config and database."
        "$VENV_DIR/bin/python" "$ROOT/setup_new_user.py" --init-db
    else
        log "Ensuring local config and runtime directories."
        "$VENV_DIR/bin/python" "$ROOT/setup_new_user.py"
    fi
}

systemd_escape_path() {
    printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/ /\\x20/g' -e 's/%/%%/g'
}

install_systemd_user_service() {
    [ "$INSTALL_SYSTEMD_USER" -eq 1 ] || return 0

    if ! have_cmd systemctl; then
        echo "[install] systemctl not found; cannot install user service." >&2
        exit 1
    fi

    service_dir="$HOME/.config/systemd/user"
    service_path="$service_dir/dreadfox-trader.service"
    root_escaped=$(systemd_escape_path "$ROOT")
    runner_escaped=$(systemd_escape_path "$ROOT/run_app.sh")

    mkdir -p "$service_dir"
    cat > "$service_path" <<EOF
[Unit]
Description=DreadFox Trader local app
After=network-online.target

[Service]
Type=simple
WorkingDirectory=$root_escaped
Environment=CRYPTID_HOST=$HOST
Environment=CRYPTID_PORT=$PORT
ExecStart=$runner_escaped
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
EOF

    systemctl --user daemon-reload
    systemctl --user enable dreadfox-trader.service
    log "Installed user service at $service_path."
    log "Start it with: systemctl --user start dreadfox-trader.service"
    log "For auto-start before login, run: loginctl enable-linger \"$USER\""
}

start_app() {
    [ "$START_AFTER_INSTALL" -eq 1 ] || return 0

    if [ "$INSTALL_SYSTEMD_USER" -eq 1 ]; then
        log "Starting user service."
        systemctl --user start dreadfox-trader.service
    else
        log "Starting app in the foreground."
        CRYPTID_HOST="$HOST" CRYPTID_PORT="$PORT" "$ROOT/run_app.sh"
    fi
}

cd "$ROOT"
install_system_packages
install_python_packages
initialize_project
install_systemd_user_service

cat <<EOF

[install] Installation complete.
[install] Start command:

  cd "$ROOT"
  ./run_app.sh

[install] Local URL: http://127.0.0.1:$PORT
[install] LAN URL:   http://<this-device-ip>:$PORT

EOF

start_app
