# 🦊 Cryptid Exchange

**Cryptid Exchange** is a local-first algorithmic trading orchestration platform designed to:

- Run configurable trading algorithms
- Manage broker connections (multi-broker support)
- Display live portfolio dashboards
- Monitor active trading runs
- Serve as a modular foundation for AI-assisted trading systems

This project is built with:

- **FastAPI** (backend)
- **Jinja2 + HTMX** (frontend)
- **SQLite** (local state)
- **Schwab REST API**
- **Robinhood via robin_stocks**

---

# Use Restrictions

This software is provided for personal use by individual users only.

Companies, corporations, partnerships, funds, institutions, and other business or commercial entities are prohibited from using, copying, modifying, distributing, hosting, deploying, or operating this software.

This software may not be used by any individual who owns, controls, or beneficially owns more than 10% of any publicly traded company.

No commercial, institutional, corporate, or business use is permitted without prior written permission from the repository owner.

---

# 🏗 Architecture Overview

The system is organized into four core domains:

## 1️⃣ Algorithms
- Base trading scripts live in `app/scripts/`
- Users create configurable algorithm instances
- Algorithms are launched as subprocesses
- Each run has:
  - Its own run directory
  - Logs
  - Optional status JSON

## 2️⃣ Runs
- Stored in the `runs` table
- Tracks:
  - PID
  - Status
  - Parameters
  - Start/End timestamps
- Logs are streamed into `algo.log`

## 3️⃣ Broker Layer (Multi-Broker Support)

The broker system supports multiple brokerage connections simultaneously.

### Current Supported Brokers

| Broker      | Connection Type | Portfolio Support |
|-------------|----------------|------------------|
| Schwab      | OAuth2        | ✅ Balances + Positions |
| Robinhood   | Credential-based (robin_stocks) | ✅ Balances + Positions |

Broker connections are stored in:

```
broker_connections
```

Each connection contains:
- broker type (`schwab`, `robinhood`)
- label
- status
- metadata
- encrypted secrets

---

# 🔌 Broker Integration Details

## Schwab

Uses OAuth2:

1. `/broker/connect`
2. Redirect to Schwab
3. Callback exchanges auth code for token
4. Token saved locally

Endpoints used:
- Account mapping
- Account balances
- Positions

Automatic token refresh is handled before portfolio calls.

---

## Robinhood

Uses `robin_stocks` library.

Login flow:
- Username
- Password
- Optional MFA code

After login:
- Account profile retrieved
- Holdings retrieved
- Portfolio snapshot normalized

⚠️ Robinhood may require additional challenge flows depending on account security settings.

---

# 📊 Portfolio Dashboard

Available at:

```
/broker
```

Features:

- View all connected broker accounts
- Add / remove broker connections
- Portfolio summary shown globally in header
- Per-account:
  - Net liquidation value
  - Cash
  - Buying power
  - Positions table

Portfolio summary is refreshed automatically using HTMX.

---

# 🚀 Running the Application

## 1️⃣ Install Dependencies

```bash
pip install -r requirements.txt
```

Minimum requirements:

```bash
fastapi
uvicorn
jinja2
httpx
python-dotenv
robin_stocks
```

---

## 2️⃣ Optional Environment Variables

```bash
cp .env.example .env
```

Robinhood-only installs do not need Schwab environment variables. Configure Robinhood from `/broker`.

Schwab users should prefer the Schwab settings form on `/broker`. `SCHWAB_*` environment variables are still supported as machine-local overrides.

Optional for HTTPS:

```bash
export CERT_FILE=path/to/cert.pem
export KEY_FILE=path/to/key.pem
```

HTTPS is optional. If `CERT_FILE`/`KEY_FILE` are blank or point to files that do not exist on the current machine, the server falls back to HTTP.

## Safe Development Launch

Use one worker with reload disabled by default:

```bash
./run_app.sh
```

Equivalent direct command:

```bash
python -m app.main --http --host 127.0.0.1 --port 8000 --workers 1
```

Development reload is opt-in and excludes generated runtime data:

```bash
CRYPTID_RELOAD=1 ./run_app.sh
```

---

## 3️⃣ Initialize Database

```bash
python -m app.main --init-db
```

---

## 4️⃣ Run Server

```bash
python -m app.main
```

Default:

```
http://127.0.0.1:8000
```

---

# 🧠 Algorithm Execution Model

When an algorithm is launched:

1. Run directory created:
   ```
   app/data/runs/run_<timestamp>_<algo_id>/
   ```

2. Parameters saved to:
   ```
   params.json
   ```

3. Process spawned:
   ```bash
   python <script> --run-dir <dir> --params-json <json>
   ```

4. Logs written to:
   ```
   algo.log
   ```

Optional:
- `status.json` can be written by the script for UI heartbeat.

---

# 🔐 Security Considerations

⚠️ Current state:

- Broker secrets are stored locally and encrypted when `APP_SECRET_KEY` or `CRYPTID_SECRET_KEY` is set.
- Robinhood credentials are used for login but the password is not stored by the app.
- Robinhood session pickle files are machine/user-specific and must not be copied to another user.
- Local runtime databases and logs can contain broker metadata, run history, and machine paths.

Recommended next steps:

- Restrict server access to localhost only.
- Add user authentication layer if exposed beyond local network.

This project is currently intended for:

> Local development and controlled environments.

---

# 🧩 Future Expansion

Planned / Supported Directions:

- Additional broker connectors
- Order history + activity panels
- Real-time streaming account updates
- AI portfolio analysis assistant
- Encrypted secret vault
- Multi-user support

---

# 📁 Project Structure

```
app/
 ├── brokers/
 │    ├── registry.py
 │    ├── schwab_connector.py
 │    └── robinhood_connector.py
 │
 ├── scripts/
 │    └── FoxBalance.py
 │
 ├── templates/
 │    ├── layout.html
 │    ├── broker.html
 │    └── ...
 │
 ├── static/
 │    └── styles.css
 │
 ├── data/
 │    ├── cryptid_exchange.sqlite3
 │    └── runs/
 │
 └── main.py
```

---

# ⚡ Design Philosophy

Cryptid Exchange is:

- Modular
- Local-first
- Broker-agnostic
- Algorithm-centric
- Built for expansion

It is designed to evolve into a structured trading command center capable of integrating advanced strategy engines and AI-driven analysis layers.

---

# Clone Setup Notes

Schwab is optional. The server can boot and run Robinhood workflows without any Schwab API key or token.

Recommended clone flow:

1. Clone the repository:

```bash
git clone https://github.com/DreadFoxKaito/DreadFoxTrader.1.0.git
cd DreadFoxTrader.1.0
```

2. Run the installer for your platform.

Linux / Raspberry Pi:

```bash
./install.sh
```

Windows:

```powershell
.\install_windows.bat
```

The installer creates `.venv/`, installs `requirements.txt`, generates local `.env`
settings, and initializes the database. The Linux installer also installs supported
system packages when possible.

Start the app after installation:

Linux / Raspberry Pi:

```bash
./run_app.sh
```

Windows:

```powershell
.\run_windows.bat
```

Optional installer modes:

```bash
./install.sh --start
./install.sh --systemd-user
./install.sh --clean-runtime
./install.sh --with-sound
./install.sh --with-ai
./install.sh --with-strategy
```

Windows uses PowerShell-style switches:

```powershell
.\install_windows.bat -Start
.\install_windows.bat -CleanRuntime
.\install_windows.bat -WithSound
.\install_windows.bat -WithAi
.\install_windows.bat -WithStrategy
```

On rpm-ostree systems such as Bazzite, install host packages with `rpm-ostree`, reboot, then rerun:

```bash
./install.sh --no-system-packages
```

For Windows-specific setup details, see `WINDOWS_INSTALL.md`.

Manual setup is still supported:

```bash
python3 -m venv .venv
. .venv/bin/activate
python setup_new_user.py --install-deps --init-db
```

If the normal app entrypoint is already importable, this shortcut also runs the safe setup checks:

```bash
python -m app.main --setup-new-user
```

3. Leave all `SCHWAB_*` values blank for Robinhood-only installs.
4. Do not copy `app/data/`, `.env`, `.venv/`, `.idea/`, run logs, or broker session pickle files from another machine/user.
5. If a copied folder contains old runtime state for another user, run:

```bash
./install.sh --clean-runtime
```

6. Start the server with HTTP if no local TLS certs are configured:

```bash
./run_app.sh
```

7. Open `/broker` and connect Robinhood using the Robinhood form.
8. Configure Schwab from `/broker` only on machines that will use Schwab.

Generate a new secret with:

```bash
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

Legacy Schwab token import is disabled by default. To import an existing `app/data/schwab_token.json` into broker connections, set:

```bash
CRYPTID_IMPORT_LEGACY_SCHWAB_TOKEN=1
```
