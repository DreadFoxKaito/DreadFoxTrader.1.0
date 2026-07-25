# Running DreadFox Trader On A Raspberry Pi 4B

This app does not need PyCharm. Run it from its own project directory with Python.

## Raspberry Pi Setup

Use 64-bit Raspberry Pi OS if possible.

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip build-essential libffi-dev libssl-dev
```

Unpack the clean project archive, then enter the project directory:

```bash
mkdir -p ~/apps
tar -xzf DreadFoxTrader.1.0-clean-20260725.tar.gz -C ~/apps
cd ~/apps/DreadFoxTrader.1.0
```

Create a local virtual environment inside the project:

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
python -m pip install -r requirements.txt
```

Initialize the local config and database:

```bash
python setup_new_user.py --init-db
```

Start the app:

```bash
python -m app.main --http --host 0.0.0.0 --port 8000
```

Open it on the Pi at:

```text
http://127.0.0.1:8000
```

Open it from another device on the same network at:

```text
http://<raspberry-pi-ip>:8000
```

Find the Pi IP with:

```bash
hostname -I
```

## Optional AI Packages

The default install uses `requirements.txt`. Avoid `requirements-ai.txt` on a Raspberry Pi unless you specifically need local semantic embeddings, because it pulls in a larger ML stack.

## Notes

- Keep `.env` private. Each new user should run `python setup_new_user.py`.
- Do not share `app/data/`, `.env`, `.venv/`, `.idea/`, logs, databases, or broker session files.
- For Schwab, each user should enter their own app credentials.
- For Robinhood, each user should connect from the `/broker` page.
