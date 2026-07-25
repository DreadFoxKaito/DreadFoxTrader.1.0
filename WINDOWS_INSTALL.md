# Running DreadFox Trader On Windows

Windows users can run the same app from a normal project directory. PyCharm is not required.

## Requirements

- Windows 10 or Windows 11
- Python 3.11 or newer
- Git for Windows, unless downloading the repository ZIP from GitHub
- PowerShell, included with Windows

Install Python with:

```powershell
winget install -e --id Python.Python.3.13
```

Install Git with:

```powershell
winget install -e --id Git.Git
```

Close and reopen the terminal after installing Python or Git so `python`, `py`, and `git` are on `PATH`.

## Install From GitHub

Open PowerShell:

```powershell
mkdir $HOME\apps -Force
cd $HOME\apps
git clone https://github.com/DreadFoxKaito/DreadFoxTrader.1.0.git
cd DreadFoxTrader.1.0
.\install_windows.bat
```

The installer creates `.venv`, installs `requirements.txt`, creates a machine-local `.env`, and initializes the app database.

Start the app:

```powershell
.\run_windows.bat
```

Open:

```text
http://127.0.0.1:8000
```

To open it from another device on the same network, find the Windows host IP:

```powershell
ipconfig
```

Then browse to:

```text
http://<windows-ip>:8000
```

## Optional Installer Modes

```powershell
.\install_windows.bat -Start
.\install_windows.bat -CleanRuntime
.\install_windows.bat -WithSound
.\install_windows.bat -WithAi
.\install_windows.bat -WithStrategy
.\install_windows.bat -BindHost 127.0.0.1 -Port 8000
```

Avoid `-WithAi` on low-memory systems unless local semantic embeddings are required; it installs a larger ML stack.

## Notes

- Keep `.env` private.
- Do not share `app\data`, `.env`, `.venv`, `.idea`, logs, databases, or broker session files.
- For Robinhood, connect from the `/broker` page.
- For Schwab, each user should enter their own app credentials.
