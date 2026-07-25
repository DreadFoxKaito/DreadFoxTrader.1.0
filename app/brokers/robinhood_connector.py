from __future__ import annotations

import pickle
import secrets
import time
from pathlib import Path
from typing import Any, Optional

from . import (
    BrokerAuthError,
    BrokerConnector,
    BrokerConnectorError,
    NormalizedAccount,
    NormalizedPortfolioSnapshot,
    NormalizedPosition,
    safe_float,
)

# DB + encrypted secrets helpers (Step 11)
from ..db import (
    create_broker_connection,
    get_broker_connection,
    read_connection_metadata,
    read_connection_secrets,
    set_broker_status,
    update_broker_connection,
)

# RobinStocks (robin_stocks) imports
try:
    import robin_stocks.robinhood as rh  # type: ignore
    from robin_stocks.robinhood import authentication as rh_auth  # type: ignore
    from robin_stocks.robinhood import helper as rh_helper  # type: ignore
    from robin_stocks.robinhood import urls as rh_urls  # type: ignore
except Exception as e:  # pragma: no cover
    rh = None  # type: ignore
    rh_auth = None  # type: ignore
    rh_helper = None  # type: ignore
    rh_urls = None  # type: ignore
    _IMPORT_ERR = e
else:
    _IMPORT_ERR = None


def _utc_ts() -> int:
    return int(time.time())


def _ensure_session_dir(db_path: str) -> Path:
    """
    Keep Robinhood session artifacts alongside the DB in app/data/
    """
    data_dir = Path(db_path).resolve().parent
    sess_dir = data_dir / "robinhood_sessions"
    sess_dir.mkdir(parents=True, exist_ok=True)
    return sess_dir


def _resolve_pickle_config(
    *, db_path: str, connection_id: int, secrets: Optional[dict[str, Any]] = None
) -> tuple[str, str, Path]:
    """
    Determine the robin_stocks pickle directory/name and the full file path.

    robin_stocks builds the file as: <pickle_path>/robinhood<pickle_name>.pickle
    """
    secrets = secrets or {}
    legacy_path = secrets.get("pickle_path")
    legacy_name: Optional[str] = None
    legacy_dir: Optional[str] = None

    if legacy_path:
        p = Path(str(legacy_path)).expanduser().resolve()
        legacy_dir = str(p.parent)
        name = p.name
        if name.startswith("robinhood") and name.endswith(".pickle"):
            legacy_name = name[len("robinhood") : -len(".pickle")]
        elif name.endswith(".pickle"):
            legacy_name = name[: -len(".pickle")]
        else:
            legacy_name = name

    pickle_name = secrets.get("pickle_name") or legacy_name or f"_{connection_id}"

    pickle_dir = secrets.get("pickle_dir") or legacy_dir
    if not pickle_dir:
        legacy_default = Path.home() / ".tokens"
        legacy_file = legacy_default / f"robinhood{pickle_name}.pickle"
        if legacy_file.exists():
            pickle_dir = str(legacy_default)
        else:
            pickle_dir = str(_ensure_session_dir(db_path))

    pickle_file = Path(str(pickle_dir)).expanduser().resolve() / f"robinhood{pickle_name}.pickle"
    return str(pickle_dir), str(pickle_name), pickle_file


def _generate_device_token() -> str:
    if rh_auth is not None:
        try:
            return rh_auth.generate_device_token()
        except Exception:
            pass
    raw = secrets.token_hex(16)
    return f"{raw[:8]}-{raw[8:12]}-{raw[12:16]}-{raw[16:20]}-{raw[20:32]}"


def _validate_session() -> bool:
    if rh_helper is None:
        return False
    try:
        data = rh_helper.request_get("https://api.robinhood.com/user/")
    except Exception:
        return False
    return isinstance(data, dict) and bool(data)


def _restore_session_from_pickle(
    pickle_file: Path, *, expires_in: int, scope: str, validate: bool = True
) -> Optional[dict[str, Any]]:
    if rh_helper is None or rh_urls is None:
        return None
    if not pickle_file.exists():
        return None
    try:
        with open(pickle_file, "rb") as f:
            data = pickle.load(f)
    except Exception:
        return None

    access_token = data.get("access_token")
    token_type = data.get("token_type")
    refresh_token = data.get("refresh_token")
    device_token = data.get("device_token")
    if not access_token or not token_type:
        return None

    rh_helper.set_login_state(True)
    rh_helper.update_session("Authorization", f"{token_type} {access_token}")
    if validate:
        if not _validate_session():
            rh_helper.set_login_state(False)
            rh_helper.update_session("Authorization", None)
            return None

    return {
        "access_token": access_token,
        "token_type": token_type,
        "expires_in": expires_in,
        "scope": scope,
        "detail": f"logged in using authentication in {pickle_file.name}",
        "backup_code": None,
        "refresh_token": refresh_token,
        "device_token": device_token,
    }


def _store_pickle(
    pickle_file: Path,
    *,
    token_type: str,
    access_token: str,
    refresh_token: Optional[str],
    device_token: str,
) -> None:
    try:
        pickle_file.parent.mkdir(parents=True, exist_ok=True)
        with open(pickle_file, "wb") as f:
            pickle.dump(
                {
                    "token_type": token_type,
                    "access_token": access_token,
                    "refresh_token": refresh_token,
                    "device_token": device_token,
                },
                f,
            )
    except Exception:
        pass


def _pickle_debug_info(pickle_file: Path) -> dict[str, Any]:
    info: dict[str, Any] = {
        "pickle_path": str(pickle_file),
        "pickle_exists": False,
    }
    try:
        if not pickle_file.exists():
            return info
        info["pickle_exists"] = True
        stat = pickle_file.stat()
        info["pickle_size"] = int(stat.st_size)
        info["pickle_mtime"] = int(stat.st_mtime)
        with open(pickle_file, "rb") as f:
            data = pickle.load(f)
        if isinstance(data, dict):
            info["pickle_keys"] = sorted([str(k) for k in data.keys()])
            access_token = data.get("access_token")
            refresh_token = data.get("refresh_token")
            info["access_token_len"] = len(access_token) if isinstance(access_token, str) else 0
            info["refresh_token_len"] = len(refresh_token) if isinstance(refresh_token, str) else 0
        else:
            info["pickle_error"] = "pickle payload not a dict"
    except Exception as e:
        info["pickle_error"] = str(e)
    return info


def _update_debug_metadata(
    *, db_path: str, connection_id: int, metadata: Optional[dict[str, Any]], debug: dict[str, Any]
) -> None:
    merged = dict(metadata or {})
    merged["debug"] = debug
    try:
        update_broker_connection(db_path=db_path, connection_id=connection_id, metadata=merged)
    except Exception:
        pass


def _load_account_profile_safe(portfolio: Optional[dict[str, Any]] = None) -> Optional[dict[str, Any]]:
    if rh_helper is None:
        return None

    account_url = None
    if portfolio and isinstance(portfolio, dict):
        acct = portfolio.get("account")
        if isinstance(acct, str) and acct:
            account_url = acct

    if account_url:
        data = rh_helper.request_get(account_url)
        if isinstance(data, dict):
            return data

    data = rh_helper.request_get("https://api.robinhood.com/accounts/", "indexzero")
    if isinstance(data, dict):
        return data
    return None


def _await_verification_workflow(
    *,
    device_token: str,
    workflow_id: str,
    timeout_s: int,
    poll_s: int,
    mfa_code: Optional[str],
) -> None:
    if rh_helper is None:
        raise BrokerAuthError("Robinhood verification failed: helper not available.", status_code=401)

    pathfinder_url = "https://api.robinhood.com/pathfinder/user_machine/"
    machine_payload = {"device_id": device_token, "flow": "suv", "input": {"workflow_id": workflow_id}}
    machine_data = rh_helper.request_post(url=pathfinder_url, payload=machine_payload, json=True)
    machine_id = machine_data.get("id") if isinstance(machine_data, dict) else None
    if not machine_id:
        raise BrokerAuthError("Robinhood verification failed to start. Retry login.", status_code=401)

    inquiries_url = f"https://api.robinhood.com/pathfinder/inquiries/{machine_id}/user_view/"
    start_time = time.time()

    while time.time() - start_time < timeout_s:
        time.sleep(poll_s)
        inquiries_response = rh_helper.request_get(inquiries_url)
        if not inquiries_response:
            continue

        challenge = (inquiries_response.get("context") or {}).get("sheriff_challenge")
        if challenge:
            challenge_type = challenge.get("type")
            challenge_status = challenge.get("status")
            challenge_id = challenge.get("id")

            if challenge_type == "prompt" and challenge_id:
                prompt_url = f"https://api.robinhood.com/push/{challenge_id}/get_prompts_status/"
                while time.time() - start_time < timeout_s:
                    time.sleep(poll_s)
                    prompt_status = rh_helper.request_get(prompt_url)
                    if isinstance(prompt_status, dict) and prompt_status.get("challenge_status") == "validated":
                        break
                break

            if challenge_type in ("sms", "email") and challenge_status == "issued" and challenge_id:
                if not mfa_code:
                    raise BrokerAuthError(
                        "Robinhood verification code required. Enter the code and retry.",
                        status_code=401,
                    )
                challenge_url = f"https://api.robinhood.com/challenge/{challenge_id}/respond/"
                rh_helper.request_post(url=challenge_url, payload={"response": str(mfa_code)})
                break

            if challenge_status == "validated":
                break

        workflow_status = (inquiries_response.get("verification_workflow") or {}).get("workflow_status")
        if workflow_status == "workflow_status_approved":
            return

    inquiries_payload = {"sequence": 0, "user_input": {"status": "continue"}}
    inquiries_response = rh_helper.request_post(url=inquiries_url, payload=inquiries_payload, json=True)
    result = (inquiries_response or {}).get("type_context", {}).get("result")
    if result != "workflow_status_approved":
        raise BrokerAuthError("Robinhood verification timed out. Approve in the app and retry.", status_code=401)


def _login_with_robin_stocks(
    *,
    username: str,
    password: str,
    mfa_code: Optional[str],
    expires_in: int,
    scope: str,
    store_session: bool,
    pickle_file: Path,
    by_sms: bool = True,
    approval_timeout_s: int = 180,
) -> dict[str, Any]:
    if rh_helper is None or rh_urls is None:
        raise BrokerConnectorError("robin_stocks helper/urls missing.", status_code=500)

    restored = _restore_session_from_pickle(pickle_file, expires_in=expires_in, scope=scope)
    if restored:
        return restored

    device_token = _generate_device_token()
    payload = {
        "client_id": "c82SH0WZOsabOXGP2sxqcj34FxkvfnWRZBKlBjFS",
        "expires_in": expires_in,
        "grant_type": "password",
        "password": password,
        "scope": scope,
        "username": username,
        "device_token": device_token,
        "challenge_type": "sms" if by_sms else "email",
        "try_passkeys": False,
        "token_request_path": "/login",
        "create_read_only_secondary_token": True,
    }

    if mfa_code:
        payload["mfa_code"] = mfa_code

    url = rh_urls.login_url()
    data = rh_helper.request_post(url, payload)
    if not data:
        raise BrokerAuthError("Robinhood login failed: no response from API.", status_code=401)

    if isinstance(data, dict) and data.get("verification_workflow"):
        workflow_id = data["verification_workflow"].get("id")
        if not workflow_id:
            raise BrokerAuthError("Robinhood verification missing workflow id.", status_code=401)
        _await_verification_workflow(
            device_token=device_token,
            workflow_id=workflow_id,
            timeout_s=approval_timeout_s,
            poll_s=5,
            mfa_code=mfa_code,
        )
        data = rh_helper.request_post(url, payload) or {}

    if isinstance(data, dict) and data.get("mfa_required"):
        if not mfa_code:
            raise BrokerAuthError("Robinhood MFA required. Enter code and retry.", status_code=401)
        payload["mfa_code"] = mfa_code
        res = rh_helper.request_post(url, payload, jsonify_data=False)
        if res is None or not hasattr(res, "status_code"):
            raise BrokerAuthError("Robinhood MFA verification failed.", status_code=401)
        if res.status_code != 200:
            raise BrokerAuthError("Robinhood MFA code rejected.", status_code=401)
        try:
            data = res.json()
        except Exception:
            data = {}

    if isinstance(data, dict) and data.get("challenge"):
        challenge_id = data.get("challenge", {}).get("id")
        if not challenge_id:
            raise BrokerAuthError("Robinhood challenge missing id.", status_code=401)
        if not mfa_code:
            raise BrokerAuthError("Robinhood challenge code required. Enter code and retry.", status_code=401)
        challenge_url = rh_urls.challenge_url(challenge_id)
        rh_helper.request_post(challenge_url, {"response": str(mfa_code)})
        rh_helper.update_session("X-ROBINHOOD-CHALLENGE-RESPONSE-ID", challenge_id)
        data = rh_helper.request_post(url, payload) or {}

    if not isinstance(data, dict):
        raise BrokerAuthError("Robinhood login failed: unexpected response.", status_code=401)

    if "access_token" not in data:
        detail = data.get("detail") or "Robinhood login failed. Approve in the app and retry."
        raise BrokerAuthError(detail, status_code=401)

    token = f"{data['token_type']} {data['access_token']}"
    rh_helper.update_session("Authorization", token)
    rh_helper.set_login_state(True)
    data["detail"] = "logged in with brand new authentication code."

    if store_session:
        _store_pickle(
            pickle_file,
            token_type=data.get("token_type", ""),
            access_token=data.get("access_token", ""),
            refresh_token=data.get("refresh_token"),
            device_token=device_token,
        )

    return data


def _to_number(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _price_from_quote(quote: dict[str, Any], *, prefer_extended: bool = True) -> Optional[float]:
    if not isinstance(quote, dict):
        return None
    last = _to_number(quote.get("last_trade_price")) or 0.0
    last_ext = _to_number(
        quote.get("last_extended_hours_trade_price") or quote.get("extended_hours_market_price")
    ) or 0.0
    if prefer_extended:
        if last_ext > 0:
            return float(last_ext)
        if last > 0:
            return float(last)
    if last > 0:
        return float(last)
    if last_ext > 0:
        return float(last_ext)
    return None


def _price_from_crypto_quote(quote: dict[str, Any]) -> Optional[float]:
    if not isinstance(quote, dict):
        return None
    for key in ("mark_price", "bid_price", "ask_price", "open_price", "high_price", "low_price"):
        v = _to_number(quote.get(key))
        if v is not None and v > 0:
            return float(v)
    return None


DASHBOARD_MIN_CRYPTO_POSITION_VALUE = 1.0


class RobinhoodConnector(BrokerConnector):
    broker_id = "robinhood"
    broker_name = "Robinhood"

    def link(self, *, db_path: str, label: str, **kwargs: Any) -> int:
        """
        Link a Robinhood account (creates broker_connections row + stores an encrypted session reference).

        Expected kwargs:
          - username: str
          - password: str
          - mfa_code: Optional[str]
          - store_session: bool (default True)
          - expires_in: int (default 86400)
          - scope: str (default "internal")
        """
        if rh is None:
            raise BrokerConnectorError(
                f"robin_stocks is not installed/importable: {_IMPORT_ERR}",
                status_code=500,
            )

        username = str(kwargs.get("username") or "").strip()
        password = str(kwargs.get("password") or "")
        mfa_code = (kwargs.get("mfa_code") or None)
        store_session = bool(kwargs.get("store_session", True))
        expires_in = int(kwargs.get("expires_in", 86400))
        scope = str(kwargs.get("scope", "internal"))
        by_sms = bool(kwargs.get("by_sms", True))
        approval_timeout_s = int(kwargs.get("approval_timeout_s", 180))

        if not username or not password:
            raise BrokerAuthError("Robinhood username/password required.", status_code=400)

        # 1) Create connection row FIRST so we can name the pickle file after the connection id.
        try:
            connection_id = create_broker_connection(
                db_path=db_path,
                broker="robinhood",
                label=label or "Robinhood",
                status="linking",
                metadata={"username": username, "storage": "pickle"},
                secrets={"pickle_dir": "", "pickle_name": "", "pickle_path": ""},  # will update after login
                allow_plaintext=True,
            )
        except Exception as e:
            raise BrokerConnectorError(
                f"Failed to create broker connection row: {e}",
                status_code=500,
            )

        pickle_dir, pickle_name, pickle_file = _resolve_pickle_config(
            db_path=db_path,
            connection_id=connection_id,
        )

        # 2) Login and store session pickle. We do NOT store the password in DB.
        try:
            login_resp = _login_with_robin_stocks(
                username=username,
                password=password,
                mfa_code=mfa_code,
                expires_in=expires_in,
                scope=scope,
                store_session=store_session,
                pickle_file=Path(pickle_file),
                by_sms=by_sms,
                approval_timeout_s=approval_timeout_s,
            )
        except Exception as e:
            debug = _pickle_debug_info(Path(pickle_file))
            debug["login_error"] = str(e)
            debug["login_ts"] = _utc_ts()
            set_broker_status(
                db_path=db_path,
                connection_id=connection_id,
                status="needs_auth",
                metadata={"username": username, "storage": "pickle", "error": str(e), "debug": debug},
            )
            # Still store the pickle path (encrypted) because it can be useful for later cleanup/debug
            try:
                update_broker_connection(
                    db_path=db_path,
                    connection_id=connection_id,
                    secrets={
                        "pickle_dir": pickle_dir,
                        "pickle_name": pickle_name,
                        "pickle_path": str(pickle_file),
                    },
                    allow_plaintext=True,
                )
            except Exception:
                pass
            raise BrokerAuthError(f"Robinhood login failed: {e}", status_code=401)

        if not login_resp:
            debug = _pickle_debug_info(Path(pickle_file))
            debug["login_error"] = "login returned falsy"
            debug["login_ts"] = _utc_ts()
            set_broker_status(
                db_path=db_path,
                connection_id=connection_id,
                status="needs_auth",
                metadata={
                    "username": username,
                    "storage": "pickle",
                    "error": "login returned falsy",
                    "debug": debug,
                },
            )
            try:
                update_broker_connection(
                    db_path=db_path,
                    connection_id=connection_id,
                    secrets={
                        "pickle_dir": pickle_dir,
                        "pickle_name": pickle_name,
                        "pickle_path": str(pickle_file),
                    },
                    allow_plaintext=True,
                )
            except Exception:
                pass
            raise BrokerAuthError("Robinhood login failed (no session created).", status_code=401)

        # 3) Mark connected + persist pickle path (encrypted)
        debug = _pickle_debug_info(Path(pickle_file))
        if isinstance(login_resp, dict):
            debug["login_detail"] = login_resp.get("detail")
        debug["login_ts"] = _utc_ts()
        update_broker_connection(
            db_path=db_path,
            connection_id=connection_id,
            status="connected",
            metadata={"username": username, "storage": "pickle", "debug": debug},
            secrets={
                "pickle_dir": pickle_dir,
                "pickle_name": pickle_name,
                "pickle_path": str(pickle_file),
            },
            allow_plaintext=True,
        )

        return connection_id

    def unlink(self, *, db_path: str, connection_id: int) -> None:
        """
        Remove a Robinhood connection and delete its stored session pickle if present.
        Note: registry.unlink_connection() will also delete the DB row.
        """
        row = get_broker_connection(db_path, connection_id)
        if not row:
            return

        secrets = read_connection_secrets(row, default={})
        _, _, pickle_file = _resolve_pickle_config(
            db_path=db_path,
            connection_id=connection_id,
            secrets=secrets,
        )
        legacy_path = secrets.get("pickle_path")
        try:
            if pickle_file.exists():
                pickle_file.unlink()
        except Exception:
            pass
        if legacy_path:
            try:
                lp = Path(str(legacy_path)).expanduser().resolve()
                if lp.exists() and lp != pickle_file:
                    lp.unlink()
            except Exception:
                pass

    def portfolio_snapshot(self, *, db_path: str, connection_id: int) -> NormalizedPortfolioSnapshot:
        if rh is None:
            raise BrokerConnectorError(
                f"robin_stocks is not installed/importable: {_IMPORT_ERR}",
                status_code=500,
            )

        row = get_broker_connection(db_path, connection_id)
        if not row:
            raise BrokerConnectorError(f"Robinhood connection not found (id {connection_id}).", status_code=404)

        label = str(row["label"] or "Robinhood")
        status = str(row["status"] or "")
        metadata = read_connection_metadata(row)
        secrets = read_connection_secrets(row, default={})

        if status not in ("connected", "needs_auth", "needs_attention", "error", "ok", ""):
            raise BrokerAuthError(f"Robinhood connection status is '{status}'. Re-link required.", status_code=401)

        pickle_dir, pickle_name, pickle_file = _resolve_pickle_config(
            db_path=db_path,
            connection_id=connection_id,
            secrets=secrets,
        )
        legacy_path = secrets.get("pickle_path")
        if legacy_path and not pickle_file.exists():
            try:
                lp = Path(str(legacy_path)).expanduser().resolve()
                if lp.exists():
                    pickle_file.parent.mkdir(parents=True, exist_ok=True)
                    pickle_file.write_bytes(lp.read_bytes())
            except Exception:
                pass
        debug = _pickle_debug_info(pickle_file)
        _update_debug_metadata(db_path=db_path, connection_id=connection_id, metadata=metadata, debug=debug)
        restored = _restore_session_from_pickle(pickle_file, expires_in=86400, scope="internal", validate=True)
        if not restored:
            set_broker_status(
                db_path=db_path,
                connection_id=connection_id,
                status="needs_auth",
                metadata={**(metadata or {}), "error": "missing robinhood session pickle", "debug": debug},
            )
            raise BrokerAuthError("Robinhood session not found. Re-link required.", status_code=401)
        debug["session_validated"] = True
        debug["session_ts"] = _utc_ts()
        _update_debug_metadata(db_path=db_path, connection_id=connection_id, metadata=metadata, debug=debug)

        # --- Fetch balances/profile ---
        portfolio = None
        account_profile = None
        profile_errors: list[str] = []

        try:
            portfolio = rh.profiles.load_portfolio_profile()
        except Exception as e:
            profile_errors.append(f"portfolio_profile: {e}")

        try:
            account_profile = _load_account_profile_safe(portfolio)
        except Exception as e:
            profile_errors.append(f"account_profile: {e}")

        if not portfolio and not account_profile:
            msg = "; ".join(profile_errors) if profile_errors else "unknown error"
            raise BrokerConnectorError(f"Robinhood profile fetch failed: {msg}", status_code=502)

        user_profile: dict[str, Any] = {}
        if isinstance(portfolio, dict):
            user_profile["equity"] = portfolio.get("equity")
            user_profile["extended_hours_equity"] = portfolio.get("extended_hours_equity")
            user_profile["withdrawable_amount"] = portfolio.get("withdrawable_amount")
            user_profile["unwithdrawable_deposits"] = portfolio.get("unwithdrawable_deposits")

        if isinstance(account_profile, dict):
            cash = account_profile.get("cash")
            uncleared = account_profile.get("uncleared_deposits")
            try:
                if cash is not None and uncleared is not None:
                    user_profile["cash"] = f"{float(cash) + float(uncleared):.2f}"
                elif cash is not None:
                    user_profile["cash"] = cash
            except Exception:
                user_profile["cash"] = cash

        account_number = None
        if isinstance(account_profile, dict):
            account_number = account_profile.get("account_number") or account_profile.get("rhs_account_number")
        if account_number:
            debug["account_number"] = account_number
            _update_debug_metadata(db_path=db_path, connection_id=connection_id, metadata=metadata, debug=debug)

        # --- Fetch holdings (stocks) via open positions (matches DreadFox.Stock.py flow) ---
        try:
            if account_number:
                positions_data = rh.account.get_open_stock_positions(account_number=account_number)
            elif rh_helper is not None:
                positions_data = rh_helper.request_get(
                    "https://api.robinhood.com/positions/",
                    "pagination",
                    {"nonzero": "true"},
                )
            else:
                positions_data = []
        except Exception as e:
            raise BrokerConnectorError(f"Robinhood open_stock_positions failed: {e}", status_code=502)

        positions_data = positions_data if isinstance(positions_data, list) else []

        positions: list[NormalizedPosition] = []
        for pos in positions_data:
            if not isinstance(pos, dict):
                continue
            inst_url = pos.get("instrument")
            instrument: dict[str, Any] = {}
            if inst_url:
                try:
                    instrument = rh.account.get_instrument_by_url(inst_url)
                except Exception:
                    instrument = {}
            symbol = instrument.get("symbol") or pos.get("symbol") or "-"
            quote: Optional[dict[str, Any]] = None
            try:
                quote = rh.stocks.get_stock_quote_by_symbol(str(symbol))
            except Exception:
                quote = None

            qty = _to_number(pos.get("quantity"))
            avg = _to_number(pos.get("average_buy_price"))
            price = _price_from_quote(quote, prefer_extended=True) if isinstance(quote, dict) else None
            mv = None
            if qty is not None and price is not None:
                mv = qty * price
            upl = None
            if qty is not None and price is not None and avg is not None:
                upl = (price - avg) * qty

            positions.append(
                NormalizedPosition(
                    symbol=str(symbol),
                    quantity=qty,
                    average_price=avg,
                    market_price=price,
                    market_value=mv,
                    unrealized_pl=upl,
                    asset_type=str(instrument.get("type") or "stock"),
                    raw={"position": pos, "instrument": instrument, "quote": quote},
                )
            )

        # --- Fetch crypto positions (best-effort) ---
        try:
            crypto_pos = rh.crypto.get_crypto_positions()
        except Exception:
            crypto_pos = None

        crypto_quote_cache: dict[str, Optional[dict[str, Any]]] = {}

        def _crypto_quote(symbol: str) -> Optional[dict[str, Any]]:
            sym = str(symbol or "").strip().upper()
            if not sym:
                return None
            if sym in crypto_quote_cache:
                return crypto_quote_cache[sym]
            data: Optional[dict[str, Any]] = None
            try:
                q = rh.crypto.get_crypto_quote(sym)
                if isinstance(q, dict):
                    data = q
            except Exception:
                data = None
            crypto_quote_cache[sym] = data
            return data

        if isinstance(crypto_pos, list):
            for cp in crypto_pos:
                if not isinstance(cp, dict):
                    continue

                qty = safe_float(cp.get("quantity"))
                if qty is None:
                    qty = safe_float(cp.get("quantity_available"))
                if qty is None or qty <= 0:
                    continue

                sym = str(cp.get("symbol") or "").strip().upper()
                currency = cp.get("currency")
                if not sym and isinstance(currency, dict):
                    sym = str(currency.get("code") or currency.get("symbol") or "").strip().upper()
                if not sym:
                    pair = cp.get("currency_pair")
                    if isinstance(pair, dict):
                        asset_cur = pair.get("asset_currency")
                        if isinstance(asset_cur, dict):
                            sym = str(asset_cur.get("code") or asset_cur.get("symbol") or "").strip().upper()
                        if not sym:
                            sym = str(pair.get("asset_currency_code") or "").strip().upper()
                if not sym:
                    sym = "CRYPTO"

                quote = _crypto_quote(sym) if sym != "CRYPTO" else None
                price = _price_from_crypto_quote(quote) if isinstance(quote, dict) else None

                cost_basis_total = None
                cost_bases = cp.get("cost_bases")
                if isinstance(cost_bases, list):
                    total_cost = 0.0
                    has_cost = False
                    for cb in cost_bases:
                        if not isinstance(cb, dict):
                            continue
                        direct_cost = safe_float(cb.get("direct_cost_basis") or cb.get("cost_basis"))
                        if direct_cost is None:
                            continue
                        total_cost += float(direct_cost)
                        has_cost = True
                    if has_cost:
                        cost_basis_total = total_cost

                avg = safe_float(cp.get("average_buy_price") or cp.get("average_price"))
                basis_qty = qty
                if cost_basis_total is not None and basis_qty is not None and basis_qty > 0:
                    avg = float(cost_basis_total) / float(basis_qty)

                mv = None
                if price is not None:
                    mv = float(qty) * float(price)

                upl = None
                if mv is not None and cost_basis_total is not None:
                    upl = float(mv) - float(cost_basis_total)
                elif price is not None and avg is not None:
                    upl = (float(price) - float(avg)) * float(qty)

                include_value = mv
                if include_value is None:
                    include_value = cost_basis_total
                if include_value is None and avg is not None:
                    include_value = float(qty) * float(avg)
                if include_value is None or include_value <= DASHBOARD_MIN_CRYPTO_POSITION_VALUE:
                    continue

                positions.append(
                    NormalizedPosition(
                        symbol=str(sym),
                        quantity=qty,
                        average_price=avg,
                        market_price=price,
                        market_value=mv,
                        unrealized_pl=upl,
                        asset_type="crypto",
                        raw={"position": cp, "quote": quote, "cost_bases": cost_bases},
                    )
                )

        # --- Fetch option positions (best-effort) ---
        try:
            if account_number:
                opt_pos = rh.options.get_open_option_positions(account_number=account_number)
            else:
                opt_pos = rh.options.get_open_option_positions()
        except Exception:
            opt_pos = None

        option_market_cache: dict[str, Optional[dict[str, Any]]] = {}
        option_instrument_cache: dict[str, Optional[dict[str, Any]]] = {}

        def _option_id_from_position(p: dict[str, Any]) -> Optional[str]:
            opt_id = p.get("option_id") or p.get("optionId")
            if isinstance(opt_id, str) and opt_id:
                return opt_id
            opt_url = p.get("option") or p.get("option_url") or p.get("instrument")
            if isinstance(opt_url, str) and opt_url:
                return opt_url.rstrip("/").split("/")[-1]
            return None

        def _option_market_data(option_id: Optional[str]) -> Optional[dict[str, Any]]:
            if not option_id:
                return None
            if option_id in option_market_cache:
                return option_market_cache[option_id]
            data: Optional[dict[str, Any]] = None
            try:
                md = rh.options.get_option_market_data_by_id(option_id)
                if isinstance(md, list) and md:
                    first = md[0]
                    if isinstance(first, dict):
                        data = first
                elif isinstance(md, dict):
                    data = md
            except Exception:
                data = None
            option_market_cache[option_id] = data
            return data

        def _option_instrument_data(option_id: Optional[str]) -> Optional[dict[str, Any]]:
            if not option_id:
                return None
            if option_id in option_instrument_cache:
                return option_instrument_cache[option_id]
            data: Optional[dict[str, Any]] = None
            try:
                inst = rh.options.get_option_instrument_data_by_id(option_id)
                if isinstance(inst, dict):
                    data = inst
            except Exception:
                data = None
            option_instrument_cache[option_id] = data
            return data

        def _fmt_strike(val: Optional[str]) -> str:
            if val is None:
                return ""
            try:
                f = float(val)
                if abs(f - int(f)) < 1e-9:
                    return str(int(f))
                return f"{f:.2f}".rstrip("0").rstrip(".")
            except Exception:
                return str(val)

        if isinstance(opt_pos, list):
            for p in opt_pos:
                if not isinstance(p, dict):
                    continue
                sym = p.get("chain_symbol") or p.get("symbol") or "OPTION"
                qty = safe_float(p.get("quantity"))
                avg_price_raw = safe_float(p.get("average_price") or p.get("average_open_price"))
                market_price = safe_float(p.get("mark_price"))
                market_value_raw = safe_float(p.get("market_value") or p.get("equity"))
                market_value = market_value_raw
                unreal_pl = safe_float(p.get("profit_loss") or p.get("unrealized_profit_loss"))

                opt_id = _option_id_from_position(p)
                inst = _option_instrument_data(opt_id)
                if inst:
                    sym = inst.get("chain_symbol") or sym
                md = _option_market_data(opt_id)
                bid_price = None
                if md:
                    bid_price = safe_float(md.get("bid_price"))
                    md_mark = safe_float(
                        md.get("adjusted_mark_price")
                        or md.get("mark_price")
                        or md.get("last_trade_price")
                    )
                    if bid_price is not None:
                        market_price = bid_price
                    elif md_mark is not None:
                        market_price = md_mark

                multiplier = safe_float(p.get("shares_per_contract") or p.get("trade_value_multiplier")) or 100.0
                if multiplier <= 0:
                    multiplier = 100.0

                avg_price = avg_price_raw
                if avg_price_raw is not None and qty is not None and qty > 0:
                    if market_value_raw is not None and market_value_raw > 0:
                        exp_per_share = avg_price_raw * qty * multiplier
                        exp_per_contract = avg_price_raw * qty
                        if exp_per_share > 0 and exp_per_contract > 0:
                            if abs(market_value_raw - exp_per_contract) < abs(market_value_raw - exp_per_share):
                                avg_price = avg_price_raw / multiplier
                    elif market_price is not None and avg_price_raw > market_price * 10 and avg_price_raw > 50:
                        avg_price = avg_price_raw / multiplier

                if market_value is None and market_price is not None and qty is not None:
                    market_value = market_price * qty * multiplier

                if unreal_pl is None and market_price is not None and avg_price is not None and qty is not None:
                    unreal_pl = (market_price - avg_price) * qty * multiplier

                asset_label = "option"
                if inst:
                    exp = inst.get("expiration_date") or ""
                    strike = _fmt_strike(inst.get("strike_price"))
                    opt_type = str(inst.get("type") or "").lower()
                    suffix = "C" if opt_type.startswith("call") else ("P" if opt_type.startswith("put") else opt_type)
                    if exp or strike:
                        asset_label = f"opt {exp} {strike}{suffix}".strip()

                positions.append(
                    NormalizedPosition(
                        symbol=str(sym),
                        quantity=qty,
                        average_price=avg_price,
                        market_price=market_price,
                        market_value=market_value,
                        unrealized_pl=unreal_pl,
                        asset_type=asset_label,
                        raw={"position": p, "market_data": md, "instrument": inst},
                    )
                )

        # Normalize balances into keys the UI already knows how to pick.
        balances: dict[str, Any] = {}
        if isinstance(user_profile, dict):
            balances["equity"] = user_profile.get("equity")
            balances["extended_hours_equity"] = user_profile.get("extended_hours_equity")
            balances["cashBalance"] = user_profile.get("cash")
            balances["cash"] = user_profile.get("cash")
            balances["dividend_total"] = user_profile.get("dividend_total")
            balances["withdrawable_amount"] = user_profile.get("withdrawable_amount")
            balances["unwithdrawable_deposits"] = user_profile.get("unwithdrawable_deposits")
        if isinstance(account_profile, dict):
            balances["buyingPower"] = account_profile.get("buying_power")
            balances["crypto_buying_power"] = account_profile.get("crypto_buying_power")

        acct = NormalizedAccount(
            account_id=str(metadata.get("username") or "Robinhood"),
            account_type="brokerage",
            balances=balances,
            positions=positions,
            raw={
                "user_profile": user_profile,
                "positions": positions_data,
                "crypto_positions": crypto_pos,
                "option_positions": opt_pos,
            },
        )

        return NormalizedPortfolioSnapshot(
            broker="robinhood",
            label=label,
            connection_id=int(connection_id),
            accounts=[acct],
            raw={
                "user_profile": user_profile,
                "positions": positions_data,
                "crypto_positions": crypto_pos,
                "option_positions": opt_pos,
            },
        )
