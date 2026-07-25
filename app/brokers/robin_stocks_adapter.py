from __future__ import annotations

import inspect
import logging
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Optional
from zoneinfo import ZoneInfo

try:
    import robin_stocks.robinhood as rh  # type: ignore
except Exception as exc:  # pragma: no cover
    rh = None  # type: ignore
    _IMPORT_ERR: Optional[BaseException] = exc
else:
    _IMPORT_ERR = None

LOG = logging.getLogger(__name__)
_ORDER_KILL_SWITCH = False
_ET_TZ = ZoneInfo("America/New_York")

SECRET_KEYS = {"account_number", "authorization", "access_token", "refresh_token", "token", "password", "mfa_code"}
STOCK_INTERVALS = {"5minute", "10minute", "hour", "day", "week"}
STOCK_SPANS = {"day", "week", "month", "3month", "year", "5year"}
STOCK_BOUNDS = {"regular", "trading", "extended"}
CRYPTO_INTERVALS = {"15second", "5minute", "10minute", "hour", "day", "week"}
CRYPTO_SPANS = {"hour", "day", "week", "month", "3month", "year", "5year"}
CRYPTO_BOUNDS = {"24_7", "regular", "trading", "extended"}
TIME_IN_FORCE = {"gfd", "gtc"}
TRAIL_TYPES = {"amount", "percentage"}
TERMINAL_BAD_STATES = {"rejected", "failed", "cancelled", "canceled"}
THROTTLE_MARKERS = ("throttle", "too many", "rate limit", "429")


@dataclass
class RobinStocksResult:
    accepted: bool = False
    submitted: bool = False
    blocked: bool = False
    reason: str = ""
    robin_stocks_function: Optional[str] = None
    sanitized_payload: dict[str, Any] = field(default_factory=dict)
    raw_response: Any = None
    order_id: Optional[str] = None
    state: Optional[str] = None
    status: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def __bool__(self) -> bool:
        return bool(self.accepted and self.submitted and not self.blocked)

    def get(self, key: str, default: Any = None) -> Any:
        return self.to_dict().get(key, default)


@dataclass
class SessionOrderDecision:
    can_submit: bool
    can_execute_now: bool
    will_queue: bool
    blocked: bool
    reason: str
    detected_session: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _common_extended_hours_label(now_dt: Optional[datetime] = None) -> Optional[str]:
    now = now_dt or datetime.now(timezone.utc)
    if now.tzinfo is None or now.utcoffset() is None:
        now = now.replace(tzinfo=timezone.utc)
    now_et = now.astimezone(_ET_TZ)
    minute = (int(now_et.hour) * 60) + int(now_et.minute)
    weekday = int(now_et.weekday())
    if weekday >= 5:
        return None
    if (7 * 60) <= minute < (9 * 60 + 30):
        return "premarket"
    if (16 * 60) <= minute < (20 * 60):
        return "after_hours"
    return None


def sanitize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, val in payload.items():
        if key.lower() in SECRET_KEYS or "token" in key.lower() or "auth" in key.lower():
            out[key] = "***REDACTED***"
        elif hasattr(val, "to_dict") and callable(getattr(val, "to_dict", None)):
            out[key] = val.to_dict()
        else:
            out[key] = val
    return out


def _blocked(reason: str, *, function_name: Optional[str] = None, payload: Optional[dict[str, Any]] = None) -> RobinStocksResult:
    result = RobinStocksResult(
        accepted=False,
        submitted=False,
        blocked=True,
        reason=reason,
        robin_stocks_function=function_name,
        sanitized_payload=sanitize_payload(payload or {}),
    )
    LOG.warning("RobinStocks order blocked: %s payload=%s", reason, result.sanitized_payload)
    return result


def set_order_kill_switch(active: bool = True) -> None:
    global _ORDER_KILL_SWITCH
    _ORDER_KILL_SWITCH = bool(active)


def order_kill_switch_active() -> bool:
    return _ORDER_KILL_SWITCH or str(os.environ.get("ROBIN_STOCKS_ADAPTER_KILL_SWITCH", "")).lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _kill_switch_block(payload: dict[str, Any]) -> Optional[RobinStocksResult]:
    if order_kill_switch_active():
        return _blocked("ADAPTER_ORDER_KILL_SWITCH_ACTIVE", payload=payload)
    return None


def _module_function(function_name: str) -> Optional[Callable[..., Any]]:
    if rh is None:
        return None
    obj: Any = rh
    for part in function_name.split("."):
        obj = getattr(obj, part, None)
        if obj is None:
            return None
    return obj if callable(obj) else None


def _filter_args(fn: Callable[..., Any], payload: dict[str, Any]) -> dict[str, Any]:
    sig = inspect.signature(fn)
    return {k: v for k, v in payload.items() if k in sig.parameters}


def _extract_response_error(resp: Any) -> Optional[str]:
    if resp is None:
        return "EMPTY_RESPONSE_FROM_ROBIN_STOCKS"
    if not isinstance(resp, dict):
        return None
    for key in ("detail", "error", "message", "non_field_errors"):
        val = resp.get(key)
        if val:
            text = str(val)
            low = text.lower()
            if any(marker in low for marker in THROTTLE_MARKERS):
                return f"ROBIN_STOCKS_THROTTLED: {text}"
            return text
    state = str(resp.get("state") or resp.get("status") or "").lower()
    if state in TERMINAL_BAD_STATES:
        return f"ROBIN_STOCKS_ORDER_{state.upper()}"
    return None


def _normalize_response(resp: Any, *, function_name: str, payload: dict[str, Any]) -> RobinStocksResult:
    error = _extract_response_error(resp)
    order_id = None
    state = None
    status = None
    if isinstance(resp, dict):
        order_id = resp.get("id") or resp.get("order_id") or resp.get("cancel_url", "").rstrip("/").split("/")[-1] or None
        state = resp.get("state")
        status = resp.get("status")
    missing_id = isinstance(resp, dict) and not order_id
    accepted = error is None and not missing_id
    reason = error or ("MISSING_ORDER_ID_FROM_ROBIN_STOCKS" if missing_id else "SUBMITTED")
    result = RobinStocksResult(
        accepted=bool(accepted),
        submitted=True,
        blocked=False,
        reason=reason,
        robin_stocks_function=function_name,
        sanitized_payload=sanitize_payload(payload),
        raw_response=resp,
        order_id=str(order_id) if order_id else None,
        state=str(state) if state is not None else None,
        status=str(status) if status is not None else None,
    )
    LOG.info(
        "RobinStocks wrapper=%s payload=%s raw_response=%s accepted=%s submitted=%s blocked=%s reason=%s",
        function_name,
        result.sanitized_payload,
        resp,
        result.accepted,
        result.submitted,
        result.blocked,
        result.reason,
    )
    return result


def _call(function_name: str, payload: dict[str, Any]) -> RobinStocksResult:
    if _IMPORT_ERR is not None or rh is None:
        return _blocked(f"ROBIN_STOCKS_UNAVAILABLE: {_IMPORT_ERR}", function_name=function_name, payload=payload)
    fn = _module_function(function_name)
    if fn is None:
        return _blocked("UNSUPPORTED_BY_ROBIN_STOCKS", function_name=function_name, payload=payload)
    args = _filter_args(fn, payload)
    LOG.info("RobinStocks selected wrapper=%s payload=%s", function_name, sanitize_payload(args))
    try:
        resp = fn(**args)
    except Exception as exc:
        return RobinStocksResult(
            accepted=False,
            submitted=True,
            blocked=False,
            reason=f"ROBIN_STOCKS_EXCEPTION: {exc}",
            robin_stocks_function=function_name,
            sanitized_payload=sanitize_payload(args),
            raw_response=None,
        )
    return _normalize_response(resp, function_name=function_name, payload=args)


def _call_stock_trailing_stop_legacy(
    *,
    symbol: str,
    quantity: float,
    side: str,
    trail_amount: float,
    trail_type: str,
    time_in_force: str,
    account_number: Optional[str] = None,
    extended_hours: bool = False,
    jsonify: bool = True,
) -> RobinStocksResult:
    """
    Preserve the legacy working call shape:
    rh.orders.order_trailing_stop(symbol=..., quantity=..., trailAmount=...,
                                  trailType='amount', timeInForce='gtc',
                                  side='buy'|'sell')
    """
    fn_name = "orders.order_trailing_stop"
    legacy_payload: dict[str, Any] = {
        "symbol": symbol,
        "quantity": int(quantity) if float(quantity).is_integer() else quantity,
        "trailAmount": float(trail_amount),
        "trailType": trail_type,
        "timeInForce": time_in_force,
        "side": side,
    }
    if account_number is not None:
        legacy_payload["account_number"] = account_number
    if extended_hours:
        legacy_payload["extendedHours"] = True
    if not jsonify:
        legacy_payload["jsonify"] = False
    return _call(fn_name, legacy_payload)


def validate_stock_order_session(
    *,
    market_session: Optional[str],
    symbol: str,
    side: str,
    order_type: str,
    extendedHours: bool,
    timeInForce: str,
    market_hours: Optional[str] = None,
    require_immediate_execution: bool = False,
    now_dt: Optional[datetime] = None,
) -> SessionOrderDecision:
    session = str(market_session or "").strip().lower()
    typ = str(order_type or "").strip().lower()
    mh = str(market_hours or "").strip().lower()
    if not session:
        decision = SessionOrderDecision(True, True, False, False, "NO_SESSION_CHECK", "")
    elif session in {"regular", "regular_hours", "open"}:
        decision = SessionOrderDecision(True, True, False, False, "REGULAR_SESSION", session)
    elif session in {"premarket", "pre_market"}:
        if typ in {"limit", "limit_midpoint"} and bool(extendedHours):
            decision = SessionOrderDecision(True, True, False, False, "PREMARKET_EXTENDED_LIMIT_ALLOWED", session)
        elif typ == "market":
            decision = SessionOrderDecision(False, False, False, True, "MARKET_ORDER_NOT_SUPPORTED_FOR_PREMARKET", session)
        elif typ == "trailing_stop":
            decision = SessionOrderDecision(True, False, True, bool(require_immediate_execution), "PREMARKET_TRAILING_STOP_WILL_QUEUE", session)
        else:
            decision = SessionOrderDecision(False, False, False, True, "ORDER_TYPE_NOT_SUPPORTED_FOR_PREMARKET", session)
    elif session in {"after_hours", "afterhours", "postmarket", "extended"}:
        if typ in {"limit", "limit_midpoint"} and bool(extendedHours):
            decision = SessionOrderDecision(True, True, False, False, "EXTENDED_LIMIT_ALLOWED", session)
        elif typ == "market":
            decision = SessionOrderDecision(False, False, False, True, "MARKET_ORDER_NOT_SUPPORTED_FOR_EXTENDED_HOURS", session)
        elif typ == "trailing_stop":
            decision = SessionOrderDecision(True, False, True, bool(require_immediate_execution), "EXTENDED_TRAILING_STOP_WILL_QUEUE", session)
        else:
            decision = SessionOrderDecision(False, False, False, True, "ORDER_TYPE_NOT_SUPPORTED_FOR_EXTENDED_HOURS", session)
    elif session in {"closed", "overnight"}:
        if typ in {"limit", "limit_midpoint"} and bool(extendedHours) and mh == "all_day_hours":
            decision = SessionOrderDecision(True, True, False, False, "ALL_DAY_LIMIT_ALLOWED", session)
        elif (
            session == "closed"
            and typ in {"limit", "limit_midpoint"}
            and bool(extendedHours)
            and mh == "extended_hours"
            and _common_extended_hours_label(now_dt)
        ):
            decision = SessionOrderDecision(True, True, False, False, "STALE_CLOSED_EXTENDED_LIMIT_ALLOWED", session)
        else:
            decision = SessionOrderDecision(False, False, False, True, "MARKET_SESSION_CLOSED", session)
    else:
        decision = SessionOrderDecision(False, False, False, True, "UNKNOWN_MARKET_SESSION", session)

    now_utc = now_dt or datetime.now(timezone.utc)
    LOG.info(
        "RobinStocks session decision utc=%s et=%s session=%s symbol=%s asset_type=stock side=%s "
        "order_type=%s extendedHours=%s timeInForce=%s can_submit=%s can_execute_now=%s "
        "will_queue=%s blocked=%s reason=%s",
        now_utc.isoformat(),
        now_utc.astimezone(_ET_TZ).isoformat(),
        decision.detected_session,
        symbol,
        side,
        typ,
        bool(extendedHours),
        timeInForce,
        decision.can_submit,
        decision.can_execute_now,
        decision.will_queue,
        decision.blocked,
        decision.reason,
    )
    return decision


def _validate_common(symbol: str, quantity: Optional[float], time_in_force: str) -> Optional[str]:
    if not str(symbol or "").strip():
        return "MISSING_SYMBOL"
    if quantity is not None and float(quantity) <= 0:
        return "INVALID_QUANTITY"
    if str(time_in_force).lower() not in TIME_IN_FORCE:
        return "INVALID_TIME_IN_FORCE"
    return None


def _normalize_stock_market_hours(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    mh = str(value or "").strip().lower()
    if not mh:
        return None
    aliases = {
        "regular": "regular_hours",
        "regular_hours": "regular_hours",
        "extended": "extended_hours",
        "extended_hours": "extended_hours",
        "premarket": "extended_hours",
        "after_hours": "extended_hours",
        "afterhours": "extended_hours",
        "postmarket": "extended_hours",
        "all_day": "all_day_hours",
        "all_day_hours": "all_day_hours",
        "overnight": "all_day_hours",
        "seamless": "all_day_hours",
        "24_5": "all_day_hours",
        "24hour": "all_day_hours",
        "24_hour": "all_day_hours",
    }
    return aliases.get(mh, mh)


def place_stock_order(
    *,
    symbol: str,
    side: str,
    order_type: str,
    quantity: float,
    price: Optional[float] = None,
    limitPrice: Optional[float] = None,
    stopPrice: Optional[float] = None,
    trailAmount: Optional[float] = None,
    trailType: str = "amount",
    timeInForce: str = "gtc",
    extendedHours: bool = False,
    market_hours: Optional[str] = None,
    market_session: Optional[str] = None,
    require_immediate_execution: bool = False,
    account_number: Optional[str] = None,
    jsonify: bool = True,
) -> RobinStocksResult:
    kill_block = _kill_switch_block(locals())
    if kill_block is not None:
        return kill_block
    side = str(side).lower().strip()
    typ = str(order_type).lower().strip()
    err = _validate_common(symbol, quantity, timeInForce)
    if err:
        return _blocked(err, payload=locals())
    if side not in {"buy", "sell"}:
        return _blocked("INVALID_SIDE", payload=locals())
    normalized_market_hours = _normalize_stock_market_hours(market_hours)
    if normalized_market_hours is None and bool(extendedHours):
        normalized_market_hours = "extended_hours"
    if normalized_market_hours is not None and normalized_market_hours not in {"regular_hours", "extended_hours", "all_day_hours"}:
        return _blocked("INVALID_MARKET_HOURS", payload=locals())
    if normalized_market_hours in {"extended_hours", "all_day_hours"}:
        extendedHours = True
    elif normalized_market_hours == "regular_hours" and bool(extendedHours):
        return _blocked("CONFLICTING_MARKET_HOURS_AND_EXTENDED_HOURS", payload=locals())
    if market_hours is not None:
        market_hours = normalized_market_hours
    if normalized_market_hours in {"extended_hours", "all_day_hours"} and typ not in {"limit", "limit_midpoint"}:
        return _blocked("EXTENDED_MARKET_HOURS_REQUIRES_LIMIT_ORDER", payload=locals())

    limit = limitPrice if limitPrice is not None else price
    fn: Optional[str] = None
    payload: dict[str, Any] = {
        "symbol": symbol,
        "quantity": int(quantity) if float(quantity).is_integer() else quantity,
        "account_number": account_number,
        "timeInForce": timeInForce,
        "extendedHours": bool(extendedHours),
        "jsonify": jsonify,
    }
    session_decision = validate_stock_order_session(
        market_session=market_session,
        symbol=symbol,
        side=side,
        order_type=typ,
        extendedHours=bool(extendedHours),
        timeInForce=timeInForce,
        market_hours=normalized_market_hours,
        require_immediate_execution=bool(require_immediate_execution),
    )
    if session_decision.blocked:
        return _blocked(session_decision.reason, payload=locals())

    if typ == "market":
        fn = f"orders.order_{side}_market"
    elif typ in {"limit", "limit_midpoint"}:
        if limit is None or float(limit) <= 0:
            return _blocked("MISSING_LIMIT_PRICE", payload=locals())
        payload["limitPrice"] = float(limit)
        if normalized_market_hours in {"extended_hours", "all_day_hours"}:
            fn = "orders.order"
            payload["side"] = side
            payload["market_hours"] = normalized_market_hours
        else:
            fn = f"orders.order_{side}_limit"
    elif typ in {"stop", "stop_loss"}:
        if stopPrice is None or float(stopPrice) <= 0:
            return _blocked("MISSING_STOP_PRICE", payload=locals())
        fn = f"orders.order_{side}_stop_loss"
        payload["stopPrice"] = float(stopPrice)
    elif typ == "stop_limit":
        if stopPrice is None or float(stopPrice) <= 0 or limit is None or float(limit) <= 0:
            return _blocked("MISSING_STOP_OR_LIMIT_PRICE", payload=locals())
        fn = f"orders.order_{side}_stop_limit"
        payload.update({"stopPrice": float(stopPrice), "limitPrice": float(limit)})
    elif typ == "trailing_stop":
        if trailAmount is None or float(trailAmount) <= 0:
            return _blocked("MISSING_TRAIL_AMOUNT", payload=locals())
        if trailType not in TRAIL_TYPES:
            return _blocked("INVALID_TRAIL_TYPE", payload=locals())
        fn = "orders.order_trailing_stop"
        if _module_function(fn) is None:
            legacy_payload = {
                "symbol": symbol,
                "quantity": int(quantity) if float(quantity).is_integer() else quantity,
                "trailAmount": float(trailAmount),
                "trailType": trailType,
                "timeInForce": timeInForce,
                "side": side,
            }
            if account_number is not None:
                legacy_payload["account_number"] = account_number
            if extendedHours:
                legacy_payload["extendedHours"] = True
            if not jsonify:
                legacy_payload["jsonify"] = False
            return _blocked("TRAILING_STOP_WRAPPER_UNAVAILABLE", function_name=fn, payload=legacy_payload)
        return _call_stock_trailing_stop_legacy(
            symbol=symbol,
            quantity=quantity,
            side=side,
            trail_amount=float(trailAmount),
            trail_type=trailType,
            time_in_force=timeInForce,
            account_number=account_number,
            extended_hours=bool(extendedHours),
            jsonify=bool(jsonify),
        )
    else:
        return _blocked("UNSUPPORTED_BY_ROBIN_STOCKS", payload=locals())

    return _call(fn, payload)


def place_crypto_order(
    *,
    symbol: str,
    side: str,
    order_type: str,
    quantity: Optional[float] = None,
    amountInDollars: Optional[float] = None,
    limitPrice: Optional[float] = None,
    timeInForce: str = "gtc",
    jsonify: bool = True,
    extendedHours: Optional[bool] = None,
    **unsupported: Any,
) -> RobinStocksResult:
    kill_block = _kill_switch_block(locals())
    if kill_block is not None:
        return kill_block
    side = str(side).lower().strip()
    typ = str(order_type).lower().strip()
    err = _validate_common(symbol, quantity, timeInForce)
    if err:
        return _blocked(err, payload=locals())
    if side not in {"buy", "sell"}:
        return _blocked("INVALID_SIDE", payload=locals())
    if extendedHours is not None:
        return _blocked("EXTENDED_HOURS_NOT_SUPPORTED_FOR_CRYPTO_24_7", payload=locals())
    if typ in {"trailing_stop", "stop", "stop_loss", "stop_limit"}:
        return _blocked("TRAILING_STOP_UNSUPPORTED_BY_ROBIN_STOCKS" if typ == "trailing_stop" else "UNSUPPORTED_BY_ROBIN_STOCKS", payload=locals())

    payload: dict[str, Any] = {"symbol": symbol, "timeInForce": timeInForce, "jsonify": jsonify}
    if typ == "market":
        if amountInDollars is not None:
            if float(amountInDollars) <= 0:
                return _blocked("INVALID_AMOUNT_IN_DOLLARS", payload=locals())
            payload["amountInDollars"] = float(amountInDollars)
            fn = f"orders.order_{side}_crypto_by_price"
        elif quantity is not None:
            payload["quantity"] = float(quantity)
            fn = f"orders.order_{side}_crypto_by_quantity"
        else:
            return _blocked("MISSING_QUANTITY_OR_AMOUNT", payload=locals())
    elif typ == "limit":
        if limitPrice is None or float(limitPrice) <= 0:
            return _blocked("MISSING_LIMIT_PRICE", payload=locals())
        payload["limitPrice"] = float(limitPrice)
        if amountInDollars is not None:
            payload["amountInDollars"] = float(amountInDollars)
            fn = f"orders.order_{side}_crypto_limit_by_price"
        elif quantity is not None:
            payload["quantity"] = float(quantity)
            fn = f"orders.order_{side}_crypto_limit"
        else:
            return _blocked("MISSING_QUANTITY_OR_AMOUNT", payload=locals())
    else:
        return _blocked("UNSUPPORTED_BY_ROBIN_STOCKS", payload=locals())
    return _call(fn, payload)


def place_option_order(
    *,
    symbol: str,
    side: str,
    order_type: str,
    quantity: int,
    expirationDate: str,
    strike: str | float,
    optionType: str,
    positionEffect: str,
    creditOrDebit: str,
    price: Optional[float] = None,
    limitPrice: Optional[float] = None,
    stopPrice: Optional[float] = None,
    account_number: Optional[str] = None,
    timeInForce: str = "gtc",
    jsonify: bool = True,
    extendedHours: Optional[bool] = None,
) -> RobinStocksResult:
    kill_block = _kill_switch_block(locals())
    if kill_block is not None:
        return kill_block
    if extendedHours is not None:
        return _blocked("EXTENDED_HOURS_NOT_SUPPORTED_FOR_OPTIONS_WRAPPER", payload=locals())
    side = str(side).lower().strip()
    typ = str(order_type).lower().strip()
    err = _validate_common(symbol, quantity, timeInForce)
    if err:
        return _blocked(err, payload=locals())
    if side not in {"buy", "sell"}:
        return _blocked("INVALID_SIDE", payload=locals())
    limit = limitPrice if limitPrice is not None else price
    payload = {
        "positionEffect": positionEffect,
        "creditOrDebit": creditOrDebit,
        "symbol": symbol,
        "quantity": int(quantity),
        "expirationDate": expirationDate,
        "strike": strike,
        "optionType": optionType,
        "account_number": account_number,
        "timeInForce": timeInForce,
        "jsonify": jsonify,
    }
    if typ == "limit":
        if limit is None or float(limit) <= 0:
            return _blocked("MISSING_LIMIT_PRICE", payload=locals())
        payload["price"] = float(limit)
        fn = f"orders.order_{side}_option_limit"
    elif typ == "stop_limit":
        if limit is None or float(limit) <= 0 or stopPrice is None or float(stopPrice) <= 0:
            return _blocked("MISSING_STOP_OR_LIMIT_PRICE", payload=locals())
        payload.update({"limitPrice": float(limit), "stopPrice": float(stopPrice)})
        fn = f"orders.order_{side}_option_stop_limit"
    else:
        return _blocked("UNSUPPORTED_BY_ROBIN_STOCKS", payload=locals())
    return _call(fn, payload)


def get_stock_historicals(symbol: str, *, interval: str, span: str, bounds: str = "regular", info: Optional[str] = None) -> list[dict[str, Any]]:
    if interval not in STOCK_INTERVALS:
        raise ValueError(f"Unsupported robin_stocks stock interval: {interval}")
    if span not in STOCK_SPANS:
        raise ValueError(f"Unsupported robin_stocks stock span: {span}")
    if bounds not in STOCK_BOUNDS:
        raise ValueError(f"Unsupported robin_stocks stock bounds: {bounds}")
    if rh is None:
        raise RuntimeError(f"robin_stocks unavailable: {_IMPORT_ERR}")
    data = rh.stocks.get_stock_historicals(symbol, interval=interval, span=span, bounds=bounds, info=info)
    return data if isinstance(data, list) else []


def get_crypto_historicals(symbol: str, *, interval: str, span: str, bounds: str = "24_7", info: Optional[str] = None) -> list[dict[str, Any]]:
    if interval not in CRYPTO_INTERVALS:
        raise ValueError(f"Unsupported robin_stocks crypto interval: {interval}")
    if span not in CRYPTO_SPANS:
        raise ValueError(f"Unsupported robin_stocks crypto span: {span}")
    if bounds not in CRYPTO_BOUNDS:
        raise ValueError(f"Unsupported robin_stocks crypto bounds: {bounds}")
    if rh is None:
        raise RuntimeError(f"robin_stocks unavailable: {_IMPORT_ERR}")
    data = rh.crypto.get_crypto_historicals(symbol, interval=interval, span=span, bounds=bounds, info=info)
    return data if isinstance(data, list) else []


def _row_time(row: dict[str, Any]) -> Optional[datetime]:
    raw = row.get("begins_at") or row.get("beginsAt") or row.get("time")
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None


def resample_5m_to_10m(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[datetime, list[dict[str, Any]]] = {}
    for row in rows:
        ts = _row_time(row)
        if ts is None:
            continue
        minute = (ts.minute // 10) * 10
        key = ts.replace(minute=minute, second=0, microsecond=0)
        buckets.setdefault(key, []).append(row)
    out: list[dict[str, Any]] = []
    for key in sorted(buckets):
        chunk = sorted(buckets[key], key=lambda r: _row_time(r) or key)
        try:
            opens = float(chunk[0].get("open_price") or chunk[0].get("open") or 0)
            closes = float(chunk[-1].get("close_price") or chunk[-1].get("close") or 0)
            highs = [float(r.get("high_price") or r.get("high") or 0) for r in chunk]
            lows = [float(r.get("low_price") or r.get("low") or 0) for r in chunk]
        except (TypeError, ValueError):
            continue
        out.append({
            "begins_at": key.isoformat().replace("+00:00", "Z"),
            "open_price": str(opens),
            "close_price": str(closes),
            "high_price": str(max(highs)),
            "low_price": str(min(lows)),
            "session": chunk[-1].get("session"),
            "interpolated": False,
        })
    return out


def get_10m_stock_historicals(
    symbol: str,
    *,
    span: str = "week",
    bounds: str = "regular",
    min_candles: int = 150,
    allow_partial: bool = False,
) -> list[dict[str, Any]]:
    rows = get_stock_historicals(symbol, interval="10minute", span=span, bounds=bounds)
    if len(rows) >= min_candles:
        return rows
    lower = get_stock_historicals(symbol, interval="5minute", span=span, bounds=bounds)
    resampled = resample_5m_to_10m(lower)
    if len(resampled) < min_candles:
        if bool(allow_partial):
            return resampled if len(resampled) > len(rows) else rows
        LOG.warning("INSUFFICIENT_CANDLES_FOR_10M_CALCULATION symbol=%s count=%s min=%s", symbol, len(resampled), min_candles)
        raise RuntimeError("INSUFFICIENT_CANDLES_FOR_10M_CALCULATION")
    return resampled
