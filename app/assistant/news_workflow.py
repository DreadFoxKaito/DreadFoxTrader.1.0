from __future__ import annotations

import json
import re
import threading
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import quote, quote_plus

import httpx


ProgressCallback = Callable[[dict[str, Any]], None]

TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,11}$")
USER_AGENT = "DreadfoxTrader/1.0 (+openai assistant news workflow)"


class WorkflowStopped(RuntimeError):
    """Raised when the user stops an assistant workflow."""


def _stop_requested(stop_event: Optional[threading.Event]) -> bool:
    return bool(stop_event and stop_event.is_set())


def _raise_if_stopped(stop_event: Optional[threading.Event]) -> None:
    if _stop_requested(stop_event):
        raise WorkflowStopped("Assistant news workflow stopped.")


@dataclass(frozen=True)
class AssistantNewsWorkflowConfig:
    openai_api_key: str
    model: str
    system_prompt: str
    openai_base_url: str = "https://api.openai.com/v1"
    openai_organization: str = ""
    openai_project: str = ""
    articles_per_ticker: int = 4
    include_article_text: bool = True
    include_market_data: bool = True
    max_article_chars: int = 1800
    request_timeout: float = 15.0
    openai_timeout: float = 180.0
    max_input_chars: int = 180000
    max_output_tokens: int = 6000


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def default_news_system_prompt() -> str:
    return """You are ZENKO NEWS TAPE, a market-news triage assistant inside Dreadfox Trader.

ROLE
You read one combined packet containing fresh ticker news and same-day market tape data for every requested ticker. You do not execute trades. You do not claim certainty.

HARD RULES
- Use only the provided articles, summaries, timestamps, source URLs, and market data.
- Never invent prices, catalysts, sources, analyst ratings, or company events.
- Separate news-driven conviction from ordinary price drift.
- If evidence is thin, say "insufficient evidence" and keep the score low.
- Probabilities are research estimates, not guarantees or financial advice.

FINAL REVIEW METHOD
After reading the full packet, produce the final review in one pass:
1. Identify the dominant cross-ticker driver.
2. Rate every ticker from 0-100 for predictable positive-return potential today.
3. Rank the strongest candidates and state direction, driver, evidence, confidence, and invalidation risk.
4. Flag downtrend/avoid names and no-trade names with thin or contradictory evidence.
5. Include SQQQ only when reviewed Nasdaq-heavy tickers show broad bearish pressure; include TQQQ only when they show broad bullish pressure.

Keep the answer decisive, compact, and source-grounded."""


def parse_ticker_symbols(raw: Any, *, max_symbols: int = 100) -> list[str]:
    if isinstance(raw, str):
        parts = re.split(r"[\s,;|]+", raw)
    elif isinstance(raw, (list, tuple, set)):
        parts = [str(item) for item in raw]
    else:
        parts = []

    out: list[str] = []
    seen: set[str] = set()
    for part in parts:
        symbol = str(part or "").strip().upper()
        if symbol.startswith("$"):
            symbol = symbol[1:]
        symbol = symbol.replace("/", "-")
        if not symbol or not TICKER_RE.match(symbol):
            continue
        if symbol in seen:
            continue
        out.append(symbol)
        seen.add(symbol)
        if len(out) >= max_symbols:
            break
    return out


def fetch_nasdaq100_tickers(*, timeout: float = 15.0) -> list[str]:
    url = "https://en.wikipedia.org/wiki/Nasdaq-100"
    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"}
    resp = httpx.get(url, timeout=timeout, follow_redirects=True, headers=headers)
    resp.raise_for_status()
    match = re.search(r'<table[^>]+id="constituents"[^>]*>(.*?)</table>', resp.text, re.S | re.I)
    if not match:
        return []
    tickers = re.findall(
        r"<tr>\s*<td>\s*([A-Z][A-Z0-9.\-]{0,11})\s*</td>\s*<td>\s*<a\b",
        match.group(1),
        re.S,
    )
    return parse_ticker_symbols(tickers, max_symbols=100)


class _ReadableTextParser(HTMLParser):
    ignored_tags = {"script", "style", "noscript", "svg", "canvas", "form", "nav", "footer"}

    def __init__(self) -> None:
        super().__init__()
        self._ignored_depth = 0
        self.parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        if tag.lower() in self.ignored_tags:
            self._ignored_depth += 1

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in self.ignored_tags and self._ignored_depth:
            self._ignored_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._ignored_depth:
            return
        text = data.strip()
        if text:
            self.parts.append(text)


def strip_html(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", str(value or ""))
    text = unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def limit_text(value: Any, *, max_chars: int, label: str = "text") -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    max_chars = max(0, int(max_chars))
    if not max_chars or len(text) <= max_chars:
        return text
    marker = f" ... [{label} truncated to fit model context] ... "
    if max_chars <= len(marker) + 40:
        return text[:max_chars].rstrip()
    front = max_chars // 2
    back = max_chars - front - len(marker)
    return (text[:front].rstrip() + marker + text[-back:].lstrip()).strip()


def model_prompt_char_budget(num_ctx: int, *, reserve_tokens: int = 1024, low: int = 4000, high: int = 90000) -> int:
    try:
        ctx = int(num_ctx)
    except Exception:
        ctx = 4096
    usable_tokens = max(1024, ctx - int(reserve_tokens))
    # Use a conservative char/token estimate and keep room for prompt scaffolding.
    return max(int(low), min(int(high), usable_tokens * 3))


def extract_readable_text(html_text: str, *, max_chars: int) -> str:
    parser = _ReadableTextParser()
    try:
        parser.feed(html_text)
    except Exception:
        pass
    text = re.sub(r"\s+", " ", " ".join(parser.parts)).strip()
    if len(text) < 240:
        meta = _extract_meta_description(html_text)
        if len(meta) > len(text):
            text = meta
    return text[:max(200, int(max_chars))]


def _extract_meta_description(html_text: str) -> str:
    patterns = [
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']description["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:description["\']',
    ]
    for pattern in patterns:
        match = re.search(pattern, html_text, re.I | re.S)
        if match:
            return strip_html(match.group(1))
    return ""


def _parse_pubdate(value: str) -> tuple[str, float]:
    raw = str(value or "").strip()
    if not raw:
        return "", 0.0
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat(), float(dt.timestamp())
    except Exception:
        return raw, 0.0


def _yahoo_symbol(symbol: str) -> str:
    return str(symbol or "").strip().upper().replace(".", "-")


def fetch_yahoo_rss_news(client: httpx.Client, symbol: str, *, limit: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    feed_url = (
        "https://feeds.finance.yahoo.com/rss/2.0/headline"
        f"?s={quote_plus(_yahoo_symbol(symbol))}&region=US&lang=en-US"
    )
    try:
        resp = client.get(feed_url)
        if resp.status_code >= 400:
            return out
        root = ET.fromstring(resp.text)
    except Exception:
        return out

    seen: set[str] = set()
    for item in root.findall(".//item"):
        title = strip_html(item.findtext("title") or "")
        if not title:
            continue
        url = (item.findtext("link") or "").strip()
        dedupe_key = url or title.lower()
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        published_at, published_ts = _parse_pubdate(item.findtext("pubDate") or "")
        out.append(
            {
                "ticker": symbol,
                "title": title,
                "url": url,
                "source": strip_html(item.findtext("source") or "Yahoo Finance RSS"),
                "published_at": published_at,
                "published_ts": published_ts,
                "summary": strip_html(item.findtext("description") or ""),
                "provider": "Yahoo Finance RSS",
            }
        )
        if len(out) >= max(1, int(limit)):
            break

    out.sort(key=lambda item: float(item.get("published_ts") or 0), reverse=True)
    return out


def fetch_article_text(client: httpx.Client, url: str, *, max_chars: int) -> str:
    clean_url = str(url or "").strip()
    if not clean_url.startswith(("http://", "https://")):
        return ""
    try:
        resp = client.get(clean_url, follow_redirects=True)
        if resp.status_code >= 400:
            return ""
        content_type = resp.headers.get("content-type", "")
        if "html" not in content_type.lower() and "text" not in content_type.lower():
            return ""
        return extract_readable_text(resp.text, max_chars=max_chars)
    except Exception:
        return ""


def fetch_market_snapshot(client: httpx.Client, symbol: str) -> dict[str, Any]:
    url = (
        "https://query1.finance.yahoo.com/v8/finance/chart/"
        f"{quote(_yahoo_symbol(symbol), safe='')}?range=1d&interval=5m&includePrePost=true"
    )
    snapshot: dict[str, Any] = {"ticker": symbol, "source": "Yahoo Finance chart", "status": "unavailable"}
    try:
        resp = client.get(url)
        if resp.status_code >= 400:
            snapshot["error"] = f"HTTP {resp.status_code}"
            return snapshot
        data = resp.json()
        result = ((data.get("chart") or {}).get("result") or [None])[0]
        if not isinstance(result, dict):
            return snapshot
        meta = result.get("meta") if isinstance(result.get("meta"), dict) else {}
        indicators = result.get("indicators") if isinstance(result.get("indicators"), dict) else {}
        quote_rows = indicators.get("quote") if isinstance(indicators.get("quote"), list) else []
        quote_row = quote_rows[0] if quote_rows and isinstance(quote_rows[0], dict) else {}
        closes_raw = quote_row.get("close") if isinstance(quote_row.get("close"), list) else []
        closes = [float(v) for v in closes_raw if isinstance(v, (int, float)) and v > 0]
        current = _float_or_none(meta.get("regularMarketPrice"))
        if current is None and closes:
            current = closes[-1]
        previous_close = _float_or_none(meta.get("chartPreviousClose") or meta.get("previousClose"))
        first = closes[0] if closes else None
        day_change_pct = _pct_change(current, previous_close)
        intraday_change_pct = _pct_change(current, first)
        last_window = closes[-6:] if len(closes) >= 6 else closes
        short_slope_pct = _pct_change(last_window[-1], last_window[0]) if len(last_window) >= 2 else None
        trend = "flat"
        if (day_change_pct is not None and day_change_pct <= -0.4) or (
            intraday_change_pct is not None and intraday_change_pct <= -0.35
        ):
            trend = "downtrend"
        elif (day_change_pct is not None and day_change_pct >= 0.4) or (
            intraday_change_pct is not None and intraday_change_pct >= 0.35
        ):
            trend = "uptrend"
        snapshot.update(
            {
                "status": "ok",
                "currency": meta.get("currency"),
                "exchange": meta.get("exchangeName") or meta.get("fullExchangeName"),
                "regular_market_price": current,
                "previous_close": previous_close,
                "day_change_pct": _round_or_none(day_change_pct, 3),
                "intraday_change_pct": _round_or_none(intraday_change_pct, 3),
                "short_slope_pct": _round_or_none(short_slope_pct, 3),
                "trend": trend,
                "points": len(closes),
                "market_time": meta.get("regularMarketTime"),
            }
        )
        return snapshot
    except Exception as exc:
        snapshot["error"] = str(exc)
        return snapshot


def _float_or_none(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _pct_change(current: Optional[float], base: Optional[float]) -> Optional[float]:
    if current is None or base is None or abs(base) < 1e-12:
        return None
    return ((current - base) / base) * 100.0


def _round_or_none(value: Optional[float], digits: int) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), digits)


def fetch_ticker_context(
    client: httpx.Client,
    symbol: str,
    config: AssistantNewsWorkflowConfig,
) -> dict[str, Any]:
    articles = fetch_yahoo_rss_news(client, symbol, limit=config.articles_per_ticker)
    if config.include_article_text:
        for article in articles:
            article["article_text"] = fetch_article_text(
                client,
                str(article.get("url") or ""),
                max_chars=config.max_article_chars,
            )
    market = fetch_market_snapshot(client, symbol) if config.include_market_data else {}
    return {"ticker": symbol, "market": market, "articles": articles}


def _openai_headers(config: AssistantNewsWorkflowConfig) -> dict[str, str]:
    api_key = str(config.openai_api_key or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not configured on the server.")
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    if config.openai_organization:
        headers["OpenAI-Organization"] = str(config.openai_organization).strip()
    if config.openai_project:
        headers["OpenAI-Project"] = str(config.openai_project).strip()
    return headers


def _extract_openai_output_text(data: dict[str, Any]) -> str:
    direct = data.get("output_text")
    if isinstance(direct, str) and direct.strip():
        return direct.strip()

    parts: list[str] = []
    output = data.get("output")
    if isinstance(output, list):
        for item in output:
            if not isinstance(item, dict):
                continue
            content = item.get("content")
            if isinstance(content, list):
                for content_item in content:
                    if not isinstance(content_item, dict):
                        continue
                    text = content_item.get("text")
                    if isinstance(text, str) and text:
                        parts.append(text)
            text = item.get("text")
            if isinstance(text, str) and text:
                parts.append(text)
    return "\n".join(part.strip() for part in parts if part.strip()).strip()


def openai_responses_call(
    *,
    config: AssistantNewsWorkflowConfig,
    user_prompt: str,
    stop_event: Optional[threading.Event] = None,
) -> str:
    _raise_if_stopped(stop_event)
    payload: dict[str, Any] = {
        "model": config.model,
        "instructions": config.system_prompt,
        "input": user_prompt,
        "store": False,
    }
    if config.max_output_tokens > 0:
        payload["max_output_tokens"] = int(config.max_output_tokens)

    url = f"{config.openai_base_url.rstrip('/')}/responses"
    try:
        resp = httpx.post(url, headers=_openai_headers(config), json=payload, timeout=config.openai_timeout)
    except httpx.HTTPError as exc:
        raise RuntimeError(f"OpenAI request failed: {exc}") from exc
    _raise_if_stopped(stop_event)
    if resp.status_code >= 400:
        detail = ""
        try:
            body = resp.json()
            error = body.get("error") if isinstance(body, dict) else None
            if isinstance(error, dict):
                detail = str(error.get("message") or error.get("code") or "")
            elif isinstance(error, str):
                detail = error
        except Exception:
            detail = limit_text(resp.text, max_chars=500, label="OpenAI error")
        suffix = f": {detail}" if detail else ""
        raise RuntimeError(f"OpenAI API returned HTTP {resp.status_code}{suffix}")
    try:
        data = resp.json()
    except Exception as exc:
        raise RuntimeError("OpenAI response was not valid JSON.") from exc
    output_text = _extract_openai_output_text(data)
    if not output_text:
        raise RuntimeError("OpenAI response did not contain output text.")
    return output_text


def build_visible_ticker_notes(ticker_context: dict[str, Any], *, max_articles: int = 6) -> dict[str, Any]:
    market = ticker_context.get("market") if isinstance(ticker_context.get("market"), dict) else {}
    articles = [article for article in (ticker_context.get("articles") or []) if isinstance(article, dict)]
    market_note = (
        f"trend={market.get('trend', 'unknown')}; "
        f"day_change_pct={market.get('day_change_pct')}; "
        f"intraday_change_pct={market.get('intraday_change_pct')}; "
        f"price={market.get('regular_market_price')}"
    )
    article_notes: list[dict[str, Any]] = []
    for article in articles[:max_articles]:
        article_notes.append(
            {
                "title": str(article.get("title") or "Untitled").strip(),
                "source": str(article.get("source") or article.get("provider") or "source").strip(),
                "published_at": str(article.get("published_at") or "").strip(),
                "url": str(article.get("url") or "").strip(),
                "summary": limit_text(article.get("summary"), max_chars=260, label="summary"),
                "article_text_chars": len(str(article.get("article_text") or "")),
            }
        )
    return {
        "ticker": ticker_context.get("ticker"),
        "market_note": market_note,
        "article_count": len(articles),
        "article_notes": article_notes,
    }


def build_combined_news_markdown(
    *,
    generated_at: str,
    model: str,
    ticker_contexts: list[dict[str, Any]],
) -> str:
    lines = [
        "# Assistant News Workflow Source Packet",
        "",
        f"- generated_at: {generated_at}",
        f"- model: {model}",
        f"- tickers: {len(ticker_contexts)}",
        "",
    ]
    for item in ticker_contexts:
        ticker = item.get("ticker") or "UNKNOWN"
        market = item.get("market") or {}
        articles = item.get("articles") or []
        lines.extend(
            [
                f"## {ticker}",
                "",
                f"- status: {item.get('status', 'collected')}",
                f"- article_count: {len(articles)}",
                f"- market_trend: {market.get('trend', 'unknown') if isinstance(market, dict) else 'unknown'}",
                f"- day_change_pct: {market.get('day_change_pct') if isinstance(market, dict) else None}",
                f"- intraday_change_pct: {market.get('intraday_change_pct') if isinstance(market, dict) else None}",
                f"- regular_market_price: {market.get('regular_market_price') if isinstance(market, dict) else None}",
                "",
                "### Sources",
            ]
        )
        for article in articles[:10]:
            if not isinstance(article, dict):
                continue
            title = str(article.get("title") or "Untitled").strip()
            source = str(article.get("source") or article.get("provider") or "source").strip()
            published = str(article.get("published_at") or "").strip()
            url = str(article.get("url") or "").strip()
            suffix = f" ({published})" if published else ""
            lines.append(f"- {title} - {source}{suffix} {url}".rstrip())
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def _ticker_prompt_section(ticker_context: dict[str, Any], *, max_chars: int) -> str:
    ticker = str(ticker_context.get("ticker") or "UNKNOWN").strip()
    market = ticker_context.get("market") if isinstance(ticker_context.get("market"), dict) else {}
    articles = [article for article in (ticker_context.get("articles") or []) if isinstance(article, dict)]
    per_article_budget = max(350, min(1800, (max(900, max_chars) - 900) // max(1, len(articles))))
    lines = [
        f"## {ticker}",
        "",
        "Market data:",
        json.dumps(market, indent=2, sort_keys=True),
        "",
        f"Articles ({len(articles)}):",
    ]
    if not articles:
        lines.append("- No recent articles were found for this ticker.")
    for idx, article in enumerate(articles, start=1):
        title = str(article.get("title") or "Untitled").strip()
        source = str(article.get("source") or article.get("provider") or "source").strip()
        published = str(article.get("published_at") or "").strip()
        url = str(article.get("url") or "").strip()
        summary = limit_text(article.get("summary") or "", max_chars=420, label=f"{ticker} article summary")
        article_text = limit_text(
            article.get("article_text") or "",
            max_chars=per_article_budget,
            label=f"{ticker} article text",
        )
        lines.extend(
            [
                f"{idx}. {title}",
                f"   Source: {source}",
                f"   Published: {published or 'unknown'}",
                f"   URL: {url or 'unavailable'}",
                f"   Summary: {summary or 'none'}",
            ]
        )
        if article_text:
            lines.append(f"   Article text excerpt: {article_text}")
    return limit_text("\n".join(lines).strip(), max_chars=max_chars, label=f"{ticker} ticker packet")


def build_openai_final_prompt(
    *,
    ticker_contexts: list[dict[str, Any]],
    generated_at: str,
    max_chars: int,
) -> str:
    clean_items = [item for item in ticker_contexts if isinstance(item, dict)]
    available = max(6000, int(max_chars or 180000))
    per_ticker_budget = max(1200, min(9000, (available - 4000) // max(1, len(clean_items))))
    sections = [_ticker_prompt_section(item, max_chars=per_ticker_budget) for item in clean_items]
    prompt = f"""Read all collected ticker news and same-day tape below, then produce one final daily trading-news review.

Generated at: {generated_at}

Required sections:
1. PRIMARY MARKET DRIVER
Identify the dominant cross-ticker force behind today's activity.

2. TICKER RATINGS
Rate every reviewed ticker from 0-100 for predictable positive-return potential today. Include direction, rating, confidence, main driver, and the strongest confirming evidence.

3. BEST POSITIVE-RETURN CANDIDATES
Rank up to 8 tickers with the highest probability of a predictable positive return today. For each: ticker, edge rating, driver, confirming data/news, invalidation risk.

4. NASDAQ BREADTH ETF CHECK
Infer broad Nasdaq pressure from the reviewed ticker packet only. If most reviewed Nasdaq-heavy tickers show bearish news/tape, include SQQQ as the preferred Nasdaq downside proxy candidate. If most reviewed Nasdaq-heavy tickers show bullish news/tape, include TQQQ as the preferred Nasdaq upside proxy candidate. If the packet is mixed, thin, or not Nasdaq-heavy enough, say "no leveraged Nasdaq ETF signal." For SQQQ/TQQQ, include direction, evidence count, confidence, and a clear daily-reset/leveraged-ETF risk note.

5. DOWNTREND / AVOID LIST
List tickers where news plus tape suggest downside pressure or poor predictability.

6. NO-TRADE / INSUFFICIENT EVIDENCE
Name important tickers where evidence is too thin or contradictory.

7. EXECUTION NOTES
State practical risk gates: recency, market session timing, liquidity, and what would invalidate the setup.

Keep it decisive, but do not guarantee returns or claim trade execution.

Collected ticker packet:
{"\n\n".join(sections)}"""
    return limit_text(prompt, max_chars=available, label="combined OpenAI news prompt")


def run_news_workflow(
    *,
    tickers: list[str],
    config: AssistantNewsWorkflowConfig,
    output_dir: Optional[Path] = None,
    progress_callback: Optional[ProgressCallback] = None,
    stop_event: Optional[threading.Event] = None,
) -> dict[str, Any]:
    _raise_if_stopped(stop_event)
    generated_at = utc_now_iso()
    clean_tickers = parse_ticker_symbols(tickers, max_symbols=100)
    if not clean_tickers:
        raise ValueError("No valid tickers supplied.")
    if not str(config.openai_api_key or "").strip():
        raise RuntimeError("OPENAI_API_KEY is not configured on the server.")

    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)

    def progress(**payload: Any) -> None:
        _raise_if_stopped(stop_event)
        if progress_callback:
            progress_callback({"ts": utc_now_iso(), **payload})

    headers = {"User-Agent": USER_AGENT, "Accept": "*/*"}
    timeout = httpx.Timeout(config.request_timeout)
    ticker_contexts: list[dict[str, Any]] = []
    started = time.time()

    with httpx.Client(timeout=timeout, headers=headers, follow_redirects=True) as client:
        for index, symbol in enumerate(clean_tickers, start=1):
            _raise_if_stopped(stop_event)
            progress(
                stage="collecting",
                message=f"Collecting news and market data for {symbol}",
                current_ticker=symbol,
                completed=index - 1,
                total=len(clean_tickers),
            )
            ticker_context = fetch_ticker_context(client, symbol, config)
            _raise_if_stopped(stop_event)
            visible_notes = build_visible_ticker_notes(ticker_context)
            _raise_if_stopped(stop_event)

            ticker_contexts.append(
                {
                    "ticker": symbol,
                    "status": "collected",
                    "market": ticker_context.get("market") or {},
                    "articles": ticker_context.get("articles") or [],
                    "source_note": "Collected for the combined OpenAI final review.",
                }
            )
            progress(
                stage="ticker_collected",
                message=f"Collected news and market data {index}/{len(clean_tickers)} for {symbol}",
                current_ticker=symbol,
                completed=index,
                total=len(clean_tickers),
                prompt_index=index,
                prompt_total=len(clean_tickers),
                model=config.model,
                model_status="collected",
                article_count=visible_notes.get("article_count"),
                article_notes=visible_notes.get("article_notes"),
                market_note=visible_notes.get("market_note"),
            )

    combined_markdown = build_combined_news_markdown(
        generated_at=generated_at,
        model=config.model,
        ticker_contexts=ticker_contexts,
    )
    _raise_if_stopped(stop_event)
    final_prompt = build_openai_final_prompt(
        ticker_contexts=ticker_contexts,
        generated_at=generated_at,
        max_chars=config.max_input_chars,
    )
    progress(
        stage="packet_ready",
        message=f"Built one combined source packet for {len(clean_tickers)} tickers",
        completed=len(clean_tickers),
        total=len(clean_tickers),
        model=config.model,
        prompt_chars=len(final_prompt),
    )
    files: dict[str, str] = {}
    if output_dir is not None:
        impressions_md = output_dir / "impressions.md"
        impressions_json = output_dir / "impressions.json"
        news_packet_md = output_dir / "news_packet.md"
        news_packet_json = output_dir / "news_packet.json"
        impressions_md.write_text(combined_markdown, encoding="utf-8")
        news_packet_md.write_text(combined_markdown, encoding="utf-8")
        packet_json = json.dumps(
            {
                "generated_at": generated_at,
                "model": config.model,
                "tickers": clean_tickers,
                "max_input_chars": config.max_input_chars,
                "ticker_contexts": ticker_contexts,
            },
            indent=2,
        )
        impressions_json.write_text(packet_json, encoding="utf-8")
        news_packet_json.write_text(packet_json, encoding="utf-8")
        files["impressions_md"] = str(impressions_md)
        files["impressions_json"] = str(impressions_json)
        files["news_packet_md"] = str(news_packet_md)
        files["news_packet_json"] = str(news_packet_json)

    progress(
        stage="final_ai",
        message=f"Sending one combined {len(clean_tickers)}-ticker packet to OpenAI",
        completed=len(clean_tickers),
        total=len(clean_tickers),
        model=config.model,
        prompt_chars=len(final_prompt),
    )
    try:
        final_summary = openai_responses_call(
            config=config,
            user_prompt=final_prompt,
            stop_event=stop_event,
        )
        final_status = "ok"
    except WorkflowStopped:
        raise
    except Exception as exc:
        final_status = "error"
        final_summary = f"FINAL_OPENAI_ERROR: {exc}\n\nThe collected ticker news packet was still saved."

    if output_dir is not None:
        final_md = output_dir / "final_summary.md"
        final_md.write_text(final_summary.strip() + "\n", encoding="utf-8")
        files["final_summary_md"] = str(final_md)

    elapsed = round(time.time() - started, 2)
    progress(stage="complete", message="Workflow complete", completed=len(clean_tickers), total=len(clean_tickers))
    return {
        "generated_at": generated_at,
        "elapsed_sec": elapsed,
        "model": config.model,
        "tickers": clean_tickers,
        "articles_per_ticker": config.articles_per_ticker,
        "final_status": final_status,
        "final_summary": final_summary,
        "combined_impressions": combined_markdown,
        "model_journal": "",
        "impressions": ticker_contexts,
        "files": files,
    }
