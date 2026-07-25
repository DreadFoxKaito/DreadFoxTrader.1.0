"""
Prompt templates for AI assistant
"""

from __future__ import annotations

from typing import Any, Dict


def system_prompt_zenko() -> str:
    """Main ZENKO PRIME system prompt"""
    return """You are ZENKO PRIME — a nine-tailed Kitsune spirit embedded inside Dreadfox Trader.

You are ancient: a survivor of war economies, gold empires, market panics, and algorithmic flash crashes.
You are highly intelligent, confident, slightly cocky, ruthlessly concise, and occasionally witty.
You do not ramble. You do not apologize for clarity.

ROLE
You are a local-first trading assistant with advanced context memory. You do NOT execute trades. You do NOT have broker control.
You interpret platform snapshots, analyze patterns, and advise on performance, risk, and operational issues.

WHAT YOU RECEIVE
Context (JSON) is provided with your prompts:
- Portfolio: current holdings, P/L, weights, risk metrics
- Cryptids: running algorithms with signals, status, health
- Indicators: technical indicators for held positions (when included)
- Memory: relevant historical context from past conversations and events
- Events: significant changes that triggered this analysis (when applicable)

Treat Context as the only source of truth. If a field is missing, do not assume it exists.

HARD RULES
- Never claim you placed trades, changed settings, or linked brokers
- Never invent prices, indicators, positions, allocations, or PnL
- Quote exact values when you reference data
- If data is insufficient, say so plainly and request ONE missing item
- Use memory context to inform your analysis (avoid repeating past mistakes)

METHOD
You view markets like thermodynamics:
Capital is energy. Volatility is heat. Liquidity is oxygen. Trend is momentum. Panic is entropy.
Use the metaphor only when it sharpens the point.

When analyzing events or answering queries:
1. Acknowledge what changed or what was asked
2. Provide context from memory if relevant
3. Give clear, actionable insight
4. Warn of risks or anomalies
5. Suggest next steps (reversible actions only)

OUTPUT STYLE (unless user asks otherwise)
Concise, confident, direct; light dry wit allowed.
No generic trading lectures. Assume the user is competent.
Probabilities only; no guarantees.

If data is insufficient:
"The wind carries no scent — insufficient data."
Then ask for the single missing input.

Now answer using the provided Context."""


def prompt_event_analysis(event: Dict[str, Any], context: Dict[str, Any]) -> str:
    """Prompt for analyzing a specific event"""
    import json

    event_desc = event.get("description", "Unknown event")
    event_type = event.get("event_type", "unknown")
    event_data = event.get("data", {})

    context_json = json.dumps(context, indent=2)

    return f"""An event has been detected that requires your analysis:

EVENT: {event_desc}
TYPE: {event_type}
DATA: {json.dumps(event_data, indent=2)}

Current Context:
{context_json}

Provide:
1. What happened and why (root cause if discernible)
2. Immediate implications for the portfolio
3. Risk assessment
4. Recommended actions (if any)

Be concise but thorough."""


def prompt_portfolio_review(context: Dict[str, Any]) -> str:
    """Prompt for general portfolio review"""
    import json

    context_json = json.dumps(context, indent=2)

    return f"""Provide a strategic portfolio review based on current state:

Context:
{context_json}

Analyze:
1. Portfolio health and composition
2. Risk exposure and concentration
3. Cryptid performance and signal quality
4. Key opportunities or concerns
5. Strategic recommendations

Focus on actionable insights."""


def prompt_ticker_explanation(ticker: str, context: Dict[str, Any]) -> str:
    """Prompt for explaining ticker behavior"""
    import json

    context_json = json.dumps(context, indent=2)

    return f"""Explain what's happening with {ticker}:

Context:
{context_json}

Provide:
1. Current position details (if held)
2. Recent price action and indicators
3. Cryptid signals and consensus
4. Why the ticker is behaving this way
5. Strategic outlook

Be specific and data-driven."""


def prompt_cryptid_comparison(context: Dict[str, Any]) -> str:
    """Prompt for comparing cryptid performance"""
    import json

    context_json = json.dumps(context, indent=2)

    return f"""Compare the performance and behavior of running cryptids:

Context:
{context_json}

Analyze:
1. Performance metrics (P/L, trades, signals)
2. Signal quality and consistency
3. Divergences and consensus patterns
4. Operational health
5. Which strategies are working best right now

Provide clear comparative assessment."""


def prompt_scheduled_summary(time_of_day: str, context: Dict[str, Any]) -> str:
    """Prompt for scheduled summaries (morning, midday, EOD)"""
    import json

    context_json = json.dumps(context, indent=2)

    prompts = {
        "morning": "Provide a morning briefing: key positions, overnight changes, market setup for today, and watchpoints.",
        "midday": "Provide a midday check-in: performance so far, signal changes, any emerging patterns or concerns.",
        "eod": "Provide an end-of-day summary: today's P/L, what worked/didn't, key events, and prep for tomorrow."
    }

    instruction = prompts.get(time_of_day, "Provide a portfolio summary.")

    return f"""{instruction}

Context:
{context_json}

Keep it concise but informative."""


def compress_context_for_model(context: Dict[str, Any], max_positions: int = 10) -> Dict[str, Any]:
    """
    Compress context for models with smaller context windows
    Keeps only most important information
    """
    compressed = {
        "generated_at": context.get("generated_at"),
        "timestamp": context.get("timestamp_readable")
    }

    # Compress portfolio (top positions only)
    portfolio = context.get("portfolio", [])
    if portfolio:
        compressed_portfolio = []
        for broker_snap in portfolio[:1]:  # Only first broker
            for account in broker_snap.get("accounts", [])[:1]:  # Only first account
                positions = account.get("positions", [])[:max_positions]  # Top N positions
                compressed_portfolio.append({
                    "broker": broker_snap.get("broker"),
                    "positions": positions
                })
        compressed["portfolio"] = compressed_portfolio

    # Keep full performance and risk metrics (small)
    compressed["portfolio_performance"] = context.get("portfolio_performance", {})
    compressed["risk_metrics"] = context.get("risk_metrics", {})

    # Compress cryptids (running only, no logs)
    cryptids = context.get("cryptids", [])
    if cryptids:
        compressed["cryptids"] = [
            {
                "id": c.get("id"),
                "name": c.get("name"),
                "status": c.get("status"),
                "pnl": c.get("pnl"),
                "signal_counts": c.get("signal_counts")
            }
            for c in cryptids if c.get("status") == "running"
        ][:5]  # Max 5 cryptids

    # Keep signal summary (small)
    compressed["signal_summary"] = context.get("signal_summary", {})

    # Skip indicators (expensive)
    # Keep only top 3 memory items
    memory = context.get("relevant_memory", []) or context.get("recent_memory", [])
    if memory:
        compressed["memory"] = memory[:3]

    return compressed
