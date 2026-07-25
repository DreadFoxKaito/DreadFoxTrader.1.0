"""
Strategic AI Agent - ZENKO PRIME
Ties together memory, context, and Ollama for intelligent analysis
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional

from .context_builder import ContextBuilder
from .memory import MemoryManager
from .model_pipeline import AnalysisGenerator, OllamaClient
from .prompts import system_prompt_zenko


class StrategicAgent:
    """
    Main AI assistant agent
    Combines memory, context building, and Ollama generation
    """

    def __init__(
        self,
        db_path: Path,
        runs_dir: Path,
        memory_dir: Path,
        ollama_url: Optional[str] = None,
        default_model: Optional[str] = None
    ):
        self.db_path = db_path
        self.runs_dir = runs_dir
        self.memory_dir = memory_dir

        # Initialize components
        self.memory = MemoryManager(db_path, memory_dir)
        self.context_builder = ContextBuilder(db_path, runs_dir, self.memory)

        ollama_url = ollama_url or os.getenv("OLLAMA_URL", "http://localhost:11434")
        default_model = default_model or os.getenv("OLLAMA_MODEL", "qwen2.5:14b")

        self.ollama = OllamaClient(base_url=ollama_url, default_model=default_model)
        self.generator = AnalysisGenerator(self.ollama, self.memory)

        self.system_prompt = system_prompt_zenko()

    def analyze_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze a specific event

        Args:
            event: Event dict

        Returns:
            Analysis dict
        """
        # Build context with memory search
        event_desc = event.get("description", "")
        context = self.context_builder.build_full_context(
            include_portfolio=True,
            include_cryptids=True,
            include_indicators=False,  # Too expensive for events
            include_memory=True,
            memory_query=event_desc,
            max_memory_items=5
        )

        # Generate analysis
        return self.generator.analyze_event(event, context, self.system_prompt)

    def chat(
        self,
        user_message: str,
        include_portfolio: bool = True,
        include_cryptids: bool = True,
        include_indicators: bool = False,
        query_type: str = "balanced"
    ) -> Dict[str, Any]:
        """
        Handle user chat query

        Args:
            user_message: User's question
            include_portfolio: Include portfolio data
            include_cryptids: Include cryptid status
            include_indicators: Include technical indicators (expensive)
            query_type: Routing hint (quick, balanced, strategic, deep)

        Returns:
            Response dict with 'response', 'model_used', 'context_summary'
        """
        # Build context with memory search
        context = self.context_builder.build_full_context(
            include_portfolio=include_portfolio,
            include_cryptids=include_cryptids,
            include_indicators=include_indicators,
            include_memory=True,
            memory_query=user_message,
            max_memory_items=5
        )

        # Generate response
        result = self.generator.chat_query(
            user_message=user_message,
            context=context,
            system_prompt=self.system_prompt,
            query_type=query_type
        )

        # Add context summary
        result["context_summary"] = {
            "portfolio_included": include_portfolio,
            "cryptids_included": include_cryptids,
            "indicators_included": include_indicators,
            "memory_items": len(context.get("relevant_memory", [])),
            "num_positions": self._count_positions(context),
            "num_cryptids": len(context.get("cryptids", []))
        }

        return result

    def portfolio_review(self) -> Dict[str, Any]:
        """
        Generate comprehensive portfolio review

        Returns:
            Analysis dict
        """
        from .prompts import prompt_portfolio_review

        # Full context
        context = self.context_builder.build_full_context(
            include_portfolio=True,
            include_cryptids=True,
            include_indicators=False,
            include_memory=True,
            memory_query="portfolio performance issues opportunities",
            max_memory_items=10
        )

        # Build prompt
        user_prompt = prompt_portfolio_review(context)

        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": user_prompt}
        ]

        # Generate with strategic model
        import time
        start = time.time()
        response = self.generator.router.chat(messages, query_type="strategic")
        duration = time.time() - start

        analysis_text = response.get("message", {}).get("content", "")
        model_used = response.get("model", "unknown")

        # Store in memory
        if analysis_text:
            self.memory.store_analysis(
                analysis_text=analysis_text,
                model_used=model_used,
                recommendations=[]
            )

        return {
            "analysis": analysis_text,
            "model_used": model_used,
            "duration_sec": duration,
            "timestamp": int(time.time())
        }

    def explain_ticker(self, ticker: str) -> Dict[str, Any]:
        """
        Explain what's happening with a specific ticker

        Args:
            ticker: Ticker symbol

        Returns:
            Analysis dict
        """
        from .prompts import prompt_ticker_explanation

        # Build context focused on this ticker
        context = self.context_builder.build_full_context(
            include_portfolio=True,
            include_cryptids=True,
            include_indicators=True,  # Worth it for single ticker
            include_memory=True,
            memory_query=f"{ticker} price movement signals",
            max_memory_items=5
        )

        user_prompt = prompt_ticker_explanation(ticker, context)

        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": user_prompt}
        ]

        import time
        start = time.time()
        response = self.generator.router.chat(messages, query_type="analysis")
        duration = time.time() - start

        analysis_text = response.get("message", {}).get("content", "")

        return {
            "analysis": analysis_text,
            "model_used": response.get("model", "unknown"),
            "duration_sec": duration,
            "ticker": ticker,
            "timestamp": int(time.time())
        }

    def scheduled_summary(self, time_of_day: str = "eod") -> Dict[str, Any]:
        """
        Generate scheduled summary (morning, midday, eod)

        Args:
            time_of_day: One of 'morning', 'midday', 'eod'

        Returns:
            Summary dict
        """
        from .prompts import prompt_scheduled_summary

        # Build context
        context = self.context_builder.build_full_context(
            include_portfolio=True,
            include_cryptids=True,
            include_indicators=False,
            include_memory=True,
            memory_query="portfolio changes today",
            max_memory_items=10
        )

        user_prompt = prompt_scheduled_summary(time_of_day, context)

        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": user_prompt}
        ]

        import time
        start = time.time()
        response = self.generator.router.chat(messages, query_type="balanced")
        duration = time.time() - start

        summary_text = response.get("message", {}).get("content", "")

        # Store in memory
        if summary_text:
            self.memory.store_conversation(
                user_message=f"Scheduled {time_of_day} summary",
                assistant_response=summary_text,
                context={"time_of_day": time_of_day}
            )

        return {
            "summary": summary_text,
            "model_used": response.get("model", "unknown"),
            "duration_sec": duration,
            "time_of_day": time_of_day,
            "timestamp": int(time.time())
        }

    def get_memory_stats(self) -> Dict[str, Any]:
        """Get memory system statistics"""
        return self.memory.get_stats()

    def search_memory(self, query: str, limit: int = 10) -> list:
        """Search memory with semantic similarity"""
        results = self.memory.search(query, limit=limit, max_age_days=90)
        return [
            {
                "text": r[1],
                "metadata": r[2],
                "relevance": r[3]
            }
            for r in results
        ]

    def _count_positions(self, context: Dict[str, Any]) -> int:
        """Count total positions in context"""
        count = 0
        for broker_snap in context.get("portfolio", []):
            for account in broker_snap.get("accounts", []):
                count += len(account.get("positions", []))
        return count
