"""
Ollama API integration with model routing
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Optional

import httpx


class OllamaClient:
    """
    Client for Ollama API with streaming and model routing
    """

    def __init__(
        self,
        base_url: str = "http://localhost:11434",
        default_model: str = "qwen2.5:14b",
        *,
        num_gpu: Optional[int] = None,
        keep_alive: Optional[str] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.default_model = default_model
        if num_gpu is None:
            try:
                num_gpu = int(os.getenv("ASSISTANT_OLLAMA_NUM_GPU", "0"))
            except Exception:
                num_gpu = 0
        self.num_gpu = max(0, int(num_gpu))
        self.keep_alive = str(keep_alive or os.getenv("ASSISTANT_OLLAMA_KEEP_ALIVE", "0s") or "0s")

    def chat(
        self,
        messages: List[Dict[str, str]],
        model: Optional[str] = None,
        stream: bool = False,
        timeout: float = 120.0
    ) -> Dict[str, Any]:
        """
        Send chat request to Ollama

        Args:
            messages: List of message dicts with 'role' and 'content'
            model: Model to use (default: self.default_model)
            stream: Whether to stream response
            timeout: Request timeout in seconds

        Returns:
            Response dict with 'message' containing 'content'
        """
        model = model or self.default_model

        payload: Dict[str, Any] = {
            "model": model,
            "messages": messages,
            "stream": stream,
            "keep_alive": self.keep_alive,
            "options": {"num_gpu": self.num_gpu},
        }

        response = httpx.post(
            f"{self.base_url}/api/chat",
            json=payload,
            timeout=timeout
        )

        response.raise_for_status()
        return response.json()

    def generate(
        self,
        prompt: str,
        model: Optional[str] = None,
        system: Optional[str] = None,
        timeout: float = 120.0
    ) -> str:
        """
        Generate completion (non-chat interface)

        Args:
            prompt: User prompt
            model: Model to use
            system: System prompt
            timeout: Request timeout

        Returns:
            Generated text
        """
        model = model or self.default_model

        payload: Dict[str, Any] = {
            "model": model,
            "prompt": prompt,
            "stream": False,
            "keep_alive": self.keep_alive,
            "options": {"num_gpu": self.num_gpu},
        }

        if system:
            payload["system"] = system

        response = httpx.post(
            f"{self.base_url}/api/generate",
            json=payload,
            timeout=timeout
        )

        response.raise_for_status()
        data = response.json()
        return data.get("response", "")

    def list_models(self) -> List[str]:
        """List available models"""
        allow_server_probe = str(os.getenv("ASSISTANT_OLLAMA_ALLOW_SERVER_PROBE", "0")).strip().lower()
        if allow_server_probe not in ("1", "true", "yes", "on"):
            return []
        try:
            response = httpx.get(f"{self.base_url}/api/tags", timeout=10.0)
            response.raise_for_status()
            data = response.json()
            return [m["name"] for m in data.get("models", [])]
        except Exception:
            return []


class ModelRouter:
    """
    Routes queries to appropriate models based on complexity
    """

    def __init__(self, ollama_client: OllamaClient):
        self.client = ollama_client

        # Default every tier to the configured local model. Per-tier overrides
        # can be supplied when multiple Ollama models are intentionally installed.
        self.models = {
            "fast": os.getenv("OLLAMA_MODEL_FAST", self.client.default_model),
            "balanced": os.getenv("OLLAMA_MODEL_BALANCED", self.client.default_model),
            "strategic": os.getenv("OLLAMA_MODEL_STRATEGIC", self.client.default_model),
            "deep": os.getenv("OLLAMA_MODEL_DEEP", self.client.default_model),
        }

    def select_model(self, query_type: str, context_size: int = 0) -> str:
        """
        Select appropriate model based on query type and context

        Args:
            query_type: Type of query (quick, analysis, strategic, diagnostic)
            context_size: Rough size of context in characters

        Returns:
            Model name to use
        """
        # Quick queries use fast model
        if query_type in ("quick", "status", "simple"):
            return self.models.get("fast", self.client.default_model)

        # Large context needs efficient model
        if context_size > 50000:
            return self.models.get("strategic", self.client.default_model)

        # Strategic analysis uses larger model
        if query_type in ("strategic", "portfolio_review", "root_cause"):
            return self.models.get("strategic", self.client.default_model)

        # Deep reasoning for complex problems
        if query_type in ("deep", "diagnostic", "complex"):
            return self.models.get("deep", self.client.default_model)

        # Default balanced model
        return self.models.get("balanced", self.client.default_model)

    def chat(
        self,
        messages: List[Dict[str, str]],
        query_type: str = "balanced",
        model_override: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Route chat request to appropriate model

        Args:
            messages: Chat messages
            query_type: Type of query for routing
            model_override: Force specific model

        Returns:
            Response from Ollama
        """
        if model_override:
            model = model_override
        else:
            # Estimate context size
            context_size = sum(len(m.get("content", "")) for m in messages)
            model = self.select_model(query_type, context_size)

        print(f"[ModelRouter] Using model: {model} (type: {query_type})")

        return self.client.chat(messages, model=model)


class AnalysisGenerator:
    """
    High-level API for generating AI analyses
    """

    def __init__(self, ollama_client: OllamaClient, memory_manager: Optional[Any] = None):
        self.client = ollama_client
        self.router = ModelRouter(ollama_client)
        self.memory = memory_manager

    def analyze_event(
        self,
        event: Dict[str, Any],
        context: Dict[str, Any],
        system_prompt: str
    ) -> Dict[str, Any]:
        """
        Generate analysis for an event

        Returns:
            Dict with 'analysis', 'model_used', 'duration_sec'
        """
        from .prompts import prompt_event_analysis, compress_context_for_model

        # Determine query type based on event
        event_type = event.get("event_type", "")
        severity = event.get("severity", "info")

        if "crash" in event_type or severity == "critical":
            query_type = "diagnostic"
        elif "consensus" in event_type or "regime" in event_type:
            query_type = "strategic"
        else:
            query_type = "analysis"

        # Build prompt
        user_prompt = prompt_event_analysis(event, context)

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]

        # Generate
        start = time.time()
        response = self.router.chat(messages, query_type=query_type)
        duration = time.time() - start

        analysis_text = response.get("message", {}).get("content", "")
        model_used = response.get("model", "unknown")

        # Store in memory
        if self.memory and analysis_text:
            self.memory.store_analysis(
                analysis_text=analysis_text,
                event_id=event.get("event_id"),
                model_used=model_used
            )

        return {
            "analysis": analysis_text,
            "model_used": model_used,
            "duration_sec": duration,
            "timestamp": int(time.time())
        }

    def chat_query(
        self,
        user_message: str,
        context: Dict[str, Any],
        system_prompt: str,
        query_type: str = "balanced"
    ) -> Dict[str, Any]:
        """
        Handle user chat query

        Returns:
            Dict with 'response', 'model_used', 'duration_sec'
        """
        import json

        # Add context to user message
        context_json = json.dumps(context, indent=2)
        full_prompt = f"{user_message}\n\nContext:\n{context_json}"

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": full_prompt}
        ]

        # Generate
        start = time.time()
        response = self.router.chat(messages, query_type=query_type)
        duration = time.time() - start

        response_text = response.get("message", {}).get("content", "")
        model_used = response.get("model", "unknown")

        # Store conversation in memory
        if self.memory and response_text:
            self.memory.store_conversation(
                user_message=user_message,
                assistant_response=response_text,
                context=context
            )

        return {
            "response": response_text,
            "model_used": model_used,
            "duration_sec": duration,
            "timestamp": int(time.time())
        }
