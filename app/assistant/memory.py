"""
Memory management system with semantic search
Integrates embeddings and vector store for context-aware AI
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, List, Optional, Tuple

from .embeddings import get_embedding_generator
from .vector_store import VectorStore


class MemoryManager:
    """
    Manages long-term memory for the AI assistant
    Stores conversations, events, analyses with semantic search
    """

    def __init__(self, db_path: Path, memory_dir: Path):
        self.db_path = db_path
        self.memory_dir = memory_dir
        self.memory_dir.mkdir(parents=True, exist_ok=True)

        # Lazy initialization to avoid blocking startup
        self._embedder: Optional[Any] = None
        self._vector_store: Optional[VectorStore] = None

    @property
    def embedder(self) -> Any:
        """Lazy load embedder on first use"""
        if self._embedder is None:
            self._embedder = get_embedding_generator(cache_dir=self.memory_dir)
        return self._embedder

    @property
    def vector_store(self) -> VectorStore:
        """Lazy load vector store on first use"""
        if self._vector_store is None:
            self._vector_store = VectorStore(self.db_path)
        return self._vector_store

    def store_conversation(
        self,
        user_message: str,
        assistant_response: str,
        context: Optional[dict[str, Any]] = None
    ) -> int:
        """
        Store a conversation exchange

        Args:
            user_message: User's question/prompt
            assistant_response: AI's response
            context: Optional context dict

        Returns:
            Memory ID
        """
        # Combine user and assistant for better context
        full_text = f"User: {user_message}\nAssistant: {assistant_response}"
        embedding = self.embedder.embed(full_text)

        metadata = {
            "type": "conversation",
            "user_message": user_message,
            "assistant_response": assistant_response,
            "context": context or {}
        }

        return self.vector_store.add(
            content_text=full_text,
            embedding=embedding,
            content_type="conversation",
            metadata=metadata
        )

    def store_event(
        self,
        event_type: str,
        description: str,
        tickers: Optional[List[str]] = None,
        data: Optional[dict[str, Any]] = None
    ) -> int:
        """
        Store an event

        Args:
            event_type: Type of event (signal_flip, portfolio_swing, etc)
            description: Human-readable description
            tickers: Related tickers
            data: Event data

        Returns:
            Memory ID
        """
        embedding = self.embedder.embed(description)

        metadata = {
            "type": "event",
            "event_type": event_type,
            "tickers": tickers or [],
            "data": data or {}
        }

        return self.vector_store.add(
            content_text=description,
            embedding=embedding,
            content_type="event",
            metadata=metadata
        )

    def store_analysis(
        self,
        analysis_text: str,
        event_id: Optional[int] = None,
        model_used: Optional[str] = None,
        recommendations: Optional[List[str]] = None
    ) -> int:
        """
        Store an AI analysis

        Args:
            analysis_text: The analysis content
            event_id: Related event ID if any
            model_used: Which model generated this
            recommendations: List of recommendations

        Returns:
            Memory ID
        """
        embedding = self.embedder.embed(analysis_text)

        metadata = {
            "type": "analysis",
            "event_id": event_id,
            "model_used": model_used,
            "recommendations": recommendations or []
        }

        return self.vector_store.add(
            content_text=analysis_text,
            embedding=embedding,
            content_type="analysis",
            metadata=metadata
        )

    def search(
        self,
        query: str,
        content_type: Optional[str] = None,
        limit: int = 5,
        min_similarity: float = 0.3,
        max_age_days: Optional[int] = None
    ) -> List[Tuple[int, str, dict[str, Any], float]]:
        """
        Search memory by semantic similarity

        Args:
            query: Search query
            content_type: Filter by type (conversation, event, analysis)
            limit: Max results
            min_similarity: Minimum similarity threshold (0-1)
            max_age_days: Only search recent memory

        Returns:
            List of (id, text, metadata, similarity)
        """
        query_embedding = self.embedder.embed(query)

        max_age_seconds = None
        if max_age_days:
            max_age_seconds = max_age_days * 86400

        return self.vector_store.search(
            query_embedding=query_embedding,
            content_type=content_type,
            limit=limit,
            min_similarity=min_similarity,
            max_age_seconds=max_age_seconds
        )

    def get_recent_context(
        self,
        hours: int = 24,
        content_type: Optional[str] = None,
        limit: int = 20
    ) -> List[dict[str, Any]]:
        """
        Get recent memory items for context building

        Args:
            hours: How many hours back to look
            content_type: Filter by type
            limit: Max items to return

        Returns:
            List of memory items with metadata
        """
        # This uses time-based retrieval instead of semantic search
        # Useful for building chronological context
        max_age = hours * 3600

        # Search with empty query to get recent items
        results = self.vector_store.search(
            query_embedding=self.embedder.embed(""),
            content_type=content_type,
            limit=limit,
            max_age_seconds=max_age,
            min_similarity=0.0
        )

        return [
            {
                "id": r[0],
                "text": r[1],
                "metadata": r[2],
                "similarity": r[3]
            }
            for r in results
        ]

    def cleanup_old(self, days: int = 90) -> int:
        """
        Remove old memory to keep storage manageable

        Args:
            days: Keep only memory newer than this

        Returns:
            Number of items deleted
        """
        return self.vector_store.delete_old(days * 86400)

    def get_stats(self) -> dict[str, Any]:
        """Get memory statistics"""
        return {
            "total_items": self.vector_store.count(),
            "conversations": self.vector_store.count("conversation"),
            "events": self.vector_store.count("event"),
            "analyses": self.vector_store.count("analysis")
        }
