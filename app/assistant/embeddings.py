"""
Embedding generation for semantic memory
Uses sentence-transformers for local CPU inference
"""

from __future__ import annotations

import hashlib
import pickle
from pathlib import Path
from typing import Any, List, Optional

try:
    from sentence_transformers import SentenceTransformer
    EMBEDDINGS_AVAILABLE = True
except ImportError:
    SentenceTransformer = None
    EMBEDDINGS_AVAILABLE = False

import numpy as np


class EmbeddingGenerator:
    """
    Generates embeddings for text using local sentence-transformers model
    Model: all-MiniLM-L6-v2 (384 dimensions, fast, good quality)
    """

    def __init__(self, model_name: str = "all-MiniLM-L6-v2", cache_dir: Optional[Path] = None):
        if not EMBEDDINGS_AVAILABLE:
            raise RuntimeError(
                "sentence-transformers not installed. "
                "Install with: pip install sentence-transformers"
            )

        self.model_name = model_name
        self.cache_dir = cache_dir
        self._model: Optional[SentenceTransformer] = None
        self._cache: dict[str, np.ndarray] = {}

    @property
    def model(self) -> SentenceTransformer:
        """Lazy load model on first use"""
        if self._model is None:
            self._model = SentenceTransformer(self.model_name)
        return self._model

    def _cache_key(self, text: str) -> str:
        """Generate cache key from text"""
        return hashlib.sha256(text.encode('utf-8')).hexdigest()[:16]

    def embed(self, text: str, use_cache: bool = True) -> np.ndarray:
        """
        Generate embedding for text

        Args:
            text: Input text to embed
            use_cache: Whether to use cached embeddings

        Returns:
            numpy array of shape (384,)
        """
        if not text or not text.strip():
            # Return zero vector for empty text
            return np.zeros(384, dtype=np.float32)

        cache_key = self._cache_key(text)

        if use_cache and cache_key in self._cache:
            return self._cache[cache_key]

        # Generate embedding
        embedding = self.model.encode(text, convert_to_numpy=True, show_progress_bar=False)

        # Cache result
        if use_cache:
            self._cache[cache_key] = embedding

        return embedding

    def embed_batch(self, texts: List[str], use_cache: bool = True) -> np.ndarray:
        """
        Generate embeddings for multiple texts

        Args:
            texts: List of input texts
            use_cache: Whether to use cached embeddings

        Returns:
            numpy array of shape (len(texts), 384)
        """
        if not texts:
            return np.zeros((0, 384), dtype=np.float32)

        embeddings = []
        texts_to_embed = []
        indices_to_embed = []

        for i, text in enumerate(texts):
            if not text or not text.strip():
                embeddings.append(np.zeros(384, dtype=np.float32))
                continue

            cache_key = self._cache_key(text)
            if use_cache and cache_key in self._cache:
                embeddings.append(self._cache[cache_key])
            else:
                embeddings.append(None)
                texts_to_embed.append(text)
                indices_to_embed.append(i)

        # Batch generate uncached embeddings
        if texts_to_embed:
            batch_embeddings = self.model.encode(
                texts_to_embed,
                convert_to_numpy=True,
                show_progress_bar=False,
                batch_size=32
            )

            for idx, text, embedding in zip(indices_to_embed, texts_to_embed, batch_embeddings):
                embeddings[idx] = embedding
                if use_cache:
                    cache_key = self._cache_key(text)
                    self._cache[cache_key] = embedding

        return np.array(embeddings)

    def similarity(self, embedding1: np.ndarray, embedding2: np.ndarray) -> float:
        """
        Calculate cosine similarity between two embeddings

        Returns:
            Similarity score between -1 and 1
        """
        return float(np.dot(embedding1, embedding2) / (np.linalg.norm(embedding1) * np.linalg.norm(embedding2)))

    def save_cache(self, path: Path) -> None:
        """Save embedding cache to disk"""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'wb') as f:
            pickle.dump(self._cache, f)

    def load_cache(self, path: Path) -> None:
        """Load embedding cache from disk"""
        if not path.exists():
            return
        try:
            with open(path, 'rb') as f:
                self._cache = pickle.load(f)
        except Exception:
            self._cache = {}


# Global singleton instance
_EMBEDDING_GENERATOR: Optional[EmbeddingGenerator] = None


def get_embedding_generator(cache_dir: Optional[Path] = None) -> EmbeddingGenerator:
    """Get or create global embedding generator instance"""
    global _EMBEDDING_GENERATOR
    if _EMBEDDING_GENERATOR is None:
        _EMBEDDING_GENERATOR = EmbeddingGenerator(cache_dir=cache_dir)
        if cache_dir:
            cache_file = cache_dir / "embedding_cache.pkl"
            _EMBEDDING_GENERATOR.load_cache(cache_file)
    return _EMBEDDING_GENERATOR
