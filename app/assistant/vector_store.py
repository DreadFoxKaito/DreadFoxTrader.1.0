"""
Vector storage and similarity search using SQLite
Lightweight alternative to ChromaDB, uses existing database
"""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, List, Optional, Tuple

import numpy as np


class VectorStore:
    """
    SQLite-based vector storage with cosine similarity search
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init_schema()

    def _init_schema(self) -> None:
        """Create vector storage tables"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cur.executescript("""
            CREATE TABLE IF NOT EXISTS assistant_memory_vectors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                content_type TEXT NOT NULL,
                content_text TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                embedding_blob BLOB NOT NULL
            );

            CREATE TABLE IF NOT EXISTS assistant_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                event_uid TEXT,
                event_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                description TEXT,
                tickers TEXT,
                data_json TEXT,
                context_json TEXT,
                ai_analysis_id INTEGER,
                acknowledged INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS assistant_analyses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                event_id INTEGER,
                model_used TEXT,
                prompt_type TEXT,
                analysis_text TEXT,
                reasoning_text TEXT,
                recommendations_json TEXT
            );
        """)

        cur.executescript("""
            CREATE INDEX IF NOT EXISTS idx_assistant_memory_vectors_ts
              ON assistant_memory_vectors(ts);
            CREATE INDEX IF NOT EXISTS idx_assistant_memory_vectors_content_type
              ON assistant_memory_vectors(content_type);

            CREATE INDEX IF NOT EXISTS idx_assistant_events_ts
              ON assistant_events(ts);
            CREATE INDEX IF NOT EXISTS idx_assistant_events_type
              ON assistant_events(event_type);
            CREATE INDEX IF NOT EXISTS idx_assistant_events_severity
              ON assistant_events(severity);
            CREATE INDEX IF NOT EXISTS idx_assistant_events_uid
              ON assistant_events(event_uid);

            CREATE INDEX IF NOT EXISTS idx_assistant_analyses_ts
              ON assistant_analyses(ts);
            CREATE INDEX IF NOT EXISTS idx_assistant_analyses_event
              ON assistant_analyses(event_id);
        """)

        # Lightweight migrations for older local DBs.
        cur.execute("PRAGMA table_info(assistant_events)")
        event_cols = {str(r["name"]) for r in cur.fetchall()}
        if "event_uid" not in event_cols:
            cur.execute("ALTER TABLE assistant_events ADD COLUMN event_uid TEXT")
        if "description" not in event_cols:
            cur.execute("ALTER TABLE assistant_events ADD COLUMN description TEXT")

        conn.commit()
        conn.close()

    def add(
        self,
        content_text: str,
        embedding: np.ndarray,
        content_type: str = "text",
        metadata: Optional[dict[str, Any]] = None
    ) -> int:
        """
        Add a vector to the store

        Args:
            content_text: The text content
            embedding: numpy array of shape (384,)
            content_type: Type of content (text, event, analysis, etc)
            metadata: Optional metadata dict

        Returns:
            ID of inserted record
        """
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        embedding_bytes = embedding.astype(np.float32).tobytes()
        metadata_json = json.dumps(metadata or {})
        ts = int(time.time())

        cur.execute(
            """
            INSERT INTO assistant_memory_vectors
            (ts, content_type, content_text, metadata_json, embedding_blob)
            VALUES (?, ?, ?, ?, ?)
            """,
            (ts, content_type, content_text, metadata_json, embedding_bytes)
        )

        conn.commit()
        vector_id = cur.lastrowid
        conn.close()
        return vector_id

    def search(
        self,
        query_embedding: np.ndarray,
        content_type: Optional[str] = None,
        limit: int = 10,
        min_similarity: float = 0.0,
        max_age_seconds: Optional[int] = None
    ) -> List[Tuple[int, str, dict[str, Any], float]]:
        """
        Search for similar vectors

        Args:
            query_embedding: Query vector of shape (384,)
            content_type: Filter by content type
            limit: Maximum number of results
            min_similarity: Minimum similarity threshold
            max_age_seconds: Only return results newer than this

        Returns:
            List of (id, content_text, metadata, similarity_score)
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Build query
        query = "SELECT id, ts, content_text, metadata_json, embedding_blob FROM assistant_memory_vectors"
        params = []

        conditions = []
        if content_type:
            conditions.append("content_type = ?")
            params.append(content_type)

        if max_age_seconds:
            cutoff = int(time.time()) - max_age_seconds
            conditions.append("ts >= ?")
            params.append(cutoff)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        cur.execute(query, params)
        rows = cur.fetchall()
        conn.close()

        # Calculate similarities
        results = []
        for row in rows:
            embedding = np.frombuffer(row["embedding_blob"], dtype=np.float32)
            similarity = self._cosine_similarity(query_embedding, embedding)

            if similarity >= min_similarity:
                metadata = json.loads(row["metadata_json"] or "{}")
                results.append((
                    int(row["id"]),
                    str(row["content_text"]),
                    metadata,
                    float(similarity)
                ))

        # Sort by similarity and limit
        results.sort(key=lambda x: x[3], reverse=True)
        return results[:limit]

    def _cosine_similarity(self, a: np.ndarray, b: np.ndarray) -> float:
        """Calculate cosine similarity between two vectors"""
        denom = float(np.linalg.norm(a) * np.linalg.norm(b))
        if denom <= 1e-12:
            return 0.0
        return float(np.dot(a, b) / denom)

    def get_by_id(self, vector_id: int) -> Optional[Tuple[str, np.ndarray, dict[str, Any]]]:
        """Retrieve a specific vector by ID"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cur.execute(
            "SELECT content_text, embedding_blob, metadata_json FROM assistant_memory_vectors WHERE id = ?",
            (int(vector_id),)
        )
        row = cur.fetchone()
        conn.close()

        if not row:
            return None

        embedding = np.frombuffer(row["embedding_blob"], dtype=np.float32)
        metadata = json.loads(row["metadata_json"] or "{}")
        return (str(row["content_text"]), embedding, metadata)

    def delete_old(self, max_age_seconds: int) -> int:
        """Delete vectors older than max_age_seconds"""
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        cutoff = int(time.time()) - max_age_seconds

        cur.execute("DELETE FROM assistant_memory_vectors WHERE ts < ?", (cutoff,))
        deleted = cur.rowcount

        conn.commit()
        conn.close()
        return deleted

    def count(self, content_type: Optional[str] = None) -> int:
        """Count vectors in store"""
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        if content_type:
            cur.execute("SELECT COUNT(*) FROM assistant_memory_vectors WHERE content_type = ?", (content_type,))
        else:
            cur.execute("SELECT COUNT(*) FROM assistant_memory_vectors")

        count = cur.fetchone()[0]
        conn.close()
        return int(count)
