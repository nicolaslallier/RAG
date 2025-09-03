"""
Document ingestion utilities.

Audience: Solution Architects & Developers
- Purpose: Provide a high-level ingestion flow that accepts a document payload,
  generates an embedding (placeholder implementation), stores it into the vector table,
  writes an audit record, emits a Service Bus event, and logs details to console.
- Note: The embedding function is a deterministic placeholder. Replace with a real
  model-based embedding generator when integrating an ML/AI component.
"""

import hashlib
import logging
import os
from typing import Any, Dict, Optional, Tuple, List

from app.db_utils import insert_document, insert_ingestion_audit
from app.service_bus import send_topic_message


logger = logging.getLogger(__name__)


# ---------------- Embeddings ----------------

def _deterministic_embedding_768(text: str) -> List[float]:
    """Create a deterministic 768-dim embedding from input text."""
    target_dim = 768
    seed = text.encode("utf-8")
    values: List[float] = []
    counter = 0
    while len(values) < target_dim:
        h = hashlib.sha256(seed + counter.to_bytes(4, "big")).digest()
        for i in range(0, len(h), 4):
            if len(values) >= target_dim:
                break
            chunk = h[i:i+4]
            val = int.from_bytes(chunk, "big") / 0xFFFFFFFF
            values.append(val)
        counter += 1
    return values


def embed_passage(text: str) -> List[float]:
    """Embed a passage (chunk) – placeholder implementation.

    Mirrors the pattern: embedder.encode([f"passage: {c}"]).
    """
    return _deterministic_embedding_768(f"passage: {text}")


def embed_query(text: str) -> List[float]:
    """Embed a query – placeholder implementation.

    Mirrors the pattern: embedder.encode([f"query: {q}"]).
    """
    return _deterministic_embedding_768(f"query: {text}")


# ---------------- Chunking ----------------

def chunk_text(text: str, max_chars: int = 900, overlap: int = 150) -> List[str]:
    """Chunk text into overlapping windows suitable for embedding.

    This keeps chunks below ~1 MB so FTS indexing won't overflow tsvector limits.
    """
    t = " ".join(text.split())
    chunks: List[str] = []
    start = 0
    while start < len(t):
        end = min(start + max_chars, len(t))
        chunks.append(t[start:end])
        start = end - overlap
        if start < 0:
            start = 0
        if end == len(t):
            break
    return chunks


def extract_pdf_pages(file_bytes: bytes) -> List[tuple[int, str]]:
    """Extract text per-page from a PDF byte stream.

    Falls back gracefully if extraction fails.
    """
    try:
        from pypdf import PdfReader  # imported lazily
        import io
        reader = PdfReader(io.BytesIO(file_bytes))
        pages: List[tuple[int, str]] = []
        for i, page in enumerate(reader.pages, start=1):
            try:
                txt = page.extract_text() or ""
            except Exception:
                txt = ""
            pages.append((i, txt))
        return pages
    except Exception:
        # If not a valid PDF, treat as a single-page text
        text = file_bytes.decode("utf-8", errors="ignore")
        return [(1, text)]


# ---------------- Ingestion ----------------

def ingest_document(
    name: str,
    content: str,
    metadata: Optional[Dict[str, Any]] = None,
    *,
    doc_id: Optional[str] = None,
    section: Optional[str] = None,
    page_no: Optional[int] = None,
    chunk_id: Optional[int] = None,
) -> Dict[str, Any]:
    """Ingest a single document chunk into the vector store and emit operational events."""
    logger.info("Ingesting document: name='%s' doc_id='%s' length=%s", name, doc_id or name, len(content))

    embedding = embed_passage(content)
    new_id = insert_document(
        doc_id=doc_id or name,
        section=section,
        page_no=page_no,
        chunk_id=chunk_id,
        content=content,
        embedding=embedding,
        metadata=metadata,
    )

    audit_id = insert_ingestion_audit(
        name=name,
        status="ingested",
        detail="Document chunk stored with vector embedding",
        content_length=len(content),
        metadata={**(metadata or {}), "doc_id": doc_id or name, "section": section, "page_no": page_no, "chunk_id": chunk_id},
    )

    namespace = os.getenv("SB_NAMESPACE", "sbc-jarvis-cac-prd.servicebus.windows.net")
    topic = os.getenv("SB_TOPIC_NAME", "sbt-jarvis")
    send_topic_message(namespace, topic, {
        "event": "document_ingested",
        "name": name,
        "doc_id": doc_id or name,
        "row_id": new_id,
        "audit_id": audit_id,
        "section": section,
        "page_no": page_no,
        "chunk_id": chunk_id,
    })

    logger.info("✅ Ingested '%s' (doc_id=%s, row_id=%s, audit_id=%s, section=%s, page_no=%s, chunk_id=%s)",
                name, doc_id or name, new_id, audit_id, section, page_no, chunk_id)

    return {
        "row_id": new_id,
        "audit_id": audit_id,
        "name": name,
        "doc_id": doc_id or name,
        "section": section,
        "page_no": page_no,
        "chunk_id": chunk_id,
    }
