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


def _deterministic_embedding_768(text: str) -> List[float]:
    """Create a deterministic 768-dim embedding from input text.

    Implementation detail:
    - Uses repeated SHA256 digests to derive pseudo-random but deterministic values.
    - Produces values in [0, 1]. Suitable for testing and schema validation only.
    - Replace with a real embedding model in production.
    """
    target_dim = 768
    seed = text.encode("utf-8")
    values: List[float] = []
    counter = 0
    while len(values) < target_dim:
        h = hashlib.sha256(seed + counter.to_bytes(4, "big")).digest()
        # Use bytes to make 8 floats per digest chunk of 4 bytes each
        for i in range(0, len(h), 4):
            if len(values) >= target_dim:
                break
            chunk = h[i:i+4]
            # Convert 4 bytes to int and scale to [0,1]
            val = int.from_bytes(chunk, "big") / 0xFFFFFFFF
            values.append(val)
        counter += 1
    return values


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
    """Ingest a single document chunk into the vector store and emit operational events.

    Fields:
    - name: original filename or identifier (logged, stored in audit)
    - doc_id: logical grouping id for a document, defaults to sanitized name if not provided
    - section/page_no/chunk_id: optional attributes for provenance and ordering
    """
    logger.info("Ingesting document: name='%s' doc_id='%s' length=%s", name, doc_id or name, len(content))

    embedding = _deterministic_embedding_768(content)
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

    # Emit Service Bus event
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
