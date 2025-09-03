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


def ingest_document(name: str, content: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Ingest a single document into the vector store and emit operational events.

    Steps:
    1) Generate a 768-dim embedding for the content
    2) Insert into `documents` table and return the new document id
    3) Insert into `ingestion_audit` with status "ingested"
    4) Emit a Service Bus event `document_ingested` with the document name
    5) Log ingestion details to console for observability during development/testing
    """
    logger.info("Ingesting document: name='%s' length=%s", name, len(content))

    embedding = _deterministic_embedding_768(content)
    doc_id = insert_document(name=name, content=content, embedding=embedding, metadata=metadata)
    audit_id = insert_ingestion_audit(
        name=name,
        status="ingested",
        detail="Document stored with vector embedding",
        content_length=len(content),
        metadata=metadata,
    )

    # Emit Service Bus event
    namespace = os.getenv("SB_NAMESPACE", "sbc-jarvis-cac-prd.servicebus.windows.net")
    topic = os.getenv("SB_TOPIC_NAME", "sbt-jarvis")
    send_topic_message(namespace, topic, {
        "event": "document_ingested",
        "name": name,
        "document_id": doc_id,
        "audit_id": audit_id,
    })

    # Console detail for developer visibility
    logger.info("✅ Ingested document '%s' (doc_id=%s, audit_id=%s)", name, doc_id, audit_id)

    return {
        "document_id": doc_id,
        "audit_id": audit_id,
        "name": name,
    }
