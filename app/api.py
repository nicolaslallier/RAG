"""
HTTP API exposing operational endpoints for monitoring and integration tests.

Routes:
- GET /health: returns current status of database and Service Bus
- POST /ingester/document: ingest a document into the vector DB
  Supports JSON body or multipart/form-data with parts: file (bytes), spec (json)
"""

import json
from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.health import check_health
from app.ingestion import ingest_document


app = FastAPI(title="INGESTER-RAG", version="1.0.0")


class IngestRequest(BaseModel):
    name: str = Field(..., description="Document name or identifier")
    content: str = Field(..., description="Raw document content to embed and store")
    metadata: dict | None = Field(default=None, description="Optional metadata for the document")


@app.get("/health", response_class=JSONResponse)
async def health() -> JSONResponse:
    """Return the current health status of core components."""
    return JSONResponse(check_health())


@app.post("/ingester/document/json", response_class=JSONResponse)
async def ingester_document_json(req: IngestRequest) -> JSONResponse:
    """Ingest a document via JSON body."""
    try:
        result = ingest_document(req.name, req.content, req.metadata)
        return JSONResponse({"status": "ok", **result})
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/ingester/document", response_class=JSONResponse)
async def ingester_document_multipart(
    file: UploadFile = File(..., description="Raw document file"),
    spec: str = Form(..., description="JSON string containing at least 'name' and optional 'metadata'")
) -> JSONResponse:
    """Ingest a document via multipart form with file bytes and JSON spec.

    spec example: {"name":"manual.pdf","metadata":{"source":"upload"}}
    """
    try:
        try:
            spec_obj = json.loads(spec)
        except json.JSONDecodeError as e:  # pragma: no cover
            raise HTTPException(status_code=400, detail=f"Invalid spec JSON: {e}")

        name = spec_obj.get("name") or file.filename
        metadata = spec_obj.get("metadata")
        content_bytes = await file.read()
        content = content_bytes.decode("utf-8", errors="ignore")

        result = ingest_document(name, content, metadata)
        return JSONResponse({"status": "ok", **result})
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
