"""
HTTP API exposing operational endpoints for monitoring and integration tests.

Routes:
- GET /health: returns current status of database and Service Bus
- POST /ingester/document: ingest a document into the vector DB
"""

from fastapi import FastAPI, HTTPException
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


@app.post("/ingester/document", response_class=JSONResponse)
async def ingester_document(req: IngestRequest) -> JSONResponse:
    """Ingest a document and return identifiers."""
    try:
        result = ingest_document(req.name, req.content, req.metadata)
        return JSONResponse({"status": "ok", **result})
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
