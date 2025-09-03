"""
HTTP API exposing operational endpoints for monitoring and integration tests.

Routes:
- GET /health: returns current status of database and Service Bus
- POST /ingester/document: ingest a document into the vector DB
  Supports JSON body or multipart/form-data with parts: file (bytes), spec (json)
- POST /ask: embed query, retrieve similar chunks, and build a prompt; optional generation
"""

import json
import logging
from typing import Optional
from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.health import check_health
from app.ingestion import ingest_document, extract_pdf_pages, chunk_text, embed_query
from app.db_utils import ensure_database_and_schema, find_similar_chunks
from app.generation import generate_answer, build_prompt


app = FastAPI(title="INGESTER-RAG", version="1.0.0")
logger = logging.getLogger(__name__)


class IngestRequest(BaseModel):
    name: str = Field(..., description="Original document name or identifier")
    content: str = Field(..., description="Raw chunk content to embed and store")
    metadata: dict | None = Field(default=None, description="Optional metadata for the document")
    doc_id: str | None = Field(default=None, description="Logical document id (defaults to name)")
    section: str | None = Field(default=None, description="Section title if available")
    page_no: int | None = Field(default=None, description="Original page number")
    chunk_id: int | None = Field(default=None, description="Chunk order within page")


class AskRequest(BaseModel):
    doc_id: str = Field(..., description="Target document id to search within")
    question: str = Field(..., description="User question to embed and retrieve context for")
    top_k: int = Field(3, description="Number of top chunks to include in prompt")
    fetch_k: int = Field(5, description="Number of chunks to fetch from DB before trimming to top_k")
    generate: bool = Field(False, description="If true, call the local model to produce an answer")
    model_id: str | None = Field(default=None, description="HF model id override")


@app.on_event("startup")
async def _startup() -> None:
    try:
        ensure_database_and_schema()
        logger.info("Startup DB/schema ensure complete")
    except Exception as exc:  # pragma: no cover
        logger.exception("Startup DB/schema ensure failed: %s", exc)


@app.get("/health", response_class=JSONResponse)
async def health() -> JSONResponse:
    return JSONResponse(check_health())


@app.post("/ingester/document/json", response_class=JSONResponse)
async def ingester_document_json(req: IngestRequest) -> JSONResponse:
    try:
        result = ingest_document(
            name=req.name,
            content=req.content,
            metadata=req.metadata,
            doc_id=req.doc_id,
            section=req.section,
            page_no=req.page_no,
            chunk_id=req.chunk_id,
        )
        return JSONResponse({"status": "ok", **result})
    except Exception as exc:
        logger.exception("JSON ingestion failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/ingester/document", response_class=JSONResponse)
async def ingester_document_multipart(
    file: UploadFile = File(..., description="Raw document file (PDF or text)"),
    spec: str = Form(..., description="JSON string with name, optional doc_id/section/page_no/chunk_id/metadata"),
    max_chars: int = 900,
    overlap: int = 150,
) -> JSONResponse:
    try:
        try:
            spec_obj = json.loads(spec)
        except json.JSONDecodeError as e:  # pragma: no cover
            raise HTTPException(status_code=400, detail=f"Invalid spec JSON: {e}")

        name = spec_obj.get("name") or file.filename
        metadata = spec_obj.get("metadata")
        doc_id = spec_obj.get("doc_id") or name

        file_bytes = await file.read()
        pages = extract_pdf_pages(file_bytes)

        ingested = []
        for page_no, page_text in pages:
            for j, chunk in enumerate(chunk_text(page_text, max_chars=max_chars, overlap=overlap)):
                if not chunk.strip():
                    continue
                result = ingest_document(
                    name=name,
                    content=chunk,
                    metadata=metadata,
                    doc_id=doc_id,
                    section=spec_obj.get("section"),
                    page_no=page_no,
                    chunk_id=j,
                )
                ingested.append(result)

        return JSONResponse({"status": "ok", "ingested": ingested, "chunks": len(ingested)})
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Multipart ingestion failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/ask", response_class=JSONResponse)
async def ask(req: AskRequest) -> JSONResponse:
    try:
        q_vec = embed_query(req.question)
        rows = find_similar_chunks(req.doc_id, q_vec, limit=req.fetch_k)
        from textwrap import shorten
        context_chunks = [f"[p.{r[2]}] {shorten(r[1], width=900)}" for r in rows[: req.top_k]]
        prompt = build_prompt(context_chunks, req.question)

        result = {"status": "ok", "prompt": prompt, "matches": [
            {"id": r[0], "page_no": r[2], "section": r[3], "distance": r[4]} for r in rows
        ]}

        if req.generate:
            try:
                answer = generate_answer(context_chunks, req.question, model_id=req.model_id)
                result["answer"] = answer
            except Exception as exc:
                logger.exception("Local generation failed: %s", exc)
                result["answer_error"] = str(exc)

        return JSONResponse(result)
    except Exception as exc:
        logger.exception("/ask failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))
