"""
HTTP API exposing operational endpoints for monitoring and integration tests.

Routes:
- GET /health: returns current status of database and Service Bus
"""

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from app.health import check_health


app = FastAPI(title="INGESTER-RAG", version="1.0.0")


@app.get("/health", response_class=JSONResponse)
async def health() -> JSONResponse:
    """Return the current health status of core components."""
    return JSONResponse(check_health())
