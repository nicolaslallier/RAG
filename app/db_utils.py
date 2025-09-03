"""
Database utilities for PostgreSQL and pgvector schema management.

Audience: Solution Architects
- Purpose: Provide simple, high-level functions to ensure the target database exists,
  initialize the `pgvector` extension and schema, and run a connectivity smoke test.
- Inputs: Reads `DATABASE_URL` and `DB_NAME` from environment variables.
- Outcome: The system can self-provision its database schema at startup.
"""

import logging
import os
from urllib.parse import urlparse, urlunparse
from typing import Any, Dict, Optional, List, Tuple

import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json
from dotenv import load_dotenv


logger = logging.getLogger(__name__)


def load_database_connection_string() -> str:
    """Return the database connection string from env or a sensible default.

    Default targets the `JARVIS` database. This ensures that all operations
    (including schema setup) occur in the correct logical database by default.
    """
    load_dotenv()
    default_connection_string = (
        "postgres://pgadmin:SuperSecret123@psql-jarvis-cae-prd.postgres.database.azure.com:5432/JARVIS?sslmode=require"
    )
    return os.getenv("DATABASE_URL", default_connection_string)


def connection_string_for_db(target_db: str, base_cs: str | None = None) -> str:
    """Return a connection string with the database name replaced by `target_db`.

    Useful for connecting to the admin `postgres` database to create `target_db`
    if it does not already exist.
    """
    if base_cs is None:
        base_cs = load_database_connection_string()
    parsed = urlparse(base_cs)
    new_path = f"/{target_db}"
    return urlunparse((parsed.scheme, parsed.netloc, new_path, '', parsed.query, ''))


def ensure_database_and_schema(target_db: str | None = None) -> bool:
    """Ensure the database and pgvector schema exist.

    High-level behavior:
    1) Connects to the admin database (`postgres`) to check for `target_db` and creates it if missing.
    2) Connects to `target_db` to install `pgvector` and create tables and indexes.
       - documents(doc_id, section, page_no, chunk_id, content, embedding, metadata)
       - ingestion_audit(name, status, detail, content_length, metadata)

    Returns:
        True if the database was created during this call, False if it already existed.
    """
    load_dotenv()
    if target_db is None:
        target_db = os.getenv("DB_NAME", "JARVIS")

    admin_cs = connection_string_for_db("postgres")
    created = False

    # 1) Ensure database exists
    logger.info("Ensuring database '%s' exists", target_db)
    try:
        admin_conn = psycopg2.connect(admin_cs)
        admin_conn.autocommit = True
        with admin_conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_db,))
            exists = cur.fetchone() is not None
            if not exists:
                cur.execute(sql.SQL("CREATE DATABASE {};").format(sql.Identifier(target_db)))
                created = True
                logger.info("✅ Created database '%s'", target_db)
            else:
                logger.info("Database '%s' already exists", target_db)
    finally:
        if 'admin_conn' in locals():
            admin_conn.close()

    # 2) Ensure schema (pgvector, tables, and indexes)
    target_cs = connection_string_for_db(target_db)
    try:
        conn = psycopg2.connect(target_cs)
        with conn:
            with conn.cursor() as cur:
                logger.info("Ensuring pgvector extension and application schema in '%s'", target_db)
                cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS documents (
                        id SERIAL PRIMARY KEY,
                        doc_id TEXT NOT NULL,
                        section TEXT,
                        page_no INT,
                        chunk_id INT,
                        content TEXT NOT NULL,
                        embedding vector(768) NOT NULL,
                        metadata JSONB DEFAULT '{}'
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_documents_embedding
                    ON documents USING ivfflat (embedding vector_cosine_ops)
                    WITH (lists = 100);
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_documents_fts
                    ON documents USING gin (to_tsvector('simple', content));
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ingestion_audit (
                        id SERIAL PRIMARY KEY,
                        name TEXT NOT NULL,
                        status TEXT NOT NULL,
                        detail TEXT,
                        content_length INTEGER,
                        metadata JSONB,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                    """
                )
                logger.info("✅ Schema ensured (documents, ingestion_audit)")
    finally:
        if 'conn' in locals():
            conn.close()

    return created


def test_database_connection() -> bool:
    """Run a light-weight connectivity test against the target database.

    Behavior:
    - Verifies that the server is reachable and returns version and connection info.
    - Checks that the application `documents` table can be queried.
    """
    cs = load_database_connection_string()
    try:
        masked = cs.replace(cs.split('@')[0].split('//')[1], '***:***')
        logger.info("Connecting to PostgreSQL ... %s", masked)
        conn = psycopg2.connect(cs)
        cursor = conn.cursor()

        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        logger.info("PostgreSQL version: %s", version)

        cursor.execute("SELECT current_database(), current_user, inet_server_addr(), inet_server_port();")
        db, user, ip, port = cursor.fetchone()
        logger.info("DB=%s user=%s ip=%s port=%s", db, user, ip, port)

        cursor.execute("SELECT COUNT(*) FROM documents;")
        count = cursor.fetchone()[0]
        logger.info("documents rows: %s", count)
        conn.commit()
        return True
    except Exception as exc:
        logger.error("Database connectivity test failed: %s", exc)
        return False
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()


def _format_vector_literal(embedding: list[float]) -> str:
    """Format a Python list of floats into a pgvector literal string like '[0.1,0.2,...]'."""
    return '[' + ','.join(str(float(x)) for x in embedding) + ']'


def insert_document(
    doc_id: str,
    content: str,
    embedding: list[float],
    section: Optional[str] = None,
    page_no: Optional[int] = None,
    chunk_id: Optional[int] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> int:
    """Insert into `documents(doc_id, section, page_no, chunk_id, content, embedding, metadata)` and return id."""
    if "\x00" in content:
        content = content.replace("\x00", "")

    cs = load_database_connection_string()
    conn = psycopg2.connect(cs)
    try:
        with conn:
            with conn.cursor() as cur:
                vec_literal = _format_vector_literal(embedding)
                cur.execute(
                    """
                    INSERT INTO documents (doc_id, section, page_no, chunk_id, content, embedding, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s::vector, COALESCE(%s, '{}'::jsonb))
                    RETURNING id;
                    """,
                    (doc_id, section, page_no, chunk_id, content, vec_literal, Json(metadata) if metadata is not None else None),
                )
                new_id = cur.fetchone()[0]
                return new_id
    finally:
        conn.close()


def insert_ingestion_audit(name: str, status: str, detail: str = "", content_length: int | None = None, metadata: Optional[Dict[str, Any]] = None) -> int:
    """Record an ingestion event for traceability and returns the audit id."""
    cs = load_database_connection_string()
    conn = psycopg2.connect(cs)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO ingestion_audit (name, status, detail, content_length, metadata)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id;
                    """,
                    (name, status, detail, content_length, Json(metadata) if metadata is not None else None),
                )
                new_id = cur.fetchone()[0]
                return new_id
    finally:
        conn.close()


def find_similar_chunks(doc_id: str, query_embedding: List[float], limit: int = 5) -> List[Tuple[int, str, Optional[int], Optional[str], float]]:
    """Return top-N most similar chunks for a given document id using cosine distance.

    Output rows: (id, content, page_no, section, distance)
    """
    cs = load_database_connection_string()
    conn = psycopg2.connect(cs)
    try:
        with conn.cursor() as cur:
            vec_literal = _format_vector_literal(query_embedding)
            cur.execute(
                """
                SELECT id, content, page_no, section, (embedding <=> %s::vector) AS distance
                FROM documents
                WHERE doc_id = %s
                ORDER BY distance ASC
                LIMIT %s
                """,
                (vec_literal, doc_id, limit),
            )
            rows = cur.fetchall()
            return rows
    finally:
        conn.close()
