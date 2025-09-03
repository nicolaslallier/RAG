#!/usr/bin/env python3
"""
Main module that:
- Configures Azure Application Insights logging via azure-monitor-opentelemetry
- Ensures the JARVIS database and pgvector schema exist
- Runs the PostgreSQL connection test
- Tests Azure Service Bus connectivity by sending a test message to a topic
"""

import os
import sys
import json
import logging
import time
from typing import Optional
from urllib.parse import urlparse, urlunparse

from dotenv import load_dotenv

# App Insights / OpenTelemetry
try:
    from azure.monitor.opentelemetry import configure_azure_monitor
    _AI_AVAILABLE = True
except Exception:  # pragma: no cover
    _AI_AVAILABLE = False

# Database
import psycopg2

# Service Bus
from azure.identity import DefaultAzureCredential
from azure.servicebus import ServiceBusClient, ServiceBusMessage


logger = logging.getLogger("ingester.main")


def configure_logging() -> None:
    load_dotenv()

    logging_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    logging_level = getattr(logging, logging_level_str, logging.INFO)

    logging.basicConfig(
        level=logging_level,
        format='%(asctime)s %(levelname)s %(name)s - %(message)s'
    )

    appinsights_conn = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")
    if _AI_AVAILABLE and appinsights_conn:
        try:
            configure_azure_monitor(connection_string=appinsights_conn)
            logger.info("Azure Monitor (App Insights) configured for logs/traces/metrics")
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed to configure Azure Monitor: %s", exc)
    else:
        if not _AI_AVAILABLE:
            logger.warning("azure-monitor-opentelemetry not installed or failed to import")
        else:
            logger.warning("APPLICATIONINSIGHTS_CONNECTION_STRING not set; logs won't be sent to App Insights")


def load_database_connection_string() -> str:
    load_dotenv()
    default_connection_string = "postgres://pgadmin:SuperSecret123@psql-jarvis-cae-prd.postgres.database.azure.com:5432/JARVIS?sslmode=require"
    return os.getenv("DATABASE_URL", default_connection_string)


def _connection_string_for_db(target_db: str) -> str:
    """Return a connection string with database replaced by target_db."""
    base = load_database_connection_string()
    parsed = urlparse(base)
    new_path = f"/{target_db}"
    new_cs = urlunparse((parsed.scheme, parsed.netloc, new_path, '', parsed.query, ''))
    return new_cs


def ensure_database_and_schema() -> bool:
    """Ensure database exists and pgvector schema is ready. Returns True if created, False if already existed."""
    target_db = os.getenv("DB_NAME", "JARVIS")

    # 1) Ensure database exists by connecting to 'postgres' db
    admin_cs = _connection_string_for_db("postgres")
    created = False
    try:
        logger.info("Ensuring database '%s' exists", target_db)
        admin_conn = psycopg2.connect(admin_cs)
        admin_conn.autocommit = True
        with admin_conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_db,))
            exists = cur.fetchone() is not None
            if not exists:
                cur.execute(f"CREATE DATABASE {psycopg2.sql.Identifier(target_db).string}")
                created = True
                logger.info("✅ Created database '%s'", target_db)
            else:
                logger.info("Database '%s' already exists", target_db)
    except Exception as exc:
        logger.error("❌ Failed ensuring database exists: %s", exc)
        raise
    finally:
        if 'admin_conn' in locals():
            admin_conn.close()

    # 2) Ensure pgvector extension and schema in target database
    target_cs = _connection_string_for_db(target_db)
    try:
        conn = psycopg2.connect(target_cs)
        with conn:
            with conn.cursor() as cur:
                logger.info("Ensuring pgvector extension and schema in '%s'", target_db)
                cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS documents (
                        id SERIAL PRIMARY KEY,
                        content TEXT,
                        embedding vector(768),
                        metadata JSONB
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS idx_documents_embedding_ivfflat
                    ON documents
                    USING ivfflat (embedding vector_cosine_ops)
                    WITH (lists = 100);
                    """
                )
                logger.info("✅ pgvector extension, table and index ensured")
    except Exception as exc:
        logger.error("❌ Failed ensuring schema: %s", exc)
        raise
    finally:
        if 'conn' in locals():
            conn.close()

    # 3) Notify via Service Bus
    status = "created" if created else "present"
    namespace = os.getenv("SB_NAMESPACE", "sbc-jarvis-cac-prd.servicebus.windows.net")
    topic = os.getenv("SB_TOPIC_NAME", "sbt-jarvis")
    _ = test_service_bus_send(namespace, topic, message_body=json.dumps({
        "event": "db_status",
        "database": target_db,
        "status": status,
        "ts": int(time.time())
    }))

    return created


def test_database_connection() -> bool:
    connection_string = load_database_connection_string()
    try:
        logger.info("Attempting to connect to PostgreSQL database...")
        logger.info("Connection string: %s", connection_string.replace(connection_string.split('@')[0].split('//')[1], '***:***'))

        conn = psycopg2.connect(connection_string)
        logger.info("✅ Successfully connected to PostgreSQL database!")

        cursor = conn.cursor()

        logger.info("Testing basic query...")
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        logger.info("PostgreSQL version: %s", version[0])

        logger.info("Getting database information...")
        cursor.execute("SELECT current_database(), current_user, inet_server_addr(), inet_server_port();")
        db_info = cursor.fetchone()
        logger.info("Database: %s", db_info[0])
        logger.info("User: %s", db_info[1])
        logger.info("Server IP: %s", db_info[2])
        logger.info("Server Port: %s", db_info[3])

        logger.info("Testing table presence and count...")
        cursor.execute("SELECT COUNT(*) FROM documents;")
        count = cursor.fetchone()[0]
        logger.info("documents rows: %s", count)

        conn.commit()
        logger.info("✅ Database connectivity test completed successfully!")
        return True
    except psycopg2.Error as e:
        logger.error("❌ PostgreSQL error: %s", e)
        return False
    except Exception as e:
        logger.error("❌ Unexpected error: %s", e)
        return False
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()
            logger.info("Database connection closed")


def test_service_bus_send(namespace_fqdn: str, topic_name: str, message_body: str = "Hello from ingester-rag") -> bool:
    """Send a single test message to a Service Bus topic.

    Auth priority:
    1) SB_CONNECTION_STRING env var
    2) AAD via DefaultAzureCredential against namespace
    """
    logger.info("Testing Service Bus send: namespace=%s topic=%s", namespace_fqdn, topic_name)

    sb_conn = os.getenv("SB_CONNECTION_STRING")
    try:
        if sb_conn:
            logger.info("Using SB connection string authentication")
            client = ServiceBusClient.from_connection_string(sb_conn)
        else:
            logger.info("Using AAD (DefaultAzureCredential) authentication")
            credential = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
            client = ServiceBusClient(fully_qualified_namespace=namespace_fqdn, credential=credential)

        with client:
            sender = client.get_topic_sender(topic_name=topic_name)
            with sender:
                message = ServiceBusMessage(message_body if isinstance(message_body, str) else json.dumps(message_body))
                sender.send_messages(message)
                logger.info("✅ Sent message to Service Bus topic")
        return True
    except Exception as exc:
        logger.error("❌ Failed to send to Service Bus: %s", exc)
        return False


def main() -> int:
    configure_logging()

    logger.info("Starting foundation checks: ensure DB/schema, logging, DB test, Service Bus")

    # Ensure DB and schema
    try:
        created = ensure_database_and_schema()
        logger.info("Database ensure completed: %s", "created" if created else "present")
    except Exception:
        logger.error("Failed to ensure database and schema")
        return 1

    # Database test (connect and basic checks)
    db_ok = test_database_connection()

    # Service Bus test send simple ping
    namespace = os.getenv("SB_NAMESPACE", "sbc-jarvis-cac-prd.servicebus.windows.net")
    topic = os.getenv("SB_TOPIC_NAME", "sbt-jarvis")
    sb_ok = test_service_bus_send(namespace, topic, message_body=json.dumps({
        "event": "ping",
        "ts": int(time.time())
    }))

    if db_ok and sb_ok:
        logger.info("All checks passed")
        return 0
    else:
        logger.error("One or more checks failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
