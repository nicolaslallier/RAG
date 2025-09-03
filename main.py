#!/usr/bin/env python3
"""
Main module that:
- Configures Azure Application Insights logging via azure-monitor-opentelemetry
- Runs the PostgreSQL connection test
- Tests Azure Service Bus connectivity by sending a test message to a topic
"""

import os
import sys
import json
import logging
import time
from typing import Optional

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
    default_connection_string = "postgres://pgadmin:SuperSecret123@psql-jarvis-cae-prd.postgres.database.azure.com:5432/postgres?sslmode=require"
    return os.getenv("DATABASE_URL", default_connection_string)


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

        logger.info("Testing table creation and data insertion...")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS connection_test (
                id SERIAL PRIMARY KEY,
                test_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )

        cursor.execute(
            """
            INSERT INTO connection_test (test_message)
            VALUES (%s)
            RETURNING id, test_message, created_at;
            """,
            ("Connection test successful!",),
        )
        result = cursor.fetchone()
        logger.info("✅ Test data inserted: ID=%s, Message='%s', Created=%s", result[0], result[1], result[2])

        cursor.execute("SELECT COUNT(*) FROM connection_test;")
        count = cursor.fetchone()[0]
        logger.info("Total test records: %s", count)

        cursor.execute("DROP TABLE IF EXISTS connection_test;")
        logger.info("Test table cleaned up")

        conn.commit()
        logger.info("✅ All database operations completed successfully!")
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
                message = ServiceBusMessage(json.dumps({
                    "message": message_body,
                    "ts": int(time.time()),
                    "host": os.getenv("HOSTNAME", "local")
                }))
                sender.send_messages(message)
                logger.info("✅ Sent test message to Service Bus topic")
        return True
    except Exception as exc:
        logger.error("❌ Failed to send to Service Bus: %s", exc)
        return False


def main() -> int:
    configure_logging()

    logger.info("Starting main foundation checks: logging, DB, Service Bus")

    # 1) Database test
    db_ok = test_database_connection()
    if not db_ok:
        logger.error("Database connectivity test failed")
    else:
        logger.info("Database connectivity test succeeded")

    # 2) Service Bus test
    namespace = os.getenv("SB_NAMESPACE", "sbc-jarvis-cac-prd.servicebus.windows.net")
    topic = os.getenv("SB_TOPIC_NAME", "sbt-jarvis")
    sb_ok = test_service_bus_send(namespace, topic)

    if db_ok and sb_ok:
        logger.info("All checks passed")
        return 0
    else:
        logger.error("One or more checks failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
