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

# Service Bus
from azure.identity import DefaultAzureCredential
from azure.servicebus import ServiceBusClient, ServiceBusMessage

# Local module
import test_db_connection as dbtest


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

    logger.info("Starting main workflow: DB test and Service Bus test")

    # 1) Database test
    db_ok = dbtest.test_connection()
    if not db_ok:
        logger.error("Database connectivity test failed")
    else:
        logger.info("Database connectivity test succeeded")

    # 2) Service Bus test
    namespace = os.getenv("SB_NAMESPACE", "sbc-jarvis-cac-prd.servicebus.windows.net")
    # topic URL sample provided includes https://.../sbt-jarvis; we only need the name
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
