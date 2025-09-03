#!/usr/bin/env python3
"""
Entry point orchestrating logging, database readiness, and Service Bus notifications.

Audience: Solution Architects
This file reads like a high-level runbook. Each step delegates to a focused module under `app/`:
- `app.logging_setup` sets up logging and optional Azure Application Insights export
- `app.db_utils` ensures the database/schema and runs a connectivity check
- `app.service_bus` sends status and heartbeat messages to a topic
"""

import json
import logging
import os
import sys
import time

from app.logging_setup import configure_logging
from app.db_utils import ensure_database_and_schema, test_database_connection
from app.service_bus import send_topic_message


logger = logging.getLogger("ingester.main")


def main() -> int:
    """Run the foundational startup checks and notifications.

    Steps:
    1) Initialize logging and optional telemetry (App Insights)
    2) Ensure the `JARVIS` database and `pgvector` schema exist
    3) Notify via Service Bus whether the DB was created or already present
    4) Run a database connectivity smoke test
    5) Send a simple heartbeat message to Service Bus
    """
    configure_logging()

    # 1) Ensure DB and schema
    try:
        created = ensure_database_and_schema()
        logger.info("Database ensure completed: %s", "created" if created else "present")
    except Exception:
        logger.exception("Failed to ensure database and schema")
        return 1

    # 2) Notify DB status
    namespace = os.getenv("SB_NAMESPACE", "sbc-jarvis-cac-prd.servicebus.windows.net")
    topic = os.getenv("SB_TOPIC_NAME", "sbt-jarvis")
    send_topic_message(namespace, topic, {
        "event": "db_status",
        "database": os.getenv("DB_NAME", "JARVIS"),
        "status": "created" if created else "present",
        "ts": int(time.time()),
    })

    # 3) Connectivity smoke test
    db_ok = test_database_connection()

    # 4) Heartbeat
    send_topic_message(namespace, topic, {
        "event": "ping",
        "ts": int(time.time()),
    })

    return 0 if db_ok else 1


if __name__ == "__main__":
    sys.exit(main())
