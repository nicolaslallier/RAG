"""
Health check helpers that validate dependencies and return structured status.

Audience: Solution Architects & Testers
- Purpose: Provide a single function to check the status of critical components.
- Components: Database connectivity and Service Bus send capability.
- Output: JSON-serializable dictionary for integration tests and monitoring.
"""

import time
import os
from typing import Any, Dict

from app.db_utils import test_database_connection
from app.service_bus import send_topic_message


def check_health() -> Dict[str, Any]:
    """Return component status for DB and Service Bus.

    The Service Bus check sends a lightweight heartbeat to verify send capability.
    """
    now = int(time.time())

    # DB status
    db_ok = test_database_connection()

    # SB status
    namespace = os.getenv("SB_NAMESPACE", "sbc-jarvis-cac-prd.servicebus.windows.net")
    topic = os.getenv("SB_TOPIC_NAME", "sbt-jarvis")
    sb_ok = send_topic_message(namespace, topic, {"event": "health-ping", "ts": now})

    overall = db_ok and sb_ok

    return {
        "status": "ok" if overall else "degraded",
        "timestamp": now,
        "components": {
            "database": {
                "ok": db_ok,
                "detail": "Connected and query succeeded" if db_ok else "Connection or query failed",
            },
            "service_bus": {
                "ok": sb_ok,
                "detail": "Heartbeat sent" if sb_ok else "Send failed",
            },
        },
    }
