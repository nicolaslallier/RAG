"""
Logging setup and Azure Application Insights integration.

Audience: Solution Architects
- Purpose: Centralize logging configuration and enable telemetry export to Azure Application Insights.
- Inputs: Uses environment variables `LOG_LEVEL` and `APPLICATIONINSIGHTS_CONNECTION_STRING`.
- Outcome: All Python logs flow consistently; if configured, they are exported to App Insights.
"""

import logging
import os
from dotenv import load_dotenv


def configure_logging() -> None:
    """Configure application logging and optional Azure Application Insights export.

    High-level behavior:
    - Sets the global logging format and level for the process.
    - If `APPLICATIONINSIGHTS_CONNECTION_STRING` is present, enables Azure Monitor export
      (logs/traces/metrics) using the OpenTelemetry SDK.

    This function is intentionally safe: if Azure packages are unavailable or the
    connection string is missing, standard logging still works.
    """
    load_dotenv()

    # 1) Configure Python logging level and format
    logging_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    logging_level = getattr(logging, logging_level_str, logging.INFO)
    logging.basicConfig(level=logging_level, format='%(asctime)s %(levelname)s %(name)s - %(message)s')
    logger = logging.getLogger(__name__)

    # 2) Optionally configure Azure Monitor (Application Insights)
    appinsights_conn = os.getenv("APPLICATIONINSIGHTS_CONNECTION_STRING")
    if not appinsights_conn:
        logger.warning("APPLICATIONINSIGHTS_CONNECTION_STRING not set; logs will not be exported to App Insights")
        return

    try:
        # Import inside function to avoid hard dependency when not needed
        from azure.monitor.opentelemetry import configure_azure_monitor  # type: ignore
        configure_azure_monitor(connection_string=appinsights_conn)
        logger.info("Azure Monitor (App Insights) configured for logs/traces/metrics")
    except Exception as exc:  # pragma: no cover
        logger.warning("Failed to configure Azure Monitor: %s", exc)
