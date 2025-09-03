"""
Azure Service Bus helpers for sending operational messages.

Audience: Solution Architects
- Purpose: Provide a simple way to send status/heartbeat messages to a Service Bus topic.
- Inputs: Uses `SB_CONNECTION_STRING` when present, otherwise falls back to Azure AD (Managed Identity / DefaultAzureCredential).
- Outcome: Allows other systems to subscribe to operational events (e.g., database created/present, health pings).
"""

import json
import logging
import os
import time

from azure.identity import DefaultAzureCredential
from azure.servicebus import ServiceBusClient, ServiceBusMessage


logger = logging.getLogger(__name__)


def send_topic_message(namespace_fqdn: str, topic_name: str, payload: dict | str) -> bool:
    """Send a single message to a Service Bus topic.

    Behavior:
    - If `SB_CONNECTION_STRING` is set, uses SAS connection string authentication.
    - Otherwise, uses Azure AD via `DefaultAzureCredential` against the provided namespace.
    """
    sb_conn = os.getenv("SB_CONNECTION_STRING")
    try:
        if sb_conn:
            logger.info("Service Bus auth: connection string")
            client = ServiceBusClient.from_connection_string(sb_conn)
        else:
            logger.info("Service Bus auth: AAD (DefaultAzureCredential)")
            credential = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
            client = ServiceBusClient(fully_qualified_namespace=namespace_fqdn, credential=credential)

        if isinstance(payload, dict):
            body = json.dumps(payload)
        else:
            body = payload

        with client:
            sender = client.get_topic_sender(topic_name=topic_name)
            with sender:
                sender.send_messages(ServiceBusMessage(body))
        logger.info("Sent message to Service Bus topic '%s'", topic_name)
        return True
    except Exception as exc:
        logger.error("Failed to send to Service Bus: %s", exc)
        return False
