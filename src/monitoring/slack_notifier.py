from __future__ import annotations

import logging
import os
from typing import Any

import requests


logger = logging.getLogger(__name__)


DEFAULT_TIMEOUT_SECONDS = 10


def _as_bool(
    value: str | None,
    default: bool = False,
) -> bool:
    """
    Convert a common environment-style string to bool.
    """

    if value is None:
        return default

    return value.strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }


def slack_alerts_enabled() -> bool:
    """
    Return whether Slack alert delivery is enabled.
    """

    return _as_bool(
        os.getenv("SLACK_ALERTS_ENABLED"),
        default=True,
    )


def slack_recovery_enabled() -> bool:
    """
    Return whether recovery messages should be sent.
    """

    return _as_bool(
        os.getenv("SLACK_SEND_RECOVERY"),
        default=True,
    )


def get_slack_webhook_url() -> str | None:
    """
    Read the Slack Incoming Webhook URL from environment.

    Never hard-code the webhook URL in source control.
    """

    value = os.getenv("SLACK_WEBHOOK_URL")

    if not value:
        return None

    value = value.strip()

    return value or None


def _post_to_slack(
    payload: dict[str, Any],
) -> bool:
    """
    Send one payload to Slack.

    Returns True when Slack accepts the message.

    Notification errors are logged and returned as False.
    They are intentionally not raised because monitoring
    should not replace the original pipeline task result.
    """

    if not slack_alerts_enabled():
        logger.info(
            "slack_notification_skipped reason=disabled"
        )
        return False

    webhook_url = get_slack_webhook_url()

    if not webhook_url:
        logger.warning(
            "slack_notification_skipped reason=missing_webhook_url"
        )
        return False

    try:
        response = requests.post(
            webhook_url,
            json=payload,
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )

        response.raise_for_status()

        logger.info(
            "slack_notification_sent status_code=%s",
            response.status_code,
        )

        return True

    except Exception:
        logger.exception(
            "slack_notification_failed"
        )

        return False


def send_failure_notification(
    *,
    alert_id: str,
    dag_id: str | None,
    dag_run_id: str | None,
    task_id: str,
    map_index: int | None,
    alert_type: str,
    severity: str,
    error_message: str | None,
) -> bool:
    """
    Send an ACT pipeline failure notification to Slack.
    """

    mapped_suffix = (
        f" [{map_index}]"
        if map_index is not None
        else ""
    )

    message = (
        "🚨 *ACT Pipeline Failure*\n"
        f"*Severity:* {severity}\n"
        f"*Alert Type:* {alert_type}\n"
        f"*DAG:* {dag_id or 'unknown'}\n"
        f"*Run:* {dag_run_id or 'unknown'}\n"
        f"*Task:* {task_id}{mapped_suffix}\n"
        f"*Alert ID:* {alert_id}\n"
        f"*Error:* {error_message or 'No exception message available'}"
    )

    payload = {
        "text": message,
    }

    return _post_to_slack(
        payload
    )


def send_recovery_notification(
    *,
    dag_id: str | None,
    dag_run_id: str | None,
    task_id: str,
    map_index: int | None,
    resolved_count: int,
) -> bool:
    """
    Send an ACT pipeline recovery notification to Slack.
    """

    if not slack_recovery_enabled():
        logger.info(
            "slack_recovery_notification_skipped reason=disabled"
        )
        return False

    if resolved_count <= 0:
        return False

    mapped_suffix = (
        f" [{map_index}]"
        if map_index is not None
        else ""
    )

    message = (
        "✅ *ACT Pipeline Recovered*\n"
        f"*DAG:* {dag_id or 'unknown'}\n"
        f"*Run:* {dag_run_id or 'unknown'}\n"
        f"*Task:* {task_id}{mapped_suffix}\n"
        f"*Resolved Alerts:* {resolved_count}"
    )

    payload = {
        "text": message,
    }

    return _post_to_slack(
        payload
    )
