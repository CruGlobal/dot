"""
Google Sheets Trigger Cloud Function

A reusable HTTP Cloud Function that checks Google Sheets for changes
and triggers dbt jobs via Pub/Sub.

Request JSON:
{
  "sheets": "[{\"id\": \"...\", \"name\": \"...\"}]",  // JSON string (Terraform limitation)
  "dbt_job_id": "920201",
  "include_weekends": "false"  // optional, string "true"/"false", defaults to false
}
"""

import os
import json
import logging
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List

import functions_framework
import pytz
from google.cloud import pubsub_v1

from sheets_client import SheetsClient


logger = logging.getLogger("primary_logger")
logger.propagate = False


class CloudLoggingFormatter(logging.Formatter):
    """
    Produces messages compatible with google cloud logging
    """

    def format(self, record: logging.LogRecord) -> str:
        s = super().format(record)
        return json.dumps(
            {
                "message": s,
                "severity": record.levelname,
                "timestamp": {"seconds": int(record.created), "nanos": 0},
            }
        )


def setup_logging():
    """
    Sets up logging for the application.
    """
    global logger

    # Remove any existing handlers
    if logger.handlers:
        for handler in logger.handlers:
            logger.removeHandler(handler)

    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = CloudLoggingFormatter(fmt="%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    sys.excepthook = handle_unhandled_exception


def handle_unhandled_exception(exc_type, exc_value, exc_traceback):
    """
    Handles unhandled exceptions by logging the exception details.
    """
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logger.exception(
        "Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback)
    )


def get_lookback_time(include_weekends: bool = False) -> datetime:
    """
    Get the cutoff time for change detection.

    Args:
        include_weekends: If True, always look back 24 hours (for daily schedules).
                          If False, on Monday look back 72 hours to cover the weekend.

    Returns:
        datetime: The cutoff time - sheets modified after this time are considered changed.
    """
    est = pytz.timezone("America/New_York")
    now = datetime.now(est)

    # Daily schedule (including weekends) - always 24 hours
    if include_weekends:
        return now - timedelta(hours=24)

    # Weekday-only schedule - on Monday, look back to Friday
    if now.weekday() == 0:  # Monday
        return now - timedelta(hours=72)

    return now - timedelta(hours=24)


def publish_pubsub_message(data: Dict[str, Any], topic_name: str) -> None:
    """
    Publishes a message to a Pub/Sub topic.

    Args:
        data: The message data as a dictionary.
        topic_name: The name of the Pub/Sub topic.
    """
    google_cloud_project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not google_cloud_project_id:
        raise ValueError("GOOGLE_CLOUD_PROJECT environment variable not set")

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(google_cloud_project_id, topic_name)
    data_encoded = json.dumps(data).encode("utf-8")
    future = publisher.publish(topic_path, data_encoded)
    future.result()
    logger.info(f"Published message to Pub/Sub topic '{topic_name}'")


@functions_framework.http
def trigger_sheets_check(request):
    """
    HTTP Cloud Function to check Google Sheets for changes and trigger dbt jobs.

    Request JSON:
    {
      "sheets": "[{\"id\": \"...\", \"name\": \"...\"}]",  // JSON string
      "dbt_job_id": "920201",
      "include_weekends": "false"  // optional, string "true"/"false"
    }

    Returns:
        tuple: (response_message, status_code)
    """
    setup_logging()

    # Parse request
    request_json = request.get_json(silent=True)

    # Handle octet-stream content type (same pattern as dbt-trigger)
    if (
        request_json is None
        and request.headers.get("Content-Type") == "application/octet-stream"
    ):
        try:
            request_data = request.get_data(as_text=True)
            request_json = json.loads(request_data) if request_data else None
        except Exception as e:
            logger.exception(f"Failed to parse octet-stream data: {str(e)}")
            request_json = None

    if not request_json:
        logger.error("Missing request body")
        return "Missing request body", 400

    sheets = request_json.get("sheets", [])
    dbt_job_id = request_json.get("dbt_job_id")
    include_weekends = request_json.get("include_weekends", False)

    # Handle sheets as JSON string (Terraform/Cloud Scheduler limitation)
    if isinstance(sheets, str):
        try:
            sheets = json.loads(sheets)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse sheets JSON string: {e}")
            return "Invalid sheets JSON string", 400

    # Handle include_weekends as string (Terraform/Cloud Scheduler limitation)
    if isinstance(include_weekends, str):
        include_weekends = include_weekends.lower() == "true"

    if not sheets:
        logger.error("Missing sheets in request")
        return "Missing sheets in request", 400

    if not dbt_job_id:
        logger.error("Missing dbt_job_id in request")
        return "Missing dbt_job_id in request", 400

    logger.info(
        f"Checking {len(sheets)} sheets for changes (include_weekends={include_weekends})"
    )

    lookback_time = get_lookback_time(include_weekends)
    logger.info(f"Looking for changes after {lookback_time.isoformat()}")

    try:
        sheets_client = SheetsClient()
    except Exception as e:
        logger.exception(f"Failed to initialize SheetsClient: {str(e)}")
        return "Failed to initialize Google Sheets client", 500

    changes_detected = False
    changed_sheets = []

    for sheet in sheets:
        sheet_id = sheet.get("id")
        sheet_name = sheet.get("name", sheet_id)

        if not sheet_id:
            logger.warning(f"Skipping sheet with missing id: {sheet}")
            continue

        try:
            modified_time = sheets_client.get_modified_time(sheet_id)
            logger.info(
                f"Sheet '{sheet_name}' last modified: {modified_time.isoformat()}"
            )

            if modified_time > lookback_time:
                logger.info(f"Sheet '{sheet_name}' has changes (modified after lookback)")
                changes_detected = True
                changed_sheets.append(sheet_name)
            else:
                logger.info(f"Sheet '{sheet_name}' has no changes")

        except Exception as e:
            logger.exception(f"Failed to check sheet '{sheet_name}' ({sheet_id}): {str(e)}")
            return f"Failed to check sheet '{sheet_name}'", 500

    if changes_detected:
        try:
            publish_pubsub_message({"job_id": dbt_job_id}, "cloud-run-job-completed")
            logger.info(f"Triggered dbt job {dbt_job_id} due to changes in: {', '.join(changed_sheets)}")
            return f"Changes detected in {len(changed_sheets)} sheet(s), triggered dbt job {dbt_job_id}", 200
        except Exception as e:
            logger.exception(f"Failed to publish Pub/Sub message: {str(e)}")
            return "Failed to trigger dbt job", 500
    else:
        logger.info("No changes detected in any sheets")
        return "No changes detected", 200
