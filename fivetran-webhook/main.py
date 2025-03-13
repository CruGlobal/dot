import functions_framework
import os
import logging
import sys
import json
from google.cloud import pubsub_v1
import hmac
import hashlib

logger = logging.getLogger("primary_logger")
logger.propagate = False
gcp_project_id = os.environ.get("GOOGLE_CLOUD_PROJECT", None)
fivetran_secret = os.environ.get("FIVETRAN_SECRET", None)
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(gcp_project_id, "fivetran-events")


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
    Handles unhandled exceptions by logging the exception details and sending an alert to the development team.

    This function is intended to be used as a custom excepthook function, which is called when an unhandled exception
    occurs in the application. The function logs the exception details to the primary logger, and sends an alert to
    the development team using a third-party service such as Datadog or PagerDuty.

    Args:
        exc_type (type): The type of the exception that was raised.
        exc_value (Exception): The exception object that was raised.
        exc_traceback (traceback): The traceback object that was generated when the exception was raised.
    """
    # Check if the exception is of a type that can be skipped (e.g., KeyboardInterrupt)
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    # Log the unhandled exception
    logger = logging.getLogger("primary_logger")
    logger.exception(
        "Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback)
    )


@functions_framework.http
def webhook_handler(request):
    # Get request data and signature
    request_body = request.get_data()
    signature = request.headers.get("X-Fivetran-Signature-256")

    if not signature:
        return ("Missing signature", 400)

    # Verify signature
    computed_hmac = hmac.new(
        fivetran_secret.encode("UTF-8"), request_body, hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(computed_hmac, signature):
        return ("Invalid signature", 403)

    # Publish to Pub/Sub
    try:
        publisher.publish(topic_path, request_body)
        return ("Event received", 200)
    except Exception as e:
        return (f"Error publishing to Pub/Sub: {str(e)}", 500)
