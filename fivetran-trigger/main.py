import os
import functions_framework
from fivetran_client import FivetranClient
import logging
import sys
import os
import json

from markupsafe import escape
from requests import request, auth, Session

logger = logging.getLogger("primary_logger")
# Create a global HTTP session (which provides connection pooling)
session = Session()
basic_auth = None


def init():
    global basic_auth
    basic_auth = auth.HTTPBasicAuth(env_var("API_KEY"), env_var("API_SECRET"))


def env_var(name):
    return os.environ[name]


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
    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = CloudLoggingFormatter(fmt="[%(name)s] %(message)s")
    handler.setFormatter(formatter)
    logger = logging.getLogger("primary_logger")
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
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    # Log the unhandled exception
    logger = logging.getLogger("primary_logger")
    logger.exception(
        "Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback)
    )
    # Send an alert to the development team using a third-party service such as Datadog or PagerDuty
    # TODO: Add code to send an alert to the development team


@functions_framework.http
def trigger_sync(request):
    """
    Triggers a Fivetran sync for a given connector ID.

    """
    setup_logging()
    init()

    # Try get_json first
    request_json = request.get_json(silent=True)

    # If that fails and we have octet-stream content type, try manual parsing
    if (
        request_json is None
        and request.headers.get("Content-Type") == "application/octet-stream"
    ):
        try:
            request_data = request.get_data(as_text=True)
            request_json = json.loads(request_data) if request_data else None
        except Exception as e:
            logger.error(f"Failed to parse octet-stream data: {str(e)}")
            request_json = None

    if request_json and "connector_id" in request_json:
        connector_id = request_json["connector_id"]
    else:
        logger.error("Error: Failed to retrieve connector_id")
        return "Failed to retrieve connector_id", 400

    client = FivetranClient(basic_auth)

    try:
        client.trigger_sync(
            connector_id=connector_id,
            force=True,
            wait_for_completion=False,
        )
        logger.info(
            f"Fivetran sync triggered and completed successfully, connector_id: {connector_id}"
        )
        return "Fivetran sync triggered successfully", 200
    except Exception as e:
        logger.error(
            f"connector_id: {connector_id} - Error triggering Fivetran sync: {str(e)}"
        )
        return f"Error triggering Fivetran sync: {str(e)}", 500
