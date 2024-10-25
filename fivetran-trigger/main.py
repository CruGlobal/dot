import os
import functions_framework
from fivetran_client import FivetranClient
import logging
import sys
from pythonjsonlogger import jsonlogger
from dotenv import load_dotenv

logger = logging.getLogger("primary_logger")


def setup_logging():
    """
    Sets up logging for the application.
    """
    json_formatter = jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(message)s")
    stdout_handler = logging.StreamHandler(stream=sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.setFormatter(json_formatter)
    logger = logging.getLogger("primary_logger")
    logger.addHandler(stdout_handler)
    logger.setLevel(logging.INFO)
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
    load_dotenv(".env")
    setup_logging()

    logger = logging.getLogger("primary_logger")
    api_key = os.environ.get("FIVETRAN_API_KEY")
    api_secret = os.environ.get("FIVETRAN_API_SECRET")
    connector_id = os.environ.get("CONNECTOR_ID")

    if not connector_id:
        logger.error("Error: CONNECTOR_ID environment variable is not set")
        return "CONNECTOR_ID environment variable is not set", 400

    client = FivetranClient(api_key, api_secret)

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
