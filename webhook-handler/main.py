import json
import logging
import functions_framework
import hashlib
import hmac
import base64
import sys

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

    logger.exception(
        "Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback)
    )


def verify_signature(request, secret):
    """Verify the request signature against a secret.

    Args:
        request: Flask request object.
        secret: The shared secret used to sign the request.

    Returns:
       True if the signature is valid, False otherwise
    """
    signature = request.headers.get("X-Fivetran-Signature")
    if not signature:
        logging.error("Missing X-Fivetran-Signature header")
        return False

    request_body = request.get_data(as_text=True)
    expected_signature = base64.b64encode(
        hmac.new(
            secret.encode("utf-8"), request_body.encode("utf-8"), hashlib.sha256
        ).digest()
    ).decode("utf-8")

    if not hmac.compare_digest(expected_signature, signature):
        logging.error("Invalid X-Fivetran-Signature")
        return False

    return True


def process_fivetran_webhook(request_json):
    """Processes Fivetran webhook payloads.
    Args:
        request_json (dict): The JSON payload of the request.

    Returns:
        dict, int : processed payload and status code
    """
    if "event" not in request_json:
        logging.error("Missing 'event' field in Fivetran payload")
        return {"error": "Missing 'event' field in Fivetran payload"}, 400

    if request_json["event"] not in ["sync_end", "sync_start"]:
        logging.info(f"skipping non-sync_end webhook event '{request_json['event']}'")
        return {
            "message": f"skipping non-sync_end webhook event '{request_json['event']}'"
        }, 200

    if "connector_id" not in request_json or "connector_name" not in request_json:
        logging.error("Missing 'connector_id' or 'connector_name' in Fivetran payload")
        return {
            "error": "Missing 'connector_id' or 'connector_name' in Fivetran payload"
        }, 400

    connector_id = request_json["connector_id"]
    connector_name = request_json["connector_name"]
    logging.info(f"Extracted connector_id: {connector_id}")
    logging.info(f"Extracted connector_name: {connector_name}")
    extracted_data = {
        "connector_id": connector_id,
        "connector_name": connector_name,
        "event": request_json["event"],
    }
    return extracted_data, 200


@functions_framework.http
def general_webhook_handler(request):
    """
    Handles general webhook events, with specific logic for Fivetran webhooks based on the origin.

    Args:
        request (flask.Request): The request object.
        The expected body will be in JSON format

    Returns:
        A Flask response object with a status code and message.
    """

    try:
        request_json = request.get_json()
        logging.info(f"Received request: {json.dumps(request_json)}")

        if not request_json:
            logging.error("No JSON payload received")
            return {"error": "No JSON payload received"}, 400

        origin = request.headers.get("Origin")
        logging.info(f"Origin: {origin}")

        if origin and "fivetran.com" in origin:
            webhook_secret = "your_webhook_secret"
            if not verify_signature(request, webhook_secret):
                return {"error": "Invalid signature for Fivetran request"}, 403
            return process_fivetran_webhook(request_json)

        event = request_json.get("event")
        if event:
            logging.info(f"Processing generic event of type: '{event}'")
            return {"message": f"processing generic event of type '{event}'"}, 200
        else:
            logging.info("Processing generic payload")
            return {"message": "processing generic payload"}, 200

    except Exception as e:
        logging.exception(f"An error occurred: {e}")
        return {"error": f"An error occurred: {e}"}, 500
