import functions_framework
import os
import logging
import sys
import json
import hmac
from google.cloud import pubsub_v1

logger = logging.getLogger("primary_logger")
logger.propagate = False

PUBSUB_TOPIC = "fivetran-slot-valve-events"

# Reviewed RDS instance (dbinstanceidentifier) -> active Fivetran connector_id.
# See dot docs/DESIGN_fivetran_slot_safety_valve.md Section 4. Only the active
# connector per instance is a valid drain target; the paused "dead twin"
# connectors that share each schema are intentionally excluded.
INSTANCE_TO_CONNECTOR = {
    "mpdx-api-prod": "loft_unabashed",  # el_mpdx
    "global-registry-prod": "centralized_mitigation",  # el_global_registry
    "global-registry-flat-prod": "freebee_tuberculosis",  # el_global_registry_flat
}

# Datadog alert transitions that should NOT trigger a drain. Everything else
# (Triggered, Re-Triggered, Renotify, ...) is treated as an active breach and
# forwarded; the downstream workflow is idempotent, so an unexpected transition
# string fails safe toward draining rather than silently skipping a real alert.
NON_ACTING_TRANSITIONS = {"Recovered", "Re-No Data"}

_publisher = None


def _get_publisher():
    global _publisher
    if _publisher is None:
        _publisher = pubsub_v1.PublisherClient()
    return _publisher


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


def extract_instance_id(payload: dict):
    """
    Resolve the RDS instance id from the Datadog webhook payload's tags.

    The valve monitors carry a ``dbinstanceidentifier:<instance>`` tag (see
    datadog.tf); Datadog renders $TAGS as a comma-separated string, but a list
    is handled defensively. Returns the instance id, or None if absent.
    """
    tags = payload.get("tags")
    if isinstance(tags, str):
        candidates = [t.strip() for t in tags.split(",")]
    elif isinstance(tags, list):
        candidates = [str(t).strip() for t in tags]
    else:
        candidates = []

    for tag in candidates:
        if tag.startswith("dbinstanceidentifier:"):
            return tag.split(":", 1)[1].strip() or None
    return None


@functions_framework.http
def valve_handler(request):
    """
    Receives a Datadog valve-threshold webhook, validates it, resolves the
    target Fivetran connector, and publishes a drain event to Pub/Sub. All
    connector-state handling and the actual sync happen in the
    fivetran-slot-valve Cloud Workflow that consumes the topic -- this function
    stays thin (validate, classify, publish, return) per the DOT push pattern.
    """
    setup_logging()

    # 1. Validate required configuration up front (a clear 500, rather than a
    #    misleading "Pub/Sub error" later if GOOGLE_CLOUD_PROJECT is unset).
    expected_secret = os.environ.get("WEBHOOK_SECRET")
    gcp_project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not expected_secret or not gcp_project_id:
        logger.error("WEBHOOK_SECRET or GOOGLE_CLOUD_PROJECT is not configured")
        return ("Server not configured", 500)

    provided_secret = request.headers.get("X-Valve-Secret", "")
    if not hmac.compare_digest(provided_secret, expected_secret):
        logger.warning("Rejected valve request with missing or invalid secret")
        return ("Invalid or missing valve secret", 403)

    # 2. Parse the payload.
    payload = request.get_json(silent=True)
    if not payload:
        logger.error("Missing or invalid JSON body")
        return ("Missing or invalid JSON body", 400)

    # 3. Ignore recovery / non-acting transitions -- only act on active breaches.
    transition = payload.get("alert_transition")
    if transition in NON_ACTING_TRANSITIONS:
        logger.info(f"No action for alert_transition '{transition}'")
        return (f"No action for transition '{transition}'", 200)

    # 4. Resolve the RDS instance and map it to its active connector.
    instance_id = extract_instance_id(payload)
    if not instance_id:
        logger.error(f"Could not resolve dbinstanceidentifier from payload: {payload}")
        return ("Could not resolve dbinstanceidentifier from payload", 400)

    connector_id = INSTANCE_TO_CONNECTOR.get(instance_id)
    if not connector_id:
        logger.error(f"No connector mapping for instance '{instance_id}'")
        return (f"No connector mapping for instance '{instance_id}'", 422)

    # 5. Publish the drain event for the workflow to orchestrate.
    message = {
        "instance_id": instance_id,
        "connector_id": connector_id,
        "alert_id": payload.get("alert_id"),
        "alert_title": payload.get("alert_title"),
        "alert_transition": transition,
        "link": payload.get("link"),
    }

    try:
        publisher = _get_publisher()
        topic_path = publisher.topic_path(gcp_project_id, PUBSUB_TOPIC)
        future = publisher.publish(topic_path, json.dumps(message).encode("utf-8"))
        future.result(timeout=30)
        logger.info(
            f"Published slot-valve drain event: instance={instance_id} "
            f"connector={connector_id}"
        )
        return ("Valve event accepted", 200)
    except Exception as e:
        logger.exception(f"Error publishing to Pub/Sub: {e}")
        return (f"Error publishing to Pub/Sub: {str(e)}", 500)
