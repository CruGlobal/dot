import functions_framework
import os
import logging
import sys
import json
from google.cloud import pubsub_v1
from webhook_utils import verify_dbt_signature, parse_dbt_webhook, map_dbt_to_fabric

logger = logging.getLogger("primary_logger")
logger.propagate = False
gcp_project_id = os.environ.get("GOOGLE_CLOUD_PROJECT", None)
dbt_webhook_secret = os.environ.get("DBT_WEBHOOK_SECRET", None)
publisher = pubsub_v1.PublisherClient()
fabric_topic_path = publisher.topic_path(gcp_project_id, "fabric-job-events")
retry_topic_path = publisher.topic_path(gcp_project_id, "dbt-retry-events")


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


def create_fabric_job_message(fabric_config: dict, dbt_info: dict) -> dict:
    """
    Create a generic fabric job request message.

    Args:
        fabric_config: Fabric job configuration (workspace_id, item_id, job_type)
        dbt_info: DBT webhook information for context

    Returns:
        dict: Generic fabric job request message
    """
    return {
        "workspace_id": fabric_config["workspace_id"],
        "item_id": fabric_config["item_id"],
        "refresh_workspace_id": fabric_config["refresh_workspace_id"],
        "lakehouse_dataset_id": fabric_config["lakehouse_dataset_id"],
        "job_type": fabric_config["job_type"],
        "trigger_source": "dbt_completion",
        "enable_monitoring": True,
        "source_job_id": dbt_info.get("job_id", ""),
        "source_system": "dbt",
        "execution_context": {
            "dbt_job_id": dbt_info.get("job_id", ""),
            "dbt_job_name": dbt_info.get("job_name", ""),
            "dbt_run_id": dbt_info.get("run_id", ""),
            "dbt_run_status": dbt_info.get("run_status", ""),
            "dbt_environment_id": dbt_info.get("environment_id", ""),
            "dbt_account_id": dbt_info.get("account_id", ""),
            "event_type": dbt_info.get("event_type", ""),
        },
        "execution_data": None,
    }


def create_retry_message(dbt_info: dict) -> dict:
    """
    Create a retry event message for a failed dbt job.

    Args:
        dbt_info: Parsed DBT webhook information

    Returns:
        dict: Retry event message for the dbt-retry-events topic
    """
    return {
        "job_id": dbt_info.get("job_id", ""),
        "run_id": dbt_info.get("run_id", ""),
        "job_name": dbt_info.get("job_name", ""),
        "run_status": dbt_info.get("run_status", ""),
        "run_status_code": dbt_info.get("run_status_code", ""),
        "environment_id": dbt_info.get("environment_id", ""),
        "account_id": dbt_info.get("account_id", ""),
        "attempt_number": 0,
    }


def handle_job_failure(dbt_info: dict) -> tuple:
    """
    Handle a failed dbt job completion by publishing to the retry topic.
    """
    logger.info(
        f"DBT job failed: job_id={dbt_info.get('job_id')}, "
        f"run_id={dbt_info.get('run_id')}, status={dbt_info.get('run_status')}"
    )

    try:
        retry_message = create_retry_message(dbt_info)
        message_json = json.dumps(retry_message)
        message_bytes = message_json.encode("utf-8")

        future = publisher.publish(retry_topic_path, message_bytes)
        message_id = future.result()

        logger.info(
            f"Published retry event to Pub/Sub: message_id={message_id}, "
            f"job_id={dbt_info.get('job_id')}, run_id={dbt_info.get('run_id')}"
        )

        return (
            {
                "status": "failure_processed",
                "message": "Job failure published to retry topic",
                "message_id": message_id,
                "dbt_job_id": dbt_info.get("job_id"),
                "dbt_run_id": dbt_info.get("run_id"),
            },
            200,
        )

    except Exception as e:
        logger.exception(f"Error publishing retry event to Pub/Sub: {str(e)}")
        return (f"Error publishing retry event: {str(e)}", 500)


def handle_job_success(dbt_info: dict) -> tuple:
    """
    Handle a successful dbt job completion by triggering Fabric jobs if mapped.
    """
    fabric_config = map_dbt_to_fabric(dbt_info.get("job_id", ""))
    if not fabric_config:
        logger.info(
            f"No Fabric mapping configured for DBT job ID: {dbt_info.get('job_id')} - webhook processed successfully"
        )
        return (
            "Webhook processed - no Fabric job mapping configured for this DBT job",
            200,
        )

    fabric_message = create_fabric_job_message(fabric_config, dbt_info)

    try:
        message_json = json.dumps(fabric_message)
        message_bytes = message_json.encode("utf-8")

        future = publisher.publish(fabric_topic_path, message_bytes)
        message_id = future.result()

        logger.info(
            f"Published Fabric job request to Pub/Sub: message_id={message_id}, "
            f"workspace_id={fabric_config['workspace_id']}, item_id={fabric_config['item_id']}"
        )

        return (
            {
                "status": "success",
                "message": "Fabric job request published",
                "message_id": message_id,
                "dbt_job_id": dbt_info.get("job_id"),
                "fabric_workspace_id": fabric_config["workspace_id"],
                "fabric_item_id": fabric_config["item_id"],
            },
            200,
        )

    except Exception as e:
        logger.exception(f"Error publishing to Pub/Sub: {str(e)}")
        return (f"Error publishing to Pub/Sub: {str(e)}", 500)


@functions_framework.http
def webhook_handler(request):
    """
    HTTP Cloud Function to handle DBT webhook events and trigger Fabric jobs.
    """
    setup_logging()

    try:
        # Get request data and signature
        request_body = request.get_data()
        signature = request.headers.get("authorization")

        if not signature:
            logger.warning("Missing DBT signature header")
            return ("Missing signature", 400)

        # Verify signature
        if not verify_dbt_signature(request_body, signature, dbt_webhook_secret):
            logger.error("Invalid DBT webhook signature")
            return ("Invalid signature", 403)

        # Parse request JSON
        try:
            request_json = json.loads(request_body.decode("utf-8"))
            logger.info(f"DBT webhook payload: {json.dumps(request_json, indent=2)}")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in request body: {str(e)}")
            return ("Invalid JSON", 400)

        # Parse DBT webhook data
        dbt_info = parse_dbt_webhook(request_json)
        logger.info(f"Parsed DBT info: {dbt_info}")
        if not dbt_info:
            logger.error("Failed to parse DBT webhook payload")
            return ("Invalid DBT webhook payload", 400)

        logger.info(
            f"Received DBT webhook: event_type={dbt_info.get('event_type')}, job_id={dbt_info.get('job_id')}, run_status={dbt_info.get('run_status')}"
        )

        # Only process job completion events
        if dbt_info.get("event_type") != "job.run.completed":
            logger.info(
                f"Ignoring DBT event - not a job completion: {dbt_info.get('event_type')}"
            )
            return ("Event ignored - not a job completion", 200)

        run_status_code = dbt_info.get("run_status_code")

        # Handle failed job completions (status_code 20 = Error)
        if run_status_code == 20 or dbt_info.get("run_status") == "Error":
            return handle_job_failure(dbt_info)

        # Handle successful job completions (status_code 10 = Success)
        if run_status_code == 10 or dbt_info.get("run_status") == "Success":
            return handle_job_success(dbt_info)

        # Other statuses (cancelled, etc.) — log and ignore
        logger.info(
            f"Ignoring DBT event with unhandled status: {dbt_info.get('run_status')}, status_code: {run_status_code}"
        )
        return ("Event ignored - unhandled run status", 200)

    except Exception as e:
        logger.exception(f"Unhandled error in webhook handler: {str(e)}")
        return (f"Internal server error: {str(e)}", 500)
