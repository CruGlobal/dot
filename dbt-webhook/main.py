import functions_framework
import os
import logging
import sys
import json
from google.cloud import pubsub_v1
from webhook_utils import verify_dbt_signature, parse_dbt_webhook, map_dbt_to_fabric, create_fabric_job_message

logger = logging.getLogger("primary_logger")
logger.propagate = False
gcp_project_id = os.environ.get("GOOGLE_CLOUD_PROJECT", None)
dbt_webhook_secret = os.environ.get("DBT_WEBHOOK_SECRET", None)
publisher = pubsub_v1.PublisherClient()
completed_topic_path = publisher.topic_path(gcp_project_id, "dbt-job-completed")
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
    global logger

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
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger = logging.getLogger("primary_logger")
    logger.exception(
        "Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback)
    )


def create_completion_message(dbt_info: dict) -> dict:
    """
    Create a generic dbt job completion message for the dbt-job-completed topic.
    Downstream workflows filter by job_id via Pub/Sub attribute filters.
    """
    return {
        "job_id": dbt_info.get("job_id", ""),
        "job_name": dbt_info.get("job_name", ""),
        "run_id": dbt_info.get("run_id", ""),
        "run_status": dbt_info.get("run_status", ""),
        "run_status_code": dbt_info.get("run_status_code", ""),
        "environment_id": dbt_info.get("environment_id", ""),
        "account_id": dbt_info.get("account_id", ""),
        "event_type": dbt_info.get("event_type", ""),
    }


def create_retry_message(dbt_info: dict) -> dict:
    """
    Create a retry event message for a failed dbt job.
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


def handle_job_success(dbt_info: dict) -> tuple:
    """
    Publish successful completion to the generic dbt-job-completed topic.
    Also publishes to legacy fabric-job-events topic if there's a Fabric mapping
    (backward compatibility — will be removed after Fabric migrates to the new topic).
    """
    completion_message = create_completion_message(dbt_info)

    try:
        message_json = json.dumps(completion_message)
        message_bytes = message_json.encode("utf-8")

        future = publisher.publish(
            completed_topic_path,
            message_bytes,
            job_id=dbt_info.get("job_id", ""),
            run_status=dbt_info.get("run_status", ""),
        )
        message_id = future.result()

        logger.info(
            f"Published dbt job completion to Pub/Sub: message_id={message_id}, "
            f"job_id={dbt_info.get('job_id')}, job_name={dbt_info.get('job_name')}"
        )

        # Legacy: also publish to fabric-job-events if this job has a Fabric mapping.
        # Remove this block after Fabric workflow migrates to dbt-job-completed topic.
        fabric_config = map_dbt_to_fabric(dbt_info.get("job_id", ""))
        if fabric_config:
            fabric_message = create_fabric_job_message(fabric_config, dbt_info)
            fabric_bytes = json.dumps(fabric_message).encode("utf-8")
            fabric_future = publisher.publish(fabric_topic_path, fabric_bytes)
            fabric_msg_id = fabric_future.result()
            logger.info(
                f"Published to legacy fabric-job-events: message_id={fabric_msg_id}, "
                f"workspace_id={fabric_config['workspace_id']}"
            )

        return (
            {
                "status": "success",
                "message": "Job completion published to dbt-job-completed topic",
                "message_id": message_id,
                "dbt_job_id": dbt_info.get("job_id"),
                "dbt_run_id": dbt_info.get("run_id"),
            },
            200,
        )

    except Exception as e:
        logger.exception(f"Error publishing to Pub/Sub: {str(e)}")
        return (f"Error publishing to Pub/Sub: {str(e)}", 500)


def handle_job_failure(dbt_info: dict) -> tuple:
    """
    Publish failed job to the retry topic for automatic retry processing.
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


@functions_framework.http
def webhook_handler(request):
    """
    Generic dbt Cloud webhook handler.
    Publishes all successful completions to dbt-job-completed topic with job_id
    as a message attribute. Downstream workflows (Fabric, Hightouch, etc.)
    subscribe with attribute filters and act independently.
    Publishes failures to dbt-retry-events topic for automatic retry.
    """
    setup_logging()

    try:
        request_body = request.get_data()
        signature = request.headers.get("authorization")

        if not signature:
            logger.warning("Missing DBT signature header")
            return ("Missing signature", 400)

        if not verify_dbt_signature(request_body, signature, dbt_webhook_secret):
            logger.error("Invalid DBT webhook signature")
            return ("Invalid signature", 403)

        try:
            request_json = json.loads(request_body.decode("utf-8"))
            logger.info(f"DBT webhook payload: {json.dumps(request_json, indent=2)}")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in request body: {str(e)}")
            return ("Invalid JSON", 400)

        dbt_info = parse_dbt_webhook(request_json)
        logger.info(f"Parsed DBT info: {dbt_info}")
        if not dbt_info:
            logger.error("Failed to parse DBT webhook payload")
            return ("Invalid DBT webhook payload", 400)

        logger.info(
            f"Received DBT webhook: event_type={dbt_info.get('event_type')}, "
            f"job_id={dbt_info.get('job_id')}, run_status={dbt_info.get('run_status')}"
        )

        if dbt_info.get("event_type") != "job.run.completed":
            logger.info(
                f"Ignoring DBT event - not a job completion: "
                f"{dbt_info.get('event_type')}"
            )
            return ("Event ignored - not a job completion", 200)

        run_status_code = dbt_info.get("run_status_code")

        if run_status_code == 20 or dbt_info.get("run_status") == "Error":
            return handle_job_failure(dbt_info)

        if run_status_code == 10 or dbt_info.get("run_status") == "Success":
            return handle_job_success(dbt_info)

        logger.info(
            f"Ignoring DBT event with unhandled status: "
            f"{dbt_info.get('run_status')}, status_code: {run_status_code}"
        )
        return ("Event ignored - unhandled run status", 200)

    except Exception as e:
        logger.exception(f"Unhandled error in webhook handler: {str(e)}")
        return (f"Internal server error: {str(e)}", 500)
