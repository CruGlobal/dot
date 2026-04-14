"""
dbt Cloud Webhook Handler — Generic Event Publisher

Receives webhook notifications from dbt Cloud when jobs complete.
Publishes events to Pub/Sub topics for downstream workflow consumption.

Event routing:
  - Successful completions → dbt-job-completed topic (all downstream workflows)
  - Failed completions → dbt-retry-events topic (automatic retry workflow)

The dbt-job-completed topic is the generic fan-out point for all post-dbt
orchestration. Downstream workflows (Fabric, Hightouch, etc.) subscribe with
Pub/Sub attribute filters on job_id and act independently. Adding a new
downstream integration requires only a Terraform change (new subscription +
workflow), not a code change here.

Legacy: Successful completions for jobs with a Fabric mapping also publish
to the fabric-job-events topic for backward compatibility. This will be
removed after the Fabric workflow migrates to the dbt-job-completed topic.

Related:
  - Terraform: cru-terraform/applications/data-warehouse/dot/prod/
  - Design: ~/dotfiles/design-docs/design-netsuite-hightouch-orchestration.md
  - Jira: DT-495
  - Secrets: dbt-webhook_DBT_WEBHOOK_SECRET in Secret Manager (cru-data-orchestration-prod)
"""

import functions_framework
import os
import logging
import sys
import json
from google.cloud import pubsub_v1
from webhook_utils import (
    verify_dbt_signature,
    parse_dbt_webhook,
    map_dbt_to_fabric,
    create_fabric_job_message,
)

logger = logging.getLogger("primary_logger")
logger.propagate = False

gcp_project_id = os.environ.get("GOOGLE_CLOUD_PROJECT", None)
if not gcp_project_id:
    raise EnvironmentError(
        "GOOGLE_CLOUD_PROJECT environment variable is required. "
        "Set it to the GCP project ID where Pub/Sub topics are created."
    )

dbt_webhook_secret = os.environ.get("DBT_WEBHOOK_SECRET", None)
publisher = pubsub_v1.PublisherClient()
completed_topic_path = publisher.topic_path(gcp_project_id, "dbt-job-completed")
fabric_topic_path = publisher.topic_path(gcp_project_id, "fabric-job-events")
retry_topic_path = publisher.topic_path(gcp_project_id, "dbt-retry-events")


class CloudLoggingFormatter(logging.Formatter):
    """Formats log records as JSON for Google Cloud Logging."""

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
    Build the message payload for the dbt-job-completed Pub/Sub topic.

    This message is consumed by all downstream workflows. Include every field
    that any consumer might need — adding fields later requires coordinating
    with all subscribers.
    """
    return {
        "job_id": dbt_info.get("job_id", ""),
        "job_name": dbt_info.get("job_name", ""),
        "run_id": dbt_info.get("run_id", ""),
        "run_status": dbt_info.get("run_status", ""),
        "run_status_code": dbt_info.get("run_status_code", ""),
        "run_status_humanized": dbt_info.get("run_status_humanized", ""),
        "environment_id": dbt_info.get("environment_id", ""),
        "account_id": dbt_info.get("account_id", ""),
        "event_type": dbt_info.get("event_type", ""),
    }


def create_retry_message(dbt_info: dict) -> dict:
    """
    Build the message payload for the dbt-retry-events Pub/Sub topic.

    The attempt_number starts at 0 here. The dbt-retry-workflow increments it
    on each retry attempt and re-publishes if retries are exhausted.
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
    Publish successful completion to the generic dbt-job-completed topic,
    then optionally to the legacy fabric-job-events topic if there's a mapping.

    The Fabric publish is isolated in its own try/except so a Fabric failure
    does not cause a 500 response or trigger a dbt Cloud webhook retry (which
    would duplicate the message on dbt-job-completed).
    """
    completion_message = create_completion_message(dbt_info)
    message_id = None

    try:
        message_json = json.dumps(completion_message)
        message_bytes = message_json.encode("utf-8")

        future = publisher.publish(
            completed_topic_path,
            message_bytes,
            job_id=dbt_info.get("job_id", ""),
            run_status=dbt_info.get("run_status", ""),
            environment_id=dbt_info.get("environment_id", ""),
        )
        message_id = future.result(timeout=10)

        logger.info(
            f"Published dbt job completion to Pub/Sub: message_id={message_id}, "
            f"job_id={dbt_info.get('job_id')}, job_name={dbt_info.get('job_name')}"
        )

    except Exception as e:
        logger.exception(f"Error publishing to dbt-job-completed: {str(e)}")
        return (f"Error publishing to Pub/Sub: {str(e)}", 500)

    # Legacy: also publish to fabric-job-events if this job has a Fabric mapping.
    # This block is isolated so a Fabric failure does not affect the primary
    # publish result or trigger a webhook retry.
    # Remove this block after Fabric workflow migrates to dbt-job-completed topic.
    fabric_config = map_dbt_to_fabric(dbt_info.get("job_id", ""))
    if fabric_config:
        try:
            fabric_message = create_fabric_job_message(fabric_config, dbt_info)
            fabric_bytes = json.dumps(fabric_message).encode("utf-8")
            fabric_future = publisher.publish(
                fabric_topic_path,
                fabric_bytes,
                job_id=dbt_info.get("job_id", ""),
            )
            fabric_msg_id = fabric_future.result(timeout=10)
            logger.info(
                f"Published to legacy fabric-job-events: message_id={fabric_msg_id}, "
                f"workspace_id={fabric_config['workspace_id']}"
            )
        except Exception as e:
            logger.exception(
                f"Error publishing to legacy fabric-job-events (non-fatal): {str(e)}"
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

        future = publisher.publish(
            retry_topic_path,
            message_bytes,
            job_id=dbt_info.get("job_id", ""),
            run_status=dbt_info.get("run_status", ""),
            environment_id=dbt_info.get("environment_id", ""),
        )
        message_id = future.result(timeout=10)

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
    HTTP Cloud Function entry point for dbt Cloud webhooks.

    Receives job completion notifications and routes them:
      - Success → dbt-job-completed topic (+ legacy fabric-job-events if mapped)
      - Failure → dbt-retry-events topic
      - Cancelled/other → logged and ignored (200 response)

    dbt Cloud sends webhooks with JWT Bearer tokens in the Authorization header.
    The dbt Cloud webhook should be configured at the account level with an empty
    job list so ALL job completions are received.
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

        # Route by status. Status code is authoritative; string is a fallback
        # for resilience in case the payload format changes.
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
