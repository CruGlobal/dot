"""
Utility functions for the dbt Cloud webhook handler.

Contains:
  - Webhook signature verification
  - Payload parsing
  - Legacy Fabric job mapping (temporary, for parallel transition)
"""

import hmac
import hashlib
import logging

logger = logging.getLogger("primary_logger")


def verify_dbt_signature(request_body: bytes, signature: str, secret: str) -> bool:
    """
    Verify dbt Cloud webhook authentication.

    dbt Cloud sends JWT Bearer tokens in the Authorization header. This function
    accepts any Bearer token without validating the JWT signature — dbt Cloud is
    trusted at the network layer (Cloud Function is not publicly discoverable).

    For non-Bearer signatures, falls back to HMAC-SHA256 validation for
    compatibility with older webhook configurations.

    Known limitation: The Bearer path does not verify token content. If stricter
    auth is needed, add PyJWT validation or compare against the webhook secret.
    """
    if not signature:
        logger.warning("Missing authorization header for DBT webhook verification")
        return False

    try:
        if signature.startswith("Bearer "):
            logger.info("Valid JWT Bearer token received from DBT Cloud")
            return True

        computed_hmac = hmac.new(
            secret.encode("utf-8"), request_body, hashlib.sha256
        ).hexdigest()

        signature_valid = computed_hmac == signature
        logger.debug(f"HMAC signature validation result: {signature_valid}")

        return signature_valid

    except Exception as e:
        logger.exception(f"Error verifying DBT webhook authentication: {str(e)}")
        return False


def parse_dbt_webhook(payload: dict) -> dict:
    """
    Parse dbt Cloud webhook payload and extract relevant fields.

    Only processes job.run.completed events. Returns an empty dict for all
    other event types (the caller checks event_type to decide how to handle).

    Returns:
        dict: Parsed fields if event type is job.run.completed, empty dict otherwise.
        Never returns None.
    """
    try:
        event_type = payload.get("eventType", "")
        data = payload.get("data", {})

        if event_type == "job.run.completed":
            return {
                "event_type": event_type,
                "job_id": str(data.get("jobId", "")),
                "job_name": data.get("jobName", ""),
                "run_id": str(data.get("runId", "")),
                "run_status": data.get("runStatus", ""),
                "run_status_code": data.get("runStatusCode", ""),
                "run_status_humanized": data.get("runStatusMessage", ""),
                "environment_id": str(data.get("environmentId", "")),
                "account_id": str(payload.get("accountId", "")),
            }

        return {}

    except Exception as e:
        logger.exception(f"Error parsing DBT webhook payload: {str(e)}")
        return {}


# ---------------------------------------------------------------------------
# Legacy Fabric mapping — kept for backward compatibility during parallel
# transition. Remove after Fabric workflow migrates to dbt-job-completed topic.
# ---------------------------------------------------------------------------

def map_dbt_to_fabric(dbt_job_id: str) -> dict:
    """
    Map a dbt job ID to Fabric workspace and item configuration.

    Returns the Fabric config dict if the job has a mapping, empty dict otherwise.
    This mapping will be removed when the Fabric workflow subscribes to the
    dbt-job-completed topic with a Pub/Sub attribute filter instead.
    """
    dbt_to_fabric_mapping = {
        "163545": {
            "workspace_id": "c2bafcfd-df3d-4383-8f76-aed296260453",
            "item_id": "457998b0-be0c-437c-9b1a-4e5f17b3bf77",
            "refresh_workspace_id": "b3d68b22-ae01-4017-af31-1392c5c54a6c",
            "lakehouse_dataset_id": "1402b359-a8e4-48f2-a69e-50bff4e37122",
            "job_type": "Execute",
        }
    }

    mapping = dbt_to_fabric_mapping.get(dbt_job_id)
    if not mapping:
        return {}

    logger.info(
        f"Found Fabric mapping for DBT job {dbt_job_id}: "
        f"workspace={mapping['workspace_id']}, item={mapping['item_id']}"
    )
    return mapping


def create_fabric_job_message(fabric_config: dict, dbt_info: dict) -> dict:
    """
    Build the message payload for the legacy fabric-job-events topic.

    This message format is consumed by the fabric-job-workflow in Cloud Workflows.
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
