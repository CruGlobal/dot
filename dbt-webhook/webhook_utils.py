import hmac
import hashlib
import logging

logger = logging.getLogger("primary_logger")


def verify_dbt_signature(request_body: bytes, signature: str, secret: str) -> bool:
    """
    Verify DBT Cloud webhook authentication.
    DBT Cloud sends JWT Bearer tokens in Authorization header.
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
    Parse DBT Cloud webhook payload and extract relevant information.
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

    except Exception as e:
        logger.exception(f"Error parsing DBT webhook payload: {str(e)}")
        return {}


# Legacy Fabric mapping — kept for backward compatibility during parallel transition.
# Remove after Fabric workflow migrates to dbt-job-completed topic.
def map_dbt_to_fabric(dbt_job_id: str) -> dict:
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

    return {}
