import hmac
import hashlib
import logging
import os

logger = logging.getLogger("primary_logger")


def verify_dbt_signature(request_body: bytes, signature: str, secret: str) -> bool:
    """
    Verify DBT Cloud webhook signature using HMAC-SHA256.

    Args:
        request_body: Raw request body as bytes
        signature: Signature from X-DBT-Signature header
        secret: DBT webhook secret from environment

    Returns:
        bool: True if signature is valid, False otherwise
    """
    if not signature or not secret:
        logger.warning("Missing signature or secret for DBT webhook verification")
        return False

    try:
        # Remove 'sha256=' prefix if present
        if signature.startswith("sha256="):
            signature = signature[7:]

        # Compute expected signature
        computed_hmac = hmac.new(
            secret.encode("utf-8"), request_body, hashlib.sha256
        ).hexdigest()

        # Compare signatures safely
        return hmac.compare_digest(computed_hmac, signature)

    except Exception as e:
        logger.exception(f"Error verifying DBT webhook signature: {str(e)}")
        return False


def parse_dbt_webhook(payload: dict) -> dict:
    """
    Parse DBT Cloud webhook payload and extract relevant information.

    Args:
        payload: DBT webhook JSON payload

    Returns:
        dict: Extracted information for fabric job mapping
    """
    try:
        # Extract common DBT webhook fields
        event_type = payload.get("eventType", "")
        data = payload.get("data", {})

        # Handle job completion events
        if event_type == "job.run.completed":
            job_data = data.get("job", {})
            run_data = data.get("run", {})

            return {
                "event_type": event_type,
                "job_id": str(job_data.get("id", "")),
                "job_name": job_data.get("name", ""),
                "run_id": str(run_data.get("id", "")),
                "run_status": run_data.get("status", ""),
                "run_status_humanized": run_data.get("statusHumanized", ""),
                "environment_id": str(data.get("environmentId", "")),
                "account_id": str(data.get("accountId", "")),
            }

    except Exception as e:
        logger.exception(f"Error parsing DBT webhook payload: {str(e)}")
        return {}

    return {}


def map_dbt_to_fabric(dbt_job_id: str) -> dict:
    """
    Map DBT job ID to Fabric workspace and item configuration.

    Args:
        dbt_job_id: DBT job ID from webhook

    Returns:
        dict: Fabric job configuration or None if no mapping found
    """
    # TODO: Replace with dynamic configuration from database or config file
    dbt_to_fabric_mapping = {
        # "23366": {
        #     "workspace_id": "c2bafcfd-df3d-4383-8f76-aed296260453",
        #     "item_id": "457998b0-be0c-437c-9b1a-4e5f17b3bf77",
        #     "job_type": "Execute",
        # },
        "23366": {
            "workspace_id": "c264b9e2-3d6e-483e-9898-1daf345ffcef",
            "item_id": "752c9a6a-7196-4a56-90f2-775fa3d1a965",
            "job_type": "Execute",
        }
        # Add more mappings as needed
    }

    mapping = dbt_to_fabric_mapping.get(dbt_job_id)
    if not mapping:
        logger.warning(f"No Fabric job mapping found for DBT job ID: {dbt_job_id}")
        return {}

    logger.info(
        f"Found Fabric mapping for DBT job {dbt_job_id}: workspace={mapping['workspace_id']}, item={mapping['item_id']}"
    )
    return mapping
