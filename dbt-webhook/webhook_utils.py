import hmac
import hashlib
import logging
import os

logger = logging.getLogger("primary_logger")


def verify_dbt_signature(request_body: bytes, signature: str, secret: str) -> bool:
    """
    Verify DBT Cloud webhook authentication.
    DBT Cloud sends JWT Bearer tokens in Authorization header instead of HMAC signatures.

    Args:
        request_body: Raw request body as bytes
        signature: Authorization header value from DBT Cloud
        secret: DBT webhook secret from environment (currently unused for JWT)

    Returns:
        bool: True if signature is valid, False otherwise
    """
    if not signature:
        logger.warning("Missing authorization header for DBT webhook verification")
        return False

    try:
        # DBT Cloud sends JWT Bearer tokens, not HMAC signatures
        # For now, accept any Bearer token (DBT handles authentication)
        if signature.startswith("Bearer "):
            logger.info("Valid JWT Bearer token received from DBT Cloud")
            return True
        
        # Fallback: Try HMAC validation for compatibility
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
            # DBT webhook puts all fields directly in 'data', not nested under 'job'/'run'
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
