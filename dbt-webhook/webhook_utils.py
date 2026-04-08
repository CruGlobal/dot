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

    return {}
