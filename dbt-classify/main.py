import os
import sys
import json
import logging
import functions_framework

from dbt_client import DbtReadClient
import classifier

logger = logging.getLogger("primary_logger")
logger.propagate = False

# Cru dbt Cloud account id; the caller (the retry workflow) always sends account_id,
# so this fallback should not normally be used.
DEFAULT_ACCOUNT_ID = "10206"


class CloudLoggingFormatter(logging.Formatter):
    """Produces messages compatible with google cloud logging"""

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
    handler.setFormatter(CloudLoggingFormatter(fmt="%(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    sys.excepthook = handle_unhandled_exception


def handle_unhandled_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.exception(
        "Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback)
    )


def _parse_request(request):
    request_json = request.get_json(silent=True)
    if (
        request_json is None
        and request.headers.get("Content-Type") == "application/octet-stream"
    ):
        try:
            data = request.get_data(as_text=True)
            request_json = json.loads(data) if data else None
        except Exception as e:
            logger.exception(f"Failed to parse octet-stream data: {str(e)}")
            request_json = None
    return request_json


@functions_framework.http
def classify_run(request):
    """Classify a failed dbt Cloud run as retryable (transient) or not.

    Input  (POST JSON): {"run_id": "...", "account_id": "..."}  (account_id optional)
    Output (JSON):      the verdict from classifier.decide() -- reason, is_retryable,
                        prior_is_retry, counts, failed_nodes, run_created_at.

    The retry workflow calls this so the large run_results.json is parsed here (Python)
    rather than held in a Cloud Workflow variable (which exceeds the Workflows memory
    limit on large jobs). This function makes dbt API calls by design -- it is invoked
    by a workflow, not a webhook, so the DOT "no API calls from functions" rule (which
    exists to keep webhooks fast) does not apply.
    """
    setup_logging()

    request_json = _parse_request(request)
    if not request_json or "run_id" not in request_json:
        logger.error("Request missing run_id")
        return ("Missing run_id", 400)

    run_id = str(request_json["run_id"])
    # The webhook always includes the key (built as str(payload.get("accountId", ""))),
    # so an omitted accountId arrives here as "" -- present but empty. `or` falls back
    # to the Cru account; a plain .get(..., default) only covers an absent key, not an
    # empty one, and "" would build a .../accounts//runs/... URL (404).
    account_id = str(request_json.get("account_id") or DEFAULT_ACCOUNT_ID)

    token = (os.environ.get("DBT_TOKEN") or "").strip(chr(0xFEFF)).strip()
    if not token:
        logger.error("DBT_TOKEN is missing or empty")
        return ("Server not configured: missing DBT_TOKEN", 500)

    client = DbtReadClient(access_token=token, account_id=account_id)

    # Run metadata (trigger + steps). If unreadable, decide() fails closed
    # (metadata_unavailable) since we can't confirm the run wasn't already a retry.
    try:
        run_data = client.get_run(run_id)
    except Exception as e:
        logger.exception(f"Could not fetch run metadata for run {run_id}: {e}")
        run_data = None

    # run_results.json (the large artifact). Only fetched if metadata read; a failure
    # here -> results_unavailable (cannot classify -> not retryable).
    results = None
    results_fetch_failed = False
    if run_data is not None:
        try:
            results = client.get_run_results(run_id)
        except Exception as e:
            logger.warning(f"Could not fetch run_results.json for run {run_id}: {e}")
            results_fetch_failed = True

    verdict = classifier.decide(
        run_data, results, results_fetch_failed=results_fetch_failed
    )
    logger.info(
        f"Classification for run {run_id}: reason={verdict['reason']} "
        f"is_retryable={verdict['is_retryable']} prior_is_retry={verdict['prior_is_retry']}"
    )
    return (json.dumps(verdict), 200, {"Content-Type": "application/json"})
