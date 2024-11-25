import os
import functions_framework
from dbt_client import DbtClient
import logging
import sys
import os
import json
from requests import auth, Session
import time


logger = logging.getLogger("primary_logger")
logger.propagate = False


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
    Handles unhandled exceptions by logging the exception details and sending an alert to the development team.

    This function is intended to be used as a custom excepthook function, which is called when an unhandled exception
    occurs in the application. The function logs the exception details to the primary logger, and sends an alert to
    the development team using a third-party service such as Datadog or PagerDuty.

    Args:
        exc_type (type): The type of the exception that was raised.
        exc_value (Exception): The exception object that was raised.
        exc_traceback (traceback): The traceback object that was generated when the exception was raised.
    """
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logger.exception(
        "Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback)
    )


@functions_framework.http
def trigger_dbt_job(request):
    """
    Triggers a dbt job for a given job id.

    """
    setup_logging()

    request_json = request.get_json(silent=True)

    if (
        request_json is None
        and request.headers.get("Content-Type") == "application/octet-stream"
    ):
        try:
            request_data = request.get_data(as_text=True)
            request_json = json.loads(request_data) if request_data else None
        except Exception as e:
            logger.exception(f"Failed to parse octet-stream data: {str(e)}")
            request_json = None

    if request_json and "job_id" in request_json:
        job_id = request_json["job_id"]
        if "wait_for_completion" in request_json:
            wait_for_completion_val = request_json["wait_for_completion"]
            wait_for_completion = wait_for_completion_val.lower() == "true"
    else:
        logger.exception("Failed to retrieve job_id")
        raise

    dbt_token = os.environ["DBT_TOKEN"]
    account_id = "10206"
    try:
        client = DbtClient(access_token=dbt_token, account_id=account_id)
        job_run_response = client.trigger_job(job_id)
        run_id = job_run_response["data"]["id"]
        if run_id is None:
            logger.exception(f"dbt run failed to start.")
            return
        logger.info(f"DBT run {run_id} started successfully.")
        if wait_for_completion:
            logger.info(f"Checking run details for run {run_id}.")
            is_complete = False
            exit_code = 110
            while not is_complete:
                run_details = client.get_run_details(run_id)
                is_complete = run_details["data"]["is_complete"]
                if not is_complete:
                    logger.info(
                        f"The run {run_id} is not yet completed. Waiting for 30 seconds..."
                    )
                    time.sleep(30)
                else:
                    exit_code = client.determine_run_status(run_id)
                    if exit_code != 10:
                        failed_steps = [
                            step
                            for step in run_details["data"]["run_steps"]
                            if step["status"] != 10
                        ]

                        error_details = {
                            "run_id": run_details["data"]["id"],
                            "status": run_details["data"]["status"],
                            "status_message": run_details["data"]["status_message"],
                            "failed_steps": [
                                {"step_name": step["name"]} for step in failed_steps
                            ],
                        }

                        error_message = (
                            f"DBT Run Failure Summary:\n"
                            f"Run ID: {error_details['run_id']}\n"
                            f"Status: {error_details['status']}\n"
                            f"Message: {error_details['status_message']}\n"
                            f"Failed Steps: {', '.join(step['step_name'] for step in error_details['failed_steps'])}"
                        )

                        # logger.exception(error_message)
                        raise RuntimeError(error_message)
        return "Trigger dbt job completed", 200
    except Exception as e:
        logger.exception(
            f"An error occurred when attempting to trigger dbt job: {str(e)}"
        )
        raise
