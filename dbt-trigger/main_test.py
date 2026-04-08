import pytest
import responses
import logging
import sys
from unittest import mock
from flask import Request
import main
import requests


@pytest.fixture
def mock_env_vars(monkeypatch):
    monkeypatch.setenv("DBT_TOKEN", "test_dbt_token")


@pytest.fixture
def mock_request_with_job_id():
    mock_req = mock.Mock(spec=Request)
    mock_req.get_json.return_value = {"job_id": "test_job_id"}
    return mock_req


@pytest.fixture
def mock_request_with_custom_cause():
    mock_req = mock.Mock(spec=Request)
    mock_req.get_json.return_value = {
        "job_id": "test_job_id",
        "cause": "Auto-retry (attempt 1): transient failure in run 12345",
    }
    return mock_req


@pytest.fixture
def mock_request_without_job_id():
    mock_req = mock.Mock(spec=Request)
    mock_req.get_json.return_value = {}
    return mock_req


@pytest.fixture(autouse=True)
def setup_logging():
    """Set up logging for tests using the same configuration as main.py"""
    logger = logging.getLogger("primary_logger")
    logger.handlers = []
    logger.propagate = True

    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = main.CloudLoggingFormatter(fmt="%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    yield

    logger.handlers = []
    logger.propagate = False


@responses.activate
def test_trigger_dbt_job_success(mock_env_vars, mock_request_with_job_id, caplog):
    """
    Tests the trigger_dbt_job function when the job is triggered successfully.
    """
    responses.add(
        responses.POST,
        "https://cloud.getdbt.com/api/v2/accounts/10206/jobs/test_job_id/run/",
        json={"data": {"id": "test_run_id"}},
        status=200,
    )

    response = main.trigger_dbt_job(mock_request_with_job_id)

    success_message = "DBT run test_run_id started successfully."
    log_messages = [record.message for record in caplog.records]
    assert any(
        success_message in msg for msg in log_messages
    ), f"Expected message not found in logs: {log_messages}"
    assert response[0] == "Trigger dbt job completed"
    assert response[1] == 200


@responses.activate
def test_trigger_dbt_job_missing_job_id(
    mock_env_vars, mock_request_without_job_id, caplog
):
    """
    Tests the trigger_dbt_job function when job_id is missing from request.
    """
    try:
        main.trigger_dbt_job(mock_request_without_job_id)
        pytest.fail("Expected function to raise an exception")
    except Exception as e:
        log_messages = [record.message for record in caplog.records]
        assert any(
            "Failed to retrieve job_id" in msg for msg in log_messages
        ), f"Expected error message not found in logs: {log_messages}"


@responses.activate
def test_trigger_dbt_job_invalid_credentials(
    mock_env_vars, mock_request_with_job_id, caplog
):
    """
    Tests the trigger_dbt_job function with invalid API credentials.
    """
    responses.add(
        responses.POST,
        "https://cloud.getdbt.com/api/v2/accounts/10206/jobs/test_job_id/run/",
        status=401,
        json={"message": "Invalid API key"},
    )

    try:
        main.trigger_dbt_job(mock_request_with_job_id)
        pytest.fail("Expected function to raise an exception")
    except Exception as e:
        log_messages = [record.message for record in caplog.records]
        error_message = "An error occurred when attempting to trigger dbt job"
        assert any(
            error_message in msg for msg in log_messages
        ), f"Expected error message not found in logs: {log_messages}"


@responses.activate
def test_trigger_dbt_job_server_error(mock_env_vars, mock_request_with_job_id, caplog):
    """
    Tests the trigger_dbt_job function when the server returns a 500 error.
    """
    responses.add(
        responses.POST,
        "https://cloud.getdbt.com/api/v2/accounts/10206/jobs/test_job_id/run/",
        status=500,
        json={"message": "Internal Server Error"},
    )

    try:
        main.trigger_dbt_job(mock_request_with_job_id)
        pytest.fail("Expected function to raise an exception")
    except Exception as e:
        log_messages = [record.message for record in caplog.records]
        error_message = "An error occurred when attempting to trigger dbt job"
        assert any(
            error_message in msg for msg in log_messages
        ), f"Expected error message not found in logs: {log_messages}"


@responses.activate
def test_trigger_dbt_job_timeout(mock_env_vars, mock_request_with_job_id, caplog):
    """
    Tests the trigger_dbt_job function when the request times out.
    """
    responses.add(
        responses.POST,
        "https://cloud.getdbt.com/api/v2/accounts/10206/jobs/test_job_id/run/",
        body=requests.exceptions.ConnectTimeout("Connection timed out"),
    )

    try:
        main.trigger_dbt_job(mock_request_with_job_id)
        pytest.fail("Expected function to raise an exception")
    except Exception as e:
        log_messages = [record.message for record in caplog.records]
        assert any(
            "Error in making request" in msg for msg in log_messages
        ), f"Expected error message not found in logs: {log_messages}"


@responses.activate
def test_trigger_dbt_job_with_custom_cause(
    mock_env_vars, mock_request_with_custom_cause, caplog
):
    """
    Tests the trigger_dbt_job function passes a custom cause to the dbt API.
    """
    responses.add(
        responses.POST,
        "https://cloud.getdbt.com/api/v2/accounts/10206/jobs/test_job_id/run/",
        json={"data": {"id": "retry_run_id"}},
        status=200,
    )

    response = main.trigger_dbt_job(mock_request_with_custom_cause)

    assert response[0] == "Trigger dbt job completed"
    assert response[1] == 200

    # Verify the cause was passed in the API request body (URL-encoded)
    request_body = responses.calls[0].request.body
    assert "Auto-retry" in request_body or "Auto+retry" in request_body


@responses.activate
def test_trigger_dbt_job_default_cause(mock_env_vars, mock_request_with_job_id, caplog):
    """
    Tests the trigger_dbt_job function uses default cause when none provided.
    """
    responses.add(
        responses.POST,
        "https://cloud.getdbt.com/api/v2/accounts/10206/jobs/test_job_id/run/",
        json={"data": {"id": "default_run_id"}},
        status=200,
    )

    response = main.trigger_dbt_job(mock_request_with_job_id)

    assert response[0] == "Trigger dbt job completed"
    assert response[1] == 200

    # Verify the default cause was used (URL-encoded form data)
    request_body = responses.calls[0].request.body
    assert "Triggered" in request_body and "Google+Cloud+Function" in request_body
