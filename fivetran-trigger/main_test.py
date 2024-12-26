import pytest
import responses
from unittest import mock
from flask import Request
import main
import logging
import sys


@pytest.fixture
def mock_env_vars(monkeypatch):
    monkeypatch.setenv("API_KEY", "test_api_key")
    monkeypatch.setenv("API_SECRET", "test_api_secret")


@pytest.fixture
def mock_request_with_connector():
    mock_req = mock.Mock(spec=Request)
    mock_req.get_json.return_value = {"connector_id": "test_connector_id"}
    return mock_req


@pytest.fixture
def mock_request_without_connector():
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
def test_trigger_sync_success(mock_env_vars, mock_request_with_connector, caplog):
    """
    Tests the trigger_sync function when the sync is triggered successfully.
    """
    # Mock successful connector update
    responses.add(
        responses.PATCH,
        "https://api.fivetran.com/v1/connectors/test_connector_id",
        json={"data": {}},
        status=200,
    )

    # Mock successful sync trigger
    responses.add(
        responses.POST,
        "https://api.fivetran.com/v1/connectors/test_connector_id/force",
        json={"data": {}},
        status=200,
    )

    response = main.trigger_sync(mock_request_with_connector)

    success_message = "Fivetran sync triggered and completed successfully"
    log_messages = [record.message for record in caplog.records]
    assert any(
        success_message in msg for msg in log_messages
    ), f"Expected message not found in logs: {log_messages}"
    assert response[0] == "Fivetran sync triggered successfully"
    assert response[1] == 200


@responses.activate
def test_trigger_sync_missing_connector_id(
    mock_env_vars, mock_request_without_connector, caplog
):
    """
    Tests the trigger_sync function when connector_id is missing from request.
    """
    try:
        main.trigger_sync(mock_request_without_connector)
        pytest.fail("Expected function to raise an exception")
    except Exception as e:
        log_messages = [record.message for record in caplog.records]
        assert any(
            "Failed to retrieve connector_id" in msg for msg in log_messages
        ), f"Expected error message not found in logs: {log_messages}"


@responses.activate
def test_trigger_sync_invalid_credentials(
    mock_env_vars, mock_request_with_connector, caplog
):
    """
    Tests the trigger_sync function with invalid API credentials.
    """
    responses.add(
        responses.PATCH,
        "https://api.fivetran.com/v1/connectors/test_connector_id",
        json={"message": "Invalid API key"},
        status=401,
    )

    try:
        main.trigger_sync(mock_request_with_connector)
        pytest.fail("Expected function to raise an exception")
    except Exception as e:
        log_messages = [record.message for record in caplog.records]
        assert any(
            "Error triggering Fivetran sync" in msg for msg in log_messages
        ), f"Expected error message not found in logs: {log_messages}"
