import flask
import pytest
import responses
from unittest import mock
import main
import logging
from fivetran_client import ExitCodeException


@pytest.fixture(scope="module")
def app():
    return flask.Flask(__name__)


@pytest.fixture
def mock_env_vars(monkeypatch):
    monkeypatch.setenv("API_KEY", "test_api_key")
    monkeypatch.setenv("API_SECRET", "test_api_secret")


@pytest.fixture
def mock_request_with_connector():
    mock_req = mock.Mock()
    mock_req.get_json.return_value = {"connector_id": "test_connector_id"}
    return mock_req


@pytest.fixture
def mock_request_without_connector():
    mock_req = mock.Mock()
    mock_req.get_json.return_value = {}
    return mock_req


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

    with caplog.at_level(logging.INFO):
        response = main.trigger_sync(mock_request_with_connector)

    assert response[0] == "Fivetran sync triggered successfully"
    assert response[1] == 200
    assert "Fivetran sync triggered and completed successfully" in caplog.text


@responses.activate
def test_trigger_sync_missing_connector_id(mock_env_vars, mock_request_without_connector, caplog):
    """
    Tests the trigger_sync function when connector_id is missing from request.
    """
    with pytest.raises(ValueError) as exc_info:
        main.trigger_sync(mock_request_without_connector)
    
    assert str(exc_info.value) == "Failed to retrieve connector_id"
    assert "Failed to retrieve connector_id" in caplog.text


@responses.activate
def test_trigger_sync_invalid_credentials(mock_env_vars, mock_request_with_connector, caplog):
    """
    Tests the trigger_sync function with invalid API credentials.
    """
    responses.add(
        responses.PATCH,
        "https://api.fivetran.com/v1/connectors/test_connector_id",
        json={"message": "Invalid API key"},
        status=401,
    )

    with pytest.raises(RuntimeError) as exc_info:
        main.trigger_sync(mock_request_with_connector)

    assert "Error triggering Fivetran sync" in str(exc_info.value)
    assert "Authentication failed" in caplog.text

