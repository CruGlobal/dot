import os
import flask
import pytest
import responses
from unittest import mock
import main
import logging


# Create a fake "app" for generating test request contexts.
@pytest.fixture(scope="module")
def app():
    return flask.Flask(__name__)


@pytest.fixture
def mock_env_vars(monkeypatch):
    monkeypatch.setenv("FIVETRAN_API_KEY", "test_api_key")
    monkeypatch.setenv("FIVETRAN_API_SECRET", "test_api_secret")
    monkeypatch.setenv("CONNECTOR_ID", "test_connector_id")


@pytest.fixture
def mock_request():
    return mock.Mock()


@responses.activate
def test_trigger_sync_success(mock_env_vars, mock_request, mocker, caplog):
    """
    Tests the trigger_sync function when the sync is triggered successfully.
    """
    responses.add(
        responses.POST,
        "https://api.fivetran.com/v1/connectors/test_connector_id/force",
        json={},
        status=200,
    )

    mock_client = mocker.patch("main.FivetranClient")
    mock_client.return_value.trigger_sync.return_value = None

    with caplog.at_level(logging.INFO):
        response = main.trigger_sync(mock_request)

    mock_client.return_value.trigger_sync.assert_called_once_with(
        connector_id="test_connector_id", force=True, wait_for_completion=False
    )

    assert (
        "Fivetran sync triggered and completed successfully, connector_id: test_connector_id"
        in caplog.text
    )
    assert response == ("Fivetran sync triggered successfully", 200)


@responses.activate
def test_trigger_sync_exception(mock_env_vars, mock_request, mocker, caplog):
    """
    Tests the trigger_sync function when an exception is raised.
    """
    responses.add(
        responses.POST,
        "https://api.fivetran.com/v1/connectors/test_connector_id/force",
        json={},
        status=500,
    )

    mock_client = mocker.patch("main.FivetranClient")
    mock_client.return_value.trigger_sync.side_effect = Exception("Test exception")

    with caplog.at_level(logging.ERROR):
        response = main.trigger_sync(mock_request)

    assert (
        "connector_id: test_connector_id - Error triggering Fivetran sync: Test exception"
        in caplog.text
    )
    assert response == ("Error triggering Fivetran sync: Test exception", 500)


@responses.activate
def test_trigger_sync_missing_connector_id(mock_request, mocker, caplog):
    """
    Tests the trigger_sync function when the CONNECTOR_ID environment variable is not set.
    """
    mocker.patch.dict(os.environ, {"CONNECTOR_ID": ""})

    with caplog.at_level(logging.ERROR):
        response = main.trigger_sync(mock_request)

    assert "Error: CONNECTOR_ID environment variable is not set" in caplog.text
    assert response == ("CONNECTOR_ID environment variable is not set", 400)
