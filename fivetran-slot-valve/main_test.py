import json
import logging
import sys
from unittest import mock

import pytest

import main


VALID_SECRET = "test_valve_secret"


@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    monkeypatch.setenv("WEBHOOK_SECRET", VALID_SECRET)
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "test-project")


@pytest.fixture
def mock_publisher(monkeypatch):
    publisher = mock.Mock()
    publisher.topic_path.return_value = (
        "projects/test-project/topics/fivetran-slot-valve-events"
    )
    future = mock.Mock()
    future.result.return_value = "message-id"
    publisher.publish.return_value = future
    monkeypatch.setattr(main, "_get_publisher", lambda: publisher)
    return publisher


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


class FakeRequest:
    def __init__(self, headers=None, json_body=None):
        self.headers = headers or {}
        self._json = json_body

    def get_json(self, silent=False):
        return self._json


def _payload(instance="mpdx-api-prod", transition="Triggered"):
    return {
        "alert_id": "12345",
        "alert_title": f"Fivetran Slot Valve Auto-Sync ({instance})",
        "alert_transition": transition,
        "tags": f"dbinstanceidentifier:{instance},service:fivetran-slot-valve,source:terraform",
        "link": "https://app.datadoghq.com/monitors/12345",
    }


def _request(secret=VALID_SECRET, payload=None):
    headers = {"X-Valve-Secret": secret} if secret is not None else {}
    return FakeRequest(headers=headers, json_body=payload)


def test_valid_alert_publishes_drain_event(mock_publisher):
    response = main.valve_handler(_request(payload=_payload()))

    assert response == ("Valve event accepted", 200)
    mock_publisher.publish.assert_called_once()
    _, published_bytes = mock_publisher.publish.call_args[0]
    message = json.loads(published_bytes.decode("utf-8"))
    assert message["instance_id"] == "mpdx-api-prod"
    assert message["connector_id"] == "loft_unabashed"
    assert message["alert_transition"] == "Triggered"


def test_global_registry_flat_maps_to_correct_connector(mock_publisher):
    response = main.valve_handler(
        _request(payload=_payload(instance="global-registry-flat-prod"))
    )

    assert response[1] == 200
    _, published_bytes = mock_publisher.publish.call_args[0]
    message = json.loads(published_bytes.decode("utf-8"))
    assert message["connector_id"] == "freebee_tuberculosis"


def test_missing_secret_is_rejected(mock_publisher):
    response = main.valve_handler(_request(secret=None, payload=_payload()))

    assert response[1] == 403
    mock_publisher.publish.assert_not_called()


def test_invalid_secret_is_rejected(mock_publisher):
    response = main.valve_handler(_request(secret="wrong", payload=_payload()))

    assert response[1] == 403
    mock_publisher.publish.assert_not_called()


def test_unconfigured_secret_returns_500(monkeypatch, mock_publisher):
    monkeypatch.delenv("WEBHOOK_SECRET", raising=False)

    response = main.valve_handler(_request(payload=_payload()))

    assert response[1] == 500
    mock_publisher.publish.assert_not_called()


def test_recovery_transition_takes_no_action(mock_publisher):
    response = main.valve_handler(
        _request(payload=_payload(transition="Recovered"))
    )

    assert response[1] == 200
    mock_publisher.publish.assert_not_called()


def test_missing_instance_tag_returns_400(mock_publisher):
    payload = _payload()
    payload["tags"] = "service:fivetran-slot-valve,source:terraform"
    response = main.valve_handler(_request(payload=payload))

    assert response[1] == 400
    mock_publisher.publish.assert_not_called()


def test_unknown_instance_returns_422(mock_publisher):
    response = main.valve_handler(
        _request(payload=_payload(instance="some-other-db-prod"))
    )

    assert response[1] == 422
    mock_publisher.publish.assert_not_called()


def test_empty_body_returns_400(mock_publisher):
    response = main.valve_handler(_request(payload=None))

    assert response[1] == 400
    mock_publisher.publish.assert_not_called()


def test_publish_failure_returns_500(mock_publisher):
    mock_publisher.publish.side_effect = RuntimeError("pubsub down")

    response = main.valve_handler(_request(payload=_payload()))

    assert response[1] == 500
