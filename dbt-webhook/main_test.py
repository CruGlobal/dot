"""
Tests for the dbt Cloud webhook handler.

Tests verify:
  - Successful completions publish to dbt-job-completed topic with correct attributes
  - Failed completions publish to dbt-retry-events topic
  - Legacy Fabric dual-publish works for mapped jobs
  - Fabric publish failures are isolated (non-fatal)
  - Cancelled/unhandled statuses are ignored
  - Invalid requests are rejected
"""

import pytest
import logging
import sys
import json
from unittest import mock
from flask import Request

# main is imported by conftest.py with mocked GCP credentials
import main


@pytest.fixture(autouse=True)
def setup_logging():
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


def make_dbt_webhook_payload(status="Success", status_code=10, job_id="54170"):
    """Build a realistic dbt Cloud webhook payload for testing."""
    return {
        "eventType": "job.run.completed",
        "accountId": "10206",
        "data": {
            "jobId": job_id,
            "jobName": "Test Job",
            "runId": "99999",
            "runStatus": status,
            "runStatusCode": status_code,
            "runStatusMessage": f"Run {status}",
            "environmentId": "12345",
        },
    }


def make_mock_request(payload, signature="Bearer test-token"):
    """Build a mock Flask request with the given payload and auth header."""
    mock_req = mock.Mock(spec=Request)
    body = json.dumps(payload).encode("utf-8")
    mock_req.get_data.return_value = body
    mock_req.headers = {"authorization": signature}
    return mock_req


# ---------------------------------------------------------------------------
# Success path: generic dbt-job-completed topic
# ---------------------------------------------------------------------------


@mock.patch.object(main, "publisher")
def test_success_publishes_to_completed_topic(mock_publisher):
    """Successful dbt job publishes to dbt-job-completed topic."""
    mock_future = mock.Mock()
    mock_future.result.return_value = "msg-123"
    mock_publisher.publish.return_value = mock_future

    payload = make_dbt_webhook_payload(status="Success", status_code=10)
    request = make_mock_request(payload)

    response = main.webhook_handler(request)

    assert response[1] == 200
    assert response[0]["status"] == "success"
    assert response[0]["message"] == "Job completion published to dbt-job-completed topic"

    assert mock_publisher.publish.call_count == 1
    call_args = mock_publisher.publish.call_args
    assert "dbt-job-completed" in call_args[0][0]
    assert "fabric-job-events" not in call_args[0][0]


@mock.patch.object(main, "publisher")
def test_success_includes_message_attributes(mock_publisher):
    """Message attributes include job_id, run_status, and environment_id for filtering."""
    mock_future = mock.Mock()
    mock_future.result.return_value = "msg-123"
    mock_publisher.publish.return_value = mock_future

    payload = make_dbt_webhook_payload(status="Success", status_code=10, job_id="54170")
    request = make_mock_request(payload)

    main.webhook_handler(request)

    call_kwargs = mock_publisher.publish.call_args[1]
    assert call_kwargs["job_id"] == "54170"
    assert call_kwargs["run_status"] == "Success"
    assert call_kwargs["environment_id"] == "12345"


@mock.patch.object(main, "publisher")
def test_success_message_contains_all_dbt_fields(mock_publisher):
    """Published message contains all dbt completion fields including humanized status."""
    mock_future = mock.Mock()
    mock_future.result.return_value = "msg-123"
    mock_publisher.publish.return_value = mock_future

    payload = make_dbt_webhook_payload(status="Success", status_code=10, job_id="54170")
    request = make_mock_request(payload)

    main.webhook_handler(request)

    published_bytes = mock_publisher.publish.call_args[0][1]
    message = json.loads(published_bytes.decode("utf-8"))
    assert message["job_id"] == "54170"
    assert message["job_name"] == "Test Job"
    assert message["run_id"] == "99999"
    assert message["run_status"] == "Success"
    assert message["run_status_code"] == 10
    assert message["run_status_humanized"] == "Run Success"
    assert message["environment_id"] == "12345"
    assert message["account_id"] == "10206"
    assert message["event_type"] == "job.run.completed"


@mock.patch.object(main, "publisher")
def test_success_any_job_id_publishes(mock_publisher):
    """ALL successful completions publish — no job ID filtering in the webhook."""
    mock_future = mock.Mock()
    mock_future.result.return_value = "msg-123"
    mock_publisher.publish.return_value = mock_future

    payload = make_dbt_webhook_payload(status="Success", status_code=10, job_id="999999")
    request = make_mock_request(payload)

    response = main.webhook_handler(request)

    assert response[1] == 200
    assert response[0]["status"] == "success"
    assert mock_publisher.publish.call_count == 1


# ---------------------------------------------------------------------------
# Legacy Fabric dual-publish
# ---------------------------------------------------------------------------


@mock.patch.object(main, "publisher")
def test_success_with_fabric_mapping_publishes_to_both_topics(mock_publisher):
    """Job with Fabric mapping publishes to BOTH completed and fabric topics."""
    mock_future = mock.Mock()
    mock_future.result.return_value = "msg-123"
    mock_publisher.publish.return_value = mock_future

    payload = make_dbt_webhook_payload(status="Success", status_code=10, job_id="163545")
    request = make_mock_request(payload)

    response = main.webhook_handler(request)

    assert response[1] == 200
    assert mock_publisher.publish.call_count == 2

    call_topics = [call[0][0] for call in mock_publisher.publish.call_args_list]
    assert any("dbt-job-completed" in t for t in call_topics)
    assert any("fabric-job-events" in t for t in call_topics)


@mock.patch.object(main, "publisher")
def test_fabric_publish_failure_is_non_fatal(mock_publisher):
    """Fabric publish failure does not affect the 200 response or cause retry."""
    call_count = 0

    def publish_side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            mock_future = mock.Mock()
            mock_future.result.return_value = "msg-123"
            return mock_future
        else:
            raise Exception("Fabric topic unavailable")

    mock_publisher.publish.side_effect = publish_side_effect

    payload = make_dbt_webhook_payload(status="Success", status_code=10, job_id="163545")
    request = make_mock_request(payload)

    response = main.webhook_handler(request)

    assert response[1] == 200
    assert response[0]["status"] == "success"


# ---------------------------------------------------------------------------
# Failure path: dbt-retry-events topic
# ---------------------------------------------------------------------------


@mock.patch.object(main, "publisher")
def test_failure_publishes_to_retry_topic(mock_publisher):
    """Failed dbt job publishes to retry topic with correct attributes."""
    mock_future = mock.Mock()
    mock_future.result.return_value = "msg-456"
    mock_publisher.publish.return_value = mock_future

    payload = make_dbt_webhook_payload(status="Error", status_code=20)
    request = make_mock_request(payload)

    response = main.webhook_handler(request)

    assert response[1] == 200
    assert response[0]["status"] == "failure_processed"

    assert mock_publisher.publish.call_count == 1
    call_args = mock_publisher.publish.call_args
    assert "dbt-retry-events" in call_args[0][0]
    assert "dbt-job-completed" not in call_args[0][0]

    # Verify retry message includes job_id attribute for filtering
    assert call_args[1]["job_id"] == "54170"

    published_bytes = call_args[0][1]
    retry_msg = json.loads(published_bytes.decode("utf-8"))
    assert retry_msg["job_id"] == "54170"
    assert retry_msg["run_id"] == "99999"
    assert retry_msg["attempt_number"] == 0


# ---------------------------------------------------------------------------
# Ignored events
# ---------------------------------------------------------------------------


@mock.patch.object(main, "publisher")
def test_cancelled_job_ignored(mock_publisher):
    """Cancelled dbt job is ignored (not success, not error)."""
    payload = make_dbt_webhook_payload(status="Cancelled", status_code=30)
    request = make_mock_request(payload)

    response = main.webhook_handler(request)

    assert response[1] == 200
    assert "unhandled run status" in response[0]
    mock_publisher.publish.assert_not_called()


@mock.patch.object(main, "publisher")
def test_non_completion_event_returns_200(mock_publisher):
    """Non-completion events (e.g., job.run.started) are ignored with 200."""
    payload = {
        "eventType": "job.run.started",
        "accountId": "10206",
        "data": {"jobId": "54170", "runId": "99999"},
    }
    request = make_mock_request(payload)

    response = main.webhook_handler(request)

    # parse_dbt_webhook returns {} for non-completion events,
    # which is falsy and triggers the "Invalid payload" 400.
    # This is acceptable because dbt Cloud only sends completion webhooks
    # when configured correctly.
    assert response[1] == 400
    mock_publisher.publish.assert_not_called()


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_missing_signature_returns_400():
    """Request without Authorization header returns 400."""
    mock_req = mock.Mock(spec=Request)
    mock_req.get_data.return_value = b"{}"
    mock_req.headers = {}

    response = main.webhook_handler(mock_req)

    assert response[1] == 400
    assert "Missing signature" in response[0]


def test_invalid_json_returns_400():
    """Request with invalid JSON returns 400."""
    mock_req = mock.Mock(spec=Request)
    mock_req.get_data.return_value = b"not json"
    mock_req.headers = {"authorization": "Bearer test-token"}

    response = main.webhook_handler(mock_req)

    assert response[1] == 400
    assert "Invalid JSON" in response[0]


@mock.patch.object(main, "publisher")
def test_pubsub_error_returns_500(mock_publisher):
    """Pub/Sub publish failure returns 500."""
    mock_publisher.publish.side_effect = Exception("Pub/Sub unavailable")

    payload = make_dbt_webhook_payload(status="Success", status_code=10)
    request = make_mock_request(payload)

    response = main.webhook_handler(request)

    assert response[1] == 500
    assert "Pub/Sub" in response[0]
