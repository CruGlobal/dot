import pytest
import logging
import sys
import json
from unittest import mock
from flask import Request

# main is imported by conftest.py with mocked GCP credentials
import main


@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "test-project")
    monkeypatch.setenv("DBT_WEBHOOK_SECRET", "test-secret")


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
    mock_req = mock.Mock(spec=Request)
    body = json.dumps(payload).encode("utf-8")
    mock_req.get_data.return_value = body
    mock_req.headers = {"authorization": signature}
    return mock_req


@mock.patch.object(main, "publisher")
def test_webhook_success_publishes_to_completed_topic(mock_publisher):
    """Successful dbt job publishes to dbt-job-completed topic."""
    mock_future = mock.Mock()
    mock_future.result.return_value = "msg-123"
    mock_publisher.publish.return_value = mock_future

    payload = make_dbt_webhook_payload(status="Success", status_code=10)
    request = make_mock_request(payload)

    response = main.webhook_handler(request)

    assert response[1] == 200
    body = response[0]
    assert body["status"] == "success"
    assert body["message"] == "Job completion published to dbt-job-completed topic"

    mock_publisher.publish.assert_called_once()
    call_args = mock_publisher.publish.call_args
    assert call_args[0][0] == main.completed_topic_path


@mock.patch.object(main, "publisher")
def test_webhook_success_includes_message_attributes(mock_publisher):
    """Successful completion includes job_id and run_status as Pub/Sub attributes."""
    mock_future = mock.Mock()
    mock_future.result.return_value = "msg-123"
    mock_publisher.publish.return_value = mock_future

    payload = make_dbt_webhook_payload(
        status="Success", status_code=10, job_id="54170"
    )
    request = make_mock_request(payload)

    main.webhook_handler(request)

    call_args = mock_publisher.publish.call_args
    assert call_args[1]["job_id"] == "54170"
    assert call_args[1]["run_status"] == "Success"


@mock.patch.object(main, "publisher")
def test_webhook_success_message_contains_dbt_info(mock_publisher):
    """Published message contains all dbt completion fields."""
    mock_future = mock.Mock()
    mock_future.result.return_value = "msg-123"
    mock_publisher.publish.return_value = mock_future

    payload = make_dbt_webhook_payload(
        status="Success", status_code=10, job_id="54170"
    )
    request = make_mock_request(payload)

    main.webhook_handler(request)

    call_args = mock_publisher.publish.call_args
    published_bytes = call_args[0][1]
    message = json.loads(published_bytes.decode("utf-8"))
    assert message["job_id"] == "54170"
    assert message["job_name"] == "Test Job"
    assert message["run_id"] == "99999"
    assert message["run_status"] == "Success"
    assert message["run_status_code"] == 10
    assert message["environment_id"] == "12345"
    assert message["account_id"] == "10206"


@mock.patch.object(main, "publisher")
def test_webhook_success_any_job_id_publishes(mock_publisher):
    """ALL successful completions publish — no job ID filtering in the webhook."""
    mock_future = mock.Mock()
    mock_future.result.return_value = "msg-123"
    mock_publisher.publish.return_value = mock_future

    payload = make_dbt_webhook_payload(
        status="Success", status_code=10, job_id="999999"
    )
    request = make_mock_request(payload)

    response = main.webhook_handler(request)

    assert response[1] == 200
    assert response[0]["status"] == "success"
    mock_publisher.publish.assert_called_once()


@mock.patch.object(main, "publisher")
def test_webhook_failure_publishes_to_retry_topic(mock_publisher):
    """Failed dbt job publishes to retry topic."""
    mock_future = mock.Mock()
    mock_future.result.return_value = "msg-456"
    mock_publisher.publish.return_value = mock_future

    payload = make_dbt_webhook_payload(status="Error", status_code=20)
    request = make_mock_request(payload)

    response = main.webhook_handler(request)

    assert response[1] == 200
    body = response[0]
    assert body["status"] == "failure_processed"
    assert body["message"] == "Job failure published to retry topic"

    mock_publisher.publish.assert_called_once()
    call_args = mock_publisher.publish.call_args
    assert call_args[0][0] == main.retry_topic_path

    published_bytes = call_args[0][1]
    retry_msg = json.loads(published_bytes.decode("utf-8"))
    assert retry_msg["job_id"] == "54170"
    assert retry_msg["run_id"] == "99999"
    assert retry_msg["attempt_number"] == 0


@mock.patch.object(main, "publisher")
def test_webhook_cancelled_job_ignored(mock_publisher):
    """Cancelled dbt job is ignored (not success, not error)."""
    payload = make_dbt_webhook_payload(status="Cancelled", status_code=30)
    request = make_mock_request(payload)

    response = main.webhook_handler(request)

    assert response[1] == 200
    assert "unhandled run status" in response[0]
    mock_publisher.publish.assert_not_called()


@mock.patch.object(main, "publisher")
def test_webhook_non_completion_event_ignored(mock_publisher):
    """Non-completion events return 400 (parse_dbt_webhook only handles completions)."""
    payload = {
        "eventType": "job.run.started",
        "accountId": "10206",
        "data": {"jobId": "54170", "runId": "99999"},
    }
    request = make_mock_request(payload)

    response = main.webhook_handler(request)

    assert response[1] == 400
    assert "Invalid DBT webhook payload" in response[0]
    mock_publisher.publish.assert_not_called()


def test_webhook_missing_signature():
    """Request without signature returns 400."""
    mock_req = mock.Mock(spec=Request)
    mock_req.get_data.return_value = b"{}"
    mock_req.headers = {}

    response = main.webhook_handler(mock_req)

    assert response[1] == 400
    assert "Missing signature" in response[0]


def test_webhook_invalid_json():
    """Request with invalid JSON returns 400."""
    mock_req = mock.Mock(spec=Request)
    mock_req.get_data.return_value = b"not json"
    mock_req.headers = {"authorization": "Bearer test-token"}

    response = main.webhook_handler(mock_req)

    assert response[1] == 400
    assert "Invalid JSON" in response[0]


@mock.patch.object(main, "publisher")
def test_webhook_success_with_fabric_mapping_publishes_to_both_topics(mock_publisher):
    """Successful dbt job with Fabric mapping publishes to both topics."""
    mock_future = mock.Mock()
    mock_future.result.return_value = "msg-123"
    mock_publisher.publish.return_value = mock_future

    payload = make_dbt_webhook_payload(
        status="Success", status_code=10, job_id="163545"
    )
    request = make_mock_request(payload)

    response = main.webhook_handler(request)

    assert response[1] == 200
    assert mock_publisher.publish.call_count == 2

    call_topics = [call[0][0] for call in mock_publisher.publish.call_args_list]
    assert main.completed_topic_path in call_topics
    assert main.fabric_topic_path in call_topics


@mock.patch.object(main, "publisher")
def test_webhook_pubsub_error_returns_500(mock_publisher):
    """Pub/Sub publish failure returns 500."""
    mock_publisher.publish.side_effect = Exception("Pub/Sub unavailable")

    payload = make_dbt_webhook_payload(status="Success", status_code=10)
    request = make_mock_request(payload)

    response = main.webhook_handler(request)

    assert response[1] == 500
    assert "Pub/Sub" in response[0]
