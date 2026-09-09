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


def make_mock_request(payload, signature="Bearer test-secret"):
    """Build a mock Flask request with the given payload and auth header.
    Default signature uses the test-secret configured in conftest.py."""
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
#
# These tests patch map_dbt_to_fabric to inject a mapped job so the dual-publish
# path is covered independently of the real mapping table.
# ---------------------------------------------------------------------------

FABRIC_MAPPING = {
    "workspace_id": "test-workspace",
    "item_id": "test-item",
    "refresh_workspace_id": "test-refresh-workspace",
    "lakehouse_dataset_id": "test-lakehouse-dataset",
    "job_type": "Execute",
}


def test_us_donations_maps_to_fabric_notebook():
    """dbt job 163545 (US Donations) maps to the prod Fabric Notebook run."""
    from webhook_utils import map_dbt_to_fabric

    config = map_dbt_to_fabric("163545")

    assert config["workspace_id"] == "c2bafcfd-df3d-4383-8f76-aed296260453"
    assert config["item_id"] == "84bf60cb-4059-4e20-b18a-120f640a121c"
    assert config["job_type"] == "RunNotebook"
    params = config["execution_data"]["parameters"]
    assert params["environment"] == {"value": "prod", "type": "string"}
    assert params["_inlineInstallationEnabled"] == {"value": True, "type": "bool"}
    # No Power BI refresh for the notebook run
    assert "refresh_workspace_id" not in config
    assert "lakehouse_dataset_id" not in config


def test_unmapped_job_returns_empty():
    from webhook_utils import map_dbt_to_fabric

    assert map_dbt_to_fabric("99999") == {}


def test_fabric_message_passes_execution_data_and_tolerates_missing_refresh():
    """Notebook config without refresh fields builds a message the workflow accepts."""
    from webhook_utils import create_fabric_job_message

    config = {
        "workspace_id": "ws",
        "item_id": "nb",
        "job_type": "RunNotebook",
        "execution_data": {"parameters": {"environment": {"value": "prod", "type": "string"}}},
    }
    dbt_info = {"job_id": "163545", "job_name": "US Donations", "run_id": "1"}

    msg = create_fabric_job_message(config, dbt_info)

    assert msg["workspace_id"] == "ws"
    assert msg["item_id"] == "nb"
    assert msg["job_type"] == "RunNotebook"
    assert msg["execution_data"] == config["execution_data"]
    # Empty strings make the workflow skip the Power BI refresh step
    assert msg["refresh_workspace_id"] == ""
    assert msg["lakehouse_dataset_id"] == ""
    assert msg["source_job_id"] == "163545"
    assert msg["execution_context"]["dbt_job_name"] == "US Donations"


def test_fabric_message_copyjob_keeps_refresh_fields_and_null_execution_data():
    """CopyJob-style config (no execution_data) still sends execution_data: null."""
    from webhook_utils import create_fabric_job_message

    msg = create_fabric_job_message(FABRIC_MAPPING, {"job_id": "1"})

    assert msg["job_type"] == "Execute"
    assert msg["refresh_workspace_id"] == "test-refresh-workspace"
    assert msg["lakehouse_dataset_id"] == "test-lakehouse-dataset"
    assert msg["execution_data"] is None


@mock.patch.object(main, "map_dbt_to_fabric", return_value=FABRIC_MAPPING)
@mock.patch.object(main, "publisher")
def test_success_with_fabric_mapping_publishes_to_both_topics(mock_publisher, _mock_map):
    """Job with Fabric mapping publishes to BOTH completed and fabric topics."""
    mock_future = mock.Mock()
    mock_future.result.return_value = "msg-123"
    mock_publisher.publish.return_value = mock_future

    payload = make_dbt_webhook_payload(status="Success", status_code=10, job_id="99001")
    request = make_mock_request(payload)

    response = main.webhook_handler(request)

    assert response[1] == 200
    assert mock_publisher.publish.call_count == 2

    call_topics = [call[0][0] for call in mock_publisher.publish.call_args_list]
    assert any("dbt-job-completed" in t for t in call_topics)
    assert any("fabric-job-events" in t for t in call_topics)


@mock.patch.object(main, "map_dbt_to_fabric", return_value=FABRIC_MAPPING)
@mock.patch.object(main, "publisher")
def test_fabric_publish_failure_is_non_fatal(mock_publisher, _mock_map):
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

    payload = make_dbt_webhook_payload(status="Success", status_code=10, job_id="99001")
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
    """Non-completion events (e.g., job.run.started) are gracefully ignored with 200."""
    payload = {
        "eventType": "job.run.started",
        "accountId": "10206",
        "data": {"jobId": "54170", "runId": "99999"},
    }
    request = make_mock_request(payload)

    response = main.webhook_handler(request)

    assert response[1] == 200
    assert "not a job completion" in response[0]
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
    mock_req.headers = {"authorization": "Bearer test-secret"}

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


# ---------------------------------------------------------------------------
# Signature verification
# ---------------------------------------------------------------------------


@mock.patch.object(main, "publisher")
def test_valid_hmac_signature_accepted(mock_publisher):
    """Valid HMAC-SHA256 signature (non-Bearer) is accepted."""
    import hmac as hmac_lib
    import hashlib

    mock_future = mock.Mock()
    mock_future.result.return_value = "msg-123"
    mock_publisher.publish.return_value = mock_future

    payload = make_dbt_webhook_payload(status="Success", status_code=10)
    body = json.dumps(payload).encode("utf-8")
    secret = "test-secret"
    valid_signature = hmac_lib.new(
        secret.encode("utf-8"), body, hashlib.sha256
    ).hexdigest()

    mock_req = mock.Mock(spec=Request)
    mock_req.get_data.return_value = body
    mock_req.headers = {"authorization": valid_signature}

    response = main.webhook_handler(mock_req)

    assert response[1] == 200


def test_any_bearer_token_accepted():
    """Any Bearer token is accepted — the API Gateway rewrites the original
    dbt Cloud token into its own JWT, so we cannot validate the value."""
    payload = make_dbt_webhook_payload(status="Success", status_code=10)
    request = make_mock_request(payload, signature="Bearer any-token-value")

    response = main.webhook_handler(request)

    assert response[1] == 200


def test_invalid_hmac_signature_rejected():
    """Invalid HMAC signature (non-Bearer, wrong value) returns 403."""
    payload = make_dbt_webhook_payload(status="Success", status_code=10)
    body = json.dumps(payload).encode("utf-8")

    mock_req = mock.Mock(spec=Request)
    mock_req.get_data.return_value = body
    mock_req.headers = {"authorization": "invalid-hmac-value"}

    response = main.webhook_handler(mock_req)

    assert response[1] == 403
    assert "Invalid signature" in response[0]
