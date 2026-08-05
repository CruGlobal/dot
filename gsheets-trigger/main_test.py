"""
Tests for the Google Sheets Trigger Cloud Function.

Run with: pytest main_test.py -v
"""

import pytest
import logging
import sys
from datetime import datetime, timedelta
from unittest import mock

import pytz

import main


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


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set required environment variables."""
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "test-project")


class TestGetLookbackTime:
    """Tests for the get_lookback_time function."""

    def test_weekday_no_weekends_returns_24_hours(self):
        """On Tue-Fri without weekends, should look back 24 hours."""
        est = pytz.timezone("America/New_York")
        # Tuesday at 5pm
        tuesday = datetime(2024, 1, 9, 17, 0, tzinfo=est)

        with mock.patch("main.datetime") as mock_dt:
            mock_dt.now.return_value = tuesday
            # Need to keep timedelta working
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

            result = main.get_lookback_time(include_weekends=False)

        # Should be roughly 24 hours ago
        expected = tuesday - timedelta(hours=24)
        assert abs((result - expected).total_seconds()) < 60

    def test_monday_no_weekends_returns_72_hours(self):
        """On Monday without weekends, should look back 72 hours to Friday."""
        est = pytz.timezone("America/New_York")
        # Monday at 5pm
        monday = datetime(2024, 1, 8, 17, 0, tzinfo=est)

        with mock.patch("main.datetime") as mock_dt:
            mock_dt.now.return_value = monday
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

            result = main.get_lookback_time(include_weekends=False)

        # Should be roughly 72 hours ago (Friday)
        expected = monday - timedelta(hours=72)
        assert abs((result - expected).total_seconds()) < 60

    def test_monday_with_weekends_returns_24_hours(self):
        """On Monday with weekends included, should still look back 24 hours."""
        est = pytz.timezone("America/New_York")
        monday = datetime(2024, 1, 8, 17, 0, tzinfo=est)

        with mock.patch("main.datetime") as mock_dt:
            mock_dt.now.return_value = monday
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

            result = main.get_lookback_time(include_weekends=True)

        # Should be roughly 24 hours ago
        expected = monday - timedelta(hours=24)
        assert abs((result - expected).total_seconds()) < 60

    def test_weekend_with_weekends_returns_24_hours(self):
        """On Saturday with weekends included, should look back 24 hours."""
        est = pytz.timezone("America/New_York")
        saturday = datetime(2024, 1, 6, 17, 0, tzinfo=est)

        with mock.patch("main.datetime") as mock_dt:
            mock_dt.now.return_value = saturday
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

            result = main.get_lookback_time(include_weekends=True)

        expected = saturday - timedelta(hours=24)
        assert abs((result - expected).total_seconds()) < 60


class TestTriggerSheetsCheck:
    """Tests for the trigger_sheets_check HTTP function."""

    def test_missing_request_body_returns_400(self, mock_env_vars):
        """When request body is missing, should return 400."""
        mock_request = mock.Mock()
        mock_request.get_json.return_value = None
        mock_request.headers.get.return_value = None

        response, status = main.trigger_sheets_check(mock_request)

        assert status == 400
        assert "Missing request body" in response

    def test_missing_sheets_returns_400(self, mock_env_vars):
        """When sheets are missing from request, should return 400."""
        mock_request = mock.Mock()
        mock_request.get_json.return_value = {"dbt_job_id": "920201"}
        mock_request.headers.get.return_value = None

        response, status = main.trigger_sheets_check(mock_request)

        assert status == 400
        assert "Missing sheets" in response

    def test_missing_dbt_job_id_returns_400(self, mock_env_vars):
        """When dbt_job_id is missing from request, should return 400."""
        mock_request = mock.Mock()
        mock_request.get_json.return_value = {
            "sheets": [{"id": "123", "name": "Test"}]
        }
        mock_request.headers.get.return_value = None

        response, status = main.trigger_sheets_check(mock_request)

        assert status == 400
        assert "Missing dbt_job_id" in response

    @mock.patch("main.SheetsClient")
    @mock.patch("main.publish_pubsub_message")
    def test_changes_detected_triggers_dbt(
        self, mock_publish, mock_sheets_class, mock_env_vars
    ):
        """When sheet has recent changes, should trigger dbt job."""
        # Sheet was modified 1 hour ago (within lookback window)
        recent_time = datetime.now(pytz.UTC) - timedelta(hours=1)
        mock_sheets_class.return_value.get_modified_time.return_value = recent_time

        mock_request = mock.Mock()
        mock_request.get_json.return_value = {
            "sheets": [{"id": "123", "name": "Test Sheet"}],
            "dbt_job_id": "920201",
        }
        mock_request.headers.get.return_value = None

        response, status = main.trigger_sheets_check(mock_request)

        assert status == 200
        assert "triggered dbt job 920201" in response
        mock_publish.assert_called_once_with(
            {"job_id": "920201"}, "cloud-run-job-completed"
        )

    @mock.patch("main.SheetsClient")
    @mock.patch("main.publish_pubsub_message")
    def test_no_changes_skips_dbt(
        self, mock_publish, mock_sheets_class, mock_env_vars
    ):
        """When no sheets have recent changes, should not trigger dbt job."""
        # Sheet was modified 1 week ago (outside lookback window)
        old_time = datetime.now(pytz.UTC) - timedelta(days=7)
        mock_sheets_class.return_value.get_modified_time.return_value = old_time

        mock_request = mock.Mock()
        mock_request.get_json.return_value = {
            "sheets": [{"id": "123", "name": "Test Sheet"}],
            "dbt_job_id": "920201",
        }
        mock_request.headers.get.return_value = None

        response, status = main.trigger_sheets_check(mock_request)

        assert status == 200
        assert "No changes detected" in response
        mock_publish.assert_not_called()

    @mock.patch("main.SheetsClient")
    @mock.patch("main.publish_pubsub_message")
    def test_multiple_sheets_one_changed(
        self, mock_publish, mock_sheets_class, mock_env_vars
    ):
        """When one of multiple sheets changed, should trigger dbt job."""
        old_time = datetime.now(pytz.UTC) - timedelta(days=7)
        recent_time = datetime.now(pytz.UTC) - timedelta(hours=1)

        # First sheet unchanged, second sheet changed
        mock_sheets_class.return_value.get_modified_time.side_effect = [
            old_time,
            recent_time,
        ]

        mock_request = mock.Mock()
        mock_request.get_json.return_value = {
            "sheets": [
                {"id": "123", "name": "Sheet 1"},
                {"id": "456", "name": "Sheet 2"},
            ],
            "dbt_job_id": "920201",
        }
        mock_request.headers.get.return_value = None

        response, status = main.trigger_sheets_check(mock_request)

        assert status == 200
        assert "triggered dbt job 920201" in response
        mock_publish.assert_called_once()

    @mock.patch("main.SheetsClient")
    def test_sheets_client_error_returns_500(
        self, mock_sheets_class, mock_env_vars
    ):
        """When SheetsClient fails, should return 500."""
        mock_sheets_class.return_value.get_modified_time.side_effect = Exception(
            "API Error"
        )

        mock_request = mock.Mock()
        mock_request.get_json.return_value = {
            "sheets": [{"id": "123", "name": "Test Sheet"}],
            "dbt_job_id": "920201",
        }
        mock_request.headers.get.return_value = None

        response, status = main.trigger_sheets_check(mock_request)

        assert status == 500
        assert "Failed to check sheet" in response

    @mock.patch("main.SheetsClient")
    @mock.patch("main.publish_pubsub_message")
    def test_include_weekends_parameter(
        self, mock_publish, mock_sheets_class, mock_env_vars
    ):
        """Should pass include_weekends parameter correctly."""
        recent_time = datetime.now(pytz.UTC) - timedelta(hours=1)
        mock_sheets_class.return_value.get_modified_time.return_value = recent_time

        mock_request = mock.Mock()
        mock_request.get_json.return_value = {
            "sheets": [{"id": "123", "name": "Test Sheet"}],
            "dbt_job_id": "920201",
            "include_weekends": True,
        }
        mock_request.headers.get.return_value = None

        with mock.patch("main.get_lookback_time") as mock_lookback:
            mock_lookback.return_value = datetime.now(pytz.UTC) - timedelta(hours=24)
            main.trigger_sheets_check(mock_request)
            mock_lookback.assert_called_once_with(True)

    def test_octet_stream_content_type(self, mock_env_vars):
        """Should handle application/octet-stream content type."""
        mock_request = mock.Mock()
        mock_request.get_json.return_value = None
        mock_request.headers.get.return_value = "application/octet-stream"
        mock_request.get_data.return_value = '{"sheets": [], "dbt_job_id": "920201"}'

        response, status = main.trigger_sheets_check(mock_request)

        # Should parse the octet-stream but fail on empty sheets
        assert status == 400
        assert "Missing sheets" in response

    @mock.patch("main.SheetsClient")
    @mock.patch("main.publish_pubsub_message")
    def test_sheets_as_json_string(
        self, mock_publish, mock_sheets_class, mock_env_vars
    ):
        """Should parse sheets from JSON string (Terraform format)."""
        recent_time = datetime.now(pytz.UTC) - timedelta(hours=1)
        mock_sheets_class.return_value.get_modified_time.return_value = recent_time

        mock_request = mock.Mock()
        mock_request.get_json.return_value = {
            "sheets": '[{"id": "123", "name": "Test Sheet"}]',
            "dbt_job_id": "920201",
        }
        mock_request.headers.get.return_value = None

        response, status = main.trigger_sheets_check(mock_request)

        assert status == 200
        assert "triggered dbt job 920201" in response

    def test_invalid_sheets_json_string_returns_400(self, mock_env_vars):
        """Should return 400 for invalid sheets JSON string."""
        mock_request = mock.Mock()
        mock_request.get_json.return_value = {
            "sheets": "not valid json",
            "dbt_job_id": "920201",
        }
        mock_request.headers.get.return_value = None

        response, status = main.trigger_sheets_check(mock_request)

        assert status == 400
        assert "Invalid sheets JSON string" in response

    @mock.patch("main.SheetsClient")
    @mock.patch("main.publish_pubsub_message")
    def test_include_weekends_as_string_true(
        self, mock_publish, mock_sheets_class, mock_env_vars
    ):
        """Should parse include_weekends from string 'true' (Terraform format)."""
        recent_time = datetime.now(pytz.UTC) - timedelta(hours=1)
        mock_sheets_class.return_value.get_modified_time.return_value = recent_time

        mock_request = mock.Mock()
        mock_request.get_json.return_value = {
            "sheets": [{"id": "123", "name": "Test Sheet"}],
            "dbt_job_id": "920201",
            "include_weekends": "true",
        }
        mock_request.headers.get.return_value = None

        with mock.patch("main.get_lookback_time") as mock_lookback:
            mock_lookback.return_value = datetime.now(pytz.UTC) - timedelta(hours=24)
            main.trigger_sheets_check(mock_request)
            mock_lookback.assert_called_once_with(True)

    @mock.patch("main.SheetsClient")
    @mock.patch("main.publish_pubsub_message")
    def test_include_weekends_as_string_false(
        self, mock_publish, mock_sheets_class, mock_env_vars
    ):
        """Should parse include_weekends from string 'false' (Terraform format)."""
        recent_time = datetime.now(pytz.UTC) - timedelta(hours=1)
        mock_sheets_class.return_value.get_modified_time.return_value = recent_time

        mock_request = mock.Mock()
        mock_request.get_json.return_value = {
            "sheets": [{"id": "123", "name": "Test Sheet"}],
            "dbt_job_id": "920201",
            "include_weekends": "false",
        }
        mock_request.headers.get.return_value = None

        with mock.patch("main.get_lookback_time") as mock_lookback:
            mock_lookback.return_value = datetime.now(pytz.UTC) - timedelta(hours=24)
            main.trigger_sheets_check(mock_request)
            mock_lookback.assert_called_once_with(False)


class TestPublishPubsubMessage:
    """Tests for the publish_pubsub_message function."""

    def test_missing_project_raises_error(self, monkeypatch):
        """When GOOGLE_CLOUD_PROJECT is not set, should raise ValueError."""
        monkeypatch.delenv("GOOGLE_CLOUD_PROJECT", raising=False)

        with pytest.raises(ValueError, match="GOOGLE_CLOUD_PROJECT"):
            main.publish_pubsub_message({"job_id": "123"}, "test-topic")

    @mock.patch("main.pubsub_v1.PublisherClient")
    def test_publishes_message(self, mock_publisher_class, mock_env_vars):
        """Should publish encoded JSON to the topic."""
        mock_publisher = mock_publisher_class.return_value
        mock_future = mock.Mock()
        mock_publisher.publish.return_value = mock_future

        main.publish_pubsub_message({"job_id": "920201"}, "cloud-run-job-completed")

        mock_publisher.topic_path.assert_called_once_with(
            "test-project", "cloud-run-job-completed"
        )
        mock_publisher.publish.assert_called_once()
        mock_future.result.assert_called_once()
