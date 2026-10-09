from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import pandas as pd
import pytest
import requests

import system_log
from system_log import (
    SYSTEM_LOG_COLUMNS,
    build_event_filter,
    events_to_dataframe,
    flatten_event,
    format_okta_timestamp,
    get_last_published,
    get_system_log_pages,
    get_window_start,
    merge_staging_sql,
    sync_system_log,
)

NOW = datetime(2026, 10, 9, 17, 0, 0, tzinfo=timezone.utc)

SAMPLE_EVENT = {
    "uuid": "a1b2c3",
    "published": "2026-10-08T14:03:12.345Z",
    "eventType": "user.authentication.sso",
    "legacyEventType": "app.auth.sso",
    "version": "0",
    "severity": "INFO",
    "displayMessage": "User single sign on to app",
    "outcome": {"result": "SUCCESS", "reason": None},
    "actor": {
        "id": "00u1",
        "type": "User",
        "alternateId": "jane.doe@cru.org",
        "displayName": "Jane Doe",
    },
    "client": {
        "ipAddress": "203.0.113.5",
        "device": "Computer",
        "zone": "null",
        "userAgent": {"browser": "CHROME", "os": "Mac OS X", "rawUserAgent": "Mozilla/5.0"},
        "geographicalContext": {"country": "United States", "state": "Florida", "city": "Orlando"},
    },
    "authenticationContext": {"externalSessionId": "sess1", "credentialType": None},
    "transaction": {"id": "tx1", "type": "WEB"},
    "target": [{"id": "0oa26v2u8tk7wAKB30h8", "type": "AppInstance", "displayName": "Oracle HCM"}],
}


def make_response(events, next_url=None):
    response = Mock(spec=requests.Response)
    response.json.return_value = events
    response.links = {"next": {"url": next_url}} if next_url else {}
    return response


def test_format_okta_timestamp():
    value = datetime(2026, 10, 9, 13, 5, 7, 123456, tzinfo=timezone(timedelta(hours=-4)))
    assert format_okta_timestamp(value) == "2026-10-09T17:05:07.123Z"


def test_build_event_filter():
    assert build_event_filter(("a.b", "c.d")) == 'eventType eq "a.b" or eventType eq "c.d"'


def test_get_window_start_first_run_uses_oldest_available():
    assert get_window_start(None, NOW) == NOW - timedelta(days=90) + timedelta(hours=1)


def test_get_window_start_overlaps_last_loaded_event():
    last = NOW - timedelta(days=1)
    assert get_window_start(last, NOW) == last - timedelta(hours=1)


def test_get_window_start_never_older_than_retention():
    last = NOW - timedelta(days=120)
    assert get_window_start(last, NOW) == NOW - timedelta(days=90) + timedelta(hours=1)


def test_flatten_event():
    row = flatten_event(SAMPLE_EVENT, NOW)
    assert set(row) == set(SYSTEM_LOG_COLUMNS)
    assert row["uuid"] == "a1b2c3"
    assert row["actor_alternateId"] == "jane.doe@cru.org"
    assert row["outcome_result"] == "SUCCESS"
    assert row["client_userAgent_browser"] == "CHROME"
    assert row["client_geographicalContext_city"] == "Orlando"
    assert row["transaction_id"] == "tx1"
    assert "0oa26v2u8tk7wAKB30h8" in row["target"]
    assert '"uuid": "a1b2c3"' in row["event_json"]


def test_flatten_event_handles_missing_sections():
    row = flatten_event({"uuid": "x", "published": "2026-10-08T00:00:00.000Z"}, NOW)
    assert row["actor_id"] is None
    assert row["client_userAgent_os"] is None
    assert row["target"] is None


def test_events_to_dataframe_types():
    df = events_to_dataframe([SAMPLE_EVENT], NOW)
    assert list(df.columns) == SYSTEM_LOG_COLUMNS
    for column in ("published", "_loaded_at"):
        assert isinstance(df[column].dtype, pd.DatetimeTZDtype)
        assert str(df[column].dt.tz) == "UTC"
    assert df["published"].iloc[0] == pd.Timestamp("2026-10-08T14:03:12.345Z")
    assert df["outcome_reason"].isna().iloc[0]


@patch("system_log.get_request")
def test_get_system_log_pages_follows_next_links(mock_get):
    mock_get.side_effect = [
        make_response([{"uuid": "1"}], next_url="https://signon.okta.com/api/v1/logs?after=1"),
        make_response([{"uuid": "2"}], next_url="https://signon.okta.com/api/v1/logs?after=2"),
        make_response([], next_url="https://signon.okta.com/api/v1/logs?after=2"),
    ]

    pages = list(get_system_log_pages({}, NOW - timedelta(hours=2), NOW))

    assert pages == [[{"uuid": "1"}], [{"uuid": "2"}]]
    first_params = mock_get.call_args_list[0].args[2]
    assert first_params["sortOrder"] == "ASCENDING"
    assert first_params["until"] == "2026-10-09T17:00:00.000Z"
    assert mock_get.call_args_list[1].args[0].endswith("after=1")


@patch("system_log.get_request")
def test_get_system_log_pages_stops_on_failed_page(mock_get):
    mock_get.side_effect = [
        make_response([{"uuid": "1"}], next_url="https://signon.okta.com/api/v1/logs?after=1"),
        None,
    ]

    pages = list(get_system_log_pages({}, NOW - timedelta(hours=2), NOW))

    assert pages == [[{"uuid": "1"}]]


def test_merge_staging_sql_dedupes_on_uuid():
    sql = merge_staging_sql("proj", "temp_okta", "el_okta", NOW)
    assert "merge `proj.el_okta.okta_system_log`" in sql
    assert "from `proj.temp_okta.okta_system_log_staging`" in sql
    assert "on target.uuid = staged.uuid" in sql
    assert "timestamp('2026-10-09 17:00:00')" in sql
    assert "when not matched then" in sql


@patch("system_log.query_bigquery_as_dataframe")
def test_get_last_published_returns_none_without_table(mock_query):
    mock_query.return_value = pd.DataFrame({"table_name": []})
    assert get_last_published("proj", "el_okta") is None
    assert mock_query.call_count == 1


@patch("system_log.query_bigquery_as_dataframe")
def test_get_last_published_raises_when_query_fails(mock_query):
    mock_query.return_value = None
    with pytest.raises(RuntimeError):
        get_last_published("proj", "el_okta")


@patch("system_log.query_bigquery_as_dataframe")
@patch("system_log.upload_dataframe_to_bigquery")
@patch("system_log.get_system_log_pages")
@patch("system_log.get_last_published")
@patch("system_log.get_general_credentials")
def test_sync_system_log_no_events(mock_creds, mock_last, mock_pages, mock_upload, mock_query):
    mock_creds.return_value = "SSWS token"
    mock_last.return_value = NOW - timedelta(hours=3)
    mock_pages.return_value = iter([])

    assert sync_system_log("proj", "temp_okta", "el_okta", now=NOW) == 0

    mock_upload.assert_not_called()
    mock_query.assert_not_called()


@patch("system_log.query_bigquery_as_dataframe")
@patch("system_log.upload_dataframe_to_bigquery")
@patch("system_log.get_system_log_pages")
@patch("system_log.get_last_published")
@patch("system_log.get_general_credentials")
def test_sync_system_log_stages_batches_then_merges(mock_creds, mock_last, mock_pages, mock_upload, mock_query):
    mock_creds.return_value = "SSWS token"
    mock_last.return_value = NOW - timedelta(hours=3)
    pages = [[dict(SAMPLE_EVENT, uuid=str(i))] for i in range(3)]
    mock_pages.return_value = iter(pages)
    mock_query.return_value = pd.DataFrame()

    with patch.object(system_log, "SYSTEM_LOG_PAGES_PER_BATCH", 2):
        assert sync_system_log("proj", "temp_okta", "el_okta", now=NOW) == 3

    dispositions = [call.kwargs["write_disposition"] for call in mock_upload.call_args_list]
    assert dispositions == ["WRITE_TRUNCATE", "WRITE_APPEND"]
    assert mock_pages.call_args.args[1] == NOW - timedelta(hours=4)
    queries = [call.args[0] for call in mock_query.call_args_list]
    assert "create table if not exists" in queries[0]
    assert "merge" in queries[1]


@patch("system_log.query_bigquery_as_dataframe")
@patch("system_log.upload_dataframe_to_bigquery")
@patch("system_log.get_system_log_pages")
@patch("system_log.get_last_published")
@patch("system_log.get_general_credentials")
def test_sync_system_log_raises_when_merge_fails(mock_creds, mock_last, mock_pages, mock_upload, mock_query):
    mock_creds.return_value = "SSWS token"
    mock_last.return_value = None
    mock_pages.return_value = iter([[SAMPLE_EVENT]])
    mock_query.side_effect = [pd.DataFrame(), None]

    with pytest.raises(RuntimeError, match="merge"):
        sync_system_log("proj", "temp_okta", "el_okta", now=NOW)
