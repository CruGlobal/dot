"""Tests for the blackout-window gate."""

import json
import logging
from datetime import datetime, timezone

import pytest

from dot_shared import blackout

WINDOW = {
    "starts_at": "2026-08-08T18:00:00-04:00",  # 22:00Z
    "ends_at": "2026-08-11T06:00:00-04:00",  # 10:00Z
    "reason": "PS Financials 9.2 upgrade",
    "targets": {"fivetran_connector": ["dearth_capably"], "dbt_job": ["33648"]},
}

DURING = datetime(2026, 8, 9, 12, 0, tzinfo=timezone.utc)
BEFORE = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)
AFTER = datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc)


def document(window=None, version=1, name="psfin-upgrade-2026-08"):
    return json.dumps({"version": version, "windows": {name: window or WINDOW}})


@pytest.fixture(autouse=True)
def clean_env(monkeypatch, caplog):
    monkeypatch.delenv(blackout.ENV_VAR, raising=False)
    monkeypatch.delenv(blackout.FORCE_ENV_VAR, raising=False)
    # main.py sets propagate=False on this logger; caplog needs it on.
    monkeypatch.setattr(blackout.logger, "propagate", True)
    caplog.set_level(logging.DEBUG, logger="primary_logger")


def set_document(monkeypatch, value):
    monkeypatch.setenv(blackout.ENV_VAR, value)


def test_active_window_suppresses(monkeypatch, caplog):
    set_document(monkeypatch, document())
    assert (
        blackout.check("fivetran_connector", "dearth_capably", now=DURING)
        == "psfin-upgrade-2026-08"
    )
    assert "event=invocation_suppressed" in caplog.text
    assert "blackout=psfin-upgrade-2026-08" in caplog.text
    assert "kind=fivetran_connector" in caplog.text
    assert "target=dearth_capably" in caplog.text
    assert "PS Financials 9.2 upgrade" in caplog.text


def test_window_not_yet_started(monkeypatch):
    set_document(monkeypatch, document())
    assert blackout.check("fivetran_connector", "dearth_capably", now=BEFORE) is None


def test_window_already_ended(monkeypatch):
    set_document(monkeypatch, document())
    assert blackout.check("fivetran_connector", "dearth_capably", now=AFTER) is None


def test_starts_at_is_inclusive(monkeypatch):
    set_document(monkeypatch, document())
    start = datetime(2026, 8, 8, 22, 0, tzinfo=timezone.utc)
    assert blackout.check("dbt_job", "33648", now=start) == "psfin-upgrade-2026-08"


def test_ends_at_is_exclusive(monkeypatch):
    set_document(monkeypatch, document())
    end = datetime(2026, 8, 11, 10, 0, tzinfo=timezone.utc)
    assert blackout.check("dbt_job", "33648", now=end) is None


def test_target_id_matched_as_string(monkeypatch):
    """dbt job ids arrive as ints from callers but are strings in the document."""
    set_document(monkeypatch, document())
    assert blackout.check("dbt_job", 33648, now=DURING) == "psfin-upgrade-2026-08"


def test_unknown_kind_is_not_gated(monkeypatch):
    set_document(monkeypatch, document())
    assert blackout.check("job", "okta-sync", now=DURING) is None


def test_unknown_target_in_known_kind_is_not_gated(monkeypatch):
    set_document(monkeypatch, document())
    assert blackout.check("dbt_job", "99999", now=DURING) is None


def test_zulu_timestamps(monkeypatch):
    set_document(
        monkeypatch,
        document({**WINDOW, "starts_at": "2026-08-08T22:00:00Z", "ends_at": "2026-08-11T10:00:00Z"}),
    )
    assert blackout.check("dbt_job", "33648", now=DURING) == "psfin-upgrade-2026-08"


def test_naive_timestamps_assumed_utc_with_warning(monkeypatch, caplog):
    set_document(
        monkeypatch,
        document({**WINDOW, "starts_at": "2026-08-08T22:00:00", "ends_at": "2026-08-11T10:00:00"}),
    )
    assert blackout.check("dbt_job", "33648", now=DURING) == "psfin-upgrade-2026-08"
    assert "assuming UTC" in caplog.text


def test_unparseable_timestamp_fails_open(monkeypatch, caplog):
    set_document(monkeypatch, document({**WINDOW, "ends_at": "next tuesday"}))
    assert blackout.check("dbt_job", "33648", now=DURING) is None
    assert "not a parseable timestamp" in caplog.text
    assert caplog.records[0].levelno == logging.WARNING


def test_malformed_json_fails_open(monkeypatch, caplog):
    set_document(monkeypatch, "{not json")
    assert blackout.check("dbt_job", "33648", now=DURING) is None
    assert "not valid JSON" in caplog.text


def test_unknown_version_fails_open(monkeypatch, caplog):
    set_document(monkeypatch, document(version=99))
    assert blackout.check("dbt_job", "33648", now=DURING) is None
    assert "unsupported version" in caplog.text


def test_wrong_shapes_fail_open(monkeypatch, caplog):
    set_document(monkeypatch, json.dumps({"version": 1, "windows": ["nope"]}))
    assert blackout.check("dbt_job", "33648", now=DURING) is None
    assert "not a JSON object" in caplog.text


def test_targets_not_a_list_fails_open(monkeypatch, caplog):
    set_document(monkeypatch, document({**WINDOW, "targets": {"dbt_job": "33648"}}))
    assert blackout.check("dbt_job", "33648", now=DURING) is None
    assert "is not a list" in caplog.text


def test_missing_env_var_fails_open_quietly(monkeypatch, caplog):
    assert blackout.check("dbt_job", "33648", now=DURING) is None
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_one_bad_window_does_not_hide_a_good_one(monkeypatch):
    doc = json.dumps(
        {
            "version": 1,
            "windows": {
                "broken": {"starts_at": "???", "ends_at": "???", "targets": {}},
                "good": WINDOW,
            },
        }
    )
    set_document(monkeypatch, doc)
    assert blackout.check("dbt_job", "33648", now=DURING) == "good"


def test_force_argument_bypasses(monkeypatch, caplog):
    set_document(monkeypatch, document())
    assert blackout.check("dbt_job", "33648", force=True, now=DURING) is None
    assert "event=blackout_force_bypass" in caplog.text


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "Yes"])
def test_force_env_var_bypasses(monkeypatch, caplog, value):
    set_document(monkeypatch, document())
    monkeypatch.setenv(blackout.FORCE_ENV_VAR, value)
    assert blackout.check("dbt_job", "33648", now=DURING) is None
    assert "event=blackout_force_bypass" in caplog.text


def test_force_env_var_off(monkeypatch):
    set_document(monkeypatch, document())
    monkeypatch.setenv(blackout.FORCE_ENV_VAR, "no")
    assert blackout.check("dbt_job", "33648", now=DURING) == "psfin-upgrade-2026-08"


def test_log_status_active(monkeypatch, caplog):
    set_document(monkeypatch, document())
    blackout.log_status(now=DURING)
    assert "Blackout window active: psfin-upgrade-2026-08" in caplog.text


def test_log_status_upcoming(monkeypatch, caplog):
    set_document(monkeypatch, document())
    blackout.log_status(now=datetime(2026, 8, 5, 22, 0, tzinfo=timezone.utc))
    assert (
        "Upcoming blackout window in 3 days: psfin-upgrade-2026-08 "
        "(PS Financials 9.2 upgrade)" in caplog.text
    )


def test_log_status_ignores_far_future_and_past(monkeypatch, caplog):
    set_document(monkeypatch, document())
    blackout.log_status(now=datetime(2026, 7, 1, tzinfo=timezone.utc))
    blackout.log_status(now=AFTER)
    assert "psfin-upgrade-2026-08" not in caplog.text


def test_log_status_never_raises(monkeypatch, caplog):
    set_document(monkeypatch, "{not json")
    blackout.log_status(now=DURING)
    assert "not valid JSON" in caplog.text
