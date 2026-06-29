import json
from unittest import mock

import pytest

import classifier
import main


# --------------------------------------------------------------------------
# fixtures / builders
# --------------------------------------------------------------------------
def run_data(cause="Scheduled run", steps=None, created_at="2026-06-29T00:00:00Z"):
    return {
        "trigger": {"cause": cause},
        "run_steps": steps if steps is not None else [{"status": 10}],
        "created_at": created_at,
    }


def result(status, message="", uid="model.proj.thing"):
    return {"status": status, "message": message, "unique_id": uid}


ERR_STEP = {"status": 20}  # a run step that errored


# --------------------------------------------------------------------------
# classifier.decide — the core rules
# --------------------------------------------------------------------------
def test_transient_error_is_retryable():
    results = [result("error", "Resources exceeded during query execution", "model.a")]
    v = classifier.decide(run_data(steps=[ERR_STEP]), results)
    assert v["reason"] == "transient"
    assert v["is_retryable"] is True
    assert v["failed_count"] == 1 and v["nontransient_count"] == 0


def test_test_failure_is_nontransient():
    # status "fail" is a data/assertion failure -> never transient
    v = classifier.decide(run_data(steps=[ERR_STEP]), [result("fail", "", "test.a")])
    assert v["reason"] == "nontransient"
    assert v["is_retryable"] is False
    assert v["nontransient_count"] == 1


def test_unknown_error_is_nontransient():
    results = [result("error", "Not found: Table prod.foo was not found", "model.b")]
    v = classifier.decide(run_data(steps=[ERR_STEP]), results)
    assert v["reason"] == "nontransient"
    assert v["is_retryable"] is False


def test_mixed_transient_and_nontransient_not_retryable():
    # default-deny: one non-transient node poisons the whole run
    results = [
        result("error", "Service Unavailable", "model.a"),
        result("error", "Syntax error near SELECT", "model.b"),
    ]
    v = classifier.decide(run_data(steps=[ERR_STEP, ERR_STEP]), results)
    assert v["is_retryable"] is False
    assert v["failed_count"] == 2 and v["nontransient_count"] == 1


def test_multiple_transient_errors_retryable():
    results = [
        result("error", "Could not serialize access", "model.a"),
        result("error", "try again later", "model.b"),
    ]
    v = classifier.decide(run_data(steps=[ERR_STEP, ERR_STEP]), results)
    assert v["is_retryable"] is True
    assert v["failed_count"] == 2 and v["nontransient_count"] == 0


def test_no_results_not_retryable():
    v = classifier.decide(run_data(steps=[ERR_STEP]), [])
    assert v["reason"] == "no_results"
    assert v["is_retryable"] is False


def test_uncovered_step_not_retryable():
    # a step errored, but run_results explains no failed node -> command-level failure
    v = classifier.decide(run_data(steps=[ERR_STEP]), [result("success", "", "model.ok")])
    assert v["reason"] == "uncovered_step"
    assert v["is_retryable"] is False
    assert v["failed_step_count"] == 1 and v["failed_count"] == 0


def test_already_retried_is_loop_guarded():
    rd = run_data(cause="Auto-retry for transient failure in run 123", steps=[ERR_STEP])
    v = classifier.decide(rd, [result("error", "Service Unavailable")])
    assert v["reason"] == "already_retried"
    assert v["prior_is_retry"] is True
    assert v["is_retryable"] is False


def test_metadata_unavailable_fails_closed():
    v = classifier.decide(None, None)
    assert v["reason"] == "metadata_unavailable"
    assert v["is_retryable"] is False


def test_results_unavailable_not_retryable():
    v = classifier.decide(run_data(steps=[ERR_STEP]), None, results_fetch_failed=True)
    assert v["reason"] == "results_unavailable"
    assert v["is_retryable"] is False


def test_run_created_at_passed_through():
    v = classifier.decide(run_data(created_at="2026-06-29T12:34:56Z"),
                          [result("error", "backend error")])
    assert v["run_created_at"] == "2026-06-29T12:34:56Z"


# --------------------------------------------------------------------------
# classifier helpers
# --------------------------------------------------------------------------
def test_count_failed_steps():
    assert classifier.count_failed_steps(run_data(steps=[{"status": 10}, {"status": 20}, {"status": 20}])) == 2
    assert classifier.count_failed_steps(run_data(steps=[])) == 0


def test_is_already_retried_case_insensitive():
    assert classifier.is_already_retried({"trigger": {"cause": "AUTO-RETRY ..."}}) is True
    assert classifier.is_already_retried({"trigger": {"cause": "Scheduled"}}) is False
    assert classifier.is_already_retried({}) is False


def test_classify_results_tallies():
    scan = classifier.classify_results([
        result("success", "", "model.ok"),
        result("error", "deadline exceeded", "model.t"),
        result("error", "permission denied", "model.n"),
        result("fail", "", "test.x"),
    ])
    assert scan["failed_count"] == 3          # 2 errors + 1 fail
    assert scan["nontransient_count"] == 2    # the non-transient error + the test fail


# --------------------------------------------------------------------------
# main.classify_run — handler contract
# --------------------------------------------------------------------------
class FakeRequest:
    def __init__(self, headers=None, json_body=None):
        self.headers = headers or {}
        self._json = json_body

    def get_json(self, silent=False):
        return self._json

    def get_data(self, as_text=False):
        return ""


@pytest.fixture(autouse=True)
def token_env(monkeypatch):
    monkeypatch.setenv("DBT_TOKEN", "test-token")


def test_handler_missing_run_id_returns_400():
    resp = main.classify_run(FakeRequest(json_body={}))
    assert resp[1] == 400


def test_handler_missing_token_returns_500(monkeypatch):
    monkeypatch.delenv("DBT_TOKEN", raising=False)
    resp = main.classify_run(FakeRequest(json_body={"run_id": "1"}))
    assert resp[1] == 500


def test_handler_happy_path_returns_verdict(monkeypatch):
    fake = mock.Mock()
    fake.get_run.return_value = run_data(steps=[ERR_STEP])
    fake.get_run_results.return_value = [result("error", "Resources exceeded")]
    monkeypatch.setattr(main, "DbtReadClient", lambda **kw: fake)

    resp = main.classify_run(FakeRequest(json_body={"run_id": "492", "account_id": "10206"}))
    assert resp[1] == 200
    verdict = json.loads(resp[0])
    assert verdict["reason"] == "transient" and verdict["is_retryable"] is True


def test_handler_metadata_fetch_failure_is_guard_uncertain(monkeypatch):
    fake = mock.Mock()
    fake.get_run.side_effect = RuntimeError("dbt API 500")
    monkeypatch.setattr(main, "DbtReadClient", lambda **kw: fake)

    resp = main.classify_run(FakeRequest(json_body={"run_id": "999"}))
    assert resp[1] == 200
    verdict = json.loads(resp[0])
    assert verdict["reason"] == "metadata_unavailable"
    assert verdict["is_retryable"] is False
    fake.get_run_results.assert_not_called()


def test_handler_results_fetch_failure(monkeypatch):
    fake = mock.Mock()
    fake.get_run.return_value = run_data(steps=[ERR_STEP])
    fake.get_run_results.side_effect = RuntimeError("artifact 404")
    monkeypatch.setattr(main, "DbtReadClient", lambda **kw: fake)

    resp = main.classify_run(FakeRequest(json_body={"run_id": "500"}))
    verdict = json.loads(resp[0])
    assert verdict["reason"] == "results_unavailable"
    assert verdict["is_retryable"] is False
