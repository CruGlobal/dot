import json
from unittest import mock

import pytest

import classifier
import dbt_client
import failure_details
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


def test_handler_empty_account_id_falls_back_to_default(monkeypatch):
    # The webhook always sends the key (str(payload.get("accountId", ""))), so an
    # omitted accountId arrives as "" -- it must fall back to the Cru account, not
    # build an empty-segment URL (.../accounts//runs/...). A plain .get(..., default)
    # would NOT cover this case (key present), so this guards the `or` fallback.
    captured = {}

    def make_client(**kw):
        captured.update(kw)
        fake = mock.Mock()
        fake.get_run.return_value = run_data(steps=[ERR_STEP])
        fake.get_run_results.return_value = [result("error", "Resources exceeded")]
        return fake

    monkeypatch.setattr(main, "DbtReadClient", make_client)

    resp = main.classify_run(FakeRequest(json_body={"run_id": "492", "account_id": ""}))
    assert resp[1] == 200
    assert captured["account_id"] == main.DEFAULT_ACCOUNT_ID


def test_read_client_defaults_to_account_host(monkeypatch):
    monkeypatch.delenv("DBT_BASE_URL", raising=False)
    client = dbt_client.DbtReadClient(access_token="t", account_id="10206")
    assert client.account_url == "https://kw745.us1.dbt.com/api/v2/accounts/10206"


def test_read_client_uses_dbt_base_url_override(monkeypatch):
    monkeypatch.setenv("DBT_BASE_URL", "https://example.dbt.test/api/v2/")
    client = dbt_client.DbtReadClient(access_token="t", account_id="10206")
    assert client.account_url == "https://example.dbt.test/api/v2/accounts/10206"


def test_read_client_accepts_bare_dbt_host(monkeypatch):
    monkeypatch.setenv("DBT_BASE_URL", "https://example.dbt.test")
    client = dbt_client.DbtReadClient(access_token="t", account_id="10206")
    assert client.account_url == "https://example.dbt.test/api/v2/accounts/10206"


def test_read_client_empty_dbt_base_url_uses_default(monkeypatch):
    monkeypatch.setenv("DBT_BASE_URL", "")
    client = dbt_client.DbtReadClient(access_token="t", account_id="10206")
    assert client.account_url == "https://kw745.us1.dbt.com/api/v2/accounts/10206"


def test_read_client_fetches_each_artifact_by_name(monkeypatch):
    get = mock.Mock()
    get.return_value.json.return_value = {"results": []}
    monkeypatch.setattr(dbt_client.requests, "get", get)
    client = dbt_client.DbtReadClient(access_token="t", account_id="1")
    client.get_run_sources("9")
    assert get.call_args[0][0].endswith("/runs/9/artifacts/sources.json")
    client.get_run_results("9")
    assert get.call_args[0][0].endswith("/runs/9/artifacts/run_results.json")


# --------------------------------------------------------------------------
# failure_details — the alert fields
# --------------------------------------------------------------------------
def failed_run(steps=None):
    return {
        "href": "https://dbt.test/deploy/1/projects/2/runs/521187447/",
        "job_definition_id": 1045497,
        "trigger": {"cause": "Triggered by completion of run 521184944"},
        "run_steps": steps if steps is not None else [
            {"status": 10, "name": "Invoke dbt with `dbt deps`"},
            {"status": 20, "name": "Invoke dbt with `dbt build --selector release`", "logs": ""},
        ],
    }


def freshness_step(logs=""):
    return {"status": 20, "name": "Invoke dbt with `dbt source freshness --vars 'next: true'`",
            "logs": logs}


# Step logs as dbt Cloud returns them: timestamped lines, colour codes, closing notes.
RUNTIME_ERROR_LOG = (
    "19:05:22  Found 2153 models, 520 sources\n"
    "19:05:23  Encountered an error:\n"
    "Runtime Error\n"
    "  Got a state selector method, but no comparison manifest\n"
    "19:05:23  [\x1b[33mWARNING\x1b[0m][DeprecationsSummary]: Deprecated functionality\n"
    "Summary of encountered deprecations:\n"
    "dbt command failed"
)
NODE_SUMMARY_LOG = (
    "20:36:09  Completed with 1 error, 0 partial successes, and 0 warnings:\n"
    "20:36:09  \n"
    "20:36:09\n"
    "20:36:09  \x1b[31mFailure in test test_x (tests/test_x.sql)\x1b[0m\n"
    "20:36:09\n"
    "20:36:09    Got 2 results, configured to fail if != 0\n"
    "20:36:09  \n"
    "20:36:09  Done. PASS=14 WARN=0 ERROR=1 SKIP=6 NO-OP=0 REUSED=0 TOTAL=21\n"
    "dbt command failed"
)
FRESHNESS_LOG = (
    "07:12:30  1 of 2 ERROR STALE freshness of crm.contacts [ERROR STALE in 1.20s]\n"
    "07:12:47  \x1b[31mFailure in source contacts (models/staging/crm/_src_crm.yml)\x1b[0m\n"
    "07:12:47\n"
    "07:12:47    Status: error\n"
    "07:12:47  \x1b[31mFailure in source notes (models/staging/crm/_src_crm.yml)\x1b[0m\n"
    "07:12:47\n"
    "07:12:47    Status: runtime error\n"
    "07:12:47  Done.\n"
    "07:12:47  [\x1b[33mWARNING\x1b[0m][DeprecationsSummary]: Deprecated functionality\n"
    "dbt command failed"
)


def test_describe_run_and_job_fields():
    d = failure_details.describe(failed_run(), [], job_name="WorkVivo [[workvivo_prod]]")
    assert d["run_href"] == "https://dbt.test/deploy/1/projects/2/runs/521187447/"
    assert d["job_href"] == "https://dbt.test/deploy/1/projects/2/jobs/1045497/"
    assert d["job_name"] == "WorkVivo"
    assert d["trigger_cause"] == "Triggered by completion of run 521184944"
    assert d["failed_step"] == "dbt build --selector release"


def test_describe_lists_errors_before_test_failures():
    results = [
        result("fail", "Got 2 results, configured to fail if != 0",
               "test.proj.not_null_orders_id.ab12cd34ef"),
        result("success", "", "model.proj.ok"),
        result("error", "Database Error\n  Not found: Table x", "model.proj.orders.v1"),
    ]
    d = failure_details.describe(failed_run(), results)
    assert d["failure_details"] == [
        {"resource_type": "model", "name": "orders.v1", "status": "error",
         "message": "Database Error\n  Not found: Table x"},
        {"resource_type": "test", "name": "not_null_orders_id", "status": "fail",
         "message": "Got 2 results, configured to fail if != 0"},
    ]
    assert d["failure_details_omitted"] == 0
    assert d["failed_step_error"] == ""


def test_describe_caps_the_list_and_counts_the_rest():
    results = [result("fail", "Got 1 result", f"test.proj.t{i}") for i in range(8)]
    d = failure_details.describe(failed_run(), results)
    assert len(d["failure_details"]) == failure_details.MAX_FAILURE_DETAILS
    assert d["failure_details_omitted"] == 8 - failure_details.MAX_FAILURE_DETAILS


def test_slack_text_strips_colour_codes_and_cuts_long_messages():
    assert failure_details.slack_text("  \x1b[31mboom\x1b[0m \n") == "boom"
    clipped = failure_details.slack_text("x" * 1000)
    assert len(clipped) == failure_details.MESSAGE_CHARS and clipped.endswith("…")


def test_slack_text_escapes_what_slack_would_read_as_a_link_or_mention():
    assert failure_details.slack_text("<!here> & <https://x|reset>") == (
        "&lt;!here&gt; &amp; &lt;https://x|reset&gt;"
    )


def test_slack_text_cuts_before_escaping_so_no_entity_is_split():
    escaped = failure_details.slack_text("<" * 1000)
    assert escaped == "&lt;" * (failure_details.MESSAGE_CHARS - 1) + "…"


def test_every_text_field_is_escaped():
    run = dict(failed_run(steps=[{"status": 20, "name": "Invoke dbt with `dbt run --select <x>`",
                                  "logs": RUNTIME_ERROR_LOG.replace("manifest", "<!channel>")}]),
               trigger={"cause": "<!channel>"})
    d = failure_details.describe(run, [], job_name="<!here>")
    for field in ("job_name", "trigger_cause", "failed_step", "failed_step_error"):
        assert "<" not in d[field] and "&lt;" in d[field], field
    [detail] = failure_details.result_details(
        [result("error", "ARRAY<STRUCT<a INT64>>", "model<x>.proj.m<y>")]
    )
    assert detail["message"] == "ARRAY&lt;STRUCT&lt;a INT64&gt;&gt;"
    assert detail["resource_type"] == "model&lt;x&gt;"
    assert detail["name"] == "m&lt;y&gt;"


def test_failed_step_is_clipped():
    run = failed_run(steps=[{"status": 20, "name": "Invoke dbt with `dbt build " + "x" * 5000 + "`"}])
    assert len(failure_details.describe(run, [])["failed_step"]) == failure_details.MESSAGE_CHARS


def test_node_name():
    assert failure_details.node_name("model.proj.orders.v1") == "orders.v1"
    assert failure_details.node_name("test.proj.not_null_orders_id.ab12cd34ef") == "not_null_orders_id"
    assert failure_details.node_name("test.proj.test_singular") == "test_singular"
    assert failure_details.node_name("macro.proj.createRowAccessPoliciesForAll") == "createRowAccessPoliciesForAll"
    assert failure_details.node_name("source.proj.crm.accounts") == "crm.accounts"


def test_freshness_failures_come_from_sources_json():
    sources = [
        {"unique_id": "source.proj.crm.accounts", "status": "pass"},
        {"unique_id": "source.proj.crm.contacts", "status": "error",
         "max_loaded_at": "2026-09-21T19:39:29+00:00", "max_loaded_at_time_ago_in_s": 29.5 * 86400,
         "criteria": {"error_after": {"count": 14, "period": "day"}}},
        {"unique_id": "source.proj.crm.notes", "status": "runtime error",
         "error": "Database Error\n  Access Denied"},
        {"unique_id": "source.proj.crm.leads", "status": "warn",
         "max_loaded_at_time_ago_in_s": 4 * 86400,
         "criteria": {"warn_after": {"count": 3, "period": "day"}}},
    ]
    run = failed_run(steps=[freshness_step()])
    d = failure_details.describe(run, [result("success", "", "model.proj.ok")], sources)
    assert d["failure_details"] == [
        {"resource_type": "source", "name": "crm.contacts", "status": "error",
         "message": "last loaded 29.5 days ago (2026-09-21); fails after 14 days"},
        {"resource_type": "source", "name": "crm.notes", "status": "runtime error",
         "message": "Database Error\n  Access Denied"},
    ]


def test_staleness_under_a_day_is_in_hours():
    source = {"unique_id": "source.proj.crm.contacts", "status": "error",
              "max_loaded_at_time_ago_in_s": 5.5 * 3600,
              "criteria": {"error_after": {"count": 1, "period": "hour"}}}
    [detail] = failure_details.freshness_details([source])
    assert detail["message"] == "last loaded 5.5 hours ago; fails after 1 hour"


def test_staleness_age_never_reads_as_the_limit():
    source = {"unique_id": "source.proj.crm.contacts", "status": "error",
              "max_loaded_at_time_ago_in_s": 14.02 * 86400,
              "criteria": {"error_after": {"count": 14, "period": "day"}}}
    [detail] = failure_details.freshness_details([source])
    assert detail["message"] == "last loaded 14.1 days ago; fails after 14 days"


def test_needs_sources_only_when_the_freshness_step_failed():
    assert failure_details.needs_sources(failed_run(steps=[freshness_step()])) is True
    assert failure_details.needs_sources(failed_run()) is False
    passed_freshness = [dict(freshness_step(), status=10),
                        {"status": 20, "name": "Invoke dbt with `dbt build`"}]
    assert failure_details.needs_sources(failed_run(steps=passed_freshness)) is False
    assert failure_details.needs_sources(None) is False


def test_a_non_blocking_freshness_step_is_not_the_failed_step():
    # A job's "Run source freshness" option: freshness errors first, the run carries on,
    # and a later step is what failed it.
    steps = [freshness_step(),
             {"status": 10, "name": "Invoke dbt with `dbt build --selector raw_and_staging`"},
             {"status": 20, "name": "Invoke dbt with `dbt build --selector unittest`"},
             {"status": 40, "name": "Invoke dbt with `dbt run --selector release`"}]
    run = failed_run(steps=steps)
    assert failure_details.describe(run, [])["failed_step"] == "dbt build --selector unittest"
    assert failure_details.needs_sources(run) is False


def test_failed_step_error_from_a_command_level_error():
    run = failed_run(steps=[{"status": 20, "name": "Invoke dbt with `dbt run --selector x`",
                             "logs": RUNTIME_ERROR_LOG}])
    d = failure_details.describe(run, [result("success", "", "model.proj.ok")])
    assert d["failure_details"] == []
    assert d["failed_step_error"] == (
        "Encountered an error:\nRuntime Error\n"
        "  Got a state selector method, but no comparison manifest"
    )


def test_failed_step_error_starts_at_the_last_error_summary():
    logs = ("10:00:00  Encountered an error:\nCompilation Error\n  first\n"
            "10:00:01  Encountered an error:\nRuntime Error\n  second\n"
            "dbt command failed")
    run = failed_run(steps=[{"status": 20, "name": "Invoke dbt with `dbt run`", "logs": logs}])
    assert failure_details.describe(run, None)["failed_step_error"] == (
        "Encountered an error:\nRuntime Error\n  second"
    )


def test_failed_step_error_when_run_results_is_missing():
    run = failed_run(steps=[{"status": 20, "name": "Invoke dbt with `dbt build`",
                             "logs": NODE_SUMMARY_LOG}])
    assert failure_details.describe(run, None)["failed_step_error"] == (
        "Completed with 1 error, 0 partial successes, and 0 warnings:\n"
        "Failure in test test_x (tests/test_x.sql)\n"
        "Got 2 results, configured to fail if != 0"
    )


def test_failed_step_error_for_freshness_starts_at_the_first_failed_source():
    run = failed_run(steps=[freshness_step(logs=FRESHNESS_LOG)])
    assert failure_details.describe(run, None, None)["failed_step_error"] == (
        "Failure in source contacts (models/staging/crm/_src_crm.yml)\nStatus: error\n"
        "Failure in source notes (models/staging/crm/_src_crm.yml)\nStatus: runtime error"
    )


def test_no_failed_step_error_when_a_listed_failure_says_why():
    run = failed_run(steps=[{"status": 20, "name": "Invoke dbt with `dbt build`",
                             "logs": NODE_SUMMARY_LOG}])
    d = failure_details.describe(run, [result("fail", "Got 2 results", "test.proj.t")])
    assert d["failed_step_error"] == ""


def test_failed_step_error_when_no_listed_failure_says_why():
    # A failed `dbt run-operation` often writes its macro to run_results.json with no message.
    run = failed_run(steps=[{"status": 20, "name": "Invoke dbt with `dbt run-operation grants`",
                             "logs": RUNTIME_ERROR_LOG}])
    d = failure_details.describe(run, [result("error", None, "macro.proj.grants")])
    assert d["failure_details"] == [
        {"resource_type": "macro", "name": "grants", "status": "error", "message": ""}
    ]
    assert d["failed_step_error"].startswith("Encountered an error:\nRuntime Error")


def test_failed_step_error_has_its_own_longer_limit():
    run = failed_run(steps=[{"status": 20, "name": "Invoke dbt with `dbt build`",
                             "logs": "Encountered an error:\n" + "x" * 5000}])
    step_error = failure_details.describe(run, None)["failed_step_error"]
    assert len(step_error) == failure_details.STEP_ERROR_CHARS and step_error.endswith("…")


def test_failed_step_error_is_empty_without_a_dbt_error_summary():
    run = failed_run(steps=[{"status": 20, "name": "Invoke dbt with `dbt deps`",
                             "logs": "12:00:00  Running dbt...\ndbt command failed"}])
    assert failure_details.describe(run, None)["failed_step_error"] == ""


def test_describe_without_run_metadata_keeps_only_the_given_job_name():
    d = failure_details.describe(None, None, job_name="GodTools [[godtools_prod]]")
    assert d["job_name"] == "GodTools"
    assert d["run_href"] == d["job_href"] == d["trigger_cause"] == d["failed_step"] == ""
    assert d["failure_details"] == [] and d["failure_details_omitted"] == 0
    assert d["failed_step_error"] == ""


def test_job_href_is_empty_when_the_run_link_has_an_unexpected_shape():
    run = dict(failed_run(), href="https://dbt.test/somewhere/else")
    assert failure_details.job_href(run) == ""


# --------------------------------------------------------------------------
# main.classify_run — alert fields
# --------------------------------------------------------------------------
def test_handler_adds_failure_details_to_the_verdict(monkeypatch):
    fake = mock.Mock()
    fake.get_run.return_value = failed_run()
    fake.get_run_results.return_value = [result("fail", "Got 2 results", "test.proj.t")]
    monkeypatch.setattr(main, "DbtReadClient", lambda **kw: fake)

    body = {"run_id": "521187447", "job_name": "WorkVivo [[workvivo_prod]]"}
    verdict = json.loads(main.classify_run(FakeRequest(json_body=body))[0])
    assert verdict["reason"] == "nontransient"
    assert verdict["job_name"] == "WorkVivo"
    assert verdict["failure_details"][0]["name"] == "t"
    fake.get_run_sources.assert_not_called()


def test_handler_reads_sources_json_for_a_failed_freshness_step(monkeypatch):
    fake = mock.Mock()
    fake.get_run.return_value = failed_run(steps=[freshness_step()])
    fake.get_run_results.return_value = [result("success", "", "model.proj.ok")]
    fake.get_run_sources.return_value = [{"unique_id": "source.proj.crm.contacts", "status": "error"}]
    monkeypatch.setattr(main, "DbtReadClient", lambda **kw: fake)

    verdict = json.loads(main.classify_run(FakeRequest(json_body={"run_id": "520774029"}))[0])
    assert verdict["reason"] == "uncovered_step"
    assert verdict["failure_details"][0]["name"] == "crm.contacts"


def test_handler_sources_fetch_failure_falls_back_to_the_log(monkeypatch):
    fake = mock.Mock()
    fake.get_run.return_value = failed_run(steps=[freshness_step(logs=FRESHNESS_LOG)])
    fake.get_run_results.return_value = [result("success", "", "model.proj.ok")]
    fake.get_run_sources.side_effect = RuntimeError("artifact 404")
    monkeypatch.setattr(main, "DbtReadClient", lambda **kw: fake)

    resp = main.classify_run(FakeRequest(json_body={"run_id": "520774029"}))
    assert resp[1] == 200
    verdict = json.loads(resp[0])
    assert verdict["reason"] == "uncovered_step"
    assert verdict["failure_details"] == []
    assert verdict["failed_step_error"].startswith("Failure in source contacts")


def test_handler_details_failure_never_changes_the_verdict(monkeypatch):
    fake = mock.Mock()
    fake.get_run.return_value = run_data(steps=[ERR_STEP])
    fake.get_run_results.return_value = [result("error", "Resources exceeded")]
    monkeypatch.setattr(main, "DbtReadClient", lambda **kw: fake)
    monkeypatch.setattr(failure_details, "describe", mock.Mock(side_effect=RuntimeError("bug")))

    resp = main.classify_run(FakeRequest(json_body={"run_id": "492"}))
    assert resp[1] == 200
    verdict = json.loads(resp[0])
    assert verdict["reason"] == "transient" and verdict["is_retryable"] is True
    assert "failure_details" not in verdict


def test_handler_details_never_replace_a_classifier_field(monkeypatch):
    fake = mock.Mock()
    fake.get_run.return_value = run_data(steps=[ERR_STEP])
    fake.get_run_results.return_value = [result("error", "Resources exceeded")]
    monkeypatch.setattr(main, "DbtReadClient", lambda **kw: fake)
    clashing = dict(failure_details.describe(None),
                    reason="nontransient", is_retryable=False, run_href="https://x")
    monkeypatch.setattr(failure_details, "describe", mock.Mock(return_value=clashing))

    verdict = json.loads(main.classify_run(FakeRequest(json_body={"run_id": "492"}))[0])
    assert verdict["reason"] == "transient" and verdict["is_retryable"] is True
    assert verdict["run_href"] == "https://x"


def test_handler_logs_alert_field_counts_never_their_text(monkeypatch, capsys):
    fake = mock.Mock()
    fake.get_run.return_value = failed_run()
    fake.get_run_results.return_value = [result("fail", "value 'jane@example.org' found", "test.proj.t")]
    monkeypatch.setattr(main, "DbtReadClient", lambda **kw: fake)

    main.classify_run(FakeRequest(json_body={"run_id": "521187447", "job_name": "WorkVivo"}))
    logged = capsys.readouterr().out
    assert "failures_listed=1 failures_omitted=0 has_run_href=True has_step_error=False" in logged
    assert "jane@example.org" not in logged


def test_handler_logs_the_counts_for_the_listed_failures_only(monkeypatch, capsys):
    # Seven errors with no message sort first and fill the list; the one test failure that
    # says why sorts past the cap, so the step error is still filled.
    run = failed_run(steps=[{"status": 20, "name": "Invoke dbt with `dbt build`",
                             "logs": RUNTIME_ERROR_LOG.replace("manifest", "'jane@example.org'")}])
    del run["href"]
    fake = mock.Mock()
    fake.get_run.return_value = run
    fake.get_run_results.return_value = (
        [result("error", None, f"model.proj.m{i}") for i in range(7)]
        + [result("fail", "Got 1 result", "test.proj.t")]
    )
    monkeypatch.setattr(main, "DbtReadClient", lambda **kw: fake)

    verdict = json.loads(main.classify_run(FakeRequest(json_body={"run_id": "521187447"}))[0])
    logged = capsys.readouterr().out
    assert "failures_listed=5 failures_omitted=3 has_run_href=False has_step_error=True" in logged
    assert "jane@example.org" not in logged
    assert "jane@example.org" in verdict["failed_step_error"]


@pytest.mark.parametrize("bad", [
    {"finished_at": object()},
    {"age_days": float("nan")},
    {"age_days": float("inf")},
    {"failure_details_omitted": "jane@example.org"},
])
def test_handler_drops_details_that_cannot_be_sent_and_keeps_the_verdict(monkeypatch, capsys, bad):
    fake = mock.Mock()
    fake.get_run.return_value = run_data(steps=[ERR_STEP])
    fake.get_run_results.return_value = [result("error", "Resources exceeded")]
    monkeypatch.setattr(main, "DbtReadClient", lambda **kw: fake)
    unsendable = dict(failure_details.describe(None), **bad)
    monkeypatch.setattr(failure_details, "describe", mock.Mock(return_value=unsendable))

    resp = main.classify_run(FakeRequest(json_body={"run_id": "492"}))
    assert resp[1] == 200
    assert "NaN" not in resp[0] and "Infinity" not in resp[0]
    verdict = json.loads(resp[0])
    assert verdict["reason"] == "transient" and verdict["is_retryable"] is True
    assert "failure_details" not in verdict and not set(bad) & set(verdict)
    logged = capsys.readouterr().out
    assert "alert_fields=unavailable" in logged
    assert "jane@example.org" not in logged
