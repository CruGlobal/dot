"""What failed in a dbt Cloud run, for a Slack failure alert.

No I/O lives here, like classifier.py: `describe()` takes the already-fetched run
metadata and artifacts and returns the fields a failure alert needs -- which models,
tests or sources failed and why, the failed step, and links to the run and the job.
It never changes the retry decision; classifier.decide() owns that.

Where the failures are depends on the failed step:
  dbt build / run / test / seed / snapshot / run-operation -> run_results.json
  dbt source freshness                                     -> sources.json
  a command-level error (compile, selector, deps) or a missing artifact
                                                           -> the step's log only

Every text field is Slack-escaped and meant to be posted, not logged: it is dbt and
BigQuery error output, which can quote a data value.
"""

import math
import re

from classifier import STEP_STATUS_ERROR

# The post lists at most this many failures, each message cut to MESSAGE_CHARS, so it
# stays a few lines per failure and readable on a phone. The rest are counted in
# failure_details_omitted and are one click away in the run.
MAX_FAILURE_DETAILS = 5
MESSAGE_CHARS = 400
# failed_step_error is the post's only "why" when no listed failure gives one, and dbt's
# summary names the node and its path (often twice) before the error itself.
STEP_ERROR_CHARS = 1000

ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")
# Slack reads & < > as control characters in any text, including code blocks, so raw
# error text such as "<!DOCTYPE html>" could form a link or an @-mention. "&" goes first.
SLACK_ESCAPES = (("&", "&amp;"), ("<", "&lt;"), (">", "&gt;"))
# jobs-as-code appends the job's identifier to its name: "WorkVivo [[workvivo_prod]]".
JOB_IDENTIFIER_SUFFIX = re.compile(r"\s*\[\[[^\]]*\]\]\s*$")
# dbt Cloud names a command step "Invoke dbt with `<command>`".
STEP_COMMAND = re.compile(r"^Invoke dbt with `(?P<command>.*)`$", re.DOTALL)
# A generic test's unique_id ends in a 10-character hash: "test.proj.not_null_x.ab12cd34ef".
TEST_HASH_SUFFIX = re.compile(r"^[0-9a-f]{10}$")
RUN_PATH = re.compile(r"/runs/\d+/?$")
# Each dbt log line starts with an "HH:MM:SS" timestamp.
LOG_TIMESTAMP = re.compile(r"^\d{2}:\d{2}:\d{2}[ \t]*", re.MULTILINE)
# dbt's own error summary in a step log starts at the last command-level error or
# per-node summary (or, for `dbt source freshness`, which prints no summary line, at
# its first failed source), and ends where the closing tally or deprecation notes begin.
LOG_ERROR_START = ("Encountered an error", "Completed with")
LOG_FIRST_NODE_FAILURE = "Failure in "
LOG_ERROR_END = ("Done.", "[WARNING][DeprecationsSummary]", "dbt command failed")

FRESHNESS_COMMAND = "dbt source freshness"
SECONDS_PER_HOUR = 3600
SECONDS_PER_DAY = 86400


def slack_text(text, limit=MESSAGE_CHARS) -> str:
    """`text` without colour codes or surrounding space, cut to `limit` characters, with
    Slack's control characters escaped."""
    text = ANSI_ESCAPE.sub("", text or "").strip()
    if len(text) > limit:
        text = text[: limit - 1].rstrip() + "…"
    for raw, escaped in SLACK_ESCAPES:
        text = text.replace(raw, escaped)
    return text


def node_name(unique_id: str) -> str:
    """The readable part of a unique_id: "model.proj.orders.v1" -> "orders.v1",
    "test.proj.not_null_orders_id.ab12cd34ef" -> "not_null_orders_id"."""
    parts = unique_id.split(".")[2:]
    if unique_id.startswith("test.") and len(parts) > 1 and TEST_HASH_SUFFIX.match(parts[-1]):
        parts = parts[:-1]
    return ".".join(parts) or unique_id


def step_command(step) -> str:
    """The dbt command a step ran, without dbt Cloud's "Invoke dbt with" wrapper."""
    name = (step or {}).get("name") or ""
    match = STEP_COMMAND.match(name)
    return match.group("command") if match else name


def failed_step(run_data: dict):
    """The step that failed the run, or None. A job's "Run source freshness" option runs
    freshness first and carries on when it errors, so an errored freshness step counts
    only when no other step errored."""
    errored = [s for s in run_data.get("run_steps") or [] if s.get("status") == STEP_STATUS_ERROR]
    blocking = [s for s in errored if FRESHNESS_COMMAND not in step_command(s)]
    return (blocking or errored or [None])[0]


def needs_sources(run_data) -> bool:
    """True when the step that failed the run is `dbt source freshness`, whose failures
    are in sources.json."""
    return FRESHNESS_COMMAND in step_command(failed_step(run_data or {}))


def _count(number, unit: str) -> str:
    return f"{number} {unit}" + ("" if number == 1 else "s")


def _age(seconds) -> str:
    """Rounded up to a tenth, so a source past a 14-day limit never reads "14 days"."""
    unit, size = ("day", SECONDS_PER_DAY) if seconds >= SECONDS_PER_DAY else ("hour", SECONDS_PER_HOUR)
    return _count(math.ceil(seconds * 10 / size) / 10, unit)


def _staleness(source: dict) -> str:
    """e.g. "last loaded 29.5 days ago (2026-09-21); fails after 14 days"."""
    parts = []
    age = source.get("max_loaded_at_time_ago_in_s")
    if age is not None:
        loaded = (source.get("max_loaded_at") or "")[:10]
        parts.append(f"last loaded {_age(age)} ago" + (f" ({loaded})" if loaded else ""))
    limit = (source.get("criteria") or {}).get("error_after") or {}
    if limit.get("count"):
        parts.append(f"fails after {_count(limit['count'], limit.get('period') or 'day')}")
    return "; ".join(parts)


def _detail(unique_id, status, message) -> dict:
    unique_id = unique_id or ""
    return {
        "resource_type": slack_text(unique_id.split(".", 1)[0]),
        "name": slack_text(node_name(unique_id)),
        "status": status,
        "message": slack_text(message),
    }


def result_details(results) -> list:
    """Models, tests, seeds, snapshots and operations that errored or failed, from run_results.json."""
    return [
        _detail(r.get("unique_id"), r.get("status"), r.get("message"))
        for r in results or []
        if r.get("status") in ("error", "fail")
    ]


def freshness_details(sources) -> list:
    """Sources past their error_after limit, or whose check could not run, from sources.json."""
    details = []
    for s in sources or []:
        status = s.get("status")
        if status == "error":
            details.append(_detail(s.get("unique_id"), status, _staleness(s)))
        elif status == "runtime error":
            details.append(_detail(s.get("unique_id"), status, s.get("error")))
    return details


def log_error_summary(logs) -> str:
    """dbt's own error summary from a step log, without timestamps or the closing notes."""
    text = ANSI_ESCAPE.sub("", logs or "")
    start = max(text.rfind(marker) for marker in LOG_ERROR_START)
    if start < 0:
        start = text.find(LOG_FIRST_NODE_FAILURE)
    if start < 0:
        return ""
    text = text[start:]
    ends = [i for i in (text.find(marker) for marker in LOG_ERROR_END) if i > 0]
    if ends:
        text = text[: min(ends)]
    lines = (line.rstrip() for line in LOG_TIMESTAMP.sub("", text).splitlines())
    return slack_text("\n".join(line for line in lines if line.strip()), STEP_ERROR_CHARS)


def job_href(run_data: dict) -> str:
    """The job's page, from the run's: .../projects/1/runs/2/ -> .../projects/1/jobs/<job id>/."""
    href, job_id = run_data.get("href") or "", run_data.get("job_definition_id")
    if not job_id:
        return ""
    href, replaced = RUN_PATH.subn(f"/jobs/{job_id}/", href)
    return href if replaced else ""


def describe(run_data, results=None, sources=None, job_name="") -> dict:
    """The alert fields for a failed run.

    Args:
        run_data: the run from DbtReadClient.get_run(), or None if that fetch failed
                  (every field but job_name is then empty).
        results: the run_results.json results[] list, or None.
        sources: the sources.json results[] list, or None (fetched only when
                 needs_sources() is true).
        job_name: the job name the caller already has (dbt's webhook sends it).

    Errors (a model, seed, operation or source that did not build or is stale, or a
    test that could not run) are listed before test failures, so a broken build is
    never hidden behind a list of failing tests.
    failed_step_error is filled only when no listed failure says why: none was found, or
    each has an empty message, as a failed `dbt run-operation` often does.
    """
    run_data = run_data or {}
    step = failed_step(run_data)
    details = sorted(result_details(results) + freshness_details(sources),
                     key=lambda d: d["status"] == "fail")
    listed = details[:MAX_FAILURE_DETAILS]
    return {
        "run_href": run_data.get("href") or "",
        "job_href": job_href(run_data),
        "job_name": slack_text(JOB_IDENTIFIER_SUFFIX.sub("", job_name or "")),
        "trigger_cause": slack_text((run_data.get("trigger") or {}).get("cause")),
        "failed_step": slack_text(step_command(step)),
        "failure_details": listed,
        "failure_details_omitted": len(details) - len(listed),
        "failed_step_error": (
            "" if any(d["message"] for d in listed) else log_error_summary((step or {}).get("logs"))
        ),
    }
