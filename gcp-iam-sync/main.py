"""Nightly export of IAM bindings into `el_gcp_iam`.

Writes one row per binding on each configured project, on every BigQuery dataset in those
projects, and on every table, to `iam_policy_projects`, `iam_policy_datasets` and
`iam_policy_tables`. dse-dbt reads them as `raw_gcp_iam_*`.

A level that could not read every object it found is not written, and the job exits 1 so the
Datadog monitor on failed executions fires. dse-dbt treats a binding missing from the newest
snapshot as removed, so a partial snapshot would record false end dates; a skipped night leaves
the level a day old but correct.
"""

import logging
import os
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional, TypeVar

import requests
from google.api_core import exceptions
from google.api_core.retry import Retry
from google.auth import exceptions as auth_exceptions
from google.cloud import bigquery, resourcemanager_v3
from google.cloud.logging_v2.handlers import StructuredLogHandler

import members

logger = logging.getLogger("gcp_iam_sync")

DEFAULT_DESTINATION = "cru-data-warehouse-elt-prod.el_gcp_iam"

# dbt's stored test failures: thousands of short-lived tables (about 6,000 across elt-prod and
# beta-prod) that never carry grants of their own, so reading them only spends IAM quota.
SKIPPED_TABLE_DATASETS = frozenset({"dbt_test__audit"})

RETRYABLE_REASONS = frozenset(
    {"rateLimitExceeded", "quotaExceeded", "backendError", "internalError", "badGateway"}
)
RETRYABLE_TYPES = (
    exceptions.TooManyRequests,
    exceptions.InternalServerError,
    exceptions.BadGateway,
    exceptions.ServiceUnavailable,
    exceptions.DeadlineExceeded,
    ConnectionError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
    auth_exceptions.TransportError,
)
RETRY_TIMEOUT_SECONDS = 600
FAILURES_LOGGED_PER_LEVEL = 50

# A few tables (dbt's temporary ones) can be dropped while a run reads them. More than this means
# something removed objects wholesale, and writing the snapshot would end every binding on them.
GONE_LIMIT_PER_LEVEL = 25

# Version 3 returns a conditional grant under its plain role, as the gcloud-based export always
# wrote it; version 1 renames it `<role>_withcond_<hash>`. The condition itself is not exported.
PROJECT_POLICY_VERSION = 3

T = TypeVar("T")


def is_transient(exc: Exception) -> bool:
    """Rate, quota and server errors are retried; anything else fails the read at once.

    `quotaExceeded` is included because the table level makes one IAM call per table and shares
    the per-minute IAM policy quota with the Security Macros sweeps.
    """
    if isinstance(exc, RETRYABLE_TYPES):
        return True
    errors = getattr(exc, "errors", None) or []
    first = errors[0] if errors else None
    return isinstance(first, dict) and first.get("reason") in RETRYABLE_REASONS


RETRIES: Counter[str] = Counter()


def count_retry(exc: Exception) -> None:
    """Tallies each retry by error type, so a run slowed by quota shows it in its summary."""
    RETRIES[type(exc).__name__] += 1


RETRY = Retry(predicate=is_transient, timeout=RETRY_TIMEOUT_SECONDS, on_error=count_retry)


@dataclass(frozen=True)
class Level:
    table: str
    key_columns: tuple[str, ...]

    @property
    def schema(self) -> list[bigquery.SchemaField]:
        """The destination's columns, in its order."""
        columns = (*self.key_columns, "role", "member", "type")
        return [bigquery.SchemaField(name, "STRING") for name in columns] + [
            bigquery.SchemaField("timestamp", "TIMESTAMP")
        ]


PROJECTS = Level("iam_policy_projects", ("project",))
DATASETS = Level("iam_policy_datasets", ("project", "dataset"))
TABLES = Level("iam_policy_tables", ("project", "dataset", "table"))
LEVELS = (PROJECTS, DATASETS, TABLES)


@dataclass
class LevelResult:
    rows: list[dict[str, Any]] = field(default_factory=list)
    read: int = 0
    gone: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list)


def attempt(
    description: str,
    call: Callable[[], T],
    affected: list[LevelResult],
    *,
    may_vanish: bool,
) -> Optional[T]:
    """Runs one read and returns its value, or records why it failed and returns None.

    `may_vanish` is for objects found by listing during this run: one deleted before it is read
    counts as gone. A configured project that cannot be found is a failure.
    """
    try:
        return call()
    except exceptions.NotFound as exc:
        if may_vanish:
            for result in affected:
                result.gone.append(description)
            logger.info(f"{description}: gone before it was read")
            return None
        failure = f"{description}: {exc}"
    except Exception as exc:  # any unread object makes its level incomplete
        failure = f"{description}: {type(exc).__name__}: {exc}"
    for result in affected:
        result.failures.append(failure)
    logger.warning(failure)
    return None


def collect_dataset(
    bq: bigquery.Client,
    project: str,
    dataset_id: str,
    datasets: LevelResult,
    tables: LevelResult,
) -> None:
    dataset_ref = f"{project}.{dataset_id}"
    dataset = attempt(
        f"dataset {dataset_ref}",
        lambda: bq.get_dataset(dataset_ref, retry=RETRY),
        [datasets],
        may_vanish=True,
    )
    if dataset is not None and not dataset.access_entries:
        datasets.failures.append(f"dataset {dataset_ref}: returned no access entries")
    elif dataset is not None:
        datasets.read += 1
        datasets.rows += members.dataset_rows(project, dataset_id, dataset.access_entries)

    if dataset_id in SKIPPED_TABLE_DATASETS:
        return
    table_ids = attempt(
        f"tables in {dataset_ref}",
        lambda: [item.table_id for item in bq.list_tables(dataset_ref, retry=RETRY)],
        [tables],
        may_vanish=True,
    )
    for table_id in table_ids or []:
        table_ref = f"{dataset_ref}.{table_id}"
        policy = attempt(
            f"table {table_ref}",
            lambda: bq.get_iam_policy(table_ref, retry=RETRY),
            [tables],
            may_vanish=True,
        )
        if policy is not None:
            tables.read += 1
            tables.rows += members.binding_rows(
                {"project": project, "dataset": dataset_id, "table": table_id},
                ((binding["role"], binding["members"]) for binding in policy.bindings),
                members.table_role,
            )


def collect(
    project_ids: list[str],
    projects_api: resourcemanager_v3.ProjectsClient,
    bq: bigquery.Client,
) -> dict[Level, LevelResult]:
    """Reads every level in one pass, listing each project's datasets once."""
    results = {level: LevelResult() for level in LEVELS}
    projects, datasets, tables = (results[level] for level in LEVELS)

    for project in project_ids:
        logger.info(f"Reading {project}")
        policy = attempt(
            f"project {project}",
            lambda: projects_api.get_iam_policy(
                request={
                    "resource": f"projects/{project}",
                    "options": {"requested_policy_version": PROJECT_POLICY_VERSION},
                },
                retry=RETRY,
            ),
            [projects],
            may_vanish=False,
        )
        if policy is not None:
            projects.read += 1
            projects.rows += members.binding_rows(
                {"project": project},
                ((binding.role, binding.members) for binding in policy.bindings),
                members.project_role,
            )

        dataset_ids = attempt(
            f"datasets in {project}",
            lambda: [item.dataset_id for item in bq.list_datasets(project, retry=RETRY)],
            [datasets, tables],
            may_vanish=False,
        )
        for dataset_id in dataset_ids or []:
            collect_dataset(bq, project, dataset_id, datasets, tables)

    return results


def write_level(
    bq: bigquery.Client,
    destination: str,
    level: Level,
    rows: list[dict[str, Any]],
    snapshot_time: datetime,
) -> None:
    """Appends one level's snapshot, every row stamped with the run's start time."""
    timestamp = snapshot_time.isoformat()
    job_config = bigquery.LoadJobConfig(
        schema=level.schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    bq.load_table_from_json(
        [{**row, "timestamp": timestamp} for row in rows],
        f"{destination}.{level.table}",
        job_config=job_config,
    ).result()


def run(
    project_ids: list[str],
    destination: str,
    projects_api: resourcemanager_v3.ProjectsClient,
    bq: bigquery.Client,
    snapshot_time: datetime,
) -> bool:
    """Collects and writes every complete level. Returns False when any level was not written."""
    results = collect(project_ids, projects_api, bq)
    all_written = True
    for level in LEVELS:
        result = results[level]
        if not result.failures and not result.rows:
            result.failures.append("no bindings were read")
        if len(result.gone) > GONE_LIMIT_PER_LEVEL:
            result.failures.append(f"{len(result.gone)} objects disappeared during the run")
        summary = {
            "level": level.table,
            "objects_read": result.read,
            "objects_gone": len(result.gone),
            "gone_objects": result.gone[:FAILURES_LOGGED_PER_LEVEL],
            "failures": len(result.failures),
            "rows": len(result.rows),
        }
        if result.failures:
            all_written = False
            logger.error(
                f"{level.table}: not written, {len(result.failures)} object(s) could not be read",
                extra={
                    "json_fields": {
                        **summary,
                        "failed_objects": result.failures[:FAILURES_LOGGED_PER_LEVEL],
                    }
                },
            )
            continue
        try:
            write_level(bq, destination, level, result.rows, snapshot_time)
        except Exception:
            all_written = False
            logger.exception(f"{level.table}: write failed", extra={"json_fields": summary})
            continue
        logger.info(f"{level.table}: wrote {len(result.rows)} rows", extra={"json_fields": summary})
    if RETRIES:
        logger.info(
            f"Retried {sum(RETRIES.values())} API call(s) after transient errors",
            extra={"json_fields": {"retries_by_error": dict(RETRIES)}},
        )
    return all_written


def project_ids_from_env(value: Optional[str]) -> list[str]:
    """`a, b,a` -> ["a", "b"]: trimmed, de-duplicated, order kept."""
    project_ids = list(dict.fromkeys(part.strip() for part in (value or "").split(",") if part.strip()))
    if not project_ids:
        raise ValueError("PROJECT_IDS is empty")
    return project_ids


def setup_logging() -> None:
    """JSON lines with a `severity` field, which Cloud Run turns into log severity for Datadog."""
    logger.handlers = [StructuredLogHandler(stream=sys.stdout)]
    logger.setLevel(logging.INFO)
    logger.propagate = False


def main() -> int:
    setup_logging()
    try:
        project_ids = project_ids_from_env(os.environ.get("PROJECT_IDS"))
        destination = os.environ.get("DESTINATION", DEFAULT_DESTINATION)
        bq = bigquery.Client(project=os.environ.get("GOOGLE_CLOUD_PROJECT"))
        projects_api = resourcemanager_v3.ProjectsClient()
        snapshot_time = datetime.now(timezone.utc)
        logger.info(f"Exporting IAM for {len(project_ids)} projects to {destination}")
        if run(project_ids, destination, projects_api, bq, snapshot_time):
            return 0
    except Exception:
        logger.exception("gcp-iam-sync failed")
    return 1


if __name__ == "__main__":
    sys.exit(main())
