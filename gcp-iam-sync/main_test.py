from datetime import datetime, timezone
from types import SimpleNamespace

import google.auth.exceptions
import pytest
import requests
from google.api_core import exceptions
from google.cloud import bigquery

import main
import members

SNAPSHOT_TIME = datetime(2026, 10, 5, 2, 0, tzinfo=timezone.utc)
DESTINATION = "dest-project.el_gcp_iam"

# The columns of the three `el_gcp_iam` tables, in order, as dse-dbt reads them.
EXPECTED_COLUMNS = {
    "iam_policy_projects": ["project", "role", "member", "type", "timestamp"],
    "iam_policy_datasets": ["project", "dataset", "role", "member", "type", "timestamp"],
    "iam_policy_tables": ["project", "dataset", "table", "role", "member", "type", "timestamp"],
}


def entry(entity_type, entity_id, role="READER"):
    return bigquery.AccessEntry(role, entity_type, entity_id)


AUTHORIZED_DATASET = entry(
    "dataset",
    {"dataset": {"projectId": "lmi-sheets", "datasetId": "GlobalMaster"}, "targetTypes": ["VIEWS"]},
    role=None,
)


class FakeProjects:
    """Stands in for resourcemanager_v3.ProjectsClient."""

    def __init__(self, policies, errors=None):
        self.policies = policies
        self.errors = errors or {}
        self.requested_versions = []

    def get_iam_policy(self, request, retry=None):
        self.requested_versions.append(request["options"]["requested_policy_version"])
        project = request["resource"].split("/", 1)[1]
        if project in self.errors:
            raise self.errors[project]
        bindings = [SimpleNamespace(role=role, members=list(member_list)) for role, member_list in self.policies[project]]
        return SimpleNamespace(bindings=bindings)


class FakeBigQuery:
    """Stands in for bigquery.Client. `errors` maps (method, object id) to the exception it raises."""

    def __init__(self, datasets, tables, table_policies, errors=None, load_error=None):
        self.datasets = datasets
        self.tables = tables
        self.table_policies = table_policies
        self.errors = errors or {}
        self.load_error = load_error
        self.listed_tables = []
        self.loads = {}

    def _raise_if_configured(self, method, object_id):
        if (method, object_id) in self.errors:
            raise self.errors[(method, object_id)]

    def list_datasets(self, project, retry=None):
        self._raise_if_configured("list_datasets", project)
        return [SimpleNamespace(dataset_id=dataset_id) for dataset_id in self.datasets.get(project, {})]

    def get_dataset(self, dataset_ref, retry=None):
        self._raise_if_configured("get_dataset", dataset_ref)
        project, dataset_id = dataset_ref.split(".")
        return SimpleNamespace(access_entries=self.datasets[project][dataset_id])

    def list_tables(self, dataset_ref, retry=None):
        self._raise_if_configured("list_tables", dataset_ref)
        self.listed_tables.append(dataset_ref)
        return [SimpleNamespace(table_id=table_id) for table_id in self.tables.get(dataset_ref, [])]

    def get_iam_policy(self, table_ref, retry=None):
        self._raise_if_configured("get_iam_policy", table_ref)
        bindings = [{"role": role, "members": set(member_list)} for role, member_list in self.table_policies.get(table_ref, [])]
        return SimpleNamespace(bindings=bindings)

    def load_table_from_json(self, rows, destination, job_config=None):
        if self.load_error and destination.endswith(self.load_error):
            raise exceptions.BadRequest("load failed")
        self.loads[destination.split(".")[-1]] = SimpleNamespace(rows=list(rows), job_config=job_config)
        return SimpleNamespace(result=lambda: None)


def build_world(**overrides):
    """One project with a Global Registry-like dataset, a dbt_test__audit dataset and two tables."""
    projects = FakeProjects(
        {
            "p1": [
                ("roles/bigquery.admin", ["user:a@cru.org", "deleted:serviceAccount:old@p1.iam.gserviceaccount.com?uid=7"]),
                ("organizations/1/roles/custom", ["group:g@cru.org"]),
            ]
        },
        errors=overrides.pop("project_errors", None),
    )
    bq = FakeBigQuery(
        datasets={
            "p1": {
                "mart_registry": [
                    entry("userByEmail", "r1@cru.org"),
                    entry("userByEmail", "r2@cru.org"),
                    entry("specialGroup", "projectOwners", role="OWNER"),
                    AUTHORIZED_DATASET,
                ],
                "dbt_test__audit": [entry("userByEmail", "x@cru.org")],
            }
        },
        tables={"p1.mart_registry": ["t1", "t2", "t_no_bindings"], "p1.dbt_test__audit": ["fail_1"]},
        table_policies={
            "p1.mart_registry.t1": [("roles/bigquery.dataViewer", ["user:v@cru.org", "allUsers"])],
            "p1.mart_registry.t2": [("organizations/1/roles/custom", ["serviceAccount:svc@p1.iam.gserviceaccount.com"])],
        },
        **overrides,
    )
    return projects, bq


def run_world(projects, bq, project_ids=("p1",)):
    return main.run(list(project_ids), DESTINATION, projects, bq, SNAPSHOT_TIME)


def members_of(rows):
    return sorted((row["type"], row["member"]) for row in rows)


# --- members ---------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "member, expected",
    [
        ("user:a@cru.org", ("user", "a@cru.org")),
        ("group:g@cru.org", ("group", "g@cru.org")),
        ("domain:cru.org", ("domain", "cru.org")),
        (
            "deleted:serviceAccount:old@p.iam.gserviceaccount.com?uid=7",
            ("deleted", "serviceAccount:old@p.iam.gserviceaccount.com?uid=7"),
        ),
        ("allUsers", ("specialGroup", "allUsers")),
        ("allAuthenticatedUsers", ("specialGroup", "allAuthenticatedUsers")),
    ],
)
def test_split_member(member, expected):
    assert members.split_member(member) == expected


@pytest.mark.parametrize(
    "role, project_format, table_format",
    [
        ("roles/bigquery.dataViewer", "bigquery.dataViewer", "roles/bigquery.dataViewer"),
        ("organizations/860158542774/roles/custom", "custom", "custom"),
        ("projects/p/roles/projectCustom", "projectCustom", "projectCustom"),
    ],
)
def test_role_formats_match_the_existing_export(role, project_format, table_format):
    assert members.project_role(role) == project_format
    assert members.table_role(role) == table_format


@pytest.mark.parametrize(
    "entity_type, entity_id, expected",
    [
        ("userByEmail", "a@cru.org", ("user", "a@cru.org")),
        ("userByEmail", "svc@p.iam.gserviceaccount.com", ("serviceAccount", "svc@p.iam.gserviceaccount.com")),
        ("groupByEmail", "g@cru.org", ("groupByEmail", "g@cru.org")),
        ("domain", "cru.org", ("domain", "cru.org")),
        ("specialGroup", "projectReaders", ("specialGroup", "projectReaders")),
        ("specialGroup", "allAuthenticatedUsers", ("specialGroup", "allAuthenticatedUsers")),
        (
            "iamMember",
            "deleted:serviceAccount:old@p.iam.gserviceaccount.com?uid=7",
            ("iamMember", "deleted:serviceAccount:old@p.iam.gserviceaccount.com?uid=7"),
        ),
        ("iamMember", "allUsers", ("iamMember", "allUsers")),
        ("view", {"projectId": "p", "datasetId": "d", "tableId": "v"}, ("view", "p.d.v")),
        (
            "dataset",
            {"dataset": {"projectId": "p", "datasetId": "d"}, "targetTypes": ["VIEWS"]},
            ("dataset", "p.d"),
        ),
        ("routine", {"projectId": "p", "datasetId": "d", "routineId": "r"}, ("routine", "p.d.r")),
    ],
)
def test_dataset_rows_cover_every_access_entry_type(entity_type, entity_id, expected):
    role = None if isinstance(entity_id, dict) else "READER"
    [row] = members.dataset_rows("p", "d", [entry(entity_type, entity_id, role=role)])
    assert (row["type"], row["member"]) == expected
    assert row == {"project": "p", "dataset": "d", "role": role, "member": expected[1], "type": expected[0]}


@pytest.mark.parametrize(
    "api_entry, expected",
    [
        (
            {"role": "READER", "userByEmail": "a@cru.org", "condition": {"expression": "true", "title": "t"}},
            ("user", "a@cru.org"),
        ),
        (
            {"dataset": {"dataset": {"projectId": "p", "datasetId": "d"}, "targetTypes": ["VIEWS"]}},
            ("dataset", "p.d"),
        ),
        ({"view": {"projectId": "p", "datasetId": "d", "tableId": "v"}}, ("view", "p.d.v")),
        ({"role": "OWNER", "specialGroup": "projectOwners"}, ("specialGroup", "projectOwners")),
    ],
)
def test_entries_parsed_from_the_api_format(api_entry, expected):
    """Real reads build entries with from_api_repr, which infers the type from the keys."""
    [row] = members.dataset_rows("p", "d", [bigquery.AccessEntry.from_api_repr(api_entry)])
    assert (row["type"], row["member"]) == expected


def test_unknown_entry_shape_is_kept_not_dropped():
    assert members.dataset_member("newKind", {"foo": "bar"}) == ("newKind", '{"foo": "bar"}')
    assert members.dataset_member("newKind", None) == ("newKind", "")


def test_authorized_dataset_entry_does_not_drop_the_datasets_readers():
    """The old export dropped any dataset with an authorized-dataset entry (Global Registry)."""
    readers = [entry("userByEmail", f"reader{n}@cru.org") for n in range(11)]
    rows = members.dataset_rows("cru-data-warehouse-prod-2", "mart_global_registry_for_global", readers + [AUTHORIZED_DATASET])
    assert len(rows) == 12
    assert ("dataset", "lmi-sheets.GlobalMaster") in members_of(rows)
    assert sum(row["type"] == "user" for row in rows) == 11


def test_binding_rows():
    rows = members.binding_rows(
        {"project": "p"},
        [("roles/bigquery.admin", ["user:a@cru.org", "allUsers"])],
        members.project_role,
    )
    assert rows == [
        {"project": "p", "role": "bigquery.admin", "member": "a@cru.org", "type": "user"},
        {"project": "p", "role": "bigquery.admin", "member": "allUsers", "type": "specialGroup"},
    ]


# --- run ---------------------------------------------------------------------------------------


def test_complete_run_writes_every_level_with_the_existing_columns():
    projects, bq = build_world()

    assert run_world(projects, bq) is True

    assert set(bq.loads) == set(EXPECTED_COLUMNS)
    for table, load in bq.loads.items():
        assert [field.name for field in load.job_config.schema] == EXPECTED_COLUMNS[table]
        assert load.job_config.write_disposition == bigquery.WriteDisposition.WRITE_APPEND
        assert all(row["timestamp"] == SNAPSHOT_TIME.isoformat() for row in load.rows)

    project_rows = bq.loads["iam_policy_projects"].rows
    assert {row["role"] for row in project_rows} == {"bigquery.admin", "custom"}
    assert ("deleted", "serviceAccount:old@p1.iam.gserviceaccount.com?uid=7") in members_of(project_rows)

    dataset_rows = [row for row in bq.loads["iam_policy_datasets"].rows if row["dataset"] == "mart_registry"]
    assert members_of(dataset_rows) == [
        ("dataset", "lmi-sheets.GlobalMaster"),
        ("specialGroup", "projectOwners"),
        ("user", "r1@cru.org"),
        ("user", "r2@cru.org"),
    ]

    table_rows = bq.loads["iam_policy_tables"].rows
    assert members_of(table_rows) == [
        ("serviceAccount", "svc@p1.iam.gserviceaccount.com"),
        ("specialGroup", "allUsers"),
        ("user", "v@cru.org"),
    ]
    assert {row["role"] for row in table_rows} == {"roles/bigquery.dataViewer", "custom"}


def test_project_policies_are_read_at_version_3():
    """Version 1 would rename a conditional grant `<role>_withcond_<hash>`, unlike the old export."""
    projects, bq = build_world()
    run_world(projects, bq)
    assert projects.requested_versions == [3]


def test_dbt_test_audit_datasets_are_read_but_their_tables_are_not():
    projects, bq = build_world()
    run_world(projects, bq)
    assert "p1.dbt_test__audit" not in bq.listed_tables
    assert any(row["dataset"] == "dbt_test__audit" for row in bq.loads["iam_policy_datasets"].rows)


def test_unreadable_table_skips_only_the_table_level():
    projects, bq = build_world(errors={("get_iam_policy", "p1.mart_registry.t1"): exceptions.Forbidden("denied")})

    assert run_world(projects, bq) is False
    assert set(bq.loads) == {"iam_policy_projects", "iam_policy_datasets"}

    tables = main.collect(["p1"], projects, bq)[main.TABLES]
    assert tables.gone == []
    assert tables.rows, "t2 was read, so only the failure keeps the level from being written"
    assert tables.failures == ["table p1.mart_registry.t1: Forbidden: 403 denied"]


def test_dataset_deleted_mid_run_counts_as_gone_not_failed():
    projects, bq = build_world(
        errors={
            ("get_dataset", "p1.mart_registry"): exceptions.NotFound("gone"),
            ("list_tables", "p1.mart_registry"): exceptions.NotFound("gone"),
        }
    )

    assert run_world(projects, bq) is False  # the table level has nothing left to write
    assert "iam_policy_datasets" in bq.loads
    assert {row["dataset"] for row in bq.loads["iam_policy_datasets"].rows} == {"dbt_test__audit"}


def test_missing_configured_project_fails_every_level():
    projects, bq = build_world(
        project_errors={"p_missing": exceptions.NotFound("no project")},
        errors={("list_datasets", "p_missing"): exceptions.NotFound("no project")},
    )

    assert run_world(projects, bq, project_ids=("p1", "p_missing")) is False
    assert bq.loads == {}

    results = main.collect(["p1", "p_missing"], projects, bq)
    assert results[main.PROJECTS].rows, "p1 was read, so only the failure keeps the level from being written"
    assert results[main.PROJECTS].gone == []


def test_dataset_with_no_access_entries_fails_its_level():
    projects, bq = build_world()
    bq.datasets["p1"]["mart_registry"] = []

    assert run_world(projects, bq) is False
    assert "iam_policy_datasets" not in bq.loads
    assert main.collect(["p1"], projects, bq)[main.DATASETS].failures == [
        "dataset p1.mart_registry: returned no access entries"
    ]


def test_too_many_objects_disappearing_fails_the_level():
    projects, bq = build_world()
    many = [f"t{n}" for n in range(main.GONE_LIMIT_PER_LEVEL + 1)]
    bq.tables["p1.mart_registry"] += many
    bq.errors.update({("get_iam_policy", f"p1.mart_registry.{t}"): exceptions.NotFound("gone") for t in many})

    assert run_world(projects, bq) is False
    assert "iam_policy_tables" not in bq.loads


def test_a_few_objects_disappearing_is_tolerated_and_named():
    projects, bq = build_world(errors={("get_iam_policy", "p1.mart_registry.t_no_bindings"): exceptions.NotFound("gone")})

    assert run_world(projects, bq) is True
    assert main.collect(["p1"], projects, bq)[main.TABLES].gone == ["table p1.mart_registry.t_no_bindings"]


def test_level_with_no_rows_is_not_written():
    projects = FakeProjects({"p1": []})
    bq = FakeBigQuery(datasets={}, tables={}, table_policies={})

    assert run_world(projects, bq) is False
    assert bq.loads == {}


def test_write_failure_fails_the_run_but_other_levels_still_write():
    projects, bq = build_world(load_error="iam_policy_datasets")

    assert run_world(projects, bq) is False
    assert set(bq.loads) == {"iam_policy_projects", "iam_policy_tables"}


@pytest.mark.parametrize(
    "exc, expected",
    [
        (exceptions.TooManyRequests("slow down"), True),
        (exceptions.ServiceUnavailable("try later"), True),
        (exceptions.Forbidden("quota", errors=[{"reason": "quotaExceeded"}]), True),
        (exceptions.Forbidden("rate", errors=[{"reason": "rateLimitExceeded"}]), True),
        (exceptions.Forbidden("denied", errors=[{"reason": "accessDenied"}]), False),
        (exceptions.NotFound("missing"), False),
        (requests.exceptions.ConnectionError("reset"), True),
        (requests.exceptions.Timeout("slow"), True),
        (requests.exceptions.ChunkedEncodingError("cut off"), True),
        (google.auth.exceptions.TransportError("token refresh"), True),
    ],
)
def test_only_transient_errors_are_retried(exc, expected):
    assert main.is_transient(exc) is expected


def test_retries_are_counted_by_error_type():
    main.RETRIES.clear()
    calls = []

    def flaky():
        calls.append(1)
        if len(calls) == 1:
            raise exceptions.TooManyRequests("slow down")
        return "ok"

    assert main.RETRY.with_delay(initial=0, maximum=0)(flaky)() == "ok"
    assert main.RETRIES == {"TooManyRequests": 1}
    main.RETRIES.clear()


def test_project_ids_are_trimmed_and_deduplicated():
    assert main.project_ids_from_env(" p1, p2 ,p1,, ") == ["p1", "p2"]


@pytest.mark.parametrize("value", [None, "", " , "])
def test_empty_project_list_is_rejected(value):
    with pytest.raises(ValueError):
        main.project_ids_from_env(value)


def test_main_exits_1_without_a_project_list(monkeypatch):
    monkeypatch.delenv("PROJECT_IDS", raising=False)
    assert main.main() == 1
