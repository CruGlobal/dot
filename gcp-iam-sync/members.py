"""Turns IAM policies and BigQuery dataset access entries into export rows.

Pure functions only, so every shape can be unit tested without Google credentials.

Each level keeps the role format the export has always written. dse-dbt hashes the role into
each binding's key, so changing a level's format would end and restart every binding on it.
"""

import json
from typing import Any, Callable, Iterable, Mapping

RoleFormat = Callable[[str], str]

RESOURCE_ID_KEYS = ("projectId", "datasetId", "tableId", "routineId")


def project_role(role: str) -> str:
    """`roles/bigquery.admin` -> `bigquery.admin`; a custom role keeps only its last part."""
    return role.split("/")[-1]


def table_role(role: str) -> str:
    """`roles/bigquery.dataViewer` is kept whole; `organizations/1/roles/x` becomes `x`."""
    return role.split("/roles/")[-1]


def split_member(member: str) -> tuple[str, str]:
    """Splits an IAM member string into (type, member).

    `user:a@cru.org` -> ("user", "a@cru.org")
    `deleted:serviceAccount:x@p.iam.gserviceaccount.com?uid=1` -> ("deleted", "serviceAccount:x@...?uid=1")
    `allUsers` -> ("specialGroup", "allUsers"), the public members, which have no type prefix.
    """
    if ":" not in member:
        return "specialGroup", member
    member_type, member_id = member.split(":", 1)
    return member_type, member_id


def binding_rows(
    keys: Mapping[str, str],
    bindings: Iterable[tuple[str, Iterable[str]]],
    role_format: RoleFormat,
) -> list[dict[str, Any]]:
    """One row per (role, member) in a project or table IAM policy."""
    rows = []
    for role, members in bindings:
        for member in members:
            member_type, member_id = split_member(member)
            rows.append(
                {**keys, "role": role_format(role), "member": member_id, "type": member_type}
            )
    return rows


def _resource_name(entity_type: str, entity_id: Mapping[str, Any]) -> str:
    """`{"projectId": "p", "datasetId": "d", "tableId": "t"}` -> `p.d.t`.

    An authorized dataset nests its reference under `dataset`. An unknown shape is kept as
    sorted JSON rather than dropped.
    """
    reference = entity_id.get("dataset", entity_id) if entity_type == "dataset" else entity_id
    parts = [reference[key] for key in RESOURCE_ID_KEYS if reference.get(key)]
    return ".".join(parts) if parts else json.dumps(entity_id, sort_keys=True)


def dataset_member(entity_type: str, entity_id: Any) -> tuple[str, str]:
    """Returns (type, member) for one dataset access entry.

    Service accounts are typed by their address, including Google-managed ones such as
    `...@developer.gserviceaccount.com`, and `userByEmail` becomes `user`. Authorized views,
    datasets and routines are named as `project.dataset[.object]`.
    """
    if isinstance(entity_id, Mapping):
        return entity_type, _resource_name(entity_type, entity_id)
    member = "" if entity_id is None else str(entity_id)
    if member.endswith(".gserviceaccount.com"):
        return "serviceAccount", member
    if entity_type == "userByEmail":
        return "user", member
    return entity_type, member


def dataset_rows(project: str, dataset: str, access_entries: Iterable[Any]) -> list[dict[str, Any]]:
    """One row per access entry; `role` is kept as BigQuery returns it (None for authorized resources)."""
    rows = []
    for entry in access_entries:
        member_type, member = dataset_member(entry.entity_type, entry.entity_id)
        rows.append(
            {"project": project, "dataset": dataset, "role": entry.role, "member": member, "type": member_type}
        )
    return rows
