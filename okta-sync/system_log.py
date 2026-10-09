"""
Incremental sync of the Okta System Log (sign-in events) to BigQuery.

Unlike the directory tables, which are fully replaced on every run, the System Log
is an append-only event stream that Okta keeps for only 90 days. Each run asks Okta
for events newer than the last one already loaded (minus a small overlap for events
that land late), stages them in temp_okta, and MERGEs new uuids into
el_okta.okta_system_log, which keeps history beyond Okta's retention window.
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional

import pandas as pd
import requests

from okta_sync_utils import (
    get_general_credentials,
    get_request,
    query_bigquery_as_dataframe,
    upload_dataframe_to_bigquery,
)

SYSTEM_LOG_URL = "https://signon.okta.com/api/v1/logs"
SYSTEM_LOG_TABLE = "okta_system_log"
SYSTEM_LOG_STAGING_TABLE = "okta_system_log_staging"
SYSTEM_LOG_EVENT_TYPES = ("user.authentication.sso", "user.session.start")
SYSTEM_LOG_RETENTION = timedelta(days=90)
SYSTEM_LOG_RETENTION_MARGIN = timedelta(hours=1)
SYSTEM_LOG_LATE_ARRIVAL_OVERLAP = timedelta(hours=1)
SYSTEM_LOG_PAGE_LIMIT = 1000
SYSTEM_LOG_PAGES_PER_BATCH = 50
BQ_SECRET_NAME = "CRU_DATA_WAREHOUSE_ELT_PROD"

SYSTEM_LOG_SCHEMA: List[Dict[str, str]] = [
    {"name": "uuid", "type": "STRING", "mode": "NULLABLE"},
    {"name": "published", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "eventType", "type": "STRING", "mode": "NULLABLE"},
    {"name": "legacyEventType", "type": "STRING", "mode": "NULLABLE"},
    {"name": "version", "type": "STRING", "mode": "NULLABLE"},
    {"name": "severity", "type": "STRING", "mode": "NULLABLE"},
    {"name": "displayMessage", "type": "STRING", "mode": "NULLABLE"},
    {"name": "outcome_result", "type": "STRING", "mode": "NULLABLE"},
    {"name": "outcome_reason", "type": "STRING", "mode": "NULLABLE"},
    {"name": "actor_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "actor_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "actor_alternateId", "type": "STRING", "mode": "NULLABLE"},
    {"name": "actor_displayName", "type": "STRING", "mode": "NULLABLE"},
    {"name": "client_ipAddress", "type": "STRING", "mode": "NULLABLE"},
    {"name": "client_device", "type": "STRING", "mode": "NULLABLE"},
    {"name": "client_zone", "type": "STRING", "mode": "NULLABLE"},
    {"name": "client_userAgent_browser", "type": "STRING", "mode": "NULLABLE"},
    {"name": "client_userAgent_os", "type": "STRING", "mode": "NULLABLE"},
    {"name": "client_userAgent_rawUserAgent", "type": "STRING", "mode": "NULLABLE"},
    {"name": "client_geographicalContext_country", "type": "STRING", "mode": "NULLABLE"},
    {"name": "client_geographicalContext_state", "type": "STRING", "mode": "NULLABLE"},
    {"name": "client_geographicalContext_city", "type": "STRING", "mode": "NULLABLE"},
    {"name": "authenticationContext_externalSessionId", "type": "STRING", "mode": "NULLABLE"},
    {"name": "authenticationContext_credentialType", "type": "STRING", "mode": "NULLABLE"},
    {"name": "transaction_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "target", "type": "STRING", "mode": "NULLABLE"},
    {"name": "event_json", "type": "STRING", "mode": "NULLABLE"},
    {"name": "_loaded_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
]
SYSTEM_LOG_COLUMNS = [field["name"] for field in SYSTEM_LOG_SCHEMA]


def format_okta_timestamp(value: datetime) -> str:
    """Formats a timezone-aware datetime the way the Okta API expects (ISO 8601, UTC, ms)."""
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + (
        f"{value.astimezone(timezone.utc).microsecond // 1000:03d}Z"
    )


def build_event_filter(event_types: tuple = SYSTEM_LOG_EVENT_TYPES) -> str:
    """Builds the Okta SCIM filter expression for the event types to load."""
    return " or ".join(f'eventType eq "{event_type}"' for event_type in event_types)


def get_window_start(last_published: Optional[datetime], now: datetime) -> datetime:
    """
    Returns the `since` value for this run.

    On the first run (no table yet) this is the oldest event Okta still keeps. Later runs
    start an hour before the newest loaded event so late-arriving events are not missed;
    the MERGE on uuid drops the overlap. The result is never older than Okta's retention.
    """
    oldest_available = now - SYSTEM_LOG_RETENTION + SYSTEM_LOG_RETENTION_MARGIN
    if last_published is None:
        return oldest_available
    return max(last_published - SYSTEM_LOG_LATE_ARRIVAL_OVERLAP, oldest_available)


def flatten_event(event: Dict[str, Any], loaded_at: datetime) -> Dict[str, Any]:
    """Flattens one System Log event into the okta_system_log columns, keeping the full JSON."""
    actor = event.get("actor") or {}
    client = event.get("client") or {}
    user_agent = client.get("userAgent") or {}
    geo = client.get("geographicalContext") or {}
    outcome = event.get("outcome") or {}
    auth = event.get("authenticationContext") or {}
    transaction = event.get("transaction") or {}
    return {
        "uuid": event.get("uuid"),
        "published": event.get("published"),
        "eventType": event.get("eventType"),
        "legacyEventType": event.get("legacyEventType"),
        "version": event.get("version"),
        "severity": event.get("severity"),
        "displayMessage": event.get("displayMessage"),
        "outcome_result": outcome.get("result"),
        "outcome_reason": outcome.get("reason"),
        "actor_id": actor.get("id"),
        "actor_type": actor.get("type"),
        "actor_alternateId": actor.get("alternateId"),
        "actor_displayName": actor.get("displayName"),
        "client_ipAddress": client.get("ipAddress"),
        "client_device": client.get("device"),
        "client_zone": client.get("zone"),
        "client_userAgent_browser": user_agent.get("browser"),
        "client_userAgent_os": user_agent.get("os"),
        "client_userAgent_rawUserAgent": user_agent.get("rawUserAgent"),
        "client_geographicalContext_country": geo.get("country"),
        "client_geographicalContext_state": geo.get("state"),
        "client_geographicalContext_city": geo.get("city"),
        "authenticationContext_externalSessionId": auth.get("externalSessionId"),
        "authenticationContext_credentialType": auth.get("credentialType"),
        "transaction_id": transaction.get("id"),
        "target": json.dumps(event.get("target")) if event.get("target") is not None else None,
        "event_json": json.dumps(event, sort_keys=True),
        "_loaded_at": loaded_at,
    }


def events_to_dataframe(events: List[Dict[str, Any]], loaded_at: datetime) -> pd.DataFrame:
    """Builds a DataFrame with the okta_system_log columns and types from raw events."""
    df = pd.DataFrame([flatten_event(event, loaded_at) for event in events], columns=SYSTEM_LOG_COLUMNS)
    for field in SYSTEM_LOG_SCHEMA:
        if field["type"] == "TIMESTAMP":
            df[field["name"]] = pd.to_datetime(df[field["name"]], utc=True)
        else:
            df[field["name"]] = df[field["name"]].astype("string")
    return df


def get_system_log_pages(
    headers: Dict[str, str], since: datetime, until: datetime
) -> Iterator[List[Dict[str, Any]]]:
    """
    Yields pages of System Log events between `since` and `until`, oldest first.

    `until` is required: without it Okta treats the request as a polling request and
    always returns a `next` link, so paging would never end. Paging stops early if a
    page cannot be fetched; because pages are in ascending order, what was already
    yielded is still a gap-free prefix and the next run resumes after it.
    """
    logger = logging.getLogger("primary_logger")
    params: Dict[Any, Any] = {
        "since": format_okta_timestamp(since),
        "until": format_okta_timestamp(until),
        "filter": build_event_filter(),
        "sortOrder": "ASCENDING",
        "limit": SYSTEM_LOG_PAGE_LIMIT,
    }
    url = SYSTEM_LOG_URL
    page = 0
    while True:
        page += 1
        logger.info(f"Downloading okta_system_log page {page}")
        r = get_request(url, headers, params)
        if not isinstance(r, requests.Response):
            logger.error(f"Failed to fetch okta_system_log page {page}. Stopping this run early.")
            return
        events = r.json()
        if events:
            yield events
        if "next" not in r.links or not events:
            return
        url = r.links["next"]["url"]
        params = {None: None}


def target_table_exists(project_id: str, dataset_id: str) -> bool:
    """Returns True when el_okta.okta_system_log already exists."""
    qry = f"""
    select table_name
    from `{project_id}.{dataset_id}.INFORMATION_SCHEMA.TABLES`
    where table_name = '{SYSTEM_LOG_TABLE}'
    """
    df = query_bigquery_as_dataframe(qry, BQ_SECRET_NAME)
    if df is None:
        raise RuntimeError("Could not check whether okta_system_log exists.")
    return not df.empty


def get_last_published(project_id: str, dataset_id: str) -> Optional[datetime]:
    """Returns the newest `published` already loaded, or None when there is nothing yet."""
    if not target_table_exists(project_id, dataset_id):
        return None
    qry = f"select max(published) as last_published from `{project_id}.{dataset_id}.{SYSTEM_LOG_TABLE}`"
    df = query_bigquery_as_dataframe(qry, BQ_SECRET_NAME)
    if df is None:
        raise RuntimeError("Could not read the newest published time from okta_system_log.")
    value = df["last_published"].iloc[0] if not df.empty else None
    if value is None or pd.isna(value):
        return None
    return pd.Timestamp(value).tz_convert("UTC").to_pydatetime()


def create_target_table_sql(project_id: str, dataset_id: str) -> str:
    """DDL for el_okta.okta_system_log: partitioned by day of `published`, clustered for sign-in queries."""
    columns = ",\n        ".join(f"`{field['name']}` {field['type']}" for field in SYSTEM_LOG_SCHEMA)
    return f"""
    create table if not exists `{project_id}.{dataset_id}.{SYSTEM_LOG_TABLE}` (
        {columns}
    )
    partition by date(published)
    cluster by eventType, actor_id
    options (
        description = "Okta System Log sign-in events (user.authentication.sso, user.session.start), appended by the okta-sync job. Kept beyond Okta's 90-day retention. event_json holds the full event; target is the event's target array as JSON."
    )
    """


def merge_staging_sql(project_id: str, staging_dataset_id: str, target_dataset_id: str, since: datetime) -> str:
    """MERGE that inserts staged events whose uuid is not already in the target table."""
    column_list = ", ".join(f"`{name}`" for name in SYSTEM_LOG_COLUMNS)
    since_literal = since.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return f"""
    merge `{project_id}.{target_dataset_id}.{SYSTEM_LOG_TABLE}` as target
    using (
        select *
        from `{project_id}.{staging_dataset_id}.{SYSTEM_LOG_STAGING_TABLE}`
        where uuid is not null
        qualify row_number() over (partition by uuid order by published) = 1
    ) as staged
    on target.uuid = staged.uuid
        and target.published >= timestamp('{since_literal}')
    when not matched then
        insert ({column_list})
        values ({column_list})
    """


def sync_system_log(project_id: str, staging_dataset_id: str, target_dataset_id: str, now: Optional[datetime] = None) -> int:
    """
    Loads new Okta System Log sign-in events into el_okta.okta_system_log.

    Returns the number of events staged this run (before de-duplication against the target).
    """
    logger = logging.getLogger("primary_logger")
    now = now or datetime.now(timezone.utc)
    token = get_general_credentials("OKTA_TOKEN")
    headers = {
        "Authorization": f"{token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    last_published = get_last_published(project_id, target_dataset_id)
    since = get_window_start(last_published, now)
    logger.info(
        f"Syncing okta_system_log from {format_okta_timestamp(since)} to {format_okta_timestamp(now)} "
        f"(last loaded event: {last_published.isoformat() if last_published else 'none'})"
    )

    staged = 0
    first_write = True
    batch: List[Dict[str, Any]] = []
    pages_in_batch = 0

    def stage(events: List[Dict[str, Any]]) -> None:
        nonlocal first_write, staged
        df = events_to_dataframe(events, now)
        upload_dataframe_to_bigquery(
            project_id,
            staging_dataset_id,
            SYSTEM_LOG_STAGING_TABLE,
            BQ_SECRET_NAME,
            df,
            SYSTEM_LOG_SCHEMA,
            write_disposition="WRITE_TRUNCATE" if first_write else "WRITE_APPEND",
        )
        first_write = False
        staged += len(df)
        logger.info(f"Staged {len(df)} okta_system_log events ({staged} so far)")

    for events in get_system_log_pages(headers, since, now):
        batch.extend(events)
        pages_in_batch += 1
        if pages_in_batch >= SYSTEM_LOG_PAGES_PER_BATCH:
            stage(batch)
            batch, pages_in_batch = [], 0
    if batch:
        stage(batch)

    if staged == 0:
        logger.info("No new okta_system_log events to load.")
        return 0

    if query_bigquery_as_dataframe(create_target_table_sql(project_id, target_dataset_id), BQ_SECRET_NAME) is None:
        raise RuntimeError("Could not create okta_system_log.")
    if query_bigquery_as_dataframe(merge_staging_sql(project_id, staging_dataset_id, target_dataset_id, since), BQ_SECRET_NAME) is None:
        raise RuntimeError("Could not merge staged events into okta_system_log.")
    logger.info(f"Merged {staged} staged events into {target_dataset_id}.{SYSTEM_LOG_TABLE}")
    return staged
