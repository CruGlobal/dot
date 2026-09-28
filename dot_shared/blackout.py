"""Blackout-window execution gate.

Terraform publishes a blackout document into every service/job as the env var
`CRU_BLACKOUTS`; at invocation time a workload asks "is my target inside an
active window?" and skips the work if it is. Windows expire on their own (pure
time comparison), so nothing has to be un-done. Alert muting lives elsewhere
(Datadog downtimes) -- this module only gates execution and logs.

Document shape:

    {"version": 1, "windows": {"psfin-upgrade-2026-08": {
        "starts_at": "2026-08-08T18:00:00-04:00",
        "ends_at":   "2026-08-11T06:00:00-04:00",
        "reason":    "PS Financials 9.2 upgrade",
        "targets":   {"fivetran_connector": ["dearth_capably"], "dbt_job": ["33648"]}
    }}}

A window is active when `starts_at <= now < ends_at` (start inclusive, end
exclusive), compared in UTC.

FAIL OPEN: every error path -- missing env var, malformed JSON, unknown
version, unparseable timestamps, wrong shapes -- behaves as "no blackout" and
logs (WARNING for anything malformed). A broken gate must never become a silent
outage; running when we shouldn't is the acceptable failure mode. Nothing here
raises.

Stdlib only, and imported as `dot_shared.blackout` -- see dot_shared/__init__.py
for the PYTHONPATH assumption.
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone

ENV_VAR = "CRU_BLACKOUTS"
FORCE_ENV_VAR = "CRU_BLACKOUT_FORCE"
SUPPORTED_VERSION = 1
_TRUTHY = ("1", "true", "yes")

logger = logging.getLogger("primary_logger")


def check(kind, target_id, *, force=False, now=None):
    """Return the name of an active blackout window covering this target, else None.

    `kind` is an open-vocabulary target kind ("fivetran_connector", "dbt_job",
    "job"); `target_id` the id within that kind. A match is logged as
    `event=invocation_suppressed` -- that log line is the audit trail.

    `force=True` (or `CRU_BLACKOUT_FORCE` set to 1/true/yes, which is how a
    manually invoked Cloud Run job bypasses mid-window) logs the bypass and
    returns None. `now` is injectable for tests.
    """
    try:
        moment = now or datetime.now(timezone.utc)
        for window in _active_windows(moment):
            if str(target_id) not in _targets(window, kind):
                continue
            if force or _force_from_env():
                logger.warning(
                    "event=blackout_force_bypass blackout=%s kind=%s target=%s reason=%s",
                    window["name"],
                    kind,
                    target_id,
                    window["reason"],
                )
                return None
            logger.warning(
                "event=invocation_suppressed blackout=%s kind=%s target=%s reason=%s",
                window["name"],
                kind,
                target_id,
                window["reason"],
            )
            return window["name"]
        return None
    except Exception as e:  # never let the gate break the workload
        logger.warning(f"Blackout check failed, proceeding without gating: {str(e)}")
        return None


def log_status(days_ahead=7, now=None):
    """Log active windows and any starting within `days_ahead` days (boot-time)."""
    try:
        moment = now or datetime.now(timezone.utc)
        horizon = moment + timedelta(days=days_ahead)
        for window in _windows():
            if window["starts_at"] <= moment < window["ends_at"]:
                logger.warning(
                    "Blackout window active: %s (%s) until %s",
                    window["name"],
                    window["reason"],
                    window["ends_at"].isoformat(),
                )
            elif moment < window["starts_at"] <= horizon:
                days = (window["starts_at"] - moment).days
                logger.info(
                    "Upcoming blackout window in %s days: %s (%s)",
                    days,
                    window["name"],
                    window["reason"],
                )
    except Exception as e:
        logger.warning(f"Blackout status logging failed: {str(e)}")


def _force_from_env():
    return os.environ.get(FORCE_ENV_VAR, "").strip().lower() in _TRUTHY


def _active_windows(moment):
    return [w for w in _windows() if w["starts_at"] <= moment < w["ends_at"]]


def _targets(window, kind):
    values = window["targets"].get(kind)
    if values is None:
        return ()
    if not isinstance(values, list):
        logger.warning(
            f"Blackout window '{window['name']}' targets.{kind} is not a list; ignoring"
        )
        return ()
    return tuple(str(v) for v in values)


def _windows():
    """Parse `CRU_BLACKOUTS` into well-formed windows; drop (and warn about) the rest."""
    raw = os.environ.get(ENV_VAR)
    if not raw or not raw.strip():
        logger.debug(f"{ENV_VAR} not set; no blackout windows")
        return []

    try:
        document = json.loads(raw)
    except ValueError as e:
        logger.warning(f"{ENV_VAR} is not valid JSON, ignoring it: {str(e)}")
        return []

    if not isinstance(document, dict):
        logger.warning(f"{ENV_VAR} is not a JSON object, ignoring it")
        return []

    version = document.get("version")
    if version != SUPPORTED_VERSION:
        logger.warning(
            f"{ENV_VAR} has unsupported version {version!r} "
            f"(expected {SUPPORTED_VERSION}), ignoring it"
        )
        return []

    windows = document.get("windows") or {}
    if not isinstance(windows, dict):
        logger.warning(f"{ENV_VAR} 'windows' is not a JSON object, ignoring it")
        return []

    parsed = []
    for name, window in windows.items():
        if not isinstance(window, dict):
            logger.warning(f"Blackout window '{name}' is not an object; ignoring it")
            continue
        starts_at = _parse_timestamp(window.get("starts_at"), "starts_at", name)
        ends_at = _parse_timestamp(window.get("ends_at"), "ends_at", name)
        if starts_at is None or ends_at is None:
            continue
        targets = window.get("targets") or {}
        if not isinstance(targets, dict):
            logger.warning(
                f"Blackout window '{name}' targets is not an object; ignoring it"
            )
            continue
        parsed.append(
            {
                "name": name,
                "starts_at": starts_at,
                "ends_at": ends_at,
                "reason": window.get("reason", ""),
                "targets": targets,
            }
        )
    return parsed


def _parse_timestamp(value, field, window_name):
    """RFC3339 -> aware UTC datetime, or None (with a warning) if unusable."""
    if not isinstance(value, str):
        logger.warning(
            f"Blackout window '{window_name}' {field} is missing or not a string; "
            "ignoring the window"
        )
        return None
    # Python 3.12 here (see .python-version), whose fromisoformat handles a
    # trailing 'Z'; normalize anyway so the parse is version-independent.
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as e:
        logger.warning(
            f"Blackout window '{window_name}' {field} is not a parseable "
            f"timestamp ({value!r}), ignoring the window: {str(e)}"
        )
        return None
    if parsed.tzinfo is None:
        logger.warning(
            f"Blackout window '{window_name}' {field} has no UTC offset "
            f"({value!r}); assuming UTC"
        )
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
