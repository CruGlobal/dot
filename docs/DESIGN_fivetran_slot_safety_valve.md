# Design: Fivetran / RDS Replication-Slot Safety Valve

**Status:** Draft — 2026-06-17
**Origin:** Action item from the 2026-06-16 replication-slot review (Data Engineering + DevOps Engineering).
**Reference:** Datadog notebook 14785624 — "Fivetran Slot Safety Valve: Metrics, Limits & Trigger Thresholds" (owner `team:devops-engineering-team`). Companion pipeline view: notebook 14785154.
**Related:** [DT-511](https://jira.cru.org/browse/DT-511) (the `fivetran_dbt` Workflow fires the downstream dbt build on `sync_end` regardless of sync success).

---

## 1. Problem

Three production AWS RDS Postgres instances feed the warehouse through Fivetran via logical
replication. Each Fivetran `pgoutput` replication slot retains write-ahead log (WAL) on the
RDS primary until Fivetran consumes it — a sync advances the slot's `restart_lsn` and
releases the retained WAL. Between syncs, or when a connector is paused or broken, retained
WAL accumulates.

Each instance has a hard cap (`max_slot_wal_keep_size`, set in cru-terraform). When retained
WAL reaches the cap, Postgres invalidates the slot and Fivetran must perform a full
re-snapshot. That invalidation is the mechanism behind the recurring "Invalid Replication
Slot" connector breaks. The cap protects database storage (no outage), but invalidation plus
full re-sync is disruptive and costly, and it is silent until it happens.

**Goal:** automatically drain a slot — by forcing a Fivetran sync — when retained WAL
approaches the cap, before invalidation occurs; and escalate to a human when a forced sync
cannot drain it (the paused/broken-connector case). A reliable valve also makes it safe to
relax sync schedules without risking slot invalidation.

## 2. Metric, targets, and limits

### Metric

| Metric | Use |
|--------|-----|
| `aws.rds.oldest_replication_slot_lag` | **Primary** — bytes of WAL retained by the most-lagging slot (the Fivetran `pgoutput` slot). Filter by `dbinstanceidentifier`; compare against the cap. |
| `aws.rds.transaction_logs_generation` | WAL fill-rate (bytes/sec) — estimate time-to-cap. |
| `aws.rds.transaction_logs_disk_usage` | Secondary — total WAL on disk (slot + checkpoint). |

`aws.rds.oldest_logical_replication_slot_lag` (returns −1 / unreliable on these instances)
and `aws.rds.replication_slot_disk_usage` (slot state files, ~8 KB — not WAL retained) must
not be used.

The metric is in **bytes**, not a percentage; the percentage of cap is computed per
instance. The Datadog value is CloudWatch-polled and is therefore ~5–15 minutes stale, which
is acceptable at a 50%-of-cap trigger that has hours of runway.

### Targets and caps (`max_slot_wal_keep_size`, verified in cru-terraform)

| Instance (`dbinstanceidentifier`) | Cap | Cap (MB / bytes) | cru-terraform source |
|------|-----|------|------|
| `mpdx-api-prod` | 100 GiB | 102400 / 107,374,182,400 | `applications/mpdx/api/prod/locals.tf` |
| `global-registry-prod` | 75 GiB | 76800 / 80,530,636,800 | `applications/global-registry/prod/locals.tf` |
| `global-registry-flat-prod` | 75 GiB | 76800 / 80,530,636,800 | `applications/global-registry/prod/locals.tf` |

The Fivetran connector that maps to each instance must be confirmed against the Fivetran
account and DOT configuration before implementation (see Section 8).

## 3. Thresholds (percentage of each instance's cap)

| Tier | % of cap | MPDX | Global Registry (both) | Action |
|------|----------|------|------------------------|--------|
| Auto-trigger (valve) | 50% | 50 GiB | 37.5 GiB | force a Fivetran sync to drain the slot |
| Human escalation | ~70% | 70 GiB | 50 GiB | notify `@devops-engineering-team@cru.org` |
| Hard cap (invalidation) | 100% | 100 GiB | 75 GiB | slot dropped → full re-sync (the backstop) |

The valve at 50% self-heals before the 70% human monitor fires. RDS storage autoscaling and
the cap remain the ultimate backstop below all of this.

## 4. Architecture (decision: 2026-06-17)

**Decision:** a hybrid that fits DOT's existing push-not-poll architecture (see
`ARCHITECTURE.md`) and adds no new direct database access:

- **Escalation (70%):** a Datadog monitor on `aws.rds.oldest_replication_slot_lag` per
  instance, notifying `@devops-engineering-team@cru.org`. Pure monitoring, DevOps-owned;
  partially defined already in the reference notebook.
- **Valve (50%):** a Datadog monitor at 50% of cap → webhook → thin Cloud Function
  (validate + publish to Pub/Sub) → Cloud Workflow (orchestration: status check → resume →
  force-sync → escalate-on-failure, with cooldown). This is exactly the standard DOT flow.

```
RDS → CloudWatch → Datadog  (aws.rds.oldest_replication_slot_lag, by dbinstanceidentifier)
  │  datadog_monitor: lag > 50% of cap (per instance)
  ▼  webhook
Cloud Function  (validate webhook, classify instance→connector, publish to Pub/Sub, return 200)
  │
  ▼  Pub/Sub → Eventarc
Cloud Workflow  (orchestrate the drain):
     determine_sync_status(connector)
       ├─ broken/paused → update_connector(resume); if still failing → escalate, stop
       └─ healthy       → trigger_sync(connector, force=true)
     enforce cooldown / max-attempts; escalate if N consecutive fires do not reduce the slot
  ▼
Fivetran  → sync advances restart_lsn → WAL released → slot drains → monitor recovers
```

### Why this shape (and not a poller)

- DOT's documented pattern is **push-not-poll**: thin Cloud Functions publish to Pub/Sub;
  Cloud Workflows orchestrate. A scheduled poller that queries `pg_replication_slots`
  directly would contradict that pattern and would require granting DOT credentialed network
  access to three production RDS primaries — a new, sensitive surface this design avoids.
- The metric, the CloudWatch→Datadog pipeline, and the 70% escalation monitor already exist
  or are trivial to add; the valve reuses them.
- The ~5–15 minute metric staleness is immaterial at a 50%-of-cap trigger with hours of
  runway. Fill-rate (`aws.rds.transaction_logs_generation`) should be checked per instance to
  confirm runway from 50% to 100% comfortably exceeds the metric lag plus a sync duration; if
  any instance's runway is too short, raise the trigger cadence or revisit a real-time read.

## 5. Drain mechanism and existing primitives

A forced Fivetran sync reads the slot and advances `restart_lsn`, releasing retained WAL:
`POST /v1/connectors/{connector_id}/sync` with `{"force": true}`.

`fivetran-trigger/fivetran_client.py` already provides the needed primitives:

- `trigger_sync(connector_id, force=True)` — the drain.
- `determine_sync_status(connector_id)` — the broken/paused-connector guard.
- `update_connector(...)` — resume a paused connector.
- `get_connector_details(connector_id)` — state inspection.

The valve is therefore mostly orchestration around existing client code, not new Fivetran
integration.

## 6. Required guards

| Guard | Why |
|-------|-----|
| Broken/paused-connector check before sync (`determine_sync_status`) | A paused or broken connector (schema change, auth failure — the usual cause of runaway growth) will not drain on a forced sync. Attempt resume-then-sync; if the sync fails, escalate to the human monitor rather than retrying indefinitely. |
| Cooldown / max-attempts | A slot stays "full" until the drain sync completes and Postgres releases WAL. Without a cooldown the trigger re-fires every evaluation window and hammers the Fivetran API. The cooldown must exceed a typical sync duration for the connector. |
| Monitor hysteresis | The monitor's recovery threshold sits below 50% so it does not flap around the trigger. |
| Escalate after N ineffective fires | If repeated valve syncs do not reduce the slot, the real fix is a connector repair or a higher sync cadence — page a human. |
| Webhook authentication | The Cloud Function validates the Datadog webhook secret on inbound. |

## 7. Interaction with DT-511

The `fivetran_dbt` Workflow fires the downstream dbt build on `sync_end` regardless of sync
success. A valve-triggered sync emits `sync_end` and therefore fires the domain build:

- **Extra builds** each time the valve fires (cost). Acceptable if the valve is rare; if it
  fires often, consider a drain path that does not trigger the downstream build. Decide once
  the real valve frequency is observed.
- **Failed-sync fan-out:** if a valve sync fails on a broken connector, DT-511's behavior
  fires the downstream build on a failed sync. The broken-connector guard (Section 6)
  mitigates by escalating instead of repeatedly syncing, but the first failed valve sync
  would still trip DT-511. Resolving DT-511 also benefits this design.

## 8. Sync-frequency reduction (coupled change)

The 2026-06-16 review framed two coupled changes: reduce the scheduled Fivetran sync
frequency on these connectors, and add this valve. The valve is what makes a sparser schedule
safe — it catches slot growth that a less frequent schedule would otherwise let approach the
cap. Sequence: land and prove the valve first, then relax `sync_frequency`. Reducing
frequency also reduces downstream build load (the `fivetran_dbt` Workflow fires builds on
`sync_end`); confirm the new cadence still meets warehouse-freshness needs for the data these
three connectors feed (MPDX, Global Registry, Global Registry Flat).

## 9. Open items

1. Confirm the Fivetran connector that maps to each RDS instance (Section 2).
2. Reconcile the 70% escalation monitor with whatever already exists in the reference
   notebook; define the 50% valve monitors per instance.
3. Confirm where the valve orchestration lives — extend `fivetran-trigger/` (which holds the
   client) or a dedicated Workflow — consistent with the push-not-poll stack.
4. Choose the cooldown/attempt-tracking mechanism (Workflow state, a small datastore record,
   or a pre-sync `determine_sync_status` check).
5. Decide whether valve syncs should trigger downstream dbt builds (DT-511 coupling).
6. Sequence the sync-frequency reduction after the valve is proven (Section 8).
7. Per-instance fill-rate check to confirm runway from 50% to 100% comfortably exceeds metric
   lag plus sync duration (Section 4).

## 10. Implementation outline (once open items are settled)

- [ ] Confirm connector ↔ instance mapping.
- [ ] Escalation: Datadog monitor at 70% of cap per instance → `@devops-engineering-team`.
- [ ] Valve monitor: Datadog monitor at 50% of cap per instance → webhook.
- [ ] Cloud Function: validate webhook, map instance → connector, publish to Pub/Sub.
- [ ] Cloud Workflow: `determine_sync_status` → resume if paused → `trigger_sync(force=true)`
      → escalate on failure; enforce cooldown / max-attempts.
- [ ] Decide downstream-build behavior for valve syncs (DT-511).
- [ ] Test: drive a slot toward 50% (or simulate the metric) → valve fires → slot drains →
      monitor recovers; verify cooldown and broken-connector escalation.
- [ ] Runbook entry: response when the valve escalates (slot not draining).
- [ ] After the valve is proven: relax `sync_frequency` on the three connectors and confirm
      warehouse freshness holds.
