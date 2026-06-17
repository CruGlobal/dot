# Design: Fivetran Replication-Slot Safety-Valve Sync (DSE mechanism)

**Status:** Draft — 2026-06-17
**Origin:** Agreed approach from the 2026-06-16 replication-slot review (Data Engineering + DevOps Engineering). This plan is the result of that discussion.
**Reference:** Datadog notebook 14785624 — "Fivetran Slot Safety Valve: Metrics, Limits & Trigger Thresholds" (owner `team:devops-engineering-team`) holds the metric, the per-instance caps, and the 50% / 70% / 100% thresholds. Companion pipeline view: notebook 14785154.

---

## 1. Scope and ownership boundary

Three production AWS RDS Postgres instances feed the warehouse through Fivetran via logical
replication. Each Fivetran `pgoutput` replication slot retains write-ahead log (WAL) on the
RDS primary until Fivetran consumes it; a sync advances the slot's `restart_lsn` and releases
the retained WAL. If retained WAL reaches the instance's `max_slot_wal_keep_size` cap,
Postgres invalidates the slot and Fivetran must perform a full re-snapshot — the mechanism
behind the recurring "Invalid Replication Slot" connector breaks. The cap protects database
storage (no outage), but invalidation plus full re-sync is disruptive, and it is silent until
it happens.

The agreed remedy is a safety valve: when retained WAL reaches **50% of an instance's cap**,
automatically force a Fivetran sync to drain the slot before it can approach invalidation.

**This document covers only the Data Engineering piece: the mechanism that runs the Fivetran
sync when a slot reaches 50%.** Everything else is owned by DevOps Engineering and handled
through their standard process:

| Owned by **DevOps Engineering** (not specified here) | Owned by **Data Engineering** (this doc) |
|---|---|
| The RDS instances and the `max_slot_wal_keep_size` caps | The mechanism that, on the 50% trigger, runs a Fivetran sync to drain the slot |
| The Datadog metric, the 50% / 70% / 100% thresholds, and the monitors | Connector-state handling around that sync (resume / no-op / stop) |
| Detection at 50% and the call to the DSE mechanism | Not mutating connector scheduling as a side effect |
| Human escalation (70%) and all alerting | Emitting a structured failure signal for DevOps's alerting to consume |

DevOps's monitor detects the 50% threshold and invokes the DSE mechanism. The mechanism's job
is to drain the slot via Fivetran; it does not own detection, thresholds, or escalation.

## 2. Targets

| RDS instance (`dbinstanceidentifier`) | Fivetran connector | Source (cru-terraform) |
|------|------|------|
| `mpdx-api-prod` | `loft_unabashed` | `applications/mpdx/api/prod/locals.tf` |
| `global-registry-flat-prod` | `freebee_tuberculosis` | `applications/global-registry/prod/locals.tf` |
| `global-registry-prod` | **to confirm** (see Section 7) | `applications/global-registry/prod/locals.tf` |

The instance-to-connector map must be a hard-coded, reviewed table in the mechanism's config
(mirroring `connector_to_dbt_mapping`), not inferred at runtime. The metric, caps, and
thresholds live in notebook 14785624 and are DevOps-owned — not reproduced here.

## 3. Architecture

The mechanism follows DOT's standard push pattern (`ARCHITECTURE.md`): a thin Cloud Function
validates the inbound trigger and publishes to Pub/Sub; a Cloud Workflow performs the
orchestration. It reuses the existing `fivetran-trigger/fivetran_client.py` primitives.

```
DevOps Datadog monitor (slot at 50% of cap)  →  webhook
  │
  ▼
Cloud Function  (validate the webhook secret, map instance → connector_id, publish to Pub/Sub, return 200)
  │
  ▼  Pub/Sub → Eventarc
Cloud Workflow  (drain orchestration — see Section 4)
  │
  ▼
Fivetran  →  sync advances restart_lsn  →  WAL released  →  slot drains
```

The mechanism does **not** poll for slot fullness — DevOps's monitor detects the threshold and
pushes the trigger in. There is no DSE-side database access to the RDS primaries.

## 4. Drain orchestration (the state machine)

On each trigger, the Workflow inspects connector state via `fivetran_client`
(`determine_sync_status`, `get_connector_details`) and branches:

| Connector state | Action |
|---|---|
| A sync is **already running** | **No-op.** A drain is already in progress; firing again would stack redundant syncs (handles both cooldown and duplicate triggers from monitor re-fire / at-least-once Pub/Sub delivery). |
| **Paused** | **Resume** (`update_connector(paused=False)`), then force-sync. A paused connector cannot accept an API-triggered sync. |
| **Broken** (auth / schema failure — not merely paused) | **Stop.** A force-sync will not drain a broken connector. Emit a structured failure signal and let DevOps's escalation handle continued slot growth. Do not attempt a futile sync or a repair. |
| **Healthy** | **Force-sync** via `trigger_sync(connector_id, force=True)`. |

Two properties this guarantees:

- **Schedule-neutral.** The mechanism calls `trigger_sync` directly and must **not** route
  through the existing `fivetran-trigger` Cloud Function entry point, which sets
  `schedule_type: "manual"` on every invocation. Mutating the schedule as a side effect would
  undermine the connectors' native scheduling (and the frequency change in Section 6). The
  only connector mutation the mechanism performs is resuming a paused connector.
- **A forced sync is not proof of a drain.** A `200` from the force call does not guarantee
  WAL was released — a sync already in flight, or a long historical re-sync (e.g. on
  history-mode tables), may not advance `restart_lsn` immediately. The mechanism confirms only
  that the sync was **accepted/initiated**; whether the slot actually fell is confirmed by
  **DevOps's metric** (their monitor simply re-fires if it did not drain). The mechanism does
  not re-implement metric-watching.

## 5. Drain endpoint

`fivetran_client.trigger_sync(connector_id, force=True)` issues
`POST /v1/connectors/{connector_id}/force` with body `{"force": true}`. The client already
provides every primitive the mechanism needs: `trigger_sync`, `determine_sync_status`,
`update_connector`, `get_connector_details`.

## 6. Coupled change: sync-frequency reduction (later, not part of the valve)

The 2026-06-16 review also agreed to reduce these connectors' scheduled sync frequency once
the valve is in place — a sparser schedule is safe precisely because the valve catches slot
growth between syncs. Sequence: land and prove the valve first, then relax `sync_frequency`
(via fivetran-as-code or a connector PATCH).

Headroom is large, so this is low-risk: source-freshness tolerances are wide (MPDX
`warn 1d / error 10d`; Global Registry and Global Registry Flat `warn 7d / error 14d`), the
connectors currently sync hourly, and the downstream warehouse builds run on their own
schedules. Downstream dbt builds are intentionally **decoupled** from Fivetran `sync_end`
(they run on independent schedules to control BigQuery cost), so valve-triggered syncs do not
fan out extra builds, and the `fivetran_dbt`/`sync_end` interaction (DT-511) does not apply
here. The safe minimum sync frequency is therefore bounded by each connector's downstream
build time, not by the current hourly cadence.

## 7. Open items / to-dos

1. **Confirm `global-registry-prod`'s Fivetran connector.** Only `mpdx-api-prod`
   (`loft_unabashed`) and `global-registry-flat-prod` (`freebee_tuberculosis`) are wired in
   the DOT scheduler today; `global-registry-prod` has no connector mapped there. Locating /
   confirming it is a cleanup to-do, not a design blocker.
2. Decide where the orchestration lives — extend `fivetran-trigger/` (which holds the client)
   or a dedicated Workflow — consistent with the push pattern and schedule-neutrality.
3. Sequence the sync-frequency reduction after the valve is proven (Section 6).

## 8. Implementation outline (DSE scope)

- [ ] Hard-coded, reviewed instance → connector_id map (confirm `global-registry-prod`).
- [ ] Cloud Function: validate the webhook secret, map instance → connector, publish to Pub/Sub.
- [ ] Cloud Workflow drain orchestration (Section 4): state check → no-op / resume+sync / stop / force-sync; schedule-neutral; emit a structured failure signal on the broken-connector path for DevOps's alerting.
- [ ] Confirm with DevOps the trigger contract (webhook payload + secret) their 50% monitor will send.
- [ ] Test: drive a slot toward 50% (or simulate the trigger) → mechanism force-syncs → slot drains; verify the no-op-while-syncing, paused-resume, and broken-connector-stop branches.
- [ ] Runbook note (DSE side): what the broken-connector failure signal means and how to act on it.
- [ ] After the valve is proven: relax `sync_frequency` on the connectors (Section 6).
