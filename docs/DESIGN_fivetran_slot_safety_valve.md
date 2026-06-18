# Design: Fivetran Replication-Slot Safety-Valve Sync (DSE mechanism)

**Status:** Draft — 2026-06-17
**Origin:** Agreed approach from the 2026-06-16 replication-slot review (Data Engineering + DevOps Engineering). This plan is the result of that discussion.
**Reference:** Datadog notebook 14785624 — "Fivetran Slot Safety Valve: Metrics, Limits & Trigger Thresholds" holds the metric, the per-instance caps, and the threshold rationale. Companion pipeline view: notebook 14785154.

---

## 1. Problem

Three production AWS RDS Postgres instances feed the warehouse through Fivetran via logical
replication. Each Fivetran `pgoutput` replication slot retains write-ahead log (WAL) on the
RDS primary until Fivetran consumes it; a sync advances the slot's `restart_lsn` and releases
the retained WAL. If retained WAL reaches the instance's `max_slot_wal_keep_size` cap,
Postgres invalidates the slot and Fivetran must perform a full re-snapshot — the mechanism
behind the recurring "Invalid Replication Slot" connector breaks. The cap protects database
storage (no outage), but invalidation plus full re-sync is disruptive, and it is silent until
it happens.

The agreed remedy is a safety valve: when a slot's retained WAL crosses a trigger threshold,
automatically force a Fivetran sync to drain it before it can approach invalidation.

## 2. Scope and ownership boundary

**This document covers only the Data Engineering piece: the mechanism that runs the Fivetran
sync when a slot crosses the trigger threshold.** The detection, thresholds, monitors, and
human escalation are owned by DevOps Engineering and handled through their standard process.

| Owned by **DevOps Engineering** (not specified here) | Owned by **Data Engineering** (this doc) |
|---|---|
| The RDS instances and the `max_slot_wal_keep_size` caps | The mechanism that, on the trigger, runs a Fivetran sync to drain the slot |
| The Datadog metric, the thresholds, and the monitors | Connector-state handling around that sync (resume / no-op / stop) |
| Human escalation and all alerting | Not mutating connector scheduling as a side effect |
| The signal/trigger that invokes the DSE mechanism | Emitting a structured failure signal for DevOps's alerting to consume |

## 3. Detection today (DevOps-owned) and the trigger-contract gap

DevOps has created three Datadog monitors on `aws.rds.oldest_replication_slot_lag`
(one per instance), notifying `@devops-engineering-team@cru.org`:

| Monitor (instance) | Cap | Warning | Critical |
|---|---|---|---|
| Production MPDX (`mpdx-api-prod`) | 100 GiB | 70% (70 GiB) | 90% (90 GiB) |
| Production Global Registry (`global-registry-prod`) | 75 GiB | 70% (52.5 GiB) | 90% (67.5 GiB) |
| Production Global Registry Flat (`global-registry-flat-prod`) | 75 GiB | 70% (52.5 GiB) | 90% (67.5 GiB) |

These are **human-escalation** monitors ("investigate and resume the connector"). Two
consequences for this design:

- **The valve needs its own trigger, below the human warning.** The valve must fire *before*
  a human is paged so it can self-heal; the notebook proposes 50% of cap. The existing
  monitors start at 70% (warning). A trigger at the valve threshold that **invokes the DSE
  mechanism** does not exist yet.
- **The trigger contract is a shared DSE-to-detection interface, not a solo task.** The
  current monitors only email DevOps; none calls into the DSE mechanism. The likely shape is
  a **dedicated Datadog monitor at the valve threshold** (the notebook's ~50% of cap, below
  the 70% human warning) whose **webhook notification** invokes the mechanism — Datadog
  supports `@webhook-…` notifications natively, so this is a well-understood piece, not an
  open research problem. The split: **detection owns the monitor and points its webhook at
  the mechanism's endpoint; DSE owns the endpoint and the payload spec** (at minimum the
  `dbinstanceidentifier`, from which the mechanism resolves the connector). What the webhook
  ultimately calls depends on the orchestration-placement decision in Section 6. Agreeing the
  endpoint + payload is a prerequisite to building.

## 4. Targets and connector mapping (verified via the Fivetran API)

| RDS instance (`dbinstanceidentifier`) | Active connector | `schedule_type` | Notes |
|------|------|------|------|
| `mpdx-api-prod` | `loft_unabashed` (`el_mpdx`) | manual (DOT-scheduled) | dead twin `enter_incredulity` (auto, paused) exists |
| `global-registry-flat-prod` | `freebee_tuberculosis` (`el_global_registry_flat`) | manual (DOT-scheduled) | dead twin `quicken_wow` (auto, paused) exists |
| `global-registry-prod` | `centralized_mitigation` (`el_global_registry`) | **auto (Fivetran-native)** | needs migration to DOT (Section 8); dead twin `dawdler_managing` (auto, paused) exists |

The instance-to-connector map must be a hard-coded, reviewed table in the mechanism's config
(mirroring `connector_to_dbt_mapping`), not inferred at runtime — multiple connectors share a
schema, and only the active one is the target.

## 5. The DSE mechanism

The mechanism follows DOT's push pattern (`ARCHITECTURE.md`): a thin Cloud Function validates
the inbound trigger and publishes to Pub/Sub; a Cloud Workflow performs the orchestration. It
reuses the existing `fivetran-trigger/fivetran_client.py` primitives. It does **not** poll for
slot fullness — detection pushes the trigger in. There is no DSE-side database access to the
RDS primaries.

### Drain orchestration (state machine)

On each trigger, the Workflow inspects connector state via `fivetran_client`
(`determine_sync_status`, `get_connector_details`) and branches:

| Connector state | Action |
|---|---|
| A sync is **already running** | **No-op.** A drain is already in progress; firing again would stack redundant syncs (handles cooldown and duplicate triggers from monitor re-fire / at-least-once Pub/Sub delivery). |
| **Paused** | **Resume** (`update_connector(paused=False)`), then force-sync. |
| **Broken** (auth / schema failure, not merely paused) | **Stop.** A force-sync will not drain a broken connector. Emit a structured failure signal and let DevOps's escalation handle continued growth. Do not attempt a futile sync or a repair. |
| **Healthy** | **Force-sync** via `trigger_sync(connector_id, force=True)`. |

Two required properties:

- **Schedule-neutral.** The mechanism's only deliberate connector mutation is resuming a
  paused connector. It must **not** set `schedule_type` as a side effect. (The existing
  `fivetran-trigger` Cloud Function sets `schedule_type: "manual"` on every call — see
  Section 6, which is why reusing it as-is is one of the options under review.)
- **A forced sync is not proof of a drain.** A `200` from the force call does not guarantee
  WAL was released — a sync already in flight, or a long historical re-sync, may not advance
  `restart_lsn` immediately. The mechanism confirms only that the sync was accepted; whether
  the slot actually fell is confirmed by DevOps's metric (their monitor re-fires if it did
  not drain). The mechanism does not re-implement metric-watching.

### Drain endpoint

`fivetran_client.trigger_sync(connector_id, force=True)` issues
`POST /v1/connectors/{connector_id}/force` with body `{"force": true}`. The client already
provides `trigger_sync`, `determine_sync_status`, `update_connector`, `get_connector_details`.

## 6. Orchestration placement — options for review

Where the drain orchestration lives is the central design decision, and the one this draft
most wants feedback on. DOT already triggers syncs through the `fivetran-trigger` Cloud
Function: Cloud Scheduler invokes it on a cron with a `connector_id`, and it sets
`schedule_type: "manual"` and force-syncs. The valve is a *different trigger source*
(event-driven from detection, not cron) and needs *guards* the scheduled path does not. Three
options:

### Option A — Valve calls the existing `fivetran-trigger` Cloud Function
The detection webhook POSTs a `connector_id` to the same HTTP function the scheduler uses.
- **Pros:** essentially no new code; the valve is just another caller; `manual` is already
  correct for DOT-scheduled targets.
- **Cons:** that function is intentionally simple — no broken/paused guard, no
  already-syncing check, no resume/stop logic (Section 5). It force-syncs and sets `manual`
  *unconditionally*, which is wrong for any connector not yet migrated to DOT scheduling.
  Adding the guards means bloating the shared scheduled-trigger path.

### Option B — Dedicated guarded valve component
A thin Cloud Function (validate the trigger) → Pub/Sub → a new Cloud Workflow that runs the
Section 5 state machine, reusing `fivetran_client`, separate from the scheduled trigger.
- **Pros:** clean separation; implements the guards properly; the valve's *conditional*
  semantics differ from the scheduler's *fire-and-forget*; shippable now, independent of the
  broader scheduling migration; fits the existing webhook→Pub/Sub→Workflow pattern
  (`fivetran-webhook` already does webhook→Pub/Sub).
- **Cons:** a new component; two sync-trigger paths in DOT until/unless they converge.

### Option C — Unified Fivetran orchestrator
Evolve `fivetran-trigger` into one component handling scheduled *and* event-driven syncs with
shared guards, as part of the "all connectors DOT-scheduled" direction (Section 8).
- **Pros:** strategically coherent — addresses the valve and the scheduling migration
  together; one home for all Fivetran sync logic with guards; the right place for "set
  `manual` conditionally," and the natural home for the recency-skip "adaptive cadence" in
  Section 7 (which needs the same last-sync state-check shared across the scheduled and valve
  paths).
- **Cons:** much larger scope; couples the (contained) valve to a broader refactor; needs the
  most design and review time.

**Open decision:** ship **B now and converge to C later** (the valve Workflow becomes a
building block of the unified orchestrator), or commit to **C from the start**. Option A is
viable only if the guards in Section 5 are judged unnecessary. This is the decision to settle
in review.

## 7. Coupled change: sync-frequency reduction (later)

The 2026-06-16 review also agreed to reduce these connectors' scheduled sync frequency once
the valve is in place — a sparser schedule is safe precisely because the valve catches slot
growth between syncs. Sequence: land and prove the valve first, then relax `sync_frequency`.

Headroom is large, so this is low-risk: source-freshness tolerances are wide (MPDX
`warn 1d / error 10d`; Global Registry and Global Registry Flat `warn 7d / error 14d`), the
connectors currently sync hourly, and the downstream warehouse builds run on their own
schedules. Downstream dbt builds are intentionally decoupled from Fivetran `sync_end` (they
run on independent schedules to control BigQuery cost), so valve-triggered syncs do not fan
out extra builds. The safe minimum sync frequency is bounded by each connector's downstream
build time, not by the current hourly cadence.

**Adaptive cadence (optional enhancement).** A valve-triggered sync also refreshes the data,
so a scheduled sync that fires shortly after is redundant. The scheduled trigger can be made
*recency-aware*: before syncing, check the connector's last successful sync (`fivetran_client`
already exposes it) and skip if it occurred within the interval — e.g. a valve sync at 9:50
makes the 10:00 scheduled tick a no-op. This is the same last-sync state-check the valve's
in-progress guard uses, applied to the scheduled path, giving a self-trimming cadence (sync on
schedule *unless* a sync — scheduled or valve — already ran recently) rather than literally
rescheduling the cron, which Cloud Scheduler does not support cleanly. It is an optimization,
not load-bearing — a redundant incremental sync is cheap and downstream builds are decoupled —
and because it wants the recency logic shared across the scheduled and valve paths, it favors
the unified orchestrator (Section 6, Option C). The skip window should be tied to the interval
(skip the immediately-following tick, not one that is legitimately due).

## 8. Coupled cleanup: migrate connectors to DOT scheduling

Principle: **all Fivetran connectors should be DOT-scheduled** (`schedule_type: "manual"`,
triggered by DOT via `fivetran-trigger`) rather than Fivetran-native (`auto`). DOT scheduling
is also what lets the valve drain a connector without fighting a native schedule.

A connector listed in the `fivetran_trigger` schedule block (cru-terraform
`applications/data-warehouse/dot/prod/functions.tf`) is DOT-scheduled; the rest are
Fivetran-native. The valve's `global-registry-prod` target (`centralized_mitigation`) is one
such case. Audit of active `postgres_rds` connectors still on `auto` scheduling (migration
candidates):

- `centralized_mitigation` — `el_global_registry` (the valve's gr-prod target)
- `chairmanship_bestowing` — `el_staff_accounting`
- `committee_persisting`, `define_uncooked` — `el_ministry_managed_domains`
- `communal_whoops`, `crossing_accidental` — `el_ert`
- `entrench_security` — `el_summer_missions`
- `furniture_magnanimous` — `el_staff_accounting_uat`
- `hesitate_fret` — `el_cap`

(The full principle extends to non-Postgres connectors too; the list above is the
slot-relevant subset. Paused/dead `auto` connectors — e.g. `dawdler_managing`,
`enter_incredulity`, `quicken_wow` — are not migration targets.)

This is a separate workstream that the valve depends on for `global-registry-prod`: that
connector must be migrated to DOT scheduling (add to the `fivetran_trigger` block, set
`manual`) before the valve can drain it cleanly.

## 9. Open items / decisions for review

1. **Orchestration placement (Section 6):** B-now-converge-to-C vs. C-from-the-start. The
   central decision.
2. **Trigger contract (Section 3):** define the valve trigger (a 50%-of-cap monitor or a
   webhook on an existing monitor) and the payload it sends to the mechanism.
3. **Connector scheduling migration (Section 8):** migrate `centralized_mitigation` (and the
   wider `auto` list) to DOT scheduling; sequence relative to the valve.
4. **Frequency reduction (Section 7):** sequence after the valve is proven.
5. **Adaptive cadence (Section 7):** optional recency-skip so a scheduled sync no-ops when the
   valve (or a recent scheduled run) already synced. An optimization; favors Option C (shared
   recency logic across the scheduled and valve paths).

## 10. Implementation outline (DSE scope)

- [ ] Hard-coded, reviewed instance → connector_id map (Section 4).
- [ ] Agree the trigger contract with detection (Section 3).
- [ ] Cloud Function: validate the trigger, map instance → connector, publish to Pub/Sub.
- [ ] Cloud Workflow: the Section 5 state machine (no-op / resume+sync / stop+signal /
      force-sync); schedule-neutral; emit a structured failure signal on the broken path.
- [ ] Test: drive a slot toward the trigger (or simulate it) → mechanism force-syncs → slot
      drains; verify the already-syncing, paused-resume, and broken-stop branches.
- [ ] Runbook note (DSE side): what the broken-connector failure signal means and how to act.
- [ ] Migrate `global-registry-prod` (`centralized_mitigation`) to DOT scheduling (Section 8).
- [ ] After the valve is proven: relax `sync_frequency` on the connectors (Section 7).
