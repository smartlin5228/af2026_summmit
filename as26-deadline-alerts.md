# AS26: Deadlines for DAGs — What's Shipped and What's Next

**Event:** Airflow Summit 2026

## Session summary

Airflow's **legacy SLA** feature let users set a maximum expected duration for a
DAG run and get an email when exceeded — but it was inflexible and hard to
configure. **Deadline Alerts** replaced it in **3.1** with a general-purpose
system for time-based alerting. Two release cycles since have reshaped it.

Speaker is one of the developers of Deadline Alerts.

## Version timeline

- **3.0** — legacy SLA removed (replaced by a "this is gone" notice).
- **3.1** — Deadline Alerts arrive (AIP-86), **experimental**.
- **3.2 / 3.3** — still experimental; `SyncCallback` added alongside
  `AsyncCallback` (sync runs on the *executor*, optionally a specific one;
  async runs on the *triggerer*).
- **3.4** — a lot of the substance lands here (per the speaker): supervised-
  subprocess callbacks with Connections/Variables/Assets, UI grid + run-overview
  status, named deadlines, OpenLineage events, HA-scheduler duplicate-callback
  fix, migration perf. Don't judge the feature by 3.1 — 3.4 is where it becomes
  genuinely useful. (Stable was 3.3.x around the time of the talk, so some of
  this may be very fresh / just-released.)

## What's shipped (by ~3.4)

- **Callbacks run in supervised subprocesses** with access to **Connections,
  Variables, and Assets** — so a callback can query your infrastructure and
  *respond to* problems, not just notify.
- **Deadline status visible in the UI** — Grid view and DAG run overview.
- **Named deadlines** — attach multiple alerts to one DAG for different
  stakeholders.
- **OpenLineage captures deadline events.**
- **Production-hardening fixes** — duplicate callbacks under HA schedulers;
  migration performance.

## Reactive vs. proactive — and it's already partly proactive

- **Trigger formula:** `reference datetime + interval = deadline trigger time`.
  - **Positive interval** → fires *after* the reference (reactive: "1h after
    queued and still not done").
  - **Negative interval** → fires *before* the reference (proactive: "30 min
    before this fixed need-by time"). Docs explicitly support this.
- **Reference points** available today:
  - `DAGRUN_QUEUED_AT` — from when the run was queued (resource-constraint
    monitoring).
  - `DAGRUN_LOGICAL_DATE` — from the scheduled/intended run time.
  - `FIXED_DATETIME` — an absolute wall-clock target.
  - `AVERAGE_RUNTIME` — **predictive**: computed from historical successful runs.
    This is the "warn me before it becomes a problem" case — set the deadline at
    the historical average (+ margin) and get alerted while the run is still in
    flight.
- So "deadlines are reactive, act after something's wrong" is only half true —
  negative intervals + `AVERAGE_RUNTIME` already let it fire *ahead* of a likely
  miss.
- **The payoff: an early-warning deadline + a context-aware callback = act
  before the miss.** e.g. deadline at `AVERAGE_RUNTIME − 15 min`; when it fires
  and the run isn't near done, the callback **scales workers / a pool up ahead
  of time**, or pre-warms a cluster, or bumps priority — so the run still lands
  on time instead of just paging someone once it's already late.

## What's next

- **Task-level and asset-level deadlines** — attach a deadline to an individual
  task or an asset, not just a DAG run. Still in planning/design (active
  discussion, not released).
- Possibly a **dedicated sidecar process** (triggerer-equivalent) to offload
  deadline-callback processing.
- End goal stated by the speaker: time constraints become something the
  **scheduler understands and acts on**, not just something you're alerted about
  after the fact.
- Open bug worth watching: deadline alert firing *after* a DAG failure
  (apache/airflow #60927) — semantics of "missed" vs "failed" still being
  worked out.

## My notes / observations

- **AIP-86** ("Deadline Alerts", formerly SLA) — voted through and accepted.
  SLA was *removed* in 3.0; Deadline Alerts *added* in 3.1. Old `SLAMiss` table
  replaced by a deadline/alert table keyed by dagrun.
- **The core SLA problem it fixes: "when do you start counting?"** Old SLA of
  "1 hour" was ambiguous — 1h after scheduled? queued? started? Deadline Alerts
  let the user **define the reference point** and treat the deadline as a
  calculated **"need-by" time**.
- **Legacy SLAs were evaluated inside the scheduler**; Deadline Alerts instead
  **measure at the run level** — cleaner separation from the scheduler loop.
- **What it is NOT:**
  - **Not a timeout** — it never kills anything. The run keeps going after the
    deadline passes; you just get alerted.
  - **Not a retry policy** — retries are about *failure*; deadlines are about
    *lateness*. Orthogonal concerns.
- **Visible in the UI** — deadline status shows in the Grid view and the DAG-run
  overview (not just an email like old SLAs).
- **Why it actually matters: it combines with callbacks, and the callback has
  the full context of the run when it fires.** The callback runs in a supervised
  subprocess with access to Connections, Variables, and Assets — so when a
  deadline is missed it can *inspect the run* (which tasks are done, which are
  stuck, upstream state) and *act* — page the right person, kick a remediation,
  skip/short-circuit downstream, scale something — not just emit "SLA missed".
  Old SLA callbacks were fire-and-forget notifications with almost no context.
- **Callback patterns the talk highlights:**
  - **Notify** — the basic case (same as old SLA, but richer payload).
  - **Trigger remediation** — the callback does something about it (restart a
    stuck sensor, rerun a task, scale a pool, etc.).
  - **Gather + diagnose** — on fire, the callback collects run info (task
    states, durations, logs, upstream/asset state) and **posts a diagnosis**
    rather than a bare alert — "run X is late because task Y has been queued 40m
    waiting on pool Z".
