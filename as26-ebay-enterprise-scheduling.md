# AS26: Extending Apache Airflow for Enterprise-Grade Scheduling at eBay

**Event:** Airflow Summit 2026 · Day 3

## Context

- eBay business teams depend on advanced workflow-control primitives daily.
- Built natively into their **Airflow 2.10** platform (note: 2.x, not 3.x).
- Airflow doesn't support these OOTB.

## The four primitives they built

1. **Breakpoints** — pause a pipeline at a specific task for inspection
   **without failing the run**.
2. **Skip logic** — dynamically bypass tasks or task groups **at runtime**.
3. **Calendar-aware scheduling** — model **business calendars** as custom Airflow
   **timetables**.
4. **Pipeline pause/resume** — operator-triggered suspension of **in-flight DAG
   runs**, with **state consistency**.

## Talk covers

- Engineering trade-offs
- Architectural constraints they hit
- Patterns reusable beyond eBay's stack

## My notes / observations

### Custom scheduling tailored to business needs

Their scheduling layer adds business-oriented controls on top of plain cron:

- **Earliest start time** — a run may be *scheduled* by cron but must not
  *start* before some wall-clock time.
- **Business-hours run start** — only kick off runs during business hours (on
  top of the cron cadence).
- **Custom dependency logic** — dependency rules beyond Airflow's task/DAG deps
  (cross-DAG, data-condition, calendar-condition, etc.).
- **Native / max active run control** — bespoke handling of how many concurrent
  runs of a pipeline are allowed (`max_active_runs`-style but tailored).
- **Serial-run gating on previous success** — a new run stays **queued** until
  the *previous* run of the same DAG finishes **successfully**. Stricter than
  `max_active_runs=1` (which just limits concurrency) and than
  `depends_on_past` (task-level, and only blocks, doesn't hold the whole run
  queued): here the next run won't even begin while the prior one is
  failed/running, and resumes once it goes green.

Pattern: cron decides *candidacy*, their layer decides *whether/when it actually
starts*. Mostly implemented as custom **timetables** + gating logic.

### Workload routing

- **Multi-cluster support** — route a task/DAG's work to a specific execution
  cluster (by region, capacity, data locality, cost, tenant). A routing layer
  picks where work runs rather than everything landing on one pool.
- **Conditional branching on top of `BranchPythonOperator`** — richer branch
  semantics than the stock branch operator: rules/config-driven path selection,
  probably multi-condition, reusable across DAGs instead of hand-written branch
  callables each time.

### Operational control primitives

- **Runtime skip / mark-as-success** — operator can, on a live run, skip a task
  or force it to success without touching code.
- **Active / inactive** — toggle a task (or task group) on/off, presumably at the
  DAG or run level.
- **Design-time breakpoint** — declared in the DAG: "always pause here for
  inspection."
- **Runtime breakpoint** — added by an operator to an in-flight run: "pause at
  this task now."
- **Pause & resume** — suspend an in-flight DAG run and later resume it, with
  state consistency.

**How it's implemented: a run-scoped gate at the task pre-check.**
- Before a task runs, a pre-execution check consults run-scoped state (skip list,
  breakpoints, pause flag, active/inactive) and decides: run / skip /
  mark-success / hold.
- "Run-scoped" = the control applies to *this* DAG run, not the DAG definition —
  so operators can steer one run without affecting others or redeploying.
- This single hook is the common mechanism behind all the primitives above —
  breakpoints, skip, pause/resume are all "what does the gate return for this
  task instance."

_(add during/after the talk)_

### Angle to watch

- These are platform-team-built control primitives on a shared Airflow — same
  space as our work. Which of these did they get upstreamed / could we adopt vs.
  reimplement?
- 2.10 base — how much of this does Airflow 3.x now cover natively (deferrable,
  deadline alerts, better timetables, HITL)?
