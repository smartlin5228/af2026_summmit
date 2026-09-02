# Observability: OTel + OpenLineage + the free Airflow metrics

**Status:** platform proposal. We already run **OpenTelemetry on Airflow 3**
(metrics + traces). This doc argues for adding the **OpenLineage** provider
centrally, and lays out what each layer gives us.

Background: `../as26-openlineage-root-cause.md`,
`../as26-optimising-airflow-real-world.md` (Layer 2 metrics),
`../as26-agentic-pipelines-on-airflow.md` (ops-agent idea).

---

## The problem we're solving: the ping-pong

Today, when a tenant's pipeline misbehaves:

1. Workflow-owner team: "our DAG is slow / failing, is it the platform?"
2. Infra team: digs through scheduler logs, worker metrics, the DB → "looks like
   your task, not us" (hours later)
3. Owner: "but it worked yesterday" → back to infra
4. ... repeat

Each side sees **half the picture**. Owners see their DAG code + task logs.
Infra sees cluster metrics. Neither can answer *"why did **this run** of **this
task** behave differently"* without a manual cross-correlation exercise.

**The fix is a shared context layer** where a single run can be traced end to end
— code → orchestration → infra → the data it touched — by either team, or by a
tool.

---

## The three layers (what each answers)

| Layer | Question it answers | Status for us |
|---|---|---|
| **Metrics** (OTel) | "Is *something* wrong, and roughly where?" — rates, durations, saturation, over time | ✅ enabled |
| **Traces** (OTel) | "For *this* run, where did the wall-clock go?" — span timeline across scheduler / executor / worker | ✅ enabled (Airflow 3) |
| **Lineage** (OpenLineage) | "*Why* did this run behave differently?" — what data it read/wrote, which query, row counts, schema, upstream freshness, who approved it | ❌ **the gap** |

Metrics tell you the scheduler loop is slow. Traces tell you task X's run spent
40s queued. **Only lineage tells you task X read from `raw.orders` which was 6h
stale because upstream team Y's job failed** — which is the actual root cause and
the thing that ends the ping-pong.

---

## Free Airflow metrics we should be watching

These ship with Airflow, emitted over OTel/StatsD. The ones that matter for a
platform team (grouped):

### Scheduler health
- `scheduler.scheduler_loop_duration` — the core "is the scheduler keeping up"
- `scheduler.tasks.starving` — tasks that want a pool slot and can't get one
- `scheduler.tasks.executable` — tasks ready to run
- `scheduler.critical_section_duration` / `.critical_section_busy` — contention
  on the scheduler's DB critical section (multi-scheduler / slow-DB tell)
- `scheduler_heartbeat`

### DAG parsing
- `dag_processing.total_parse_time` — whole-bundle parse time
- `dag_processing.import_errors` — broken DAGs count
- `dag_processing.file_path_queue_size` — parse backlog
- `dag_processing.last_duration.<filename>` — per-file parse time (find the slow
  file)

### Pools / executor (throughput ceilings)
- `pool.open_slots.<pool>` / `pool.used_slots.<pool>` / `pool.starving_tasks.<pool>`
- `executor.open_slots` / `executor.queued_tasks` / `executor.running_tasks`

### Runs & tasks
- `dagrun.schedule_delay.<dag_id>` — scheduled time → actual start (per-DAG
  scheduler lag)
- `dagrun.duration.success.<dag_id>` / `dagrun.duration.failed.<dag_id>`
- `dagrun.first_task_scheduling_delay.<dag_id>`
- `ti.start` / `ti.finish`, `operator_successes_<op>` / `operator_failures_<op>`
- `dag.<dag_id>.<task_id>.duration` — per-task duration (watch for drift)

### Triggerer
- `triggers.running` / `triggers.succeeded` / `triggers.failed`
- `triggers.blocked_main_thread` — a trigger is doing blocking work (see
  `../learning/04-deferrable-operators-triggers-triggerer.md`)
- `triggerer_heartbeat`

### DB (from the DB side, not Airflow, but essential)
- query latency, active connections vs pool size, lock waits, table/index sizes,
  dead-tuple ratio on `task_instance` / `dag_run` / `xcom`

**Action:** audit which of these we actually have dashboards + alerts on. The
Layer-2 diagnostic method in the optimising-Airflow note needs
`scheduler.tasks.starving`, `pool.open_slots.*`, `scheduler_loop_duration`,
`critical_section_duration`, and per-DAG `schedule_delay` — confirm those exist.

---

## What OpenLineage adds

The `apache-airflow-providers-openlineage` provider emits a structured event per
task run (START / COMPLETE / FAIL) containing:

- **Inputs / outputs** — the datasets (tables, files, topics) the task read and
  wrote, with **namespace + name** (so a table is the same node across Airflow,
  Spark, dbt, …).
- **Column-level lineage** for supported SQL operators — parses the query to map
  output columns → source columns.
- **Run facets** — error message + stack on failure, row counts, bytes,
  **query text + query ID** (hook-level, new), execution time.
- **Parent run facet** — links task run → DAG run → (optionally) an upstream
  producer's run. This is the **run-ID correlation** the AS26 talk highlighted.
- **`airflow` facet** — task/DAG metadata, operator, try number, map index.
- **Human-in-the-loop metadata** — who approved a HITL step and when.

Emitted to a **transport** (HTTP to a collector, Kafka, or console for testing).
Consumers: Marquez (the reference lineage UI), a data catalog, our own store, or
an OTel-adjacent pipeline.

### Why this specifically ends the ping-pong

- A run's OL event says *exactly* which query ran, against which warehouse,
  returning how many rows, and whether an input was stale.
- Both teams look at the **same run record**. "Your task read 0 rows because
  `raw.orders` hasn't updated since 02:00" is not debatable.
- The parent-run facet lets you walk **upstream across team boundaries** — Team
  B's failure → Team A's asset → Team A's failed run — in one hop.

---

## The AI-agent angle (why this compounds)

An **operational debugging agent** (cf. the agentic-pipelines talk, and Astro's
Investigation Agent demo) is only as good as the context it can pull. With all
three layers:

- **Metrics** → "was the platform degraded at that time?" (scheduler lag, pool
  starvation, DB latency during the window)
- **Traces** → "where did this run's wall-clock go?" (queued vs running vs a
  specific slow span)
- **Lineage** → "what data did it touch, was it fresh, what query, how many
  rows, what error"

An agent given a failed task instance can, without a human: pull the OL run
facet (error + query + row counts), check the parent/upstream OL runs for
staleness, overlay the OTel metrics for that window, and produce **"this failed
because X, and it's {your DAG bug | stale upstream from team Y | platform
degradation}"** — which is exactly the classification that today takes two teams
and several hours.

Without OpenLineage the agent is guessing from logs. With it, the agent has
structured, queryable ground truth.

---

## Rollout proposal

1. **Enable the OpenLineage provider in staging.**
   ```ini
   # in the platform-managed config (tenants don't touch this)
   [openlineage]
   transport = {"type": "http", "url": "http://ol-collector.internal:5000", "endpoint": "api/v1/lineage"}
   namespace = airflow-prod          # or per-env
   # disabled_for_operators = ...     # opt out noisy/irrelevant operators if needed
   ```
   It's **zero tenant effort** — the provider auto-instruments standard operators
   (SQL, Python, transfer, Spark, dbt via Cosmos).
2. **Stand up a collector.** Marquez for a UI + API, or route events into our
   existing observability store. Decide: do we want the lineage graph UI, or
   just the events queryable alongside metrics/traces?
3. **Verify correlation.** Confirm the OTel trace ID and the OL run ID can be
   joined (via the parent run facet / a shared attribute) so a single run is
   navigable across all three layers.
4. **Promote to tenants** — not as work for them, but as a capability: "every
   task now has lineage; here's the UI; here's how to see why a run behaved
   differently."
5. **Then** scope the ops-agent: give it read access to metrics + traces + OL
   events, start with "explain this failed task instance."

### Cost / caveats
- OL events add load — one HTTP POST per task START/COMPLETE/FAIL. Batch/async
  transport; size the collector.
- SQL parsing for column lineage is operator-dependent and imperfect on exotic
  SQL — treat column lineage as best-effort.
- Namespacing matters — get the dataset naming convention right up front (align
  with the asset URI convention from
  `cross-tenant-triggering-move-to-assets.md`) or lineage nodes won't join
  across systems.
- PII: OL run facets can include **query text**. Decide whether to redact.
