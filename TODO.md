# AS26 — TODO

Consolidated from all session notes. Grouped by type.

---

## 1. Personal learning (foundational gaps I hit repeatedly)

- [ ] **How Python imports / module loading actually works.** `sys.path` /
  `sys.modules`, finders/loaders / `importlib`, `sys.path_importer_cache`,
  namespace packages. Came up in **3** notes (multi-team, polyglot ×2).
  - Then: **"top-level code" in a DAG file** — what runs at import time, why DAG
    structure is a *side effect* of executing Python, why that's an
    arbitrary-code-execution surface.
  - Refs: `as26-multi-team-airflow.md` (per-team DAG processor),
    `as26-toward-a-polyglot-airflow.md` (execution model),
    `as26-optimising-airflow-real-world.md` Layer 1.
- [ ] **Airflow assets** (formerly datasets). `outlets=` / `schedule=[asset]`,
  asset events, data-driven scheduling, `AssetWatcher` (AIP-82), and
  `AssetAccessControl` for multi-team. This is *the* cross-team coordination
  primitive — prerequisite for evaluating multi-team. Ref:
  `as26-multi-team-airflow.md`.
- [ ] **"Event-based trigger" vs task-deferred trigger** — the 3 uses of a
  `Trigger` (task-deferred / event-based via AssetWatcher / callback-based).
  Ref: `as26-trigger-queues-datadog.md` (has a starter breakdown).
- [ ] Read upstream **`devel-common/src/tests_common/pytest_plugin.py`** in
  apache/airflow (the real thing behind Breeze / that `pytest-airflow-in-a-box`
  imitates). Ref: `as26-airflow-in-a-box.md`.

---

## 2. Platform work — evaluate for our managed Airflow

### Multi-tenancy direction (biggest decision)

- [ ] **Decide our isolation model.** Three references now:
  - per-tenant instances + fleet machinery (Capital One)
  - shared control plane + k8s-namespace "worker groups" (Datadog)
  - native multi-team / AIP-67 (upstream, experimental)
  - Key question from the Capital One talk: **how uniform are our tenants'
    workloads?** Homogeneous → templated/managed model works; heterogeneous →
    harder.
- [ ] **If considering native multi-team (AIP-67):** scope the **Auth Manager**
  work first — it needs per-user team membership + team-filtering hooks; not
  just a config flag. Especially if we run a custom Auth Manager. Multi-team is
  still experimental. Ref: `as26-multi-team-airflow.md`, memory
  `multi-team-needs-auth-manager-work`.
- [ ] **Trigger queues** (`--queues` on triggerer, PR #59239) — evaluate now
  regardless of multi-tenancy model. Even single-tenant benefit: isolate
  slow/blocking triggers (Spark/Trino polls) from latency-sensitive ones. Route
  by task `queue`. Ref: `as26-trigger-queues-datadog.md`.
- [ ] Confirm what **"ADP Airflow"** is (Datadog internal platform name) if it
  matters for comparison.

### Worker / resource sizing

- [ ] **Fix CPU-only autoscaling.** Our prod autoscales workers on CPU only →
  memory-heavy tasks OOM workers with no scaling signal → tasks fail & reschedule
  (looks like a scheduling problem, is a sizing problem). Add memory to HPA, or
  route heavy tasks to a dedicated queue. Ref:
  `as26-optimising-airflow-real-world.md`, memory
  `prod-airflow-workers-scale-on-cpu-only`.
- [ ] Derive `worker_concurrency` from
  `worker_memory / (peak_task_memory × safety_factor)` — and actually **measure
  `peak_task_memory`** (memray / cgroup `memory.peak` / per-TI max RSS, p95–max
  over month-end / backfills). Consider a dedicated queue for memory-hungry tasks.

### Metadata DB hygiene

- [ ] **Set up + monitor `airflow db clean`** (XCom, logs, task_instance,
  dag_run, …). Verify table/index sizes actually drop. This exact job silently
  failing for months = the Datadog-adjacent "fleet slowed down overnight"
  incident. Ref: `as26-debugging-airflow-incidents.md` Case 3,
  `as26-optimising-airflow-real-world.md` Layer 4.
- [ ] Audit metadata DB: unused indexes (`pg_stat_user_indexes idx_scan=0`),
  slow queries (`pg_stat_statements`), dead-tuple ratio / autovacuum on
  `task_instance` / `dag_run`, `xcom` table size (esp. before any upgrade).
- [ ] Before our next Airflow upgrade: **test against a prod-sized (restored)
  metadata DB**, time the migration, watch for locks. A fresh staging DB won't
  reproduce plan regressions.

### Observability

- [ ] Evaluate enabling the **OpenLineage provider centrally** — gives every
  tenant lineage + failure context for free; run-ID correlation helps cross-team
  debugging (Team B failure caused by Team A's stale asset). Ref:
  `as26-openlineage-root-cause.md`.
- [ ] Check we have per-component metrics that distinguish scheduler-lag causes
  (`scheduler.tasks.starving`, `pool.open_slots.*`, `executor.queued_tasks`,
  `scheduler.scheduler_loop_duration`, `scheduler.critical_section_duration`,
  `dag_processing.total_parse_time`, per-DAG schedule delay). Ref:
  `as26-optimising-airflow-real-world.md` Layer 2.
- [ ] Team-scoped metrics shipped in 3.3 (task durations/counts/queue-time by
  team) — useful for chargeback / noisy-tenant detection whether or not we adopt
  full multi-team.

### Durability / long-running tasks (AIP-103 / 105, 3.3)

- [ ] Evaluate **`ResumableJobMixin`** for our Spark/long-job operators — kills
  the "crash tax" (worker dies → resubmit whole job). Needs **cluster deploy
  mode**. Ref: `as26-resumable-task-execution-spark.md`.
- [ ] Evaluate **pluggable retry policies** (`ExceptionRetryPolicy` to start —
  fail-fast on auth/config errors, backoff on transient) for tenant DAGs or as a
  platform default.
- [ ] For any deferrable operators watching external jobs: audit the
  **triggerer-timeout / trigger-exception path** — task failed from the
  triggerer skips `execute_complete` and `on_kill`, orphaning the external job.
  Add a reaper/reconcile DAG or `on_task_instance_failed` listener. Check
  apache/airflow **#36090** (deferrable `on_kill` not called) status for our
  version. Ref: `as26-taming-ai-workloads-dag-patterns.md`.

### Agentic (if/when tenants want LLM pipelines)

- [ ] Watch the **`common-ai` provider** (`@task.agent`, AIP-99). If tenants
  start running agents: mandate **tool-level guardrails** (allow-lists,
  `allow_writes=False`, scoped connections, sandbox) — the rogue-agent demo
  showed the model *will* follow injections; only the tool layer stops damage.
- [ ] If agents run untrusted/generated code: the **`@agent` isolation pattern**
  (gVisor/Kata/Peer Pods via `executor_config`) — Peer Pods if our k8s nodes
  can't do nested virt. Ref: `as26-agent-isolation-airflow.md`.

### Testing

- [ ] Trial **`pytest-airflow-in-a-box`** for tenant DAG repos / our own
  operators — real metadata DB, catches serialization/trigger-rule/template bugs
  plain import tests miss. (3rd-party, pin a version.) Ref:
  `as26-airflow-in-a-box.md`.

### Scheduling primitives (lower priority — mostly eBay-specific)

- [ ] From eBay's list, check which are now native in 3.x vs. still need custom
  build: breakpoints, runtime skip/mark-success, pause/resume, calendar
  timetables, earliest-start-time, serial-run-on-previous-success, multi-cluster
  routing. Ref: `as26-ebay-enterprise-scheduling.md`.

---

## 3. Adopt / low-risk wins

- [ ] Publish/share the **Airflow Performance Runbook artifact** with the team
  (from `as26-optimising-airflow-real-world.md`) — it's already built.
- [ ] Add the "lightest tool first" + 5-layer diagnostic method to our runbook /
  on-call docs.
- [ ] Add the **symptom → component → first-signal** map (which component fails
  how) to on-call docs.

---

## 4. Note cleanup (housekeeping)

- [ ] Upload the **Capital One architecture photo** → `img/` and link it in
  `as26-airflow-as-a-platform.md`.
- [ ] Fill remaining `_(add during/after the talk)_` / TBD placeholders where I
  have memory of the content:
  - `as26-airflow-as-a-platform.md` — operational challenges #3 and #4
  - `as26-performance-debugging-symptoms-to-solutions.md` — mostly empty
  - `as26-trigger-queues-datadog.md` — "bespoke runtime environments" driver
  - `as26-openlineage-root-cause.md` — live observations
- [ ] Confirm exact release version for **trigger queues** (PR #59239 merged
  2025-12-31; "3.2.1" was an unverified earlier search result).
- [ ] Confirm **Deadline Alerts** 3.4 feature list against actual release notes
  once 3.4 ships.
