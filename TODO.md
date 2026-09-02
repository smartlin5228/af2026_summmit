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

- [x] **Decision proposal written** — `platform/multi-tenancy-decision.md`.
  Given our profile (1 shared instance, <10 teams growing slowly, very
  heterogeneous, all 4 pains active): **Option 1 = incremental isolation on the
  shared instance** now (per-team Pools → per-team bundles+processors → per-team
  workers → templated provisioning), **Option 3 = native AIP-67 as the
  destination**, Option 2 (2-3 instances by archetype) as fallback. NOT per-tenant
  fleet.
- [ ] **Resolve the open questions** in that doc: executor type (Celery/K8s),
  Auth Manager (FAB/custom), DAG delivery model, secrets scoping, #57081 status,
  what "blast radius" pain actually means for us (scheduler vs UI/RBAC).
- [ ] **Step 1 (do this week): per-team Pools + `max_active_tasks/runs` defaults**
  — cheapest noisy-neighbour fix.
- [ ] **Step 2: per-team DAG bundles + dedicated DAG processors** — kills dep
  conflicts + parse-time code-execution surface. Doesn't need multi-team.
- [ ] **Step 3: per-team worker isolation** (K8s pod templates or Celery queues).
- [ ] **Step 4: templated provisioning** wrapping steps 1-3 (config → bundle
  entry + processor Deployment + worker pool + Pool + RBAC, GitOps'd).
- [ ] **Scope the Auth Manager work for AIP-67** now (so it's ready when we want
  the destination). Ref: memory `multi-team-needs-auth-manager-work`.
- [ ] **Move cross-tenant `TriggerDagRunOperator` → assets.** Full recipe +
  tenant template + pre-flight checklist + rollout plan in
  `platform/cross-tenant-triggering-move-to-assets.md`. Doesn't need multi-team.
  Start: grep DAG repos for `TriggerDagRunOperator`, migrate clean pairs first.
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

- [ ] **Enable the OpenLineage provider centrally** (staging first). Zero tenant
  effort; auto-instruments standard operators. Gives lineage + failure context +
  run-ID correlation → ends the workflow-owner ↔ infra ping-pong (both sides see
  the same run record). Compounds with our existing OTel and with a future
  ops-agent. Full proposal + rollout + cost/PII caveats:
  `platform/observability-otel-openlineage.md`.
- [ ] **Audit our Airflow metric coverage.** Confirm dashboards + alerts on the
  free metrics that the Layer-2 diagnostic method needs:
  `scheduler.tasks.starving`, `pool.open_slots.*` / `starving_tasks`,
  `executor.queued_tasks`, `scheduler.scheduler_loop_duration`,
  `scheduler.critical_section_duration`, `dag_processing.total_parse_time` /
  `last_duration.<file>`, `dagrun.schedule_delay.<dag_id>`,
  `triggers.blocked_main_thread`. Full list in
  `platform/observability-otel-openlineage.md`.
- [ ] Verify the OTel **trace ID** and OpenLineage **run ID** can be joined, so a
  single run is navigable across metrics → traces → lineage.
- [ ] **Build-vs-buy spike: our own "Otto".** Astronomer's Otto = agent over
  OL + OTel + host metrics + Airflow context. We already have an internal agent
  with access to all three. Spike: wire our agent to a staging OL collector +
  our OTel backend, run "explain this failed task" on ~10 real past incidents,
  compare to what the owner/infra teams concluded manually. Ref:
  `platform/observability-otel-openlineage.md` (Otto section).
- [ ] Team-scoped metrics shipped in 3.3 (task durations/counts/queue-time by
  team) — useful for chargeback / noisy-tenant detection whether or not we adopt
  full multi-team.

### Durability / long-running tasks (AIP-103 / 105, 3.3)

- [ ] ~~`ResumableJobMixin` for Spark~~ — **N/A, we have no Spark jobs.** (The
  resumable-Spark session was attended out of curiosity.) Revisit only if we
  ever add Spark.
- [ ] Keep **AIP-103 task state store** on the radar for *any* future
  long-running / batch / agentic task — the "resume from checkpoint" primitive is
  workload-agnostic. Ref: `as26-resumable-task-execution-spark.md` (mechanism
  section), `as26-agentic-pipelines-on-airflow.md`.
- [ ] Evaluate **pluggable retry policies** (`ExceptionRetryPolicy` to start —
  fail-fast on auth/config errors, backoff on transient) for tenant DAGs or as a
  platform default. Broadly useful, not Spark-specific.
- [ ] **Only if we run deferrable operators watching external jobs** (Trino, K8s
  jobs, HTTP sensors, custom): audit the **triggerer-timeout / trigger-exception
  path** — task failed from the triggerer skips `execute_complete` and
  `on_kill`, orphaning the external job. Add a reaper/reconcile DAG or
  `on_task_instance_failed` listener. Check apache/airflow **#36090** status for
  our version. Ref: `as26-taming-ai-workloads-dag-patterns.md`,
  `learning/04-deferrable-operators-triggers-triggerer.md`.
  - First: do we even use deferrable operators today? If not, this is deferred.

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
