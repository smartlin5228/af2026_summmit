# AS26: Optimising Airflow in Real-World Deployments: Profiling, Performance Drift, and Confident Upgrades

**Event:** Airflow Summit 2026 · Aug 31 2026, 12:30–13:00, Texas Ballroom
**Speakers:** Pankaj Koti & Vara Prasad Regani (Astronomer — Airflow OSS
engineering; Pankaj is an Airflow committer and Cosmos contributor, which is why
Cosmos / dbt DAGs come up as the dynamic-generation example)

> Note: blanks below marked _(inferred)_ are filled from general Airflow
> performance knowledge + research, not verbatim from the talk. Correct against
> the actual session.

## Session summary

Performance issues in Airflow rarely appear as clear failures — they surface as
subtle signals:

- Longer task queue times
- Slower DAG parsing
- Scheduler lag
- Workers hitting limits as workloads grow

The talk shares lessons from **profiling real production deployments across
Airflow 2.x and 3.x**, combining frontline operational insight with focused
technical investigation. They analysed:

- Task latency
- DAG parsing time
- Worker behaviour
- Metadata database performance under sustained load

## Key threads

- **Config choices amplify or limit version-level improvements** —
  `parallelism`, `max_active_runs`, worker resources.
- **Performance drift in long-running environments** — accumulated DAG runs
  expose slow queries / missing indexes that fresh deployments never reveal.
- **Dynamic DAG generation** (e.g. Cosmos dbt DAGs) and custom user code can
  unintentionally hurt parsing and execution performance.

## Attendee takeaway

A practical framework to profile existing deployments, isolate bottlenecks,
optimise performance, reduce recurring issues, and approach upgrades with
confidence.

## My notes / observations

### Default debugging path (order to work through)

1. **DAG parsing** — parse time per file, top-level code.
2. **Scheduling** — queue time, scheduler lag, poll interval.
3. **Task runtime** — is the task slow once it's actually running?
4. **Metadata DB** — slow queries, locks, missing indexes under accumulated load.
5. **Upgrade effects** — did a version bump change any of the above?

### Diagnostic principle — reach for the lightest tool that answers the question

| Reach for | When |
|---|---|
| **Metrics & logs** | Always first — cheapest signal. |
| **Database inspection** (slow queries, locks, missing indexes) | Several components are slow *together*. |
| **py-spy** (sampling profiler) | A process is burning CPU. |
| **memray** (memory profiler) | Memory grows, or a process gets killed (OOM). |
| **Application profiling** | A task itself is slow *after it has started*. |

### Each Airflow component fails differently — know the symptom → signal map

- **DAG processor** — symptom: DAGs appear late after a deploy.
  - Look at: per-file parse time / parse stats, top-level code in DAG files.
  - The signal is **already in the DAG-processor logs** — no extra tooling needed.
  - Slow parsing is almost always one of ~4 root causes:
    1. Top-level `Variable.get()` / `Connection` lookups (hit the DB on every parse).
    2. API or DB calls made outside a task (at module scope).
    3. Heavy imports at the top of the file (import pandas/boto3/etc. inside the
       task or callable, not at module scope).
    4. **Expensive dynamic DAG generation at parse time** _(inferred)_ — a single
       file that loops to build many DAGs, or heavy config expansion (reading a
       big YAML/JSON, rendering a dbt manifest) that runs on every parse. The
       scheduler must load the *whole* file to get any one DAG.
  - Related: parsing too many files (no `.airflowignore`), very large/deeply
    nested DAG structures (task count × dependencies).
  - **Fix:** move external calls (API/DB) *into tasks* instead of module scope;
    lazy-import heavy libs; cache/precompute config artifacts (e.g. a committed
    manifest) instead of computing at parse; split large DAG files; use
    `.airflowignore`; raise `min_file_process_interval` if files rarely change.
- **Scheduler / executor** — symptom: tasks wait before running.
  - Look at: queue time, scheduler poll interval.
  - See the "Layer 2" research section below for how to localize which ceiling.
- **Triggerer** — symptom: deferred tasks not resuming; triggers piling up.
  - One trigger with a single, global asyncio loop; sync I/O or CPU-bound work
    in a trigger's `run()` blocks *every* deferred task on that triggerer.
  - Triggers persist yielded events in the **metadata DB** and can't use a
    custom XCom backend — heavy fan-out (10k small files) is expensive; every
    deferral is a Worker→Scheduler→Triggerer→Scheduler→Worker round-trip.
  - Look at: `triggers.running`, `triggers.blocked_main_thread`, triggerer CPU;
    add triggerers / cap concurrent deferred tasks; prefer native async
    operators for high-throughput polling.
- **Webserver / API server** — symptom: UI slow, API calls time out.
  - Usually downstream of a slow **metadata DB** (Grid/Graph views run heavy
    queries over `task_instance` / `dag_run`), or too few gunicorn workers, or
    huge DAGs rendering. Airflow 3's FastAPI server is faster but still
    DB-bound.
  - Look at: request latency, gunicorn worker saturation, the same DB slow
    queries as Layer 4.
- **Workers** — symptom: tasks slow once running, or OOM-killed. See Layer 3.

_(add more during/after the talk)_

## Layer 2 research — localizing a scheduling / task-latency bottleneck

**Goal:** a task is "slow" but hasn't started yet. Find *which* stage the time is
spent in, and *which* ceiling is binding. The lag between a task becoming
runnable and actually starting is the sum of several stages, each with its own
limit.

### The pipeline a task moves through (and the gate at each step)

| Stage | State | Gate / limit that can stall it here |
|---|---|---|
| Scheduler notices the task is runnable | `scheduled` | Scheduler loop capacity: DAG parsing stealing CPU, `scheduler_heartbeat_sec`, number of schedulers, `max_dagruns_per_loop_to_schedule` |
| Scheduler enqueues it to the executor | `scheduled` → `queued` | `[core] parallelism` (cluster-wide cap on non-finished tasks), pool free slots, `max_active_tasks_per_dag`, `max_active_runs_per_dag` |
| Executor hands it to a worker | `queued` | Executor/broker: Celery `worker_concurrency` × #workers, KubernetesExecutor pod quota / `max_pods_per_dag`, k8s API + image pull time |
| Worker starts the process | `queued` → `running` | Worker resources (CPU/mem), worker startup, `[celery] worker_prefetch_multiplier` hoarding |

### Which signal distinguishes which cause

- **Queue time (queued→running) high, workers idle (low CPU/mem):**
  executor/worker-concurrency ceiling, or prefetch hoarding — not a resource
  problem. Raise `worker_concurrency` / worker count, lower prefetch.
- **Queue time high, workers saturated (CPU/mem pinned):** genuine worker
  capacity — scale out / up.
- **Tasks stuck in `scheduled`, never reaching `queued`, and `parallelism` /
  pool is full:** hitting `[core] parallelism` or a pool. Check
  `pool` used vs. total slots; `AIRFLOW__CORE__PARALLELISM`.
- **Tasks stuck in `scheduled`, but `parallelism` and pools have headroom:**
  scheduler itself is behind — look at DAG-processor CPU, scheduler loop
  duration, `DagFileProcessor` timing, number of schedulers. Also
  `max_active_runs` / `max_active_tasks` on that specific DAG.
- **Queued spike that does NOT drain within ~10 min:** Airflow will start
  marking tasks failed / `up_for_retry` and rescheduling — compounding load.
- **Everything slow together (parse + schedule + query latency):** suspect the
  **metadata DB** (see Layer 4) — slow queries, lock contention, missing index,
  connection-pool exhaustion. This is the "rarely compute, usually the metadata
  DB" failure mode.

### Key metrics to chart

- `scheduler.tasks.starving` (tasks that want a pool slot but can't get one)
- `pool.open_slots.<pool>` / `pool.used_slots.<pool>`
- `executor.open_slots`, `executor.queued_tasks`, `executor.running_tasks`
- `scheduler.scheduler_loop_duration`
- `dag_processing.total_parse_time`, `dag_processing.last_duration.<file>`
- `scheduler.critical_section_duration` /
  `scheduler.critical_section_busy` (contention on the scheduler's DB critical
  section — a classic multi-scheduler / slow-DB tell)
- DAG-run **schedule delay** = actual start − scheduled time (per-DAG breakdown
  of scheduler lag)
- Metadata DB: query latency, active connections, lock waits, table/index bloat

### Knobs, mapped to the stage they relieve

- Scheduler loop: `parsing_processes` (parse concurrency), more scheduler
  replicas, `scheduler_heartbeat_sec`, `min_file_process_interval`,
  `dag_dir_list_interval`, `schedule_after_task_execution`.
- Enqueue ceiling: `[core] parallelism`, pool sizes,
  `max_active_tasks_per_dag`, `max_active_runs_per_dag`.
- Executor→worker: Celery `worker_concurrency`, worker count,
  `worker_prefetch_multiplier`; K8s pod quotas.
- DB: connection pool (`sql_alchemy_pool_size`, `max_overflow`), add indexes,
  clean up old runs / XCom / logs (perf drift — Layer 4).

### Process to actually do it (what the talk is likely advocating)

1. Pick one slow DAG/task. Pull its TI history: timestamps for
   `queued_dttm`, `start_date`, `end_date`, and the DAG-run's
   `data_interval_end` vs `start_date`.
2. Compute each gap: scheduled→queued, queued→running. The big gap names the
   stage.
3. Overlay cluster metrics for that window: were pools/parallelism full? were
   workers saturated? was the scheduler loop long? was DB latency up?
4. Only then change one knob, and re-measure.

**Sources (Layer 2):**
[Google Cloud – Debug task scheduling issues](https://docs.cloud.google.com/composer/docs/composer-2/debug-task-scheduling-issues),
[Google Cloud – Troubleshooting scheduler](https://docs.cloud.google.com/composer/docs/composer-2/troubleshooting-scheduling),
[Datadog – Key metrics for Airflow monitoring](https://www.datadoghq.com/blog/key-metrics-for-airflow-monitoring/),
[AWS – Performance tuning for Airflow on MWAA](https://docs.aws.amazon.com/mwaa/latest/userguide/best-practices-tuning.html),
[Astronomer – Scaling Airflow to optimize performance](https://www.astronomer.io/docs/learn/airflow-scaling-workers)

## Layer 3 — task runtime (process-level failures)

The task has started; the problem is now *inside* a running process. This is
where you drop to process-level tools (py-spy, memray) rather than metrics.

| Symptom | Likely cause | Tool / check |
|---|---|---|
| **Worker CPU pegged** | Task code hot loop, tight serialization, unbounded data in memory, N+1 DB calls in the task | `py-spy dump` / `py-spy top` on the worker/task PID; flame graph |
| **Scheduler memory leak** (RSS grows over hours/days until OOM or restart) | Leak in scheduler, a plugin, or a custom timetable/listener; large serialized DAGs held in memory | `memray` on the scheduler process; watch `process.resident_memory`; correlate with deploy of a plugin |
| **Triggerer unresponsive** (deferred tasks not resuming, triggers piling up) | One blocking / CPU-bound `run()` in a custom trigger starves the single asyncio event loop; too many triggers per triggerer | triggerer logs, `triggers.running`, `triggers.blocked_main_thread`; py-spy the triggerer; move blocking work out of `run()` |
| **CPU throttled** (task slow, host CPU looks *low*) | k8s/cgroup CPU limit too tight — container throttled despite headroom on the node | `container_cpu_cfs_throttled_periods_total` / throttled seconds; raise CPU limits or remove them; check requests vs limits |

Notes:
- CPU *throttling* vs CPU *pegged* look opposite in node metrics but both slow
  the task — always check cgroup throttle counters before concluding "not CPU".
- Triggerer is single-threaded async: one bad trigger blocks *all* deferred
  tasks on that triggerer. Classic footgun is doing sync I/O or heavy CPU in a
  trigger's `run()`.
- A scheduler "leak" is often not Airflow core — bisect by disabling plugins /
  reverting the last DAG-code change.

### Sizing workers — `worker_concurrency` is usually just a guess

Derive it from memory instead:

```
worker_concurrency ≈ worker_memory / (peak_task_memory × safety_factor)
```

- `peak_task_memory` = observed p95/max RSS of a single task (measure with
  memray / cgroup peak, don't assume).
- `safety_factor` > 1 for headroom (e.g. 1.3–1.5) — bursts, GC lag, the worker
  process itself.
- Then sanity-check against CPU: if tasks are CPU-bound, also cap at
  ~`worker_vCPU` regardless of what the memory formula says.
- Too-high concurrency → OOM-killed workers → tasks fail and reschedule →
  looks like a scheduling problem (Layer 2) but the root cause is worker sizing.

**Which component is hit hardest by a memory cap? The worker.**
- Runs arbitrary user code — memory is unbounded and outside ops' control,
  unlike other components whose memory ≈ f(deployment size).
- `worker_concurrency` holds that many tasks' peak RSS at once — concurrency
  multiplies the risk.
- OOM kill is destructive: takes the whole worker + all co-tenant tasks down →
  they reschedule → more load. (A CPU cap only slows; a memory cap kills.)
- Runners-up: **triggerer** (one process holds all concurrently-deferred tasks'
  state), **DAG processor / scheduler** (scales with # + size of serialized
  DAGs, top-level code, dynamic DAG generation — more slow-growth than hard cap).

> **Relevant to us:** our prod env currently autoscales workers on **CPU only**,
> not memory. That's a known limitation on our side — memory-heavy tasks can OOM
> a worker while CPU-based scaling sees nothing wrong. Scaling/HPA should key on
> memory too (or route heavy tasks to a dedicated queue).

**`peak_task_memory` is THE variable that decides the answer:**
- Worker memory is fixed/known; safety factor is a rough multiplier; peak task
  memory is the only uncertain input, and concurrency scales *inversely* with
  it — underestimate 2× → concurrency set 2× too high → OOM.
- Use **peak (high-water mark), not average** — the worker must hold every
  concurrent task's peak simultaneously. A task idling at 200 MB but spiking to
  2 GB during one merge costs 2 GB of budget.
- It's **not uniform** across tasks. One global `worker_concurrency` must be
  sized for the heaviest task that can land on that worker. Better: route
  memory-hungry tasks to a dedicated queue / worker pool with its own
  concurrency.
- Measure it: `memray`, cgroup `memory.peak` /
  `container_memory_max_usage_bytes` per task pod, or per-TI max RSS. Take
  p95–max over a realistic window (month-end, backfills, biggest inputs).

## Layer 4 — metadata database (performance drift)

The "rarely compute, usually the metadata DB" layer. Symptoms are diffuse:
everything slows together, fresh deployments don't reproduce it, degradation
tracks *accumulated history* not load.

### What to inspect

- **Table & index sizes** — which tables have grown unbounded. Usual suspects:
  `task_instance`, `dag_run`, `xcom`, `log`, `task_reschedule`,
  `rendered_task_instance_fields`, `job`, `sla_miss`, `task_fail`,
  `session` (Flask/FAB), `import_error`, `dataset_event` / `asset_event`.
  - Postgres: `pg_total_relation_size`, `pg_stat_user_tables` (n_live_tup,
    n_dead_tup), bloat queries.
- **Unused / redundant indexes** — indexes that cost write time and bloat but
  are never scanned.
  - Postgres: `pg_stat_user_indexes` where `idx_scan = 0`; check for
    duplicate/overlapping indexes.
- **Slow queries** — the specific statements the scheduler/webserver run hot.
  - Postgres: `pg_stat_statements` ordered by `total_exec_time` / `mean_exec_time`
    / `calls`; `auto_explain` for plans.
  - Look for the scheduler "critical section" queries, DAG-run / TI state
    lookups, `xcom` reads.
- **Dead tuples / bloat & autovacuum** — high-churn tables (`task_instance`,
  `dag_run`) need aggressive autovacuum or they bloat and plans degrade.
- **Missing indexes** — long-running deployments expose queries that were fine
  at small scale; check for seq scans on big tables. (Newer Airflow versions
  add migrations for these — an upgrade can fix a slow query for free.)
- **Connection pool** — `sql_alchemy_pool_size` + `max_overflow` vs. DB
  `max_connections`; pool exhaustion shows as latency spikes / timeouts.

### Fixes / maintenance

- **`airflow db clean`** (retention on `task_instance`, `dag_run`, `log`, `xcom`,
  etc.) — run regularly, not once. This is the cleanup job that "silently failed
  for months" in the debugging talk — monitor it.
- Archive vs. delete: `db clean` can move rows to `_archive` tables; still need
  to drop those.
- Drop unused indexes; add missing ones; `VACUUM (FULL/ANALYZE)` or
  `pg_repack` bloated tables during a window.
- Keep Airflow patch-current — schema migrations often add the index you need.
- Consider partitioning / shorter retention for `log` and `xcom`; move large
  XCom payloads to a custom backend (S3/GCS) instead of the DB.

### The drift pattern to remember

A fresh staging DB won't reproduce a prod slow query, because the planner
behaves differently at 10k rows vs. 50M rows with bloat. Test upgrades against a
**prod-sized** (restored) metadata DB, or you'll miss the regression.

### Layer 4 — the short version (what the talk actually said)

1. **Run the cleanup job** (`airflow db clean`) and **verify table/index sizes
   actually went down** afterwards — don't assume it worked.
2. **Check XCom size before an upgrade.** The `xcom` table can be huge; a
   migration that rewrites or re-indexes it can take hours / lock things / blow
   up disk. Shrink it first.
3. **Indexes matter.** Missing/unused indexes are a top cause of the slow
   queries; a big upgrade migration on an unindexed, bloated table is where
   upgrades go wrong.

## Layer 5 — upgrade effects (approaching upgrades with confidence)

The talk's framing: config choices and accumulated state can *amplify or cancel*
the performance gains of a new version. Don't upgrade blind.

### What changes across versions that can move your numbers

- **Airflow 3 architecture** _(inferred / from release notes)_: Task Execution
  API + Task SDK (workers no longer talk to the DB directly — they go through
  the API server), FastAPI REST API, scheduler improvements for high concurrency,
  backfills as first-class async scheduler concept.
  - Upside: less DB contention from workers, faster API.
  - Watch: the **API server** becomes a new component on the critical path —
    size and monitor it; new failure mode if it's under-provisioned.
- **New defaults** — `parallelism`, `max_active_tasks_per_dag`,
  `worker_concurrency`, parsing settings, executor defaults can differ between
  versions. A "slower after upgrade" report is often just a changed default.
- **Schema migrations** — can add indexes (free speedup) *or* run long on big
  tables (downtime risk). Time them on a prod-sized DB copy first.
- **Provider package bumps** — operators pulled in with the upgrade may change
  behavior / imports / memory.
- **Deprecations** — e.g. SLA → Deadline Alerts (3.1), SubDAGs removed, some
  config keys renamed.

### How to upgrade with confidence (the framework)

1. **Baseline** the current deployment: capture the Layer 1–4 metrics
   (parse time, schedule delay, queue time, task duration p50/p95, DB query
   latency, key table sizes) over a representative window.
2. **Clean first** — run `airflow db clean`, shrink XCom/logs, so the migration
   and the comparison aren't polluted by drift.
3. **Restore a prod-sized metadata DB** into a staging env; run the DB migration
   there and **time it**; watch for locks.
4. **Replay representative DAGs** on the new version in staging with the same
   config; diff the baseline metrics.
5. **Reconcile config** — explicitly set the knobs you care about rather than
   inheriting new defaults; note any that changed.
6. **Roll out** with the same dashboards up; compare against baseline for the
   first few days (perf drift / plan changes show up late).
7. Keep the old version's config diff and the migration timing recorded for next
   time.

**Sources (Layers 3–5):**
[Airflow – Best practices (top-level code, parsing)](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html),
[Airflow – Deferrable operators & triggers](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html),
[Google Cloud – Troubleshooting the triggerer](https://docs.cloud.google.com/composer/docs/composer-2/troubleshooting-triggerer),
[Apache Airflow 3 GA announcement](https://airflow.apache.org/blog/airflow-three-point-oh-is-here/),
[Medium – Scaling with Airflow 3.2: defer vs native async](https://medium.com/apache-airflow/scaling-with-airflow-3-2-when-to-defer-and-when-to-use-native-async-operators-78a1ba35237d)
