# AS26: Resumable Task Execution for Long Running Tasks such as Spark

**Event:** Airflow Summit 2026 · Day 2

> Related: `as26-agentic-pipelines-on-airflow.md` (AIP-103 / AIP-105 from the
> agentic angle) and `as26-taming-ai-workloads-dag-patterns.md` (deferrable
> watcher pattern — the *async* alternative this talk says you can avoid).
>
> **For us:** we have **no Spark jobs** — attended out of curiosity. Value to keep:
> the **AIP-103 task-state-store primitive is workload-agnostic** ("resume from
> checkpoint" for any long/batch/agentic task), and **AIP-105 pluggable retry
> policies** are broadly useful. The Spark-specific `ResumableJobMixin` is N/A.

## Session summary

Leverages **Task State Management (AIP-103)** and **Enhanced Retry Policy
(AIP-105)**, both released in **Airflow 3.3**, to enable better execution of
long-running tasks:

- **Checkpointing** — persist progress mid-task
- **Sophisticated, automated retry policies** — classify failures, decide the
  response
- **Intra-task observability** — see what's happening *inside* a running task

## Scope

- Initially focused on **Apache Spark** (one of the most widely used workload
  frameworks for data engineers).
- Extensible to **long-running tasks of any type**, including **agentic
  workflows**.

## Why it matters

- Solves a key Airflow pain point: a long task that fails near the end restarts
  from zero.
- **No custom async code required** — you don't have to write a deferrable
  operator + trigger to get resumability.

## My notes / observations

### Durability vs. Recovery — two distinct things that pair up

- **Durability** (AIP-103) — *preserve good work; resume from a known state.*
  Checkpoint progress so a restart doesn't throw away what already succeeded.
- **Recovery** (AIP-105) — *retry with variations; fail over; escalate.*
  Don't just re-run identically — classify the failure and respond: back off,
  switch endpoint/provider/params, or escalate to a human.
- They compose: recovery decides *whether and how* to retry; durability makes
  the retry cheap by resuming from the last checkpoint instead of step zero.

### The "crash tax"

The cost you pay today when a Spark task's worker dies:

1. Task submits a Spark job; the worker is now sitting there tracking it.
2. Worker crashes / is evicted → **task fails** (the Spark job may still be
   running, but Airflow lost the handle).
3. Airflow **retries the task from scratch** → submits a *brand new* Spark job.

Result: you pay for the compute already done by the orphaned first job, plus the
full re-run, plus the wall-clock. That waste is the "crash tax" — and it scales
with how long the job runs and how late the crash happens.

AIP-103 fixes this: on retry the task **reconnects to the existing Spark job**
(via `ResumableJobMixin` in `SparkSubmitOperator`) instead of resubmitting.

### The mechanism — task state is what introduces durability

- The enabling primitive is the **task state store** (AIP-103): a persistent
  key-value store scoped to a task instance, readable/writable *during*
  execution, that **survives retries** (unlike an in-memory var, unlike XCom
  which is written at the end).
- Pattern:
  1. Task does work, then `task_state_store.set("checkpoint", <handle/offset>)`.
  2. Worker dies. Task fails.
  3. Retry starts; first thing it does is `task_state_store.get("checkpoint")`.
     - Found → resume from that handle/offset.
     - Not found → start fresh.
- For Spark specifically: the checkpoint is the **Spark job/application ID**, so
  the retry re-attaches to the live job (`ResumableJobMixin`) rather than
  resubmitting.
- Backend: metadata DB by default, or a worker-side backend
  (`[workers] state_store_backend`); per-key retention + GC; optional
  `clear_on_success` so successful runs don't leave state behind.
- This is the same primitive `@task.agent(durable=True)` uses to replay agent
  steps — different checkpoint content, same store.

### Can Spark actually be reconnected? — only in cluster mode

- **cluster deploy mode** (driver runs in YARN/K8s, not in the worker): driver
  survives the worker's death. Persisted handle = **resource-manager
  application ID** (YARN app ID, K8s SparkApplication/driver pod, Livy batch ID,
  Databricks run ID, EMR step ID) — *not* Spark's internal job/stage IDs.
  On retry, poll the RM for that ID:
  - `RUNNING` → re-attach, poll to completion, pull logs.
  - `SUCCEEDED` → mark task done, **no resubmit** (crash after job finished but
    before Airflow recorded it — the clean win).
  - `FAILED` / `KILLED` → per retry policy.
  - not found (RM GC'd / TTL expired) → resubmit.
- **client deploy mode** (driver runs inside the Airflow worker): worker dies →
  driver dies → job dies. Nothing to reconnect to. This whole pattern needs
  cluster mode.
- **Nuance:** "resumable" here = *don't resubmit a job that's still alive or
  already done*. It does **not** resume a *dead* Spark job from its last stage —
  that needs the Spark app itself to checkpoint (streaming checkpoints,
  idempotent partitioned writes).

### AIP-105 — pluggable retry policies

- Framing: **"retry is an action; *whether* to retry is a decision."** Today
  Airflow only has the action (`retries=N`, `retry_delay`) — the decision is
  hardcoded ("always retry, up to N, with this delay").
- AIP-105 makes the decision **pluggable**: a policy object inspects the failure
  (the exception, its type, the context, attempt number) and returns what to do:
  - retry now / retry after backoff
  - don't retry (fail fast — e.g. auth error, bad config, data error)
  - retry with a variation (different endpoint, params, provider)
  - escalate (HITL, alert)
- Classification is **at the exception level** — a rate limit, an auth error, and
  a transient network blip are three different decisions even though all are
  "the task raised".
- Pairs with AIP-103: once the policy decides "retry", durability makes that
  retry resume from the checkpoint instead of restarting.

**Built-in policy classes shown:**
- **`ExceptionRetryPolicy`** — rule-based: map exception types → decisions
  (e.g. `ConnectionError`/`TimeoutError` → retry with backoff;
  `AuthError`/`ValueError` → fail fast; everything else → default). Deterministic,
  cheap, the sensible default for most tasks.
- **`LLMRetryPolicy`** — uses an LLM to classify the failure when the exception
  type alone isn't enough (opaque error strings, vendor errors bundled into one
  exception class, stack traces). The LLM reads the error and decides
  retry / fail / back-off. More expensive per failure, but handles the messy
  long tail. (Meta: using an LLM to decide how to recover an LLM pipeline.)
- Both are just implementations of the same policy interface — you can write your
  own.

### Spark on Kubernetes — the provider setup

- The demo/impl uses the **Spark provider running on K8s pods** (Spark's native
  Kubernetes scheduler backend: a driver pod that spawns executor pods), not
  YARN or a standalone cluster.
- Fits the resumable pattern well: the **driver pod** is a stable, addressable
  K8s object with its own lifecycle, independent of the Airflow worker. The
  persisted handle is effectively the driver pod / `SparkApplication` name +
  namespace.
- On retry, reconnect = look up that pod/CR in the cluster:
  - pod `Running` → re-attach, stream status/logs.
  - pod `Succeeded` → record success, no resubmit.
  - pod `Failed` / gone → per retry policy.
- K8s gives you the reconnect primitives for free (the API server is the source
  of truth for job state); that's likely why they started here rather than YARN.

### Task state shines for large batches within one task

- Beyond single-job reconnect: when a *single task* processes a **big batch** —
  hundreds of sub-jobs / partitions / files — the task state store keeps a
  running ledger of per-item status (done / failed / pending) as checkpoints.
- On retry the task reads the ledger and only re-does the **unfinished** items,
  not the whole batch.
- This is the "step 16 → resume at 17" idea generalized to a work queue inside
  one task instance: the checkpoint is `{item_id: status}`, updated after each
  item completes.
- Useful when splitting the batch into one-task-per-item would blow up the DAG
  (parsing pressure, scheduler load) — keep it one task, get resumability from
  the state store instead.

### Two situations (how far the durability reaches)

1. **Airflow worker saves progress → task state only.**
   - The Airflow task checkpoints *its own* progress (job handle, batch ledger,
     offset) to the task state store.
   - On retry the task resumes its tracking, but it depends on the **external
     work still being intact** — reconnect to a still-running Spark job, re-poll
     a still-alive pod. If the external job itself died, you resubmit.
   - Recovers: lost *handle* / lost *bookkeeping*. Does NOT recover: lost
     *external compute*.

2. **Airflow worker saves progress → task state, AND Spark / the pod checkpoints
   itself.**
   - Two layers of durability. Airflow keeps the handle + ledger; the external
     job independently persists its own partial results (Spark structured-
     streaming checkpoints, idempotent partitioned writes, a pod writing
     intermediate output to durable storage).
   - On retry: even if the external job *also* died, resubmitting it picks up
     from *its* checkpoint, and Airflow knows (from task state) which
     sub-work was already accepted so it doesn't double-count.
   - Recovers: lost handle **and** lost external compute — full resume.

- Point: the task state store gives you (1) for free at the Airflow layer; (2)
  requires the workload framework to cooperate. Spark-on-K8s makes (2) practical
  because both the pod lifecycle and Spark's own checkpointing are available.

_(add more during/after the talk)_
