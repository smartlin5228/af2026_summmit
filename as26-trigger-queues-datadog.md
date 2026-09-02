# AS26: Triggers at Datadog — What Are Trigger Queues and Why You Should Use Them

**Event:** Airflow Summit 2026 · Day 3

> Related: `as26-multi-team-airflow.md` (per-team triggerers via
> `airflow triggerer --team-name`) and the triggerer notes in
> `as26-optimising-airflow-real-world.md` / `as26-taming-ai-workloads-dag-patterns.md`.

## Context

- Datadog: ingests **>100 trillion events/day**.
- Adopted Airflow internally **after 3.0.0**; internal Airflow platform grew
  organically and fast — **~60 teams now** on the internal platform.
- They run on **"ADP Airflow"** — _(clarify: their internal platform name? e.g.
  "Airflow Data Platform" / a Datadog internal product name. Not Astronomer
  Astro. Confirm what ADP stands for.)_
- Platform challenges from that growth: **multi-tenancy, scalability, bespoke
  runtime environments**.

## The idea: trigger queues

- They **extended Airflow triggers** with **trigger queue assignment** to
  support multi-tenant deployments.
- Contributing it **upstream**.
- Talk covers: conceptual design + motivation for trigger queues, and why the
  pattern helps **both multi-tenant and single-tenant** Airflow.

## My notes / observations

### Datadog's multi-tenancy model

_(capturing during talk)_

- They're also covering **how their internal Airflow multi-tenancy works** —
  context for why trigger queues were needed.
- Compare against: Capital One (per-tenant instances,
  `as26-airflow-as-a-platform.md`) and upstream multi-team / AIP-67
  (`as26-multi-team-airflow.md`). Datadog adopted Airflow post-3.0.0, so their
  model predates or parallels native multi-team.
- **Their starting point = shared tenancy** (one big Airflow), **centrally
  managed by a single platform team** — same shape as ours. Pain points they
  called out:
  - **deployments clobbering each other** — teams shipping DAG/dep changes into
    one shared deployment, stepping on each other
  - **noisy neighbours** — one team's heavy workload degrading everyone
- Same story as Capital One's "shared model hit a wall" and the multi-team
  talk's motivation. Recurring theme across the summit: shared Airflow works
  until team count grows, then isolation becomes the problem to solve.
- (Relevant to us — this is our situation too.)

### Their model: "worker groups"

```
┌─ core namespace ──────────────────────────────┐
│  scheduler   ·   webserver/API   ·   triggerer │   ← shared, one platform team
└───────────────────────────────────────────────┘
        │            │             │
   ┌────┴─────┐  ┌───┴──────┐  ┌───┴──────┐
   │ tenant A │  │ tenant B │  │ tenant C │   ← worker group per tenant (own k8s ns)
   │ • DAG    │  │ • DAG    │  │ • DAG    │
   │  processor│ │  processor│ │  processor│
   │ • workers │ │ • workers │ │ • workers │
   └──────────┘  └──────────┘  └──────────┘
```

- **Core namespace** (shared): scheduler, webserver/API server, **triggerer**.
- **Per-tenant "worker group"**: its own **DAG processor** + its own **workers**
  (correction — it's the DAG *processor* that's per-tenant, not just the
  bundle). Each tenant parses its own DAGs in its own namespace/env, then the
  serialized DAGs land in the shared metadata DB for the shared scheduler.
  - matches the multi-team rationale (`as26-multi-team-airflow.md`): parsing =
    executing the tenant's arbitrary top-level Python, so the processor must be
    per-tenant for isolation + per-tenant dependencies, not just the file store.
- So execution + code are isolated per tenant; the control plane is shared —
  same broad shape as upstream multi-team, but their own implementation
  (predates / parallels AIP-67).
- **Isolation boundary = the Kubernetes namespace.** Each worker group lives in
  its own k8s namespace, so tenants get **self-managed RBAC** — the tenant owns
  the RBAC/policies/quotas/network within their namespace, the platform team
  doesn't gatekeep every change. K8s namespace primitives (ResourceQuota,
  NetworkPolicy, RBAC, ServiceAccounts) do the isolation work rather than
  Airflow-level constructs.
- Nice property: leverages infra the platform team already runs; tenant
  self-service for anything namespace-scoped.
- **The gap this leaves: the triggerer is still shared** (it's in core). That's
  exactly the hole trigger queues fill — a tenant's deferred work all lands on
  one shared triggerer with no isolation. → trigger queues let you carve the
  triggerer per tenant/queue the way worker groups already carve workers.

### What changed since 2025 → why trigger queues now

- The worker-group model was the **2025** state.
- **New need in 2026:** long-running tasks that call **external services** —
  **Spark, Trino, Kubernetes** jobs — where the task defers and a **trigger**
  polls the external system for completion.
- These are exactly the deferred workloads that should be isolated:
  - a slow/blocking Trino or Spark poll on the shared triggerer starves other
    tenants' triggers
  - different tenants hit different external systems with different creds /
    network reach — the shared core triggerer has none of that per-tenant
    context
- So the worker groups isolated the *worker* side, but the moment tenants
  started deferring to external services, the **shared triggerer** became the
  new noisy-neighbour / isolation gap. → **trigger queues**: give each tenant
  (or each class of external-service trigger) its own triggerer, routed by the
  task's queue.

### Why the single shared triggerer is the problem

Two distinct issues with one unified triggerer for all tenants:

1. **Noisy neighbours (still).** All tenants' deferred work runs on one event
   loop. A tenant with thousands of triggers, or one slow/blocking
   `Trigger.run()` (a heavy Trino/Spark poll), degrades trigger latency for
   *everyone*. Worker groups fixed this for workers; the triggerer never got the
   same treatment.
2. **One identity, not many.** A unified triggerer runs as **a single service
   identity / service account**. But triggers call external systems
   (Spark/Trino/K8s) that need **per-tenant credentials and network reach**.
   With one triggerer you either give it a union of every tenant's access (bad —
   over-broad blast radius) or you can't support per-tenant auth at all.
   Per-tenant triggerers each get their own SA / creds / network policy — same
   as the worker groups.

→ trigger queues = the mechanism to split the triggerer per tenant (or per
external-service class), routed by the task's `queue`.

### Fill in
- what drove the "bespoke runtime environments" requirement? (per-tenant DAG
  processor / worker images with the tenant's own deps?)

_(add during/after the talk)_

### Prior context (from earlier sessions)

- The **triggerer** runs async `Trigger.run()` coroutines for all deferred tasks
  in one process / event loop. Known problems: one blocking trigger starves all;
  no isolation between teams' triggers; scaling is coarse (add whole triggerers).
- Multi-team (AIP-67) added `airflow triggerer --team-name X` — but that's
  team-level. **Trigger queues** sound like finer-grained routing: assign
  triggers to named queues, run dedicated triggerers per queue.

### The upstream PR — apache/airflow #59239 "Enable triggerer queues"

- **Merged 2025-12-31**, author **zach-overflow** (Datadog).
- What it adds:
  - `--queues` CLI option on the triggerer — constrain it to specific task
    queues
  - `triggerer.queues_enabled` config flag (**off by default**)
  - a **`queue` column on the `trigger` table**, auto-populated when enabled
  - DB migration + docs
- A trigger's `queue` is taken **from its deferring task's `queue`**.
- **Scope note:** the first PR intentionally **excludes event-based and
  callback-based triggers** — only task-deferred triggers get a queue. Kept
  scope small.
- **Event-based trigger support is coming next** — speaker targeting maybe
  **3.4.0**.

> **TODO (personal): what "event-based trigger" means.** Quick version to
> expand later — a `Trigger` (async coroutine on the triggerer that waits and
> fires a `TriggerEvent`) is used 3 ways:
> 1. **Task-deferred** — operator calls `self.defer(trigger=...)`; when it fires
>    the *task resumes*. Every deferrable operator/sensor. ← what PR #59239
>    covers; queue inherited from the deferring task.
> 2. **Event-based (AIP-82, external event-driven scheduling)** — trigger
>    attached to an `Asset` via `AssetWatcher`, runs *continuously, not tied to
>    a task*, listening to an external source (SQS/Kafka/message bus). When it
>    fires it creates an **asset event** → **schedules a DAG**. No deferring
>    task → no task `queue` to inherit → needs separate design.
> 3. **Callback-based** — e.g. Deadline Alerts' `AsyncCallback` runs on the
>    triggerer. No deferring task.
> So "event-based trigger" ≈ an asset watcher listening to an external queue to
> kick off DAG runs; "task-deferred trigger" ≈ a parked task waiting to resume.
> Read: Airflow docs "Event-driven scheduling" + AIP-82.
- Motivation stated in the PR: supports **AIP-67 multi-tenancy** + team-specific
  triggerer assignment.

### How it works (from PR + docs)

- (Earlier search said "Airflow 3.2.1" — but PR #59239 merged 2025-12-31, so it
  lands in a 3.2.x point release or 3.3. Treat exact version as TBD; the PR is
  the source of truth.)
- New **`--queues` CLI option on `airflow triggerer`**. A triggerer with
  `--queues alice,bob` runs **only** triggers deferred from tasks whose **task
  queue** is `alice` or `bob`; another with `--queues test_q` runs only that.
- Turn it on with config **`queues_enabled = true`** — makes a deferring task
  **pass its assigned task queue** to the newly registered trigger instance, so
  the triggerer can filter on it.
- **The routing key = the task's existing `queue`.** Not a new concept — it
  reuses the operator's `queue=` (same thing Celery/K8s workers route on). So a
  trigger inherits the queue of the task that deferred it, and triggerers are
  pinned to queues the same way workers are.
- So the answer to my earlier questions:
  - assignment is **per task** (via `queue=` on the operator / default), not per
    DAG or per trigger class
  - it's a **parallel mechanism** to `--team-name`, not the same one — you could
    combine (a team's triggerer that also only serves certain queues)
  - single-tenant benefit: isolate slow/heavy/blocking triggers onto their own
    triggerer so they can't starve latency-sensitive ones; scale triggerers per
    workload instead of one undifferentiated pool.
