# 03 — Airflow assets (data-aware scheduling)

**Why this matters:** in the multi-team model, **assets are the only sanctioned
way one team's pipeline can drive another's** (`as26-multi-team-airflow.md`). And
"event-based triggers" from the trigger-queues note are asset watchers. New
concept — worth getting right before evaluating multi-team.

> Naming: "Asset" was called **"Dataset"** before Airflow 3.0. `Dataset` still
> works as an alias. Old docs/blogs say dataset.

---

## What an asset *is*

A **named, logical handle for a piece of data** — a table, a file, an S3 prefix,
a model artifact. Identified by a **name** and/or a **URI**.

```python
from airflow.sdk import Asset

raw_events   = Asset("s3://lake/raw/events.parquet")               # uri only
curated      = Asset(name="curated.events", uri="s3://lake/curated/events")
model        = Asset("s3://models/churn.pkl", extra={"team": "ml"})
```

**It is metadata only.** Airflow never reads or writes the actual data. An asset
is a **coordination token** — a name that producers and consumers agree on.

---

## Producing: a task emits an "asset event"

Declare `outlets=[asset]` on the task that writes the data. When that task run
**succeeds**, Airflow records an **asset event** (a row: "asset X updated at
time T by task Y", plus optional `extra` metadata).

```python
@task(outlets=[raw_events])
def ingest():
    ...  # write the parquet file
    # success → asset event for raw_events is emitted
```

Or the `@asset` decorator (task + asset in one):

```python
@asset(uri="s3://lake/raw/events.parquet", schedule="@daily")
def raw_events():
    ...
```

**Attach metadata to the event** (so consumers can read it):

```python
@asset(schedule=None)
def raw_events(self, context):
    df = extract()
    context["outlet_events"][self].extra = {"row_count": len(df)}
    # or:  yield Metadata(self, {"row_count": len(df)})
```

⚠️ The event fires on task **success**, not on the data actually changing. A
no-op task still emits. Conditionally emit if you need "only when changed".

---

## Consuming: schedule a DAG on assets

Instead of `schedule="@hourly"`, schedule on asset events:

```python
@dag(schedule=raw_events)                       # runs when raw_events updates
def transform(): ...

@dag(schedule=raw_events & reference_data)      # AND — both must have new events
def transform(): ...

@dag(schedule=raw_events | backfill_signal)     # OR — either one
def transform(): ...
```

- Airflow tracks, per consuming DAG, which asset events it has already consumed.
  When the condition is met, it creates a DagRun and marks those events consumed.
- The run gets `context["triggering_asset_events"]` — you can see exactly what
  fired it.
- Read a specific upstream asset's recent events via `inlets`:

```python
@task(inlets=[raw_events])
def transform(context):
    events = context["inlet_events"][raw_events]
    last_row_count = events[-1].extra["row_count"]
```

---

## Why assets beat the alternatives for cross-DAG / cross-team

| Mechanism | Coupling | Problem |
|---|---|---|
| `TriggerDagRunOperator` | A must name B | A owns the fan-out; A imports/knows B; hard to add consumers |
| `ExternalTaskSensor` | B polls for A's task | brittle schedule alignment; burns a worker/trigger slot; B must know A's `dag_id` + timing |
| **Asset** | A says "I produce X", B says "I consume X" | neither imports the other; metadata DB brokers it; add/remove consumers freely |

The asset is a **contract**: B depends on "X exists and is fresh", not on A's
internals. A can refactor freely while the asset name/shape holds.

**Data-driven pipeline across teams:**
```
[team-ingest] ingest → outlets=[raw.events]
        │  (asset event)
        ▼
[team-transform] @dag(schedule=raw.events) → outlets=[curated.events]
        │
        ▼
[team-reporting] @dag(schedule=curated.events)
```
No team imports another's code. (Multi-team adds `AssetAccessControl` to gate
which events cross team boundaries — see the multi-team note.)

---

## Event-driven: AssetWatcher (AIP-82)

Beyond "a task produced it" — an asset can **watch an external event source** and
emit asset events when messages arrive. This is the **"event-based trigger"** from
`as26-trigger-queues-datadog.md`.

```python
from airflow.sdk import Asset, AssetWatcher
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger

trigger = MessageQueueTrigger(queue="https://sqs.us-east-1.amazonaws.com/123/my-q")
inbound = Asset("x-events://inbound", watchers=[AssetWatcher(name="sqs", trigger=trigger)])

@dag(schedule=inbound)          # DAG runs when a message lands on the queue
def handle(): ...
```

- The watcher's trigger runs on the **triggerer** (async), continuously.
- Must inherit `BaseEventTrigger` (not every trigger qualifies).
- Sources: Kafka, SQS, Pub/Sub, Azure Service Bus (via `common.messaging`).
- **Watch for *events* (state changes), not persistent conditions** — a watcher
  that keeps seeing "file exists" will keep triggering forever.

**vs. task-produced events:** task events are a *reactive byproduct* of completed
work inside Airflow; watchers *proactively* pull from systems outside Airflow.

---

## AssetAlias — indirection

The producing task decides the concrete asset **at runtime**:

```python
@task(outlets=[AssetAlias("my-task-outputs")])
def run(*, outlet_events):
    outlet_events[AssetAlias("my-task-outputs")].add(
        Asset("s3://bucket/partition=2026-09-02"), extra={"rows": 1000}
    )
```

Consumers schedule on the alias; they get events for whatever concrete asset the
producer resolved to.

---

## Gotchas / notes

- **Dataset → Asset** rename in 3.0; `Dataset` is a back-compat alias.
- Asset-scheduled DAGs don't have cron `data_interval` semantics; logical-date
  behaviour differs. No automatic catchup/backfill like cron.
- Event fires on task **success**, not real data change (unless you gate it).
- URIs must be RFC-3986; `airflow://` is reserved; use `x-...://` for
  semantic-free names.
- There's an **Assets view** in the UI showing the producer/consumer graph.

---

## One-paragraph version

An **asset** is a named handle for a dataset (metadata only). A task with
`outlets=[asset]` emits an **asset event** on success; a DAG with
`schedule=asset` (or `a & b` / `a | b`) runs when the matching events arrive —
this is **data-aware scheduling**, and it couples producer and consumer only
through the asset name, not their code. An **AssetWatcher** lets an asset be fed
by an external queue (Kafka/SQS/…) instead of a task — that's the "event-based
trigger." In multi-team, `AssetAccessControl` on the asset gates which events
cross team boundaries, making assets the one cross-team coordination primitive.
