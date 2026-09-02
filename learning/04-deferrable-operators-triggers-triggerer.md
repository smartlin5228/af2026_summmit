# 04 — Deferrable operators, triggers, and the triggerer

**Why this matters:** this one concept underpins four AS26 notes —
`as26-taming-ai-workloads-dag-patterns.md` (the watcher pattern),
`as26-trigger-queues-datadog.md` (routing triggers by queue),
`as26-deadline-alerts.md` (async callbacks run on the triggerer), and the "you
don't need custom async code" claim in `as26-resumable-task-execution-spark.md`.

---

## The problem it solves

A task that just **waits** for something — a sensor polling for a file, an
operator polling an external job to finish — still holds a **worker slot** the
whole time, doing nothing. 500 sensors waiting = 500 slots gone.

- `mode="poke"` (default sensor): holds the slot, sleeps, pokes, repeat.
- `mode="reschedule"`: releases the slot between pokes, but a full task
  re-schedule each cycle → scheduler/DB churn, and still one slot *per poke*.
- **Deferrable**: the task releases its slot **entirely** and a single lightweight
  async coroutine on the **triggerer** does the waiting for many tasks at once.

---

## The mechanism

```
┌─ worker ─────────────┐
│ operator.execute()   │  do setup (submit job, register), then:
│   self.defer(         │
│     trigger=T,        │  ── raises TaskDeferred, worker slot RELEASED
│     method_name=...)  │
└──────────────────────┘
            │  TI state → "deferred", trigger T serialized to metadata DB
            ▼
┌─ triggerer ──────────────────────────────────┐
│ one asyncio event loop                        │
│ T.run():  while not done: await asyncio.sleep │  runs concurrently with
│           ...                                 │  ~1000 other triggers
│           yield TriggerEvent({...})           │  ── event recorded
└──────────────────────────────────────────────┘
            │  scheduler sees the event, re-queues the TI
            ▼
┌─ worker (a new slot) ────────────────────────┐
│ operator.execute_complete(context, event)     │  resume here (method_name)
│   raise  → task fails                         │
│   return → task succeeds (return value → XCom)│
└──────────────────────────────────────────────┘
```

### `self.defer(trigger, method_name, kwargs=None, timeout=None)`

- `trigger` — an instantiated `BaseTrigger` subclass. **Serialized to the DB.**
- `method_name` — the operator method to call on resume (convention:
  `execute_complete`).
- `kwargs` — extra kwargs passed to that method.
- `timeout` — a `timedelta`; if the trigger doesn't fire in time the task
  **fails from the triggerer** (see gotchas). Default `None` = wait forever.

### The trigger class

```python
import asyncio
from airflow.triggers.base import BaseTrigger, TriggerEvent

class JobDoneTrigger(BaseTrigger):
    def __init__(self, job_id: str):
        self.job_id = job_id                       # must be serializable

    def serialize(self):
        return ("my.module.JobDoneTrigger", {"job_id": self.job_id})

    async def run(self):                           # async generator
        while True:
            status = await self._poll(self.job_id) # await, never time.sleep()
            if status in ("done", "failed"):
                yield TriggerEvent({"status": status})
                return
            await asyncio.sleep(30)

    async def cleanup(self):                       # optional
        await self._close_client()                # LOCAL resources only
```

- **`run()`** — async generator. Must `await asyncio.sleep()` (never
  `time.sleep()` — that blocks the whole loop). Must `yield` at least one
  `TriggerEvent`. Returning without yielding is an error.
- **`serialize()`** — returns `(classpath, kwargs)`. This is how the trigger is
  reconstructed — see next section.
- **`cleanup()`** — always called after `run()` exits (success, timeout,
  triggerer shutdown, kill). Local resource cleanup only.
- **`on_kill()`** — called **only** on explicit human task action
  (clear/fail/mark-success), **never** on triggerer restart. → the safe place to
  cancel the external job.

### The operator side

```python
from airflow.sdk import BaseOperator

class SubmitAndWaitOperator(BaseOperator):
    def execute(self, context):
        job_id = self._submit()                    # runs on a worker
        self.defer(trigger=JobDoneTrigger(job_id), method_name="execute_complete")

    def execute_complete(self, context, event):    # runs on a worker, after event
        if event["status"] == "failed":
            raise AirflowException("job failed")
        return event.get("result")
```

**Limitation:** you can only `self.defer()` from a **class-based operator's**
method — not from a `@task` / TaskFlow Python function directly. (The
`common-ai` `@task.agent` and specific deferrable-aware decorators are exceptions
built for it.)

---

## The triggerer process

- **A single asyncio event loop** running **many** trigger coroutines at once —
  default capacity **1000** (`[triggerer] default_capacity` / `--capacity`).
- **One blocking call anywhere in any `run()` blocks the entire loop.** Every
  deferred task on that triggerer stalls. Watch `triggers.blocked_main_thread`.
  → never do sync I/O or CPU-bound work in a trigger.
- **Triggers are serialized, not held in memory.** They live in the DB as
  `(classpath, kwargs)`. Consequences:
  - On triggerer restart / redistribution, the trigger is **re-instantiated on
    another triggerer and `run()` starts from the top** — it does *not* resume
    mid-coroutine.
  - A trigger can even run on **two triggerers at once** briefly during a network
    partition.
  - → **`run()` must be idempotent / side-effect-free.** Submit the job in the
    operator's `execute()`, only *poll* in the trigger.
- **HA:** run multiple triggerers; Airflow heartbeats them and reschedules
  triggers off a dead host after `~2.1 × triggerer.job_heartbeat_sec`. In 3.2+,
  set `[triggerer] max_trigger_to_select_per_loop` (default 50) well below
  capacity to spread load.
- Triggers **persist yielded events / state in the metadata DB** — they can't use
  a custom XCom backend. High fan-out (10k tiny triggers) is expensive; every
  deferral is a `worker → scheduler → triggerer → scheduler → worker` round trip.

---

## The three uses of a `Trigger` (from the trigger-queues note)

| Use | How it starts | On fire | Covered by trigger-queues PR #59239? |
|---|---|---|---|
| **Task-deferred** (this doc) | operator calls `self.defer(...)` | the **task resumes** (`execute_complete`) | ✅ queue inherited from the deferring task |
| **Event-driven** (AIP-82, `AssetWatcher`) | attached to an `Asset`, runs continuously | creates an **asset event** → schedules a DAG | ❌ no deferring task → no queue; coming ~3.4 |
| **Callback-based** (Deadline Alerts `AsyncCallback`) | attached to a deadline | runs the **callback** | ❌ |

Event-driven triggers must inherit `BaseEventTrigger` (a subclass of
`BaseTrigger`) — not every trigger is safe to run in the continuous,
not-tied-to-a-task mode.

---

## Gotchas

1. **`on_kill()` fires only on human intent** — not on triggerer restart,
   redistribution, timeout, or normal completion. Right place to cancel an
   external job; wrong to assume it always runs. (Known bug #36090: historically
   not called on clear/fail for deferrable ops either.)
2. **A task can be failed *from the triggerer*** — `defer(timeout=...)` expires,
   or `run()` raises — and then it **never resumes on a worker**, so
   `execute_complete` and `on_kill` are **skipped**. External job orphaned. Need
   a reaper DAG or `on_task_instance_failed` listener as backstop. (See
   `as26-taming-ai-workloads-dag-patterns.md`.)
3. **`run()` restarts from the top** on any triggerer restart → no side effects
   in the trigger.
4. **Transient errors in the poll loop** — catch, back off, `continue`. Don't
   `yield` a failure event for a network blip.
5. **Not every operator has a deferrable mode** — many take `deferrable=True`
   (often defaulting from `[operators] default_deferrable`), but you have to
   check.

---

## When to use / not use

**Use it for:** long waits (minutes → hours) — external job polling, time
sensors, file/partition sensors, "wait until 9am".

**Don't** reach for it when:
- the wait is very short / high-frequency — the deferral round-trip overhead
  (worker→triggerer→worker) can exceed the work. For high-throughput async I/O,
  a **native async operator** (does the I/O in-process on the worker with
  `asyncio`) can beat deferring 10k times. (David Blain's AS 3.2 talk.)
- you just need human input — HITL operators don't defer / don't need a
  triggerer.
- you'd have to do blocking work in `run()` — that defeats the purpose and
  starves the loop.

---

## One-paragraph version

A **deferrable operator** does its setup on a worker, then calls
`self.defer(trigger=T, method_name="execute_complete")`, which frees the worker
slot and hands `T` (serialized to the DB) to the **triggerer** — a single async
event loop running ~1000 trigger coroutines concurrently. `T.run()` polls with
`await asyncio.sleep()` and `yield`s a `TriggerEvent`; the scheduler then
re-queues the task on a fresh worker, resuming at `execute_complete(context,
event)`. Triggers are re-instantiated (not resumed) on triggerer restart, so
`run()` must be idempotent; one blocking call in any `run()` stalls every
deferred task; and a deferral that times out fails the task *without* resuming
it, skipping `execute_complete`/`on_kill`.
