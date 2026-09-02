# AS26: Taming AI Workloads in Apache Airflow — DAG Patterns to Avoid Infrastructure Instability

**Event:** Airflow Summit 2026 · Day 2 · Builder Track

## The problem — a two-front battle with instability

1. **Airflow-side instability** — the workers themselves restart and lose track
   of active tasks:
   - Kubernetes pod evictions
   - Celery node scaling
2. **External AI-cluster instability** — the heavy-compute cluster has:
   - Temporary network blips
   - API timeouts
   - Compute rescheduling

**The failure mode:** with standard DAG designs, these transient hiccups make
Airflow panic → fail the task → **send a kill signal to an expensive,
perfectly healthy AI job.** Worst outcome: you lose the compute *and* the work.

## The approach

A specialized DAG design pattern that solves the dual-instability problem
**entirely at the code level** — not by managing infrastructure.

- Write resilient Airflow tasks that act as **fault-tolerant "watchers"** of the
  external job (submit → poll/observe → reconcile), rather than tasks that *own*
  the job's lifecycle.
- DAGs that:
  - Survive worker evictions
  - Patiently handle external AI-cluster timeouts
  - Accurately reflect the **true state** of the workload

## My notes / observations

- Speaker is leaning on the **deferrable-operator + trigger** model as the
  resilient pattern for long-running external AI jobs (training, big inference).

### Research — `on_kill()`, triggers, and long-running jobs

**The core insight: separate "the external job's lifecycle" from "the Airflow
task's lifecycle."** The task/trigger is a *watcher*, not the owner. Submit the
job, record its ID, then poll. If Airflow restarts the watcher, it re-attaches
by ID instead of killing and resubmitting.

**`on_kill()` — narrow and deliberate:**
- Called **only** when a user explicitly acts on the deferred task —
  mark-failed, clear, mark-success.
- **NOT called** on: triggerer restart, trigger redistribution across hosts,
  normal completion, or timeout.
- So `on_kill()` is the *right* place to cancel the expensive external job —
  it only fires on genuine human intent, never on a transient deploy/eviction.
  This is exactly the "don't kill a healthy AI job over a network blip" fix.
- ⚠️ Known bug (apache/airflow #36090): for **deferrable** operators, `on_kill()`
  historically was **not** called on clear/fail either — so the remote job
  leaks. Check whether this is fixed in the version in use; the speaker may have
  a workaround (listeners, or a reconcile task).

**Trigger lifecycle:**
- `run()` — async generator, must `await asyncio.sleep()` (never `time.sleep`),
  must `yield` a `TriggerEvent`.
- **Triggers are serialized, not held in memory.** On triggerer restart the
  trigger is deserialized on a new triggerer and `run()` **restarts from the
  beginning**. Can even run concurrently on two triggerers during a network
  partition.
  - → triggers must be **idempotent / side-effect-free**. Don't "submit the job"
    in the trigger — submit in `execute()`, only *poll* in the trigger.
- `cleanup()` — always called after `run()` exits for any reason (success,
  timeout, **triggerer shutdown**, user kill). Use it for **local** resources
  only (close connections, temp files). **Do NOT cancel the external job here** —
  it fires on every rolling deploy and would kill healthy jobs.

**The safe division of labor:**
| Event | What fires | What it should do |
|---|---|---|
| User clears / fails / kills task | `on_kill()` | Cancel the external job |
| Triggerer restart / eviction / redeploy | `cleanup()` + trigger re-created | Release local resources; **re-attach** to the still-running job and keep polling |
| Worker eviction (task in `execute()` pre-defer) | task retried | Re-submit only if no job ID recorded yet; else re-attach |
| External job network blip / API timeout | trigger's poll loop | Catch, back off, retry the poll — do **not** yield a failure event |
| Job actually finishes/fails | trigger yields `TriggerEvent` | Operator's `execute_complete` reconciles true state |

### Watcher pattern skeleton

```python
class SubmitAndWatchOperator(BaseOperator):
    def execute(self, context):
        job_id = context["ti"].xcom_pull(...) or self._submit_job()  # idempotent-ish
        context["ti"].xcom_push(key="job_id", value=job_id)
        self.defer(trigger=JobWatcherTrigger(job_id=job_id), method_name="execute_complete")

    def execute_complete(self, context, event):
        if event["status"] == "failed":
            raise AirflowException("external job failed")
        return event["result"]

    def on_kill(self):
        self._cancel_job(self.job_id)   # only on explicit human kill

class JobWatcherTrigger(BaseTrigger):
    async def run(self):
        while True:
            try:
                status = await self._poll(self.job_id)
            except (TimeoutError, TransientAPIError):
                await asyncio.sleep(self.backoff); continue      # ride out the blip
            if status in ("completed", "failed"):
                yield TriggerEvent({"status": status, "result": ...}); return
            await asyncio.sleep(30)

    async def cleanup(self):
        await self._close_client()      # local only — never cancel the job here
```

### "A job can be marked failed from the triggerer"

- The speaker noted the task can end up **failed via the triggerer**, not just
  via the worker. Two ways this happens:
  1. **Trigger yields a failure `TriggerEvent`** (or `run()` raises) → the
     operator resumes on a worker and `execute_complete` fails the task —
     normal, intended.
  2. **`trigger_timeout` / the deferral times out on the triggerer** → the task
     is failed **without ever resuming on a worker**, so **`execute_complete`
     never runs** and neither does `on_kill()`. The external AI job is then
     orphaned — still burning compute with nothing watching it.
- Also: if the trigger's `run()` throws an unhandled exception, same result —
  failed from the triggerer side, no worker-side cleanup path.
- **Implication for the pattern:** you can't rely on `execute_complete` or
  `on_kill` as your only cleanup path. Need a belt-and-suspenders reconcile:
  - a separate **reaper/reconcile DAG** that lists external jobs vs. Airflow
    task states and cancels orphans, and/or
  - **listeners** (`on_task_instance_failed`) to fire cancellation regardless of
    which component failed the task, and/or
  - set generous `trigger_timeout` and handle the timeout event explicitly
    (defer with a `timeout` and a `method_name` that runs cleanup).

_(add more during/after the talk — esp. how the speaker handles the pre-defer
worker-eviction window and the #36090 on_kill gap)_

**Sources:**
[Airflow — Deferrable Operators & Triggers](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html),
[apache/airflow #36090 — deferrable on_kill not called on fail/restart](https://github.com/apache/airflow/issues/36090)
