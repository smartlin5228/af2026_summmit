# AS26: Debugging the Undebuggable: Lessons from Real Airflow Incidents

**Event:** Airflow Summit 2026

## Session summary

Debugging Airflow failures in production can be harder than building the pipelines
themselves. Common issues with little root-cause visibility:

- Disappearing DAGs
- Hanging tasks
- Missing logs
- Zombie tasks
- Sudden performance degradation

Over the past year, while supporting multiple Airflow deployments and
integrations, the presenters investigated several such incidents across different
teams and environments. This session shares lessons from those real debugging
cases — how each issue was diagnosed and resolved.

## Incident areas walked through

- Scheduler behaviour
- Concurrency limits
- Memory pressure
- Process-level failures

For each case: symptoms → investigation approach → root cause.

## Attendee takeaways

- How to systematically debug complex Airflow failures
- Which components commonly hide the root cause
- Practical signals to watch in logs and metrics

## Cases (live notes)

### Case 1 — DAGs stopped importing after an Airflow upgrade

- **Symptom:** After an Airflow upgrade, DAGs stopped importing. Showed up as a
  serialization error. Not isolated to one DAG — imports failed across many/all
  DAGs (the serialization error was just one visible symptom).
- **Investigation:** Looked like a DAG bug at first.
- **Root cause:** Version mismatch. The Airflow upgrade bumped the Python version,
  which broke a dependency pin. So an environment/version problem masquerading as
  application code failure.
- **Lesson:** "Looks like a DAG bug but is actually a version mismatch." When an
  upgrade breaks imports broadly, suspect the runtime (Python version, dependency
  pins) before the DAG code.

### Case 2 — A successful task marked as failed

- **Symptom:** A task succeeds on retry, then flips to failed — with no new run /
  no new try.
- **Root cause:** A stale zombie callback lands late and overwrites the real
  result. The callback belonged to an earlier try number.
- **Fix:** Skip callbacks that belong to an older try number than the current one.
- **Lesson:** Late-arriving callbacks from previous attempts can clobber final
  state; guard state transitions with the try number.

### Case 3 — All DAGs slowed down overnight

- **Symptom:** Every DAG got slower overnight. No new deployment, no code change —
  degradation appeared on its own.
- **Root cause:** A maintenance/cleanup job was failing. The code caught the
  error and reported it as success anyway — so it had silently been failing for
  *months*. Underlying issue: the XCom table in the metadata DB kept growing
  because cleanup never actually completed. Airflow itself did surface signals —
  it reported the cleanup task as failed / timed out — but that was being
  swallowed.
- **Fix:** Remove the timeout on the cleanup job and let it run long enough to
  clear the accumulated backlog; stop catching-and-swallowing the error.
- **Lesson:**
  - An overly broad `try/except` that marks things "success" hides slow-building
    problems for a very long time.
  - Metadata DB bloat (XCom especially) degrades *all* DAGs, not one.
  - Trust Airflow's own failure/timeout reporting on maintenance tasks.

### Case 4 — Queued tasks vs. pods created mismatch

- **Symptom:** Discrepancy between the number of tasks queued and the number of
  pods actually created (KubernetesExecutor). Tasks sitting queued / not enough
  pods spinning up.
- **Root cause:** Config drift between layers — the infra-level setting (e.g. k8s
  namespace / cluster limits, ResourceQuota) drifted out of sync with Airflow's
  pod/task concurrency limits (`parallelism`, `max_active_tasks`, pod limits).
  Each layer was individually "fine" but they no longer agreed.
- **Lesson:** Concurrency is enforced at multiple layers (Airflow scheduler,
  executor, k8s). When throughput stalls, check that those limits are consistent
  with each other — a lower ceiling anywhere silently caps everything.

## TODO for us

- Set up / verify XCom (and general metadata DB) cleanup for our own
  deployment(s) — same failure mode could bite us.
