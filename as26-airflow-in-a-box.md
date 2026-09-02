# AS26: Airflow in a Box: Methodology or Madness?

**Event:** Airflow Summit 2026

## Session summary

Airflow testing today is a patchwork:

- You can validate code and catch obvious breakage early.
- But many production failures live in the **seams** — runtime state,
  persistence, serialization boundaries, API behavior, and how a real deployment
  executes work across components.
- The fast tools are valuable but don't fully model Airflow as a *system*.
- The default development posture nudges toward single-process behavior and away
  from realistic concurrency and state interactions.

Result: the familiar trade — **quick feedback vs. meaningful confidence**.

"Airflow in a Box" is a step toward collapsing that trade: making deeper, more
production-relevant tests accessible without spinning up a full, heavyweight
instance for every iteration.

The talk covers: methodology, quantifying "slickness", and real code.

## My notes / observations

- It's a **pytest plugin**: `pytest-airflow-in-a-box`
  (https://pypi.org/project/pytest-airflow-in-a-box/).
- Already published on **PyPI**. `pip install pytest-airflow-in-a-box`.
- Doesn't depend on Airflow directly (both AF 2.x monolith and 3.x core install
  as `apache-airflow`); an `airflow3` extra pins `apache-airflow>=3.1,<4`.
- Not to be confused with the older, unrelated `pytest-airflow` plugin (runs
  tests *inside* a DAG).
- Lets you develop and test DAGs/Airflow behavior **locally** as unit tests —
  without a full heavyweight Airflow instance per iteration.

- The machinery mirrors what powers Airflow's own upstream **Breeze** testing
  tool: `devel-common/src/tests_common/pytest_plugin.py` in apache/airflow
  (`tests_common` was moved into the `devel-common` project in PR #47281).
- **Note to self:** go read that upstream source
  (`devel-common/src/tests_common/pytest_plugin.py`).
- **Is `pytest-airflow-in-a-box` used by the upstream suite? No.** It's a
  third-party plugin (maintainer: GitHub `nredd`), not an Apache project, not in
  Airflow CI. Upstream uses its *own* `tests_common` plugin. The PyPI package
  borrows the same ideas (disposable metadata DB, isolated `AIRFLOW_HOME`, DAG
  fixtures, corpus smoke checks) and repackages them for external teams' DAG
  repos.

- Useful behavior the plugin provides:
  - **Bootstrap** — stands up a minimal Airflow environment for the test session.
  - **Metadata DB** — real metadata database (so serialization / persistence /
    state interactions get exercised, not mocked).
  - **DAG fixtures** — pytest fixtures to load and drive DAGs under test.
  - **Cleanup** — tears down / resets state between tests.

_(add more during/after the talk)_

## How the PyPI package works (from docs)

Docs: https://nredd.github.io/pytest-airflow-in-a-box/ ·
Repo: https://github.com/nredd/pytest-airflow-in-a-box

- Runs DAGs **without scheduler / webserver / live Airflow** — but against a real
  metadata DB, so it exercises trigger rules, branch skips, rendered templates,
  connection resolution, operator serialization.
- Python 3.10–3.14, pytest 8+, Airflow 3.1+ or 2.7–2.11. Linux/macOS.

### Install

```bash
uv add --dev "pytest-airflow-in-a-box[airflow3]"
# or, if Airflow is already pinned in the project:
pip install pytest-airflow-in-a-box
```

Extras: `airflow2`, `airflow3`, `postgres`, `xdist`. Sanity check:
`pytest --airflow-doctor`.

### Key fixtures

| Fixture | Purpose |
|---|---|
| `dag_bag` | Parses the DAG folder once per worker; `dag_bag.dags["id"]` |
| `run_dag` | Executes a real DAG, returns a result (`.success`, `.order`, `.xcoms`) |
| `dag_maker` | Define a DAG inline in the test; `dag_maker.run()` |
| `run_task` | Run a single operator, no DB |
| `airflow_home` | Isolated `AIRFLOW_HOME` |
| `airflow_variables` / `airflow_connections` | Seed vars / connections |
| `cap_structlog` | Assert on task log output |
| `dag_corpus` | Check all DAGs at once |
| `api_client` | Hit a live Airflow API |

### Example — test a DAG from the repo

```python
def test_my_dag(dag_bag, run_dag):
    dag = dag_bag.dags["my_dag_id"]
    result = run_dag(dag)
    assert result.success
    assert result.order == ["extract", "load"]
```

Run: `pytest --dag-folder=dags`

### Example — author a DAG inline

```python
from airflow.sdk import task

def test_dag(dag_maker):
    with dag_maker():
        @task
        def produce() -> int:
            return 21

        @task
        def consume(value: int) -> int:
            return value * 2

        consume(produce())

    result = dag_maker.run()
    assert result.success
    assert result.xcoms == {"produce": 21, "consume": 42}
    assert result.order == ["produce", "consume"]
```

### Example — branch logic

```python
from airflow.sdk import task
from pytest_airflow_in_a_box.matchers import skipped

def test_branch_skips_the_unselected_path(dag_maker):
    with dag_maker(dag_id="branching"):
        @task.branch
        def choose() -> str:
            return "chosen"

        @task
        def chosen() -> None: ...

        @task
        def rejected() -> None: ...

        choose() >> [chosen(), rejected()]

    result = dag_maker.run()
    assert result.order == ["choose", "chosen"]
    assert result["rejected"] == skipped()
```

### Example — single task, no DB

```python
from airflow.sdk import task

@task
def add(x: int, y: int) -> int:
    return x + y

def test_add(run_task):
    result = run_task(add(1, 2).operator)
    assert result.xcoms["return_value"] == 3
```

### The "fidelity ladder" — how far across boundaries a test goes

The plugin frames its fixtures as rungs, trading speed for realism:

| Rung | Fixture | What it proves |
|---|---|---|
| 0 | `render_task` / `task_context` / `run_task` | One operator, **no DB**. Fake supervisor runs the Task SDK message loop; no ORM rows. |
| 1 | `dag_maker.run_ti` | One task against the **real metadata DB** — persisted `TaskInstance`, real XCom rows; custom triggers with `run_triggerer=True`. |
| 2 | `dag_maker.run()` / `run_dag()` | Whole `DagRun`, real state — final states, per-task XComs, order. |
| 3 | `run_dag(..., executor=...)` | **Executor-driven / cross-worker.** |
| 4 | triggerer-only, `api_client` REST | Off the ladder — separate axes. |

**Rung 3 is the "cross worker boundary" bit.** Pass `executor=` only when the
test must prove that **workers can re-import the DAG and round-trip workloads
through Airflow's Task Execution API**. Details:

- DAG must come from a real file in the configured DAG folder (not `dag_maker`).
- The plugin **starts the API server lazily** and dispatches task instances
  **one at a time** — serial. So it verifies the worker↔API serialization
  round-trip, but does **not** exercise executor *concurrency*.
- Can't combine with `run_triggerer=`.
- A minimal `SerialExecutor` is provided for Airflow 3.1–3.3.

So "cross worker boundary" = real re-import + Task Execution API round-trip, not
parallelism. `xdist` is supported (extra), but worker isolation limits apply to
`clear_db` and `dag_ids=None` operations.

### Multi-team / connections & variables — no first-class "multi-team mode"

- `airflow_connections` / `airflow_variables`: **function-scoped** callables that
  commit `Connection` / `Variable` rows and **delete them at teardown**.
  Isolation is per-test, not per-team.
  ```python
  airflow_connections({"warehouse": {"conn_type": "sqlite", "host": str(database)}})
  ```
- `--dag-folder` takes a **single** folder. No documented support for multiple
  folders, bundles, per-team namespaces, or pools.
- Repo-wide check = `dag_corpus` fixture (session-scoped, read-only,
  process-portable) + `--airflow-smoke`.
- For a multi-team monorepo: point `--dag-folder` at a parent dir, run corpus
  smoke checks across all DAGs, and have each test seed the connections/vars its
  DAG needs.

### Scope philosophy

- **Test what your team owns:** TaskFlow callables, custom operators, task wiring
  (trigger rules, branching), cross-DAG behavior, retries, rendered templates,
  and extension points you own (connection types, timetables, listeners,
  executors, policies, triggers).
- **Don't** retest stock Airflow internals — Airflow's own suite owns "XCom
  transports a value", "a stock timetable schedules", "a stock operator
  serializes".
