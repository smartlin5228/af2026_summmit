# 01 — Python imports, module loading, and why DAG parsing works the way it does

**Why this matters:** three AS26 notes (multi-team DAG processor, polyglot
execution model, perf Layer 1) all hinge on one fact — **parsing a DAG file means
*executing* Python, and DAG structure is a side effect of that execution.** To
really get that, you need the import system.

---

## Part 1 — What `import` actually does

`import foo` is two steps:

1. **Load** — find module `foo`, create a module object, **execute its top-level
   code once**, store it in `sys.modules["foo"]`.
2. **Bind** — bind the name `foo` in the current namespace.

The load step only happens **once per process**. Every later `import foo`
anywhere is just a `sys.modules` dict lookup — no re-execution. (This is why
`import pandas` in 50 files costs the import once, not 50 times, *within one
process*.)

```python
import sys
sys.modules["json"]        # <module 'json' ...>  — already loaded
```

### Finding the module: `sys.path` and the finders

When `foo` isn't in `sys.modules`, Python asks the **finders** in `sys.meta_path`.
The important one is `PathFinder`, which walks **`sys.path`**:

- `sys.path` is a list of directories (and .zip files).
- Built at interpreter startup from:
  1. the directory of the script being run (or `''` = cwd for `-c` / REPL),
  2. the `PYTHONPATH` env var (colon-separated),
  3. install defaults (stdlib, then `site-packages`).
- Code can mutate it at runtime: `sys.path.append("/opt/airflow/dags")`.

For each entry on `sys.path`, `PathFinder` gets a **path entry finder** (usually
`FileFinder`) and asks "do you have `foo`?" `FileFinder` looks, in order, for:

- `foo/__init__.py`  → a **regular package**
- `foo.py`           → a **module**
- `foomodule.so` / `.pyd` → a compiled extension
- (a `foo/` dir with no `__init__.py` may become a **namespace package**, PEP 420)

First hit wins. Search stops.

### The caches (and the gotcha)

- **`sys.modules`** — loaded modules. Clearing an entry forces a re-import next
  time (rarely a good idea).
- **`sys.path_importer_cache`** — maps each `sys.path` directory string → the
  finder object for it. `FileFinder` also caches a **directory listing**.
  - **Gotcha:** if a directory is probed *before it exists*, Python caches
    `None` for it. Later imports from that path then fail **even after the
    directory appears**.
  - This is a real Airflow bug (git DAG bundles): the DAG processor probes
    `PYTHONPATH` dirs at startup, before the git checkout finishes, caches them
    empty, and imports fail. Fix / escape hatch: `importlib.invalidate_caches()`
    after creating files, or don't probe early.

### Top-level code

A module's body runs **top to bottom, once, at first import**. At indent level 0:

- `x = 1`, `def f(): ...`, `class C: ...` — definitions are cheap (they just bind
  names; the body of `f` doesn't run).
- **but any *call* you write at module level runs now**: `df = pd.read_csv(...)`,
  `conf = requests.get(...).json()`, `for i in range(50): make_dag(i)`.

```python
# my_module.py
print("this runs at import time")          # side effect on import
X = compute_something_expensive()           # runs at import time
def helper():                               # defined, not run
    return X * 2
if __name__ == "__main__":                  # runs ONLY if executed directly,
    print(helper())                         #   not when imported
```

### Packages vs. loose modules — and Airflow's `dags/`

- `dags/` is **not a package** — Airflow just **adds `dags/` (and `plugins/`,
  `config/`) to `sys.path`**.
- So each `dags/*.py` is imported as a **top-level module** named by its filename.
- Consequences:
  - `from my_helpers import thing` works from any DAG file (because `dags/` is on
    `sys.path`).
  - **Two DAG files that both have a local `utils.py` next to them collide** —
    same top-level module name, first one wins, second gets the wrong module.
    Airflow's advice: "always use full, unique package paths."

---

## Part 2 — How Airflow turns a `.py` file into a DAG

The **DAG processor**, for each file in a bundle, on a loop:

1. **Executes the file as a module** (in a subprocess, for isolation).
2. Inspects the resulting module namespace for `DAG` objects — direct
   `DAG(...)` instances, `with DAG(...) as dag:` blocks, `@dag`-decorated
   functions that got called, DagBag collection.
3. **Serializes** each DAG it finds (structure + operator params + a *reference*
   to each callable, **not the callable's code**) → writes JSON to the metadata
   DB.

```python
# a DAG file — everything here runs on EVERY parse cycle
from airflow.sdk import DAG, task
from airflow.models import Variable

ENV = Variable.get("env")                    # ❌ DB round-trip every parse
CONFIG = requests.get("http://cfg/").json()  # ❌ network call every parse
import pandas as pd                          # ❌ heavy import at module scope

with DAG("my_dag", schedule="@daily") as dag:
    @task
    def extract():
        import pandas as pd                  # ✅ import inside the task instead
        ...
    @task
    def load(x): ...

    extract() >> load()                      # `>>` is an operator call that
                                             # mutates `dag` — runs at parse
```

### The load-bearing facts

- **DAG structure is a side effect of running your Python.** There is no
  declarative DAG file — `a >> b` is a method call; a `for` loop that builds 50
  DAGs *runs the loop* every parse.
- This happens **every parse cycle** — `[dag_processor] min_file_process_interval`
  (default **30s**) — for **every file** in the bundle.
- Therefore, at module scope:
  - `Variable.get()` / `Connection.get()` → hits the metadata DB every 30s, per
    file. At 500 files that's a real load.
  - API/DB calls → run every parse; a slow one **stalls the processor** and
    delays *all* DAG updates.
  - heavy imports / compute → serialized into parse time.
- **The scheduler never imports your file.** It reads the **serialized** DAG
  (JSON) from the DB. Only two components import the file:
  - the **DAG processor** (to discover structure), and
  - the **task runner** at execution time (to get the actual callable — the
    serialized DAG only has a reference).
  - → the file is parsed **at least twice**, in different processes, often
    different environments.

### Why this is a security / isolation surface

Parsing = **arbitrary code execution** with the DAG processor's identity,
filesystem access, network reach, and secrets-backend access. If one shared
processor parses every team's files, Team A's module-level code runs next to
Team B's parsing. That's the whole reason multi-team (and Datadog's worker
groups) give **each team its own DAG processor** — plus each team needs its own
Python environment for conflicting dependencies.

---

### Concrete benefits of a per-team DAG processor

| Benefit | Scenario | Shared processor | Per-team processor |
|---|---|---|---|
| **Dependency isolation** | Team A needs `pydantic<2` (dbt), Team B needs `pydantic>=2` (langchain) | one venv can't satisfy both → one team's DAGs `ImportError` every parse | each team's processor image has its own deps; both parse clean |
| **Parse-latency / blast radius** | Team C has a module-level `requests.get()` to a down service (hangs 60s) | parse loop stalls 60s/cycle → *all* teams' DAG updates delayed | only Team C's DAGs go stale |
| **Security** | Team D's file reads the SA token / a connection at module scope | runs with platform creds + every team's secrets access | scoped to Team D's SA / Vault path / NetworkPolicy |
| **Resource accounting** | Team E generates 2000 DAGs from a 5MB YAML every parse (40s CPU) | hogs parsing slots, can't attribute | own CPU limit + `parsing_processes`; per-team parse-time dashboard |

**When you don't need it:** one team, homogeneous deps, all authors trusted,
small DAG count → a single processor with a few `parsing_processes` is simpler.
The trigger to split is (a) conflicting dependencies or (b) a trust boundary
between DAG authors.

## Try it yourself (5 min)

```python
import sys
# 1. where does Python look?
print(sys.path)
# 2. what's already loaded?
print(len(sys.modules), "modules loaded")
# 3. watch the cache
print(sys.path_importer_cache)

# 4. prove top-level runs once
# make a.py containing:  print("a loaded"); X = 1
import a        # prints "a loaded"
import a        # prints nothing — sys.modules hit
a.X            # 1

# 5. prove the missing-dir cache bug
import importlib, os
sys.path.insert(0, "/tmp/latecomer")
try:
    import somemod                      # fails, caches /tmp/latecomer as empty
except ImportError:
    pass
os.makedirs("/tmp/latecomer"); open("/tmp/latecomer/somemod.py", "w").write("v=1")
try:
    import somemod                      # STILL fails — cached
except ImportError:
    print("still cached")
importlib.invalidate_caches()
import somemod                          # now works
print(somemod.v)
```

---

## The one-paragraph version

`import` finds a module by walking `sys.path`, runs its top-level code **once**
per process, and caches it in `sys.modules`; directory→finder mappings are cached
in `sys.path_importer_cache` (which can cache a *missing* directory and needs
`importlib.invalidate_caches()` to recover). Airflow adds `dags/` to `sys.path`
and imports each file as a top-level module. Because a DAG file's structure is
**built by executing its top-level code**, the DAG processor re-runs that code
every ~30s per file — so anything at module scope (Variable/Connection lookups,
API calls, heavy imports, DAG-generation loops) is a recurring cost and a
code-execution surface, which is why multi-tenant setups give each team its own
DAG processor.
