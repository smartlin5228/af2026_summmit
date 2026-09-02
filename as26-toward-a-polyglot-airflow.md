# AS26: Toward a Polyglot Airflow

**Event:** Airflow Summit 2026 · Day 3

> Related: `as26-resumable-task-execution-spark.md` mentions AIP-108 (Language
> Task SDK for Java/Go). This talk is the deeper dive on the cross-language
> direction.

## Session summary

Building on **Airflow 3's new worker structure** and the foundation laid by the
**Go SDK**, a look at how Airflow can support a **fully cross-language
DAG-authoring experience**.

## What's covered

- How a **new language SDK** is built.
- How a **task talks to Airflow** (the wire protocol / Task Execution API).
- How **multiple languages may be mixed inside a single DAG**.
- **A new middle layer** is required between Airflow and the task — to support
  additional languages *without duplicating logic* in each SDK.
- Also touched: security, distributed workload, UI considerations.

## My notes / observations

- Speaker assumes **prior familiarity with the Task SDK work** (AIP-72 Task
  Execution Interface, the Go SDK / AIP-108). If revisiting this note: read up
  on how the Task SDK decoupled task execution from Airflow core first — this
  talk builds directly on that and doesn't re-explain it.
  - Key prior context: in Airflow 3 a task no longer imports Airflow core; it
    talks to the **Task Execution API** over HTTP via a **supervisor** process.
    That's what makes non-Python SDKs possible at all.

### How Airflow runs Python today (the baseline to generalize)

Talk breaks "running a task" into pieces — each has to be answered per language:

1. **DAG → structure.** The DAG definition (tasks, deps, schedule, params) —
   currently discovered by parsing a `.py` file. For polyglot: how is structure
   declared in / extracted from a non-Python DAG?
2. **Where is the body?** The actual task logic to execute — currently a Python
   callable / operator `execute()`. For polyglot: a Go func, a JS module, a
   compiled binary, a script? How does the runner locate and invoke it?
3. **The environment to run against.** Deps, runtime, image, resource context.
   Currently a Python venv/image with `apache-airflow` + providers. For
   polyglot: each language needs its own runtime, and the DAG may mix them.

Splitting structure from body is the key move — you can parse structure in one
place (maybe still Python-side) and hand the body to a language-specific runner.

### "We need to understand how Airflow imports / understands Python"

The crux: to generalize, first be precise about what Airflow does with a `.py`
DAG file today.

- **DAG parsing = `import` the module.** The DAG processor literally does a
  Python import of the file — runs all module-level code — then walks the module
  namespace for `DAG` objects (and things registered via the `@dag` / `@task`
  decorators, `with DAG(...)` context managers, or DagBag collection).
- The DAG structure is a **side effect of executing Python**, not data read from
  a declarative file. Task dependencies (`a >> b`), dynamic task mapping,
  branching, generated DAGs — all produced by *running code*.
- So "the structure" and "the code that builds the structure" are entangled in
  the same file, executed in the same import.
- **Two hard problems for polyglot:**
  1. If structure only exists after running Python, a Go/JS DAG needs its own
     way to *emit* structure that Airflow's scheduler can consume — likely a
     serialized/declarative representation (JSON/protobuf) the language SDK
     produces, rather than Airflow importing a Go file.
  2. Airflow's scheduler works off **serialized DAGs** in the metadata DB
     already (it doesn't re-import at schedule time). That serialized form is
     the natural integration seam — a non-Python SDK produces the same
     serialized DAG. The "middle layer" the abstract mentions is probably
     exactly this: a common DAG representation + a common task-runner protocol
     that every language SDK targets.

**TODO (personal, ties to the multi-team note):** really nail down top-level code
/ the import model — same gap showed up there. One study session covers both.

### Fact-checked execution flow (Airflow 3)

My rough model was: *DAG file → processor → structure + task code → supervisor →
python env / runner → execute.* Corrected:

1. **Parse time — DAG Processor** imports the `.py` file, walks the module for
   `DAG` objects, produces a **serialized DAG** (structure + operator params +
   an **import reference** to each callable) → metadata DB.
   - ❗ Does **not** extract task code. The function body stays in the file; the
     serialized DAG only records *where* the callable is.
2. **Schedule time — Scheduler** reads serialized DAGs from the **DB** (does not
   re-import the file), creates TaskInstances, hands them to the **Executor**.
3. **Execution time — 3-process model: executor → supervisor → task runner**
   - **Supervisor** (on the worker): calls the **Task Execution API**
     (`POST /run`) → gets a `TIRunContext` (connections, vars, XComs, TI info).
     Only component with API/DB access; also monitors heartbeats/timeouts/logs.
   - Supervisor spawns the **Task Runner** subprocess — isolated, running in the
     task's **Python environment** with the DAG's deps.
   - Task Runner **loads the DAG file again** to materialize the actual
     operator/callable (2nd parse — body, not structure), runs `execute()` /
     the `@task` fn, sends results back **through the supervisor** (custom
     binary protocol) → Execution API. Never touches the DB directly.

**Corrections to my model:**
- Processor → **structure + reference only**, not task code.
- Scheduler + Executor hops sit between "structure" and "supervisor."
- DAG file is **parsed twice**: processor (structure) and task runner (body),
  in different processes / often different environments.

**Why this matters for polyglot:** the split is already there —
*structure* (serializable, could come from any language) vs *body* (loaded by a
language-specific runner). The supervisor ↔ runner boundary (Task Execution API
+ the binary protocol) is the seam a Go/JS/… SDK plugs into: implement a runner
that speaks that protocol, and Airflow core doesn't care what language ran.

## Language-agnostic design

_(capturing during talk)_

- The goal: add languages **without re-implementing Airflow logic in each SDK**.
  Thin per-language SDK, everything hard in a shared layer.
- Expected pieces (fill in with what the speaker actually shows):
  - a **common DAG representation** (serialized / declarative) every SDK emits
  - a **common task-runner protocol** (the supervisor ↔ runner contract) every
    SDK implements
  - the **middle layer** that owns retries, XCom, context, deferral, templating,
    state transitions — so an SDK only marshals to/from the protocol

### Implementation — AIP-108

- **AIP-108**, shipped in **Airflow 3.3**.
- Languages: **Java and Go today**, **TypeScript soon**.
- **Execution flow with a non-Python SDK:**
  ```
  Executor → launches worker
           → worker launches a Python component: the "Coordinator"
           → Coordinator launches the language SDK (Java / Go / TS runtime)
  ```
- The **Coordinator** is the middle layer, and it's **still Python** — it does
  everything Airflow-specific (talks to the Task Execution API; owns context /
  XCom / retries / state / deferral / templating), then hands the language SDK
  only the actual task invocation over a protocol.
- So the language SDK stays thin: it receives "run this task with these inputs"
  from the Coordinator and returns the result. No Airflow logic duplicated per
  language.
- Maps onto the earlier flow: the Coordinator sits at the supervisor↔runner
  seam; the non-Python SDK plays the role of the runner.

### Fact-check — AIP-108 (verified against Airflow 3.3 docs/blog)

**My model held up, with nuances:**
- ✅ AIP-108, Airflow **3.3.0**, **experimental** ("Language Task SDK").
- ✅ **Java and Go** today. TypeScript = "soon" per the speaker (not in the
  3.3 docs I found — treat as roadmap).
- ✅ There **is** a Python **Coordinator** layer; the DAG + scheduling stay in
  Python, only the task body is another language.
- ✅ Coordinator **proxies Variables / Connections / XComs back through the
  Task Execution API** — matches "middle layer owns Airflow-specific stuff".
- **Nuance — two coordinator types:**
  - `JavaCoordinator` — for JVM languages.
  - `ExecutableCoordinator` — for self-contained native binaries (Go).
- **Nuance — Go has TWO integration styles, not one:**
  1. **Coordinator style** (matches my model): a **Python task runner** launches
     the Go bundle directly, no separate Go worker on the host — same mechanism
     as Java. `Executor → worker → Python runner/coordinator → Go bundle`.
  2. **Edge-worker style**: a long-running compiled `airflow-go-edge-worker`
     pulls work from the **Edge Executor API**, launches the user's compiled DAG
     bundle as a **go-plugin (gRPC) subprocess**, invokes the task over gRPC.
     No per-task Python coordinator in the hot path.
- Java currently **plugs into the existing Python Supervisor**; Go can do either.

So "executor → worker → Python coordinator → SDK" is **correct for Java and for
Go's coordinator style**, but Go also has a standalone-worker path that skips the
per-task Python component.

## Definition side — how you chain tasks into a DAG in another language

- **AIP-85**, planned for **Airflow 3.4+** (not shipped yet).
- Today's parse pipeline:
  ```
  DAG source (.py) ──▶ Importer ──▶ in-memory DAG objects ──▶ Processor ──▶ serialized data (DB)
  ```
- The **Importer** step is the Python-specific bit — it's `import`-ing the file
  to get in-memory `DAG`/task objects. Everything downstream (Processor →
  serialized DAG) is already language-neutral.
- AIP-85's move: make the **Importer pluggable / swappable** so a non-Python DAG
  source can produce the same in-memory objects (or emit serialized data
  directly), and the rest of the pipeline is unchanged.
- Net: AIP-108 solved the *execution* side (task body in another language);
  **AIP-85 is the *definition* side** (write the DAG structure itself in another
  language). Both needed for a "fully cross-language DAG-authoring experience".

### DAG source types AIP-85 would allow

Once the importer is pluggable, "a DAG source" is no longer just "a `.py` file":

- **Static files**
  - JSON
  - YAML
  - "safe" Python (parsed without full execution — restricted / sandboxed)
- **Executables** (the source *runs* to emit the DAG)
  - Python (today's model)
  - compiled binary (Go, etc.) — needs **target identification** (how the
    importer knows what to run and how to invoke it)
- **Exotic sources**
  - a database
  - a remote DB / remote service

Each just needs an importer that turns its representation into the in-memory
DAG objects (or serialized form). Structure stays declarative downstream.

**Fact-check on the source taxonomy:**
- The **importer abstraction is real and confirmed** — importers live in
  `task-sdk/src/airflow/sdk/importers/`, detached from the DB session (except
  when a DAG uses Variables, fetched via the Task SDK).
- Confirmed concrete importers: **`FSDagImporter`** (default, from `.py` files)
  and a **`NotebookImporter`** (from Jupyter notebooks) named as an example
  extension (explicitly "not in scope for the AIP, but implementable").
- The **static / executable / exotic** three-way split and the specific entries
  (JSON, YAML, "safe" Python, compiled binary + target identification, database,
  remote DB) look like the **speaker's own framing / roadmap slide**, not
  verbatim AIP-85 text. Plausible and consistent with the design, but treat as
  "illustrative of what becomes possible," not a committed list.
- ("Safe Python" = parse the file for structure without fully executing it —
  a real interest area the AIP notes for interactive / LLM-assisted authoring.)

**Fact-check — AIP-85 ("Extendable DAG parsing controls"):**
- ⚠️ Framed slightly broader than "polyglot importer". It's about making DAG
  **ingestion pluggable**: an abstract **DAG ingester** policy that decides
  *when / which* DAGs are parsed, taking a set of bundles (each with an
  **importer**) + a DAG store.
- Importer abstractions + registry are being **moved into the Task SDK**;
  shared file-parsing utils into `airflow_shared.module_loading`.
- Sample ingesters: `OnceIngester` (parse once, exit), `ContinuousIngester`
  (today's default reparse loop).
- So the *importer* is the pluggable per-source component (where a non-Python
  DAG source would slot in); the *ingester* is the pluggable scheduling of
  parsing. The speaker is using AIP-85 as the definition-side enabler for
  polyglot, which is a valid application of it, but the AIP itself is about
  extensible parsing generally.

## Execution — where the current Airflow execution model sits

_(capturing during talk — the speaker is re-walking the execution model as the
baseline for the polyglot execution design)_

- See the fact-checked flow above ("Fact-checked execution flow (Airflow 3)") —
  same picture: DAG processor → serialized DAG → scheduler → executor →
  worker → **supervisor** → **task runner** (in the task's runtime env).
- The **supervisor ↔ task runner** boundary is the key one for polyglot:
  - supervisor = Python, owns Task Execution API access, context, monitoring
  - task runner = where user code runs; today Python, but this is the swap point
- AIP-108's **Coordinator** slots in here (see Implementation section) — a
  Python coordinator drives the non-Python SDK over a protocol.

### The baked-in assumption

- Today's execution model **assumes a Python interpreter is present** and that
  the task runs in **basically the same environment as Airflow itself**
  (same/compatible venv, `apache-airflow` + providers importable).
- Getting the task code = "**find the package somehow**" — import the DAG module
  off the `PYTHONPATH` / bundle path. It's loose: no explicit contract for
  *where* the code is or *what runtime* it needs, just "Python will import it."
- That works because everything is Python and co-located. It **breaks** the
  moment the task is Go/Java/TS, or needs an isolated/incompatible environment.
- So the polyglot work has to **make explicit** what's currently implicit:
  - what artifact is the task (module ref? binary? bundle?)
  - what runtime does it need
  - how is it located and invoked
  — i.e. replace "find the package somehow" with a real contract (the Coordinator
  + protocol + executable-bundle spec).

**Fact-check — the "assumes Python + same env + find the package somehow"
claim:**
- ✅ Accurate. Airflow loads DAGs by **executing each Python source file** in a
  bundle and collecting the `DAG` objects; the bundle path (plus `dags/`,
  `config/`, `plugins/`) is put on **`sys.path` / `PYTHONPATH`**, and zip
  bundles are **inserted into `sys.path`** and importable by anything in the
  Airflow process.
- ✅ "Same environment" — the process importing/executing DAG code is a Python
  process that already has `apache-airflow` + providers installed; there's no
  isolation between DAG code and Airflow's own packages (docs warn about package
  name clashes for exactly this reason, and say "always use full package paths").
- ✅ "Find the package somehow" is a fair characterization — resolution is
  implicit `sys.path` ordering. Known fragility, e.g. the Airflow 3 git-bundle
  bug where the DAG processor probes `PYTHONPATH` before the git checkout
  finishes and Python **caches the dir as empty** (`sys.path_importer_cache`),
  breaking later imports.

> **TODO (personal placeholder): how Python modules are actually loaded.**
> Cover: `sys.path` / `sys.modules`, the import system (finders/loaders,
> `importlib`), `sys.path_importer_cache`, how Airflow adds bundle paths, why
> `dags/` isn't a package (no `__init__.py`) and the "top-level `.py` executed
> on import" model. This is the third time this gap has come up — see
> `as26-multi-team-airflow.md` and the top-level-code TODO earlier in this file.
> One focused study session closes all three.

### What the DAG bundle carries today

The bundle is expected to hold everything a task needs at execution time:

- **Resources** the tasks need (config files, SQL, templates, small data)
- **User code** (the DAG files + the team's own modules)
- **3rd-party dependencies** — either vendored into the bundle, or (more
  commonly) pre-installed in the worker image and just *assumed present*
- **Maybe even a runtime**, if it's portable (e.g. a self-contained binary, or a
  bundled interpreter) — this is the direction polyglot pushes: the bundle
  becomes a more complete, self-describing execution artifact rather than "some
  `.py` files that assume the ambient environment".

- This is why AIP-108 references an **"executable bundle spec"** — formalizing
  what a bundle contains and how a runner should execute it, per language.

**Fact-check + detail — the Executable Bundle Spec (for native SDKs: Go, Rust,
C++, Zig…):**
- Goal: "a single, language-agnostic **bundle shape** so scheduler, worker, and
  UI behave identically regardless of which compiled SDK produced the DAG."
  Consumed by `ExecutableCoordinator`.
- A bundle **is the compiled executable itself** with appended data regions —
  not a zip/tarball, no extraction:
  1. the **native binary** (ELF / Mach-O / PE)
  2. the **primary DAG source file** embedded (UTF-8, may be zero-length)
  3. a **build-time manifest** — `airflow-metadata.yaml`
  4. a **64-byte trailer**: `source_len`, `metadata_len`, `footer_ver`,
     `binary_sha256` (hash of binary region), reserved, and magic `"AFBNDL01"`.
- `airflow-metadata.yaml` keys: `airflow_bundle_metadata_version` ("1.0"),
  `sdk` (language, version, supervisor_schema_version), `source` (orig
  filename), `dags` (dag_id → task lists).
- **Discovery:** runtime walks `executables_root`, checks executable files for
  the `AFBNDL01` magic trailer, verifies the SHA-256, caches by
  `(path, inode, mtime, size)`.
- **Execution:** the runtime `exec`s the bundle directly with coordinator args
  (`--comm=<addr>` / `--logs=<addr>`). The binary talks back to the Coordinator
  over those sockets.
- So for native langs the "runtime" question is moot — the bundle **is**
  self-contained (statically linked binary). For JVM (Java) it's the
  Supervisor-plugged style instead, needing a JVM present.

_(fill in more from the speaker)_
