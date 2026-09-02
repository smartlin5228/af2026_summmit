# AS26: Multi-Team Airflow — A Customer-Driven Journey

**Event:** Airflow Summit 2026 · Day 2

> **The one I care about most today** — directly relevant to our managed Airflow
> platform. Compare against Capital One's per-tenant-instance fleet approach
> (`as26-airflow-as-a-platform.md`): this is the *opposite* bet — one
> environment, many teams, native support.

## Session summary

As deployments scale and DAG-author count grows: **how do you support many teams
with different needs on a shared platform?**

- Many orgs have built their **own multi-tenant layers** on top of Airflow.
- Airflow is now **adding native support** for this deployment type.
- Design worked **backwards from real deployment challenges + community pain
  points**.

## AIP-67 — introduced multi-team

The proposal behind this feature is **AIP-67** ("Multi-team deployment of Airflow
components"). Accepted; implementation landing across Airflow 3.x. Worked
backwards from how orgs already build multi-tenant layers.

**How it works (from the AIP):**
- A **`team_id`** threads through the system. Users belong to one or many teams;
  resource access is by team ownership.
- **DAG bundles determine the team** — a DB table maps bundle-name → `team_id`
  (many-to-one). That's the binding mechanism: which bundle a DAG came from
  decides its team.
- **Team-aware config** — `core.multi_team_configurations` points at per-team
  config files; `conf.get("core", "executor", team_id="team_a")`.
- **Team-aware executors** — the scheduler spins up a **set of executors per
  team**; `ExecutorName` + the executor loader are team-aware. This is how task
  execution gets isolated.
- **Team-scoped pools / variables / connections** — identified by `team_id`;
  `null` team_id = shared across all teams (common pools, common vars). Each team
  gets its own `default_pool`.
- **UI filtering** — the AuthManager filters what you see by your team
  membership.

## Key design decision: multi-**team**, not multi-**tenancy**

- **Not** strict multi-tenancy with complete isolation.
- A **flexible multi-team architecture** with:
  - Isolated **task execution**
  - Isolated **UI experience**
  - Isolated **connections / variables / secrets** management
  - (…and more)
- Multiple engineering teams operate within **a single Airflow environment**,
  **sharing the scheduler, API server, and database**.
- **Explicitly NOT hard, bulletproof multi-tenancy.** It's **isolation for
  cooperative teams** — the threat model is *accidents and noise between teams
  that trust each other*, not malicious tenants. Stops a team from seeing/
  editing another's connections, running code as another team, or cluttering
  their UI. Does **not** defend against a hostile tenant: shared scheduler /
  API / DB means DB load, scheduler pressure, or a bug in shared code still
  crosses the boundary. For hostile tenants or compliance isolation → separate
  environments.
- **The upside of sharing the infra:** one scheduler / API server / metadata DB
  / DAG processor / triggerer for the whole org → **costs stay down** (no N idle
  copies) and **management stays simple** (one thing to patch, monitor, upgrade
  — upgrade once, everyone moves together). Direct answer to the "full isolation
  is expensive" problem above.

## What the talk covers

- Technical details of the new architecture
- How it differs from multi-tenancy
- Demo of how to use multi-team

## My notes / observations

- **Framing question:** "how do you give everyone Airflow without everyone
  running Airflow themselves?" — i.e. the multi-tenancy problem, stated plainly.
  Every org past a certain size hits this; most have hacked their own layer.
- Two answers in the room this week:
  - **Isolate hard** → per-team instances (Capital One). Strong isolation,
    heavier ops, needs the templating/fleet machinery.
  - **Share smart** → one environment, native multi-team (this talk). Less ops
    per team, weaker isolation, shared failure domain.

### Why full isolation (per-team instances) is expensive

- **Repeated components** — every team gets its own scheduler, API server,
  metadata DB, DAG processor, triggerer. Mostly idle, all costing money and
  needing monitoring. N teams = N copies of everything.
- **Upgrades are per-instance** — you can't upgrade "the platform"; you upgrade
  each instance individually (or build the fleet-rollout machinery to do it for
  you — which is itself a big investment, cf. Capital One).
- **Operational surface scales with team count** — more things to patch,
  observe, and debug; the platform team's load grows linearly with tenants.
- → the multi-team bet: pay the isolation cost only where it actually matters
  (task execution, secrets, UI), share the expensive stateful bits (scheduler,
  API, DB) once.

### But naive "one Airflow for everyone" has no isolation at all

- Everyone **sees everything** — all DAGs, all runs, all connections/variables,
  all logs in one UI. Not acceptable when teams have sensitive creds or just
  don't want the noise.
- Everyone **can break everything** — one team's bad DAG / heavy top-level code
  / runaway task starves the shared scheduler and pools for all teams; someone
  edits/deletes another team's connection; a pause hits the wrong DAG.
- No blast-radius containment, no access boundaries, no per-team resource
  fairness.
- **Multi-team is the middle path:** keep the shared control plane, but add back
  the isolation that actually matters — visibility boundaries, credential
  separation, and isolated task execution so one team can't run code as another.

_(add during/after the talk)_

### Execution isolation — entirely separate execution paths per team

- Shared: scheduler, API server, metadata DB.
- **Not shared: the execution path.** Each team gets its **own set of
  executors** (the scheduler initializes per-team executors), its own workers /
  pods, its own queues, its own `default_pool`.
- So a team's task **runs on that team's infra with that team's identity and
  credentials** — never on another team's workers, never able to read another
  team's execution environment.
- Contains: runaway tasks / resource hogging (bounded by the team's own pool +
  workers), code-level access (can't touch another team's mounted secrets or
  env), dependency conflicts (team-specific images).
- Does **not** contain: pressure on the shared scheduler/DB from a team with
  thousands of tasks or expensive DAG parsing — that's still common.

### Security — team-specific connections, variables, secrets

- Connections / variables / secrets are **scoped by `team_id`**. A team only
  resolves its own — can't list, read, or overwrite another team's.
- `null` team_id = **shared** (platform-provided common connections/vars every
  team can use).
- Each team can point at its **own secrets backend / backend path** (e.g. its
  own Vault mount / SSM prefix), so the actual secret material is separated at
  the source, not just filtered in the UI.
- Combined with execution isolation: even if a team's DAG code tried to pull
  another team's connection by ID, it isn't in that team's scope and the worker
  doesn't have the backend access to fetch it.

### UI — a team-focused view

- One deployment, one URL, but the UI is **filtered to your team(s)** by the
  AuthManager based on your `team_id` membership.
- You see **your team's** DAGs, runs, logs, connections, variables, pools — not
  the whole org's. Cuts the noise and the accidental "paused the wrong DAG".
- Users in multiple teams see the union of their teams.
- It's a *focus/scoping* mechanism, not a hard security wall — but paired with
  the RBAC + execution + secret scoping, the effect is each team gets what
  feels like "their own Airflow" out of one shared instance.

### Flexibility — mix and match

- Not all-or-nothing. **Team-scoped where it's needed, global where it isn't.**
- The `team_id` / `null` split runs through everything: a resource with a
  `team_id` is that team's; a resource with `null` team_id is shared by all.
- So a deployment can have, e.g.:
  - team-specific executors + pools + secrets (isolate execution & creds)
  - a few **global** connections and variables everyone shares (common
    warehouse, shared config)
  - global pools alongside per-team `default_pool`s
- Lets the platform team dial the isolation per concern rather than committing
  the whole environment to one level.

### Opt-in — zero overhead when off

- Multi-team is a **feature flag**, off by default. If you don't enable it,
  nothing changes — no `team_id` lookups, no bundle→team table, no per-team
  executor sets, same single-tenant Airflow as before.
- No performance or complexity tax for the (majority) single-team deployments.
- → safe to ship into core; existing users aren't affected until they choose in.

### Auth Manager dependency

- In **Airflow 3 the Auth Manager is central** — it owns authN/authZ, RBAC,
  and (now) the team-based filtering. Multi-team is built *on top of* the Auth
  Manager: it's the component that maps a user → team(s) and enforces "you only
  see/act on your team's resources".
- **Action item for us:** our Auth Manager (whichever we run — FAB, or a custom
  one) will need work to support multi-team:
  - expose team membership per user (from our IdP / group mapping)
  - implement the team-filtering hooks the multi-team code calls
  - decide how our existing roles map onto the team model
- If we're on a custom Auth Manager, this is a non-trivial integration, not just
  a config flag. Worth scoping before committing to multi-team.

> **TODO (personal):** properly understand "top-level code" in a DAG file — what
> counts as top-level, when Airflow executes it, and why it's an
> arbitrary-code-execution surface. This is the crux of the per-team DAG
> processor argument below and I don't fully get it yet.
> (Quick version to expand later: everything in a `dags/*.py` file that runs at
> *import* time — i.e. not inside a task function — gets executed by the DAG
> processor every parse cycle, for every DAG file, just to discover the DAGs.
> That's real Python running in the processor's process: imports, module-level
> `Variable.get()`, loops that build DAGs, any `requests.get(...)` someone put
> at module scope. See `as26-optimising-airflow-real-world.md` Layer 1.)

### Why a per-team DAG processor (not just workers/triggerers)?

From the layout: DAG processors are **also per-team**. Rationale — parsing a DAG
file **executes the team's arbitrary top-level Python**, so it's a
code-execution / isolation surface just like task execution:

1. **Untrusted code at parse time.** Building the DAG object runs the module's
   top-level code. A shared processor runs Team A's top-level code in the same
   process/host as Team B's parsing → can read env / secrets / files, or crash
   it. Per-team = parsed inside that team's isolated env (own pod, own identity).
2. **Team-specific dependencies / images.** Conflicting library versions across
   teams; one shared processor has one Python env. Per-team processor runs the
   team's own image/requirements — same reason workers are per-team.
3. **Scoped secrets at parse.** Top-level `Variable.get()` / `Connection.get()`
   resolves against *that team's* secrets backend only.
4. **Parse-latency isolation.** One team's heavy top-level code won't delay DAG
   updates for everyone through a shared processor.
5. **Bundle→team binding.** Each team owns its bundle(s); a per-team processor
   watches only those — a broken bundle in one team doesn't stall another's
   DAG refresh.

(Shared stays: scheduler, API server, metadata DB. Per-team: DAG processor,
executors/workers, triggerer.)

### How you actually configure per-team components (Airflow 3.3 docs)

**Step 0 — create the teams in the metadata DB** (nothing works until they exist):
```bash
airflow teams create team_a      # also: teams list / delete / sync
airflow teams create team_b
```

**Step 1 — bind each DAG bundle to a team** via `team_name`:
```ini
[dag_processor]
dag_bundle_config_list = [
  {"name": "team_a_dags", "classpath": "...bundles.local.LocalDagBundle",
   "kwargs": {"path": "/opt/airflow/dags/team_a"}, "team_name": "team_a"},
  {"name": "team_b_dags", "classpath": "...bundles.local.LocalDagBundle",
   "kwargs": {"path": "/opt/airflow/dags/team_b"}, "team_name": "team_b"}
]
```

**Step 2 — sync the bundle→team mapping:** `airflow teams sync`

**Step 3 — run one `dag-processor` per team, scoped to that team's bundle(s):**
```bash
airflow dag-processor --bundle-name team_a_dags   # -B, repeatable
airflow dag-processor --bundle-name team_b_dags
```
- In 3.3 the dag-processor has **no `--team-name` flag** — you scope it with
  `-B/--bundle-name`. (The triggerer *does* have `--team-name`. AIP-67 mentions a
  `--team-id` for the processor; not in the 3.3 stable CLI yet.)
- Official guidance (Security Model doc): *"Deployment Managers must run separate
  Dag File Processor and Triggerer instances per team ... configuring each
  instance to only process bundles belonging to a specific team."*
- Airflow does **not** spawn these — it's a deployment concern: one Deployment/
  pod per team, own namespace/SA/secrets, own image with the team's deps.
- Each processor writes serialized DAGs to the **shared** metadata DB; the shared
  scheduler picks them up.

**Important: dedicated per-bundle DAG processors do NOT require multi-team.**
- `dag-processor -B/--bundle-name` is plain Airflow 3.x (AIP-43 processor
  separation + AIP-66 bundles). Define multiple bundles with **no `team_name`**,
  run one dag-processor per bundle → you get code / dependency / parse-latency
  isolation and per-pod/per-user separation.
- **Multi-team mode adds on top:** the `team_name` binding, `airflow teams` CLI,
  `team_id` propagation, per-team executors / pools / connections / variables /
  secrets, and UI+RBAC filtering by team.
- So: want "each tenant's DAGs parsed in their own process with their own deps"?
  Stock 3.x does it. Want team-scoped RBAC + secrets + executors? That's
  multi-team.
- ⚠️ Bug to check: apache/airflow **#57081** — a `-B`-scoped dag-processor can
  crash processing a **callback** that belongs to another bundle.

- **Per-team executors** — one config line, global first:
  ```ini
  [core]
  executor = LocalExecutor;team_a=CeleryExecutor;team_b=KubernetesExecutor
  ```
  Scheduler spins up the right executor set per team.

- **Per-team executor settings** — namespaced by team, no global fallback:
  ```
  AIRFLOW__TEAM_A___CELERY__BROKER_URL=redis://team-a-redis:6379/0
  # or  [team_a=celery] / broker_url = ...
  ```
  Resolution: team env var → team config section → defaults. **Does NOT fall
  back to global** env/config.

- **Per-team triggerer** — run one per team:
  ```bash
  airflow triggerer --team-name team_a
  airflow triggerer --team-name team_b
  airflow triggerer                     # global: teamless DAGs only
  ```

- **Deployment:** you're running N copies of {dag-processor, workers, triggerer}
  — one set per team, each with the team's image and the team's config namespace
  — against one shared {scheduler, API server, DB}. Docs don't yet give
  Helm/k8s specifics; **multi-team is flagged experimental** (no plugin support
  yet, no command/secrets-backend lookup for team settings).

### Team-based metrics — shipped in 3.3

- Metrics are **tagged/scoped by team** as of Airflow 3.3.
- So on the shared control plane you can still see per-team: task durations,
  task counts, queue time, pool usage, parse time, executor slots, failure
  rates — the usual Airflow metrics, just broken out by team.
- Matters for the platform team — lets you attribute load, spot the noisy team,
  do per-team capacity planning and chargeback even though the infra is shared.
- Pairs with per-team executors/pools: you can both *limit* a team (pool caps)
  and *observe* whether they're hitting the cap.

### Cross-team communication = assets

> **TODO (personal):** learn Airflow **assets** properly — new concept for me.
> Quick version to expand later: an *asset* (formerly "dataset") is a named,
> URI-identified thing a DAG can **produce** (declare it as an `outlets=`) or be
> **scheduled on** (`schedule=[asset]` instead of a cron). When a producing DAG
> run finishes, Airflow emits an **asset event**, which triggers any DAG
> scheduled on that asset. It's Airflow's built-in **data-driven / event-driven
> scheduling** — DAG B runs *because* DAG A produced fresh data, with no direct
> dependency between them. Read: Airflow docs "Data-aware scheduling" / Assets.
> The multi-team `access_control` stuff below is a *filter* on which asset
> events cross team boundaries.
>
> **Why this matters for us:** assets are *the* cross-team (cross-tenant)
> coordination primitive in the multi-team model. Teams are isolated on
> execution/creds/UI, so assets + asset events are the **only sanctioned way one
> team's pipeline drives another's**. If we adopt multi-team, our teams' interop
> story runs entirely through assets — this is critical, not a side feature.
> Understand assets well before evaluating multi-team for our platform.

- Teams are isolated (DAGs, connections, execution) — but they still need to
  **hand work off** to each other. The sanctioned channel is **assets**.
- **Cross-team asset events**: Team A's DAG produces an asset; Team B's DAG is
  scheduled on that asset. The asset event crosses the team boundary; the DAGs
  don't need to know about each other.
- Asset = the **common ground / contract** between teams: a named, versioned
  thing with a schema, not a direct DAG-to-DAG dependency or a shared connection.
- Keeps coupling loose: Team B depends on "the asset exists and is fresh", not
  on Team A's DAG internals. Team A can refactor freely as long as the asset
  contract holds.
- Fits the multi-team model — the metadata DB is shared, so asset state is
  naturally visible across teams; the isolation is on *execution and creds*, not
  on the coordination layer.
- **Default: cross-team asset events are OFF.** By default a consuming DAG only
  gets asset events from its **own team** or from **global (teamless)** DAGs.

**How it's configured (verified against the Airflow 3.3 docs):**

- The mechanism is `access_control=AssetAccessControl(...)` **on the `Asset`
  definition** (`from airflow.sdk import Asset, AssetAccessControl`).
- `AssetAccessControl` fields:
  | field | type | default | meaning |
  |---|---|---|---|
  | `producer_teams` | `list[str]` | `[]` | teams **allowed to produce** events that this asset's consumers will receive (on top of the consumer's own team) |
  | `consumer_teams` | `list[str] \| None` | `None` | teams **allowed to consume** events produced by this asset's producers |
  | `allow_global` | `bool` | `True` | whether teamless (global) DAGs can participate in cross-team delivery |

- **Consumer side — `producer_teams`.** The consuming team's Asset def lists the
  producing teams it trusts:
  ```python
  shared_data = Asset(
      name="my_data",
      uri="s3://bucket/shared/data.csv",
      access_control=AssetAccessControl(producer_teams=["team_analytics", "team_ml"]),
  )
  ```
  → "I'll accept freshness events on `my_data` from team_analytics / team_ml."

- **Producer side — `consumer_teams`.**
  - `None` (default) → **no restriction from the producer**; cross-team delivery
    is governed *solely* by each consumer opting in via `producer_teams`.
  - explicit list → only those teams (+ teamless, if `allow_global`) may consume.
  - `[]` (explicit empty) → locked to the producer's own team (+ teamless).

- **So it's not strictly "mutual opt-in by default."** Default posture: the
  **consumer must opt in** (`producer_teams`); the **producer only opts in if it
  chooses to restrict** (`consumer_teams`, otherwise `None` = permissive).
  Correcting my earlier note — the producer's "no" is *optional tightening*, not
  a required second handshake.
- Trust direction still holds: the consumer bears the risk (scheduling on
  someone else's data), so the consumer's `producer_teams` is the primary gate.
- Both fields live on the same-named `Asset` — producer and consumer DAGs each
  declare the `Asset` with the `access_control` fields relevant to their role.

### Is cross-team coordination limited to the asset producer/consumer model?

Question: can Team A just **directly trigger** Team B's DAG
(`TriggerDagRunOperator`, REST API, `ExternalTaskSensor`)?

- **As of Airflow 3.3, assets are the only *documented / built-out* cross-team
  mechanism.** `AssetAccessControl(producer_teams / consumer_teams)` is the one
  thing the multi-team docs actually specify for crossing the boundary.
- **Direct DAG-to-DAG triggering across teams is acknowledged but deferred.**
  AIP-67 mentions it (sketched an `allow-triggering-by-teams = ["team1", ...]`
  style allow-list on the target DAG) but explicitly says
  `TriggerDagRunOperator` / REST-API cross-team access control **needs separate
  design work** — not fully specified or implemented yet.
- Multi-team gives **REST-API-level RBAC isolation**, so today a Team A task
  hitting the API to trigger a Team B DAG would most likely be **blocked** by
  team-scoped permissions (or only work if B's DAG opts in). Behavior is
  not nailed down — treat as "don't rely on it yet."
- **Practical guidance:** design cross-team interop around **assets** (the
  supported path). If you need direct triggering, expect to wait for a follow-up
  AIP or handle it outside Airflow.
- Related: **AIP-82** (external event-driven scheduling) can achieve a similar
  effect to cross-team asset triggering — worth checking as an alternative.

### Questions to get answered

- What exactly is shared vs. isolated? (scheduler/API/DB shared — but parsing?
  pools? RBAC?)
- Blast radius: can one team's bad DAG still take down the shared scheduler?
- How are teams defined / bound — namespace, config, RBAC role?
- Migration path from a home-grown multi-tenant layer.
- Does this change our calculus vs. running N isolated instances?
