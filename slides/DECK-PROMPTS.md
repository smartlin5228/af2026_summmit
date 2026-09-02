# Airflow Summit 2026 — Team Read-Out Deck: slide prompts

**How to use:** paste each prompt below, one at a time, into your AI slide tool
(Gamma / Claude / Slides GPT / etc.). Each prompt is self-contained — it has the
actual content to render, not just "summarize X". Keep the deck's visual style
consistent by pasting the **Style preamble** once at the start, then each slide
prompt.

- **Audience:** our data-platform / infra team (we operate managed Airflow for
  internal teams).
- **Angle:** knowledge share + "what this means for us."
- **Scope:** whole conference, curated toward platform relevance.
- **Length:** ~28 slides.
- Slides tagged **[OUR TAKE]** are our analysis, not conference content — style
  them distinctly (e.g. accent background) so it's clear which is which.

---

## Style preamble (paste once)

> I'm building a ~28-slide internal read-out deck from Airflow Summit 2026 for my
> data-platform team. We operate a managed/hosted Airflow platform for internal
> engineering teams. Use a clean, technical, low-decoration style: dark text on
> light, one accent colour, monospace for code/config identifiers, generous
> whitespace, max ~6 bullets per slide. No stock photos. Diagrams as simple
> boxes-and-arrows. I'll give you slides one at a time. Some slides I'll tag
> "[OUR TAKE]" — give those a visually distinct treatment (e.g. a tinted
> background or a left accent bar) because they're our analysis, not conference
> content.

---

## Slide 1 — Title

> Title slide. Main title: "Airflow Summit 2026 — What We Saw, What It Means for
> Us". Subtitle: "Aug 31 – Sep 2, 2026 · read-out for the platform team".
> Small footer: "15 sessions · notes + fact-checks in notes_af2/". No bullets.

## Slide 2 — How to read this deck

> Title: "How to read this deck". Content:
> - Organised by **5 themes**, not chronologically: Multi-tenancy · Reliability &
>   durability · Performance & operations · Agentic Airflow · Polyglot & future.
> - Each theme: 1–3 "what was presented" slides, then one **[OUR TAKE]** slide.
> - **[OUR TAKE]** slides are our analysis / recommendations — styled
>   differently.
> - Full session notes and fact-checks live in the `notes_af2/` repo; this is the
>   curated version.

## Slide 3 — The one theme that ran through everything

> Title: "The recurring theme: shared Airflow doesn't scale to many teams".
> Content:
> - Once a shared Airflow crosses ~dozens of DAG-authoring teams, the problem
>   stops being "individual DAGs" and becomes **isolation**: noisy neighbours,
>   blast radius, deployments clobbering each other, no per-team secrets/RBAC,
>   upgrades that need org-wide coordination.
> - Three different answers were on display this year:
>   - **Capital One** — isolate hard: hundreds of per-tenant Airflow instances.
>   - **Datadog** — share the control plane, isolate execution with k8s
>     namespaces ("worker groups").
>   - **Upstream (AIP-67)** — native multi-team: shared scheduler/API/DB,
>     per-team everything else.
> - We are in the exact "shared model, one platform team" starting position all
>   three described.

## Slide 4 — AIP cheat sheet

> Title: "The AIPs behind the talks". Render as a table, columns: AIP / What /
> Status.
> - AIP-67 — Multi-team deployment — Accepted, experimental, landing across 3.x
> - AIP-82 — External event-driven scheduling (asset watchers) — Shipped 3.x
> - AIP-85 — Extendable DAG parsing (pluggable importer) — Planned 3.4+
> - AIP-86 — Deadline Alerts (replaces SLA) — 3.1 experimental; substance in 3.4
> - AIP-99 — First-class LLM support (`@task.agent`, `common-ai` provider) —
>   Shipped
> - AIP-103 — Task & Asset State store (durability) — **Shipped 3.3**
> - AIP-105 — Pluggable retry policies — **Shipped 3.3**
> - AIP-108 — Language Task SDK (Java/Go, Coordinator) — Shipped 3.3, experimental
> Footer: "3.3 was a big release for us: durability + retry + polyglot all landed."

---

## Slide 5 — Capital One: Airflow as a managed platform (keynote)

> Title: "Capital One — Airflow as a fully-managed enterprise platform". Content:
> - Started as **shared Airflow** to fill an org orchestration gap and
>   consolidate infra ops for every persona (data eng, analysts, DS,
>   non-technical).
> - Adoption outgrew it: onboarding bottlenecks, version/config drift (every
>   instance a snowflake), no fleet-wide upgrades or observability.
> - Moved to **hundreds of isolated per-tenant Airflow instances on Kubernetes**.
> - Ownership split: platform owns k8s infra, deployment pattern, version
>   lifecycle, support model. Tenants own DAGs, their dependencies, all dev.

## Slide 6 — Capital One: how the managed model works

> Title: "Capital One — the model: standardize, then automate everything".
> Content:
> - **3 pillars:** fully-managed infra · declarative standardization (a
>   constrained **DSL** — tenants declare intent, platform renders vetted k8s +
>   Airflow config) · governed guardrails (RBAC/network/secrets/images baked in,
>   can't opt out).
> - Provisioning: central UI + provisioner, **Argo CD** GitOps from a
>   control-plane repo; charts + images entirely platform-managed.
> - **Zero-downtime fleet upgrades:** one central chart/image → integration tests
>   + regression canaries → progressive wave rollout → e2e observability +
>   one-click rollback.
> - Guardrails from **day one**: CI style/perf/logic checks on tenant operators,
>   mandatory deferrable operators, central system-of-record.
> - Business case: <1 day idea→prod, 99.9% uptime SLA, compute savings + adoption.
> - **Why it works:** tenant workloads are homogeneous → you can template them.

## Slide 7 — Datadog: worker groups + trigger queues

> Title: "Datadog — shared control plane, isolated execution (~60 teams)".
> Content:
> - Same starting pain: one shared Airflow, deployments clobbering each other,
>   noisy neighbours.
> - **"Worker group" per tenant:** shared core namespace (scheduler, webserver,
>   triggerer) + per-tenant k8s namespace with its **own DAG processor +
>   workers**, self-managed RBAC via native k8s primitives.
> - The gap: **the triggerer stayed shared.** One event loop for all tenants;
>   one service identity — but triggers call Spark/Trino/K8s needing per-tenant
>   creds.
> - Their fix, contributed upstream (**PR #59239**): **trigger queues** —
>   `--queues` on the triggerer, routed by the deferring task's `queue`. Pin
>   triggerers to queues like you pin workers.
> - Helps single-tenant too: isolate slow/blocking triggers from
>   latency-sensitive ones.

## Slide 8 — Upstream multi-team (AIP-67)

> Title: "Upstream multi-team (AIP-67) — one environment, many teams". Content:
> - **Shared:** scheduler, API server, metadata DB.
> - **Per-team:** DAG processor, executors/workers, triggerer, pools,
>   connections/variables/secrets.
> - Binding: a **`team_id`** threaded everywhere; DAG **bundles** map to teams;
>   `null` team_id = shared resource.
> - UI filtered by team membership via the **Auth Manager**.
> - Explicitly **"isolation for cooperative teams,"** not bulletproof
>   multi-tenancy — shared DB/scheduler still cross the boundary.
> - Cross-team coordination is **assets only**: `AssetAccessControl(producer_teams
>   / consumer_teams)`. Direct cross-team DAG triggering is deferred.
> - Status: **experimental**.

## Slide 9 — [OUR TAKE] Multi-tenancy: our decision

> Title: "[OUR TAKE] Multi-tenancy — the decision in front of us". Content:
> - We're in the shared-model / one-platform-team position all three talks
>   started from.
> - Three viable directions: per-tenant instances (Capital One) · k8s-namespace
>   worker groups (Datadog) · native multi-team AIP-67.
> - **The deciding question (from Capital One): how uniform are our tenants'
>   workloads?** Homogeneous → a templated managed model scales. Heterogeneous →
>   templating fights us.
> - If we pursue **AIP-67**: the **Auth Manager** needs real work first
>   (per-user team membership + team-filter hooks) — not a config flag. And it's
>   still experimental.
> - **Adopt now regardless of direction:** trigger queues (`--queues`) — even
>   single-tenant it isolates slow Spark/Trino trigger polls.

---

## Slide 10 — AIP-103 + AIP-105: durability and recovery (shipped 3.3)

> Title: "Durability vs. Recovery — two new primitives in Airflow 3.3". Content:
> - **Durability (AIP-103):** a **task state store** — persistent key-value state
>   scoped to a task instance, written *during* execution, **survives retries**
>   (unlike XCom, written at the end). `task_state_store.set(k,v)` /
>   `.get(k)`. Backend: metadata DB or worker-side; per-key retention +
>   `clear_on_success`.
> - **Recovery (AIP-105):** **pluggable retry policies** — a policy inspects the
>   exception and decides: retry now / backoff / fail fast / retry-with-variation
>   / escalate. "Retry is an action; *whether* to retry is a decision."
> - They compose: recovery decides whether to retry, durability makes the retry
>   resume from the checkpoint instead of step zero.

## Slide 11 — Resumable Spark: killing the "crash tax"

> Title: "Resumable task execution — the crash tax". Content:
> - Today: task submits a Spark job → worker is evicted → task fails → retry
>   **submits a brand-new Spark job**. You pay for the orphaned job + the full
>   re-run + wall-clock.
> - **AIP-103 fix:** persist the Spark **application ID** as the checkpoint; on
>   retry, **reconnect** to the live job (`ResumableJobMixin` in
>   `SparkSubmitOperator`). If it already succeeded → just record it, no
>   resubmit.
> - **Requires cluster deploy mode** (driver runs in k8s/YARN, not in the Airflow
>   worker). Demo used Spark's native Kubernetes backend — driver pod is the
>   stable handle.
> - Also works for **big batches inside one task**: a per-item ledger in task
>   state → retry only re-does unfinished items.
> - No custom async / deferrable code needed.

## Slide 12 — Taming AI workloads: the watcher pattern

> Title: "Long external jobs — be a watcher, not the owner". Content:
> - Problem: transient infra hiccups (pod eviction, network blip) make Airflow
>   kill an expensive, healthy external AI/training job.
> - Pattern: separate the **external job's lifecycle** from the **task's**.
>   Submit + record ID + poll. On restart, re-attach by ID.
> - `on_kill()` fires **only on human intent** (clear/fail/mark-success) → the
>   right place to cancel the external job. `cleanup()` fires on **every**
>   triggerer restart → local resources only, never cancel the job there.
> - **Gotcha:** a task can be failed **from the triggerer** (deferral timeout or
>   trigger exception) → `execute_complete` and `on_kill` are **skipped** → the
>   external job is orphaned. Need a **reaper DAG** or an
>   `on_task_instance_failed` **listener** as backstop.
> - Watch apache/airflow **#36090** (deferrable `on_kill` not called on
>   clear/fail).

## Slide 13 — Deadline Alerts (AIP-86)

> Title: "Deadline Alerts — SLAs, reimagined". Content:
> - Replaces the old SLA feature (removed in 3.0). It is **not a timeout** (never
>   kills anything) and **not a retry policy** (retries = failure; deadlines =
>   *lateness*).
> - Fixes "when do you start counting?": pick a **reference point** —
>   `DAGRUN_QUEUED_AT`, `DAGRUN_LOGICAL_DATE`, `FIXED_DATETIME`, or
>   **`AVERAGE_RUNTIME`** (predictive, from history). `reference + interval =
>   trigger time`; **negative interval = fires early** (proactive).
> - Callbacks now run in **supervised subprocesses with Connections / Variables /
>   Assets** → they can *inspect the run and act*, not just notify.
> - Payoff: deadline at `AVERAGE_RUNTIME − 15m` → callback scales workers up
>   **before** the miss. Or gathers state and posts a **diagnosis** instead of a
>   bare alert.
> - Most substance lands in **3.4**. Task/asset-level deadlines still coming.

## Slide 14 — [OUR TAKE] Reliability: what to adopt

> Title: "[OUR TAKE] Reliability — concrete adoptions". Content:
> - **`airflow db clean` — set it up and monitor it.** The "fleet slowed down
>   overnight" incident was this job silently failing for months → XCom bloat →
>   every DAG slow. Verify sizes actually drop.
> - **`ResumableJobMixin`** for our Spark/long-job operators — needs cluster
>   deploy mode; check ours.
> - **`ExceptionRetryPolicy`** as a platform default — fail fast on auth/config
>   errors, backoff on transient. Stops pointless retries burning capacity.
> - **Audit deferrable operators** for the triggerer-orphan path; add a reaper
>   DAG / failure listener.
> - Deadline Alerts: revisit after 3.4 — the proactive + context-aware-callback
>   combo is genuinely useful for tenant SLAs.

---

## Slide 15 — The layered diagnostic method

> Title: "Diagnosing Airflow performance — work the layers, in order". Content:
> - Perf problems arrive as **drift**, not failures: a bit more queue time, a
>   slower parse, scheduler lag.
> - Five layers, top to bottom: **1 DAG parsing → 2 Scheduling → 3 Task runtime →
>   4 Metadata DB → 5 Upgrade effects.** A problem in an earlier layer
>   masquerades as a later one.
> - **Lightest tool first:** metrics & logs → DB inspection → py-spy (CPU pegged)
>   → memray (memory grows / OOM) → app profiling (task slow after it starts).
> - Layer 1 tell: slow parsing is almost always top-level `Variable.get()`,
>   module-scope API/DB calls, heavy imports, or expensive dynamic DAG
>   generation. Fix: move it into tasks.
> - (We have a full runbook artifact from this talk — link on the last slide.)

## Slide 16 — Localizing a scheduling bottleneck

> Title: "Where does the time go before a task runs?". Content:
> - The lag from "runnable" to "running" is a **sum of stages**, each with its
>   own ceiling. Render as a left-to-right pipeline:
>   `scheduled` → (scheduler loop capacity) → `queued` → (`[core] parallelism`,
>   pools, `max_active_tasks/runs_per_dag`) → (executor: `worker_concurrency` ×
>   workers, K8s pod quota) → `running` → (worker resources, prefetch).
> - Distinguishing signals:
>   - queue time high + workers **idle** → executor/concurrency ceiling, not
>     resources
>   - queue time high + workers **saturated** → real capacity, scale out
>   - stuck in `scheduled`, pools full → hitting `parallelism`/a pool
>   - stuck in `scheduled`, pools have room → scheduler is behind (parsing,
>     loop duration)
>   - everything slow together → metadata DB
> - Procedure: pull one slow task's `queued_dttm` / `start_date`, compute the
>   gaps, the big gap names the stage, then change one knob.

## Slide 17 — Worker sizing

> Title: "`worker_concurrency` should be arithmetic, not a guess". Content:
> - Formula: `worker_concurrency ≈ worker_memory / (peak_task_memory ×
>   safety_factor)`.
> - **`peak_task_memory` is the variable that decides the answer** — worker mem
>   is fixed, safety factor is a rough multiplier; concurrency scales
>   *inversely* with peak task memory. Guess it 2× low → concurrency 2× too high
>   → OOM.
> - Use **peak (high-water mark), not average** — the worker holds every
>   concurrent task's peak at once.
> - It isn't uniform — size for the heaviest task that can land there, or route
>   memory-hungry tasks to a dedicated queue.
> - Measure it: memray / cgroup `memory.peak` / per-TI max RSS, p95–max over
>   month-end / backfills.
> - **The worker is the component most hurt by a memory cap** — it runs arbitrary
>   user code, concurrency multiplies it, and OOM is destructive (kills every
>   co-tenant task).

## Slide 18 — Real incidents (Debugging the Undebuggable)

> Title: "Four production incidents, four root causes". Content, as a numbered
> list:
> 1. **DAGs stopped importing after an upgrade** → the upgrade bumped Python,
>    which broke a dependency pin. Looked like a DAG bug; was a version mismatch.
> 2. **A task went green, then silently red with no new run** → a stale zombie
>    callback from an *earlier try number* landed late and overwrote the result.
>    Fix: ignore callbacks for old try numbers.
> 3. **Whole fleet slowed down overnight, no deploy** → a cleanup job had been
>    catching its own error and reporting success for *months*; XCom table
>    bloated. Airflow *did* report it failed/timed out — the signal was
>    swallowed.
> 4. **Queued tasks ≠ pods created** → concurrency config drift between layers
>    (k8s ResourceQuota vs Airflow `parallelism`/pod limits). Each layer "fine",
>    together inconsistent.
> Takeaway: the root cause usually hides one layer below the symptom.

## Slide 19 — [OUR TAKE] Performance & ops

> Title: "[OUR TAKE] Performance — our gaps". Content:
> - **Our workers autoscale on CPU only.** Memory-heavy tasks OOM workers with no
>   scaling signal → they reschedule → looks like a scheduling problem, is a
>   sizing problem. **Fix: add memory to the HPA, or a dedicated queue for heavy
>   tasks.**
> - We should measure `peak_task_memory` and re-derive `worker_concurrency`.
> - Check our metrics cover the scheduler-lag distinguishers
>   (`scheduler.tasks.starving`, `pool.open_slots.*`, `scheduler_loop_duration`,
>   `critical_section_duration`, per-DAG schedule delay).
> - Adopt the 5-layer method + the symptom→component map into our on-call docs.
> - Ship the **Performance Runbook** artifact to the team (already built).

---

## Slide 20 — Agentic pipelines on Airflow

> Title: "Agents and pipelines aren't opposites". Content:
> - Most agentic problem-solving *has pipeline structure*: gather → process each
>   dimension → synthesize → evaluate. The question is **where the LLM sits
>   inside the pipeline**, not "agent or pipeline".
> - **AIP-99 `common-ai` provider:** `@task.agent` / `AgentOperator`, inference /
>   SQL-gen / branching / schema-validation / embedding, 20+ model providers via
>   PydanticAI.
> - Make **evaluation an explicit task** (separate model call, maybe a *different*
>   model — independent judge, less correlated failure). Multi-dimension:
>   correctness / safety / relevance → aggregate → pass = publish, fail =
>   regenerate or human review.
> - **Token economics:** one big agent = context grows quadratically; N scoped
>   agent calls = linear. Pick the model per step.
> - **Durability:** `@task.agent(durable=True)` caches every model + tool result
>   to object storage and **replays on retry** (demo: fail at step 16 → retry
>   resumes at 17). Matters more for AI: tokens cost money, non-determinism,
>   rate limits.

## Slide 21 — Agent governance & isolation

> Title: "Running agents safely: constrain what they can *do*". Content:
> - **Rogue-agent demo:** agent given warehouse access + a prompt that says "do
>   whatever the customer notes ask" (injection wired in). Result: `DELETE` and
>   off-limits-table access **blocked** by tool guardrails
>   (`allow_writes=False`, `allowed_tables`) — but the **injection itself was not
>   blocked**. The model followed it; only the tool layer stopped damage.
> - Lesson: you **cannot** rely on the model not being injected. Defense =
>   tool-level allow-lists, scoped connections, SQL constraints, sandbox, HITL.
> - **Isolation (separate talk):** a container isn't a security boundary (shared
>   host kernel). `@agent` decorator injects `executor_config` to route the task
>   to a sandboxed runtime:
>   - **gVisor** — user-space kernel (Sentry) intercepts syscalls
>   - **Kata** — per-pod micro-VM, real guest kernel, hardware virt boundary
>   - **Peer Pods** — pod runs as a separate cloud VM; VM-per-pod **without
>     nested virt** on your nodes
>   - KubernetesExecutor stays unchanged.

## Slide 22 — [OUR TAKE] Agentic

> Title: "[OUR TAKE] Agentic — posture for when tenants ask". Content:
> - It's coming — tenants will want LLM steps in DAGs. The `common-ai` provider
>   makes it a first-class task type.
> - **If we allow it:** mandate tool-level guardrails as platform policy —
>   allow-lists, `allow_writes=False`, scoped connections. The model *will*
>   follow injections; the tool layer is the only real control.
> - **If tenants run generated/untrusted code:** require the `@agent` isolation
>   pattern. **Peer Pods** if our k8s nodes can't do nested virt.
> - `durable=True` + AIP-103 makes agent retries affordable — relevant to our
>   cost story.
> - This overlaps our isolation work — same "per-task trust class" idea.

---

## Slide 23 — Polyglot Airflow (AIP-108 + AIP-85)

> Title: "Airflow is going polyglot". Content:
> - **AIP-108 (Language Task SDK, shipped 3.3, experimental):** write **task
>   bodies** in Java or Go (TypeScript soon). A Python **Coordinator** middle
>   layer owns everything Airflow-specific (Task Execution API, context, XCom,
>   retries); the per-language SDK stays thin.
> - **Executable Bundle Spec:** for native langs the bundle *is* the compiled
>   binary + embedded DAG source + `airflow-metadata.yaml` + an `AFBNDL01`
>   trailer. Runtime scans, verifies SHA-256, `exec`s it. Self-contained — no
>   ambient runtime assumed.
> - **AIP-85 (planned 3.4+):** makes the DAG **importer** pluggable — the
>   *definition* side. A DAG source could be JSON/YAML/"safe" Python / a compiled
>   binary / a database, not just a `.py` file.
> - Together: cross-language DAG authoring end to end.
> - Note: today's model quietly assumes "a Python interpreter, same env as
>   Airflow, find the package somehow" — polyglot forces that contract to be
>   explicit.

## Slide 24 — OpenLineage: the "why" layer

> Title: "OpenLineage — Airflow already knows the root cause". Content:
> - Dashboards show symptoms; lineage shows relationships; neither explains
>   **why**. OpenLineage emits structured context across Airflow / Spark / etc.,
>   including failure logs + execution metadata.
> - New, OOTB: **run-ID correlation** across Airflow entities · **hook-level SQL
>   lineage** (actual queries + query IDs from inside Python operators) ·
>   **human-in-the-loop metadata** (who approved what, when).
> - Foundation for AI-driven assistants / auditors / operational agents (demo:
>   Astro Observe Investigation Agent — but the context layer is available to
>   everyone).

## Slide 25 — Grab bag

> Title: "Also worth knowing". Content, three short blocks:
> - **eBay (still on 2.10):** built custom control primitives — breakpoints
>   (pause a run at a task without failing it), runtime skip / mark-success,
>   pause/resume, calendar timetables, "earliest start time", serial-run-only-
>   after-previous-success, multi-cluster routing. All via one run-scoped gate at
>   task pre-check. Check what 3.x now does natively.
> - **`pytest-airflow-in-a-box`** (PyPI, 3rd-party): test DAGs against a real
>   metadata DB with no scheduler/webserver. Catches serialization / trigger-rule
>   / template bugs that import tests miss. "Fidelity ladder" of fixtures.
> - **Airflow 2→3 migration talk:** 100+ DAGs, no big-bang — compatibility layer
>   for both versions, AI tooling for 400+ DAG edits, ephemeral per-PR k8s
>   environments, staging = prod access + data subset.

---

## Slide 26 — [OUR TAKE] What this means for us

> Title: "[OUR TAKE] What this means for our platform". Content, grouped:
> - **Multi-tenancy:** decide our model (per-tenant instances / k8s worker groups
>   / AIP-67). Deciding factor: workload uniformity. AIP-67 needs Auth Manager
>   work + is experimental.
> - **Adopt soon (low risk):** `airflow db clean` + monitoring · trigger queues
>   (`--queues`) · `ExceptionRetryPolicy` default · fix CPU-only autoscaling ·
>   OpenLineage provider centrally · the perf runbook + 5-layer method in on-call
>   docs.
> - **Evaluate (3.3 features):** `ResumableJobMixin` for Spark · task state store
>   for batch tasks · pluggable retry policies for tenant DAGs.
> - **Watch:** Deadline Alerts (3.4) · multi-team maturity · polyglot SDKs ·
>   `common-ai` provider (+ set agent guardrail policy before tenants adopt).

## Slide 27 — Action items

> Title: "Action items". Render as a table: Item / Owner / When. Pre-fill the
> Item column, leave Owner/When blank:
> - Write up our multi-tenancy options + recommendation (1-pager)
> - Scope the Auth Manager work for AIP-67
> - Stand up + alert on `airflow db clean`; audit metadata DB (indexes, XCom
>   size)
> - Add memory to worker autoscaling / dedicated heavy-task queue
> - Measure `peak_task_memory`, re-derive `worker_concurrency`
> - PoC trigger queues in staging
> - Evaluate `ResumableJobMixin` for our Spark operators
> - Audit deferrable operators for the triggerer-orphan path
> - Enable OpenLineage provider in staging
> - Share the Performance Runbook artifact; fold the 5-layer method into on-call
>   docs
> - Draft agent-guardrail platform policy

## Slide 28 — Appendix / links

> Title: "References". Content:
> - Full session notes + fact-checks: `notes_af2/` repo (`README.md` has the
>   index + AIP tracker).
> - Consolidated TODOs: `notes_af2/TODO.md`.
> - Performance Runbook artifact: [paste the claude.ai/code/artifact URL].
> - Key AIPs: 67 (multi-team), 82 (event scheduling), 85 (DAG parsing), 86
>   (deadline alerts), 99 (LLM support), 103 (task state), 105 (retry policies),
>   108 (language SDK).
> - Key PR: apache/airflow #59239 (trigger queues).
> - `pytest-airflow-in-a-box`: pypi.org/project/pytest-airflow-in-a-box
