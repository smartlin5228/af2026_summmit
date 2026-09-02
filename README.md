# Airflow Summit 2026 — Notes

Session notes from AS26 (Aug 31 – Sep 2, 2026), taken live and fact-checked
against Airflow docs / AIPs / PRs afterward. Written from the perspective of a
**managed-Airflow platform team** (we run hosted Airflow for internal teams — see
`memory`), so most notes carry a "relevant to us" slant.

## Deliverables built from these notes

- **`TODO.md`** — consolidated action items (personal learning + platform work +
  note cleanup).
- **`slides/DECK-PROMPTS.md`** — one-prompt-per-slide spec for a team share-out
  deck (whole conference, curated, with our take).

---

## Sessions

### Platform / multi-tenancy  ← most relevant to us

| Note | One-liner |
|---|---|
| [`as26-airflow-as-a-platform.md`](as26-airflow-as-a-platform.md) | **Capital One keynote.** Shared Airflow → hundreds of isolated per-tenant instances on k8s. Fully-managed model: managed infra + declarative DSL standardization + governed guardrails. Argo CD GitOps control plane, zero-downtime progressive fleet upgrades. Works because tenant workloads are homogeneous. |
| [`as26-multi-team-airflow.md`](as26-multi-team-airflow.md) | **Upstream multi-team (AIP-67).** One environment, many teams: shared scheduler/API/DB, per-team DAG processor + executors + triggerer + pools + secrets, `team_id` binding via DAG bundles, AuthManager UI filtering. "Isolation for cooperative teams," not bulletproof multi-tenancy. Cross-team coordination = assets only (`AssetAccessControl`). Experimental. |
| [`as26-trigger-queues-datadog.md`](as26-trigger-queues-datadog.md) | **Datadog (~60 teams).** Their "worker group per tenant" model (shared core ns, per-tenant k8s namespace w/ self-managed RBAC). Shared triggerer was the gap → they built **trigger queues** (`--queues` on triggerer, routed by task `queue`), merged upstream (PR #59239). Event-based trigger support ~3.4. |
| [`as26-ebay-enterprise-scheduling.md`](as26-ebay-enterprise-scheduling.md) | **eBay (still on 2.10).** Custom control primitives: breakpoints, runtime skip/mark-success, pause/resume, calendar timetables, earliest-start-time, serial-run-on-previous-success, multi-cluster routing. All via one run-scoped gate at task pre-check. |

### Reliability & durability

| Note | One-liner |
|---|---|
| [`as26-resumable-task-execution-spark.md`](as26-resumable-task-execution-spark.md) | **AIP-103 (task state store) + AIP-105 (retry policies), shipped Airflow 3.3.** The "crash tax"; `task_state_store.set/get`; `ResumableJobMixin` reconnects to a live Spark job instead of resubmitting (cluster mode only). `ExceptionRetryPolicy` / `LLMRetryPolicy`. Durability (resume) vs. recovery (retry-with-variation) compose. |
| [`as26-taming-ai-workloads-dag-patterns.md`](as26-taming-ai-workloads-dag-patterns.md) | **Deferrable "watcher" pattern** for long external AI jobs. Separate the external job's lifecycle from the task's. `on_kill()` fires only on human intent (right place to cancel); `cleanup()` fires on every restart (local resources only). Watch out: task can be failed *from the triggerer* → `execute_complete`/`on_kill` skipped → orphaned job. Needs a reaper DAG / listeners. |
| [`as26-deadline-alerts.md`](as26-deadline-alerts.md) | **AIP-86, replaces SLA.** Not a timeout, not a retry policy — about *lateness*. Fixes "when do you start counting" via reference points (`AVERAGE_RUNTIME` is predictive). Negative intervals = proactive. Context-aware callbacks (Connections/Vars/Assets) can *act* — e.g. scale workers before a miss. Substance lands in 3.4. |

### Performance & operations

| Note | One-liner |
|---|---|
| [`as26-optimising-airflow-real-world.md`](as26-optimising-airflow-real-world.md) | **Koti/Regani (Astronomer).** A layered diagnostic method: parsing → scheduling → task runtime → metadata DB → upgrades. Lightest tool first (metrics → DB → py-spy → memray). Task-latency pipeline (`scheduled→queued→running` + gates). `worker_concurrency ≈ worker_mem / (peak_task_mem × safety)`. **Published team artifact — see slide deck / artifact link.** |
| [`as26-debugging-airflow-incidents.md`](as26-debugging-airflow-incidents.md) | **Pankaj Singh.** 4 real incidents: (1) upgrade broke imports = Python-version/dep-pin mismatch, not DAG bug; (2) success→failed = stale zombie callback from old try number; (3) fleet slowdown = swallowed cleanup failure → XCom bloat for months; (4) queued/pods mismatch = concurrency config drift across layers. |
| [`as26-performance-debugging-symptoms-to-solutions.md`](as26-performance-debugging-symptoms-to-solutions.md) | Codebase-oriented perf debugging; case study of a DAG-processor OOM from one SQLAlchemy query. *(sparse notes — overlaps heavily with the Optimising talk.)* |

### Agentic Airflow

| Note | One-liner |
|---|---|
| [`as26-agentic-pipelines-on-airflow.md`](as26-agentic-pipelines-on-airflow.md) | **AIP-99 (`common-ai` provider): `@task.agent` / `AgentOperator`, 20+ providers via PydanticAI.** Agents *have* pipeline structure — put the LLM step inside a DAG. Multi-dimensional eval as explicit tasks (separate model, maybe different one). `durable=True` step replay. Token economics: scoped calls (linear) vs one big agent (quadratic). Governance: tool-level allow-lists are the real safety net (rogue-agent demo: actions blocked, injection not). |
| [`as26-agent-isolation-airflow.md`](as26-agent-isolation-airflow.md) | **`@agent` decorator = per-task trust class.** Untrusted agent code isolated via injected `executor_config` (`runtimeClassName`). Why a container isn't a boundary (shared kernel). gVisor (Sentry user-space kernel + Gofer) / Kata (per-pod micro-VM) / Peer Pods (cloud VM, no nested virt). KubernetesExecutor unchanged. |

### Polyglot / SDK

| Note | One-liner |
|---|---|
| [`as26-toward-a-polyglot-airflow.md`](as26-toward-a-polyglot-airflow.md) | **AIP-108 (Language Task SDK, 3.3, experimental): Java + Go today, TS soon.** Python **Coordinator** middle layer owns all Airflow logic; thin per-language SDK. Executable Bundle Spec: the bundle *is* the compiled binary + embedded source + `airflow-metadata.yaml` + `AFBNDL01` trailer. **AIP-85** (3.4+) makes the DAG *importer* pluggable (definition side). Includes a fact-checked Airflow-3 execution flow. |
| [`as26-openlineage-root-cause.md`](as26-openlineage-root-cause.md) | **OpenLineage as the "why" layer.** OOTB run-ID correlation across Airflow entities, hook-level SQL lineage (queries + query IDs), HITL metadata. Foundation for ops agents/auditors. Demo: Astro Observe Investigation Agent. |

### Testing & migration

| Note | One-liner |
|---|---|
| [`as26-airflow-in-a-box.md`](as26-airflow-in-a-box.md) | **`pytest-airflow-in-a-box`** (PyPI, 3rd-party by `nredd`, *not* upstream). Real metadata DB, no scheduler/webserver. "Fidelity ladder" of fixtures (`run_task` → `dag_maker` → `run_dag` → `executor=`). Mirrors upstream's own `tests_common` pytest plugin. |
| [`airflow-2-to-3-migration-talk.md`](airflow-2-to-3-migration-talk.md) | 100+ DAGs, no big-bang cutover. Compatibility layer for both versions; AI tooling for 400+ DAG changes; ephemeral per-PR k8s environments; staging = prod access + data subset. |

### Ideas

| Note | One-liner |
|---|---|
| [`idea-airflow-for-fleet-config-management.md`](idea-airflow-for-fleet-config-management.md) | Thought experiment: Airflow for cross-fleet config management? Verdict: orchestrator, not config engine — drive GitOps/Ansible/Terraform with Airflow for sequenced rollouts + drift reporting; don't reimplement a reconciler in DAGs. |

---

## Cross-cutting: AIP tracker

| AIP | What | Status | Notes |
|---|---|---|---|
| **AIP-67** | Multi-team deployment | Accepted, experimental, landing across 3.x | multi-team, trigger-queues |
| **AIP-82** | External event-driven scheduling (asset watchers) | In 3.x | trigger-queues (event-based triggers), deadline-alerts |
| **AIP-85** | Extendable DAG parsing controls (pluggable importer/ingester) | Planned 3.4+ | polyglot |
| **AIP-86** | Deadline Alerts (replaces SLA) | 3.1 experimental; substance in 3.4 | deadline-alerts |
| **AIP-99** | First-class LLM support (`common-ai` provider, `@task.agent`) | Shipped | agentic-pipelines, resumable-spark |
| **AIP-103** | Task & Asset State Management (state store, durability) | **Shipped 3.3** | resumable-spark, agentic-pipelines, trigger-queues, taming-ai |
| **AIP-105** | Enhanced / pluggable retry policies | **Shipped 3.3** | resumable-spark, agentic-pipelines |
| **AIP-108** | Language Task SDK (Java/Go, Coordinator) | Shipped 3.3, experimental | polyglot, resumable-spark |
| **AIP-72** | Task Execution Interface / Task SDK (the 3.x execution model) | Shipped (3.0) | polyglot (background) |

## Cross-cutting: the recurring theme

**Shared Airflow works until team count grows, then isolation becomes the
problem.** Three answers were on display:
- **Capital One** — isolate hard (per-tenant instances) + heavy fleet machinery.
- **Datadog** — share the control plane, isolate execution via k8s namespaces
  ("worker groups"), then close the triggerer gap with trigger queues.
- **Upstream (AIP-67)** — native multi-team: shared control plane, per-team
  processor/executor/triggerer, `team_id` everywhere.

We're in the "shared model, one platform team" starting position all three
described. See `TODO.md` and the deck's "what this means for us" slides.
