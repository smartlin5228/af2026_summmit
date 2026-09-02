# Multi-tenancy: direction for our Airflow platform

**Status:** decision proposal for the platform team. Draft for discussion.
**Inputs:** AS26 talks (`../as26-airflow-as-a-platform.md` = Capital One,
`../as26-multi-team-airflow.md` = AIP-67, `../as26-trigger-queues-datadog.md` =
Datadog).

---

## Our situation

| | |
|---|---|
| **Current architecture** | One shared Airflow instance — single scheduler / API / metadata DB / DAG processor, all tenants' DAGs in one deployment |
| **Scale** | <10 tenant teams, growing slowly |
| **Workload homogeneity** | **Very heterogeneous** — different deps, resource profiles, DAG patterns, custom operators |
| **Pain, all active** | noisy neighbours · dependency conflicts · onboarding = platform-team bottleneck · upgrade coordination pain · security / blast radius |

Two facts drive the decision:

1. **We're small and growing slowly** → we do **not** need Capital-One-scale
   fleet machinery (hundreds of instances, progressive wave rollouts,
   fleet-wide canary infra). That investment is the expensive part and it
   doesn't pay back at our size.
2. **Workloads are very heterogeneous** → we **cannot** do Capital One's "one
   template for every tenant." Templating has to cover the *isolation
   mechanism*, not the *workload*.

---

## The key reframe

> **Template the platform slice, not the workload.**

The thing that's uniform across even very different tenants is the **isolation
mechanism**: each team needs a bundle, a DAG processor, a worker pool, a
concurrency pool, an RBAC scope, a secrets scope. That's templatable.

The workload — deps, resource sizes, DAG patterns — stays as **tenant-controlled
parameters** (their image, their `requirements.txt`, their pool size). We don't
try to standardize *what* they run, only *how it's fenced off*.

This is the same idea as Capital One's "declarative standardization," but applied
to **per-team slices of one instance** instead of **per-team whole instances**.

---

## Options

### Option 1 — Incremental isolation on the one shared instance ✅ recommended

Keep the shared control plane (scheduler / API / DB). Add isolation per team,
**in priority order matching our pain**, using **stock Airflow 3.x primitives**
(no multi-team mode required):

| Pain | Fix | Mechanism |
|---|---|---|
| Dependency conflicts + parse-time security | **Per-team DAG bundle + dedicated DAG processor** | `dag_bundle_config_list` bundles + one `dag-processor -B <bundle>` per team, each its own pod / image / SA. See `../learning/01-...md`, `../as26-multi-team-airflow.md`. **Doesn't need multi-team.** |
| Noisy neighbours | **Per-team worker isolation + concurrency caps** | Separate worker pools / queues per team (K8s executor per-team pod templates, or Celery queues) + a per-team **Pool** capping parallel tasks |
| Onboarding bottleneck | **Templated provisioning** | A small config (`team: X, image: ..., pool_slots: N, bundle_repo: ...`) that generates the bundle entry + processor Deployment + worker pool + Pool + RBAC. GitOps'd. |
| Upgrade coordination | **One control plane = one upgrade** (already the upside) + CI compat checks on tenant DAGs + ephemeral per-PR test env (cf. `../airflow-2-to-3-migration-talk.md`) | — |
| Scheduler/DB blast radius | **Accept for now** at <10 teams; add per-team Pools + `max_active_tasks/runs` so no single tenant can flood; revisit if a tenant can actually take down the scheduler | — |

**Pros:** addresses the acute pain now; cheap; every piece (bundles, per-team
processors, per-team pools) is a **direct on-ramp to AIP-67** (which adds
`team_id` + RBAC on top of exactly these primitives).
**Cons:** shared scheduler/DB blast radius remains; we build a bit of
provisioning glue that AIP-67 may later replace.

### Option 2 — Split into 2–3 instances by rough workload archetype

E.g. one instance for batch-ELT teams, one for ML / experimental teams.

**Pros:** real control-plane isolation; upgrade multiplier is 2–3×, not 60×;
simple mental model; no experimental features.
**Cons:** heterogeneity means archetypes may not cleanly separate; still manual
onboarding within each; 2–3× the platform surface to run; cross-instance tenant
moves are painful.
**When:** fall back here if Option 1's shared-scheduler/DB contention becomes the
binding constraint before AIP-67 is ready.

### Option 3 — Wait for AIP-67 GA, then adopt native multi-team

Do minimal stopgaps now (per-team Pools, a couple of worker queues), invest in
the **Auth Manager** work, adopt multi-team when it's stable.

**Pros:** the "right" long-term shape; upstream-supported; `team_id` everywhere +
UI/RBAC filtering built in.
**Cons:** **experimental today**; needs non-trivial Auth Manager work (per-user
team membership + team-filter hooks — memory `multi-team-needs-auth-manager-work`);
ROI at <10 teams is thin *right now*; doesn't fix today's pain while we wait.
**This is the destination, not the first move.**

### Not recommended — per-tenant instance fleet (the Capital One model)

Overkill for <10 teams growing slowly. The fleet-upgrade / canary / provisioning
machinery is the costly part and it doesn't pay back until you have dozens of
instances. Reconsider only if growth accelerates sharply.

---

## Recommendation

1. **Now: Option 1.** Incremental isolation on the shared instance, in this
   order:
   1. Per-team **Pools** + `max_active_tasks_per_dag` / `max_active_runs_per_dag`
      defaults — cheapest fix for noisy neighbours, do this week.
   2. Per-team **DAG bundles + dedicated DAG processors** — kills dependency
      conflicts and the parse-time code-execution surface.
   3. Per-team **worker isolation** (queues / pod templates).
   4. **Templated provisioning** wrapping 1–3 so onboarding is `fill in a config`.
2. **Plan: Option 3 as the destination.** Track AIP-67 maturity; start scoping
   the Auth Manager work now so it's ready when we want it. Everything in Option
   1 is the on-ramp.
3. **Fallback: Option 2** if shared scheduler/DB contention bites before AIP-67
   is ready.
4. Revisit if team count jumps past ~25–30 or growth accelerates — that's when
   fleet thinking (Option 2 at more instances, or Capital-One-style) earns its
   cost.

---

## Open questions to resolve before committing

- **Executor:** what do we run today (Celery / Kubernetes)? Per-team worker
  isolation is cleaner on KubernetesExecutor (per-team pod templates) than
  Celery (per-team queues + dedicated workers).
- **Auth Manager:** FAB or custom? Determines the AIP-67 effort.
- **DAG delivery:** how do tenants ship DAGs today (shared git repo / per-team
  repos / baked images)? Per-team bundles want per-team sources.
- **Secrets:** one backend or can we scope per team already?
- **The #57081 bug** (`-B`-scoped dag-processor crashing on cross-bundle
  callbacks) — check status for our target version before hard-sharding
  processors.
- Is anyone actually able to take down the shared scheduler today, or is the
  "blast radius" pain currently about the *webserver/UI* and *secrets/RBAC*
  (which Option 1 fixes) rather than the scheduler (which it doesn't)?

---

## What this unblocks / relates to

- `cross-tenant-triggering-move-to-assets.md` — independent, do in parallel.
- `observability-otel-openlineage.md` — the per-team metrics get more useful once
  teams are actually separated; do in parallel.
- Onboarding template is the same shape whether we land on Option 1 or 3 —
  building it now is not wasted.
