# AS26: Airflow as a Platform — Operating High-Volume, Enterprise-Scale Orchestration

**Event:** Airflow Summit 2026 (keynote) · Day 2
**Speakers:** Capital One

## Session summary

What happens when your Airflow footprint grows from a single team's scheduler to
a fully managed enterprise data platform? You stop thinking about individual DAGs
and start thinking about:

- Fleet operations
- Tenant isolation
- Automated upgrades
- Self-service authoring at scale

The team built a **multi-tenant orchestration platform on Apache Airflow**:
**hundreds of isolated instances on Kubernetes**.

## Deep dives

- **The architecture** — strict tenant isolation + automated infrastructure
  provisioning on Kubernetes.
- **Fleet operations** — seamless, zero-downtime upgrades across hundreds of
  instances.
- **The abstraction layer** — a self-service experience that lets any team
  (technical or non-technical) go from idea to production pipeline without
  managing infrastructure.

## Takeaway

Actionable architectural patterns + real-world operational lessons, whether
scaling a current deployment or managing Airflow at enterprise level.

## My notes / observations

### Why they started with shared Airflow

- The shared model was **created to serve an orchestration gap in the company** —
  teams needed orchestration and had nowhere to run it.
- Goal was to **consolidate the underlying infra operations for all user
  personas** — one platform team runs the plumbing, every persona (data eng,
  analysts, DS, non-technical) gets orchestration without owning infra.
- So shared-Airflow was the right v1: fastest way to close the gap and centralize
  ops.
- They built a **managed Airflow model** so developers **spend less time
  maintaining** infra and more on pipelines.
  - **Similar to what we do** — managed/hosted Airflow for internal teams, same
    "devs shouldn't run the scheduler" premise. Worth comparing their tenant
    isolation + upgrade story against ours.
- **What the platform team owns** (the managed boundary):
  - Kubernetes infrastructure
  - Deployment pattern (how an instance is stood up / configured)
  - Airflow version lifecycle (upgrades, deprecations, EOL)
  - Support model (on-call, tenant support tiers, escalation)
- **What the users/tenants own:**
  - DAG authoring
  - Their dependencies (Python packages / requirements for their pipelines)
  - All development — pipeline logic, testing, connections/variables for their
    workloads
- Clean split: platform owns the runtime + lifecycle + infra; users own the code
  and everything they put into it.

### The problem it hit

- Rapid Airflow adoption inside the org exposed **scaling problems in the shared
  ("one big Airflow for everyone") model**:
  - _(noisy-neighbour: one team's heavy DAGs starve others' scheduling)_
  - _(blast radius: one bad DAG / dependency / upgrade breaks everyone)_
  - _(no isolation of secrets, connections, RBAC between teams)_
  - _(can't upgrade / change config without coordinating across all teams)_
  - _(central platform team becomes the bottleneck for every onboarding)_
- → drove the move from shared instance to **many isolated per-tenant
  instances** (fleet model).

_(add more during/after the talk — fill in which of the above they actually hit)_

### Operational & scaling challenges (their list)

1. **Onboarding bottlenecks** — users had to configure their own infra, and also
   learn Airflow itself. Slow, high-touch, platform team in the loop for every
   new tenant.
2. **Version & config drift** — no standardized deployment process, so **every
   instance becomes its own snowflake / its own challenge** to operate and
   upgrade.
3. _(third — TBD)_ — likely **inconsistent / manual upgrades** (can't roll a fix
   across the fleet).
4. _(fourth — TBD)_ — likely **support load / no observability across the
   fleet** (platform team can't see or debug hundreds of instances uniformly).

### Their solution — "fully managed Airflow"

Three pillars:

1. **Fully managed infrastructure** — platform provisions and runs everything;
   tenant never touches k8s / infra config.
2. **Declarative standardization** — a **domain-specific language** so configs
   are **pre-defined, tested, and consistent** across instances.
   - My read: yes, effectively **templated** — tenants declare intent (env size,
     schedule tier, connections, etc.) in a constrained DSL, and the platform
     renders the actual k8s + Airflow config from vetted templates. Kills the
     drift problem because nobody hand-writes deployment config.
3. **Governed enterprise guardrails** — central policy baked in: RBAC, network,
   secrets, allowed images/providers, resource limits, compliance — tenants
   can't opt out.

Net: onboarding becomes "fill in the DSL", drift goes away (one rendering
pipeline), guardrails are enforced not documented.

### Tenant deployments & isolation

- **Managed provisioning**
  - Centralized **UI** + an infra **provisioner**.
  - Deployed via **Argo CD**, backed by a **control-plane repo** (GitOps).
  - **Helm chart configuration and container images are entirely
    platform-managed** — tenant doesn't pick or pin either.
- **Resource guarantees**
  - Strict **CPU and memory limits** per tenant.
  - **Task concurrency pools** enforced (per-tenant caps on parallel tasks).
  - → one tenant can't starve or OOM the shared substrate.
- **Isolated orchestrators**
  - Each tenant gets its **own Airflow instance** (own scheduler / webserver /
    workers), not a shared scheduler with namespacing.
  - Full blast-radius isolation: a bad DAG, dependency, or config only affects
    that tenant.

### Zero-downtime fleet upgrades

Pipeline, in order:

1. **Chart & image upgrades** — new Helm chart + Airflow image versions prepared
   centrally (single source, not per-tenant).
2. **Integration testing & regression canaries** — automated integration tests +
   canary instances catch regressions before the fleet sees them.
3. **Progressive one-step rollouts** — roll out in waves/stages across the fleet
   (not big-bang), each step gated.
4. **E2E observability & simplified rollback** — full-fleet visibility during the
   rollout; one-action rollback if a wave goes bad.

Key: because charts/images are platform-managed and standardized, an upgrade is
*one* artifact rolled progressively — not hundreds of bespoke migrations.

### Why this whole model works (my takeaway)

The reason they can own the **entire end-to-end process** — provisioning,
upgrades, guardrails, rollback — is the **templated / DSL workflow**. It only
works because most tenants are doing **fundamentally similar things**:
scheduled batch pipelines, a handful of connection types, standard resource
profiles. Constrain the surface to that common shape and you can:

- standardize deployment → no drift → one upgrade artifact
- test once centrally → canary → roll to all
- enforce guardrails in the template, not per-review

The trade-off: tenants with genuinely unusual needs don't fit the template. The
bet is that's a small minority, and the platform leverage on the other 95% is
worth it. **This is the crux of whether the approach transfers to us** — how
uniform are our tenants' workloads?

### Guardrails from day one

- **CI/CD rules for tenant packages** — stringent constraints on *all* custom
  operators from the start: **style, performance, and logic** checks in CI. Plus
  a written **contribution model** to facilitate data consistency (how tenants
  add/change shared operators).
- **Mandatory deferrable operators** — tenants must use deferrable/async
  operators (frees worker slots during waits; big lever at fleet scale).
- **Observability with a system of record** — centralized telemetry + an
  authoritative system of record for fleet state (what's deployed where, run
  history, SLAs).
- Point: guardrails were designed **from day one**, not retrofitted — much
  cheaper than un-drifting later.

### Argo vs. ArgoCD (they used "Argo-backed control plane")

- **"Argo"** = the umbrella project (argoproj), a family of 4 Kubernetes-native
  tools. **"ArgoCD"** is one of them. People saying "Argo" at the conference
  almost always mean **Argo CD**.
- The four:
  | Tool | Job |
  |---|---|
  | **Argo CD** | GitOps continuous delivery — syncs k8s manifests from a Git repo to clusters. *This is the "control-plane repo" pattern Capital One described.* |
  | **Argo Workflows** | Kubernetes-native workflow/DAG engine — batch, data, ML pipelines (an Airflow-ish competitor for some use cases). |
  | **Argo Rollouts** | Progressive delivery — canary / blue-green as a Deployment replacement. Fits their "regression canaries + progressive rollouts". |
  | **Argo Events** | Event-driven automation — webhooks, S3 drops, queues trigger actions. |
- They compose: Events triggers → Workflows runs CI → **Argo CD deploys via
  GitOps** → Rollouts does the safe progressive release. Most teams start with
  Argo CD.
- So "Argo-backed control plane repo" = a Git repo of declarative tenant/infra
  config that **Argo CD** continuously reconciles onto the clusters.

### Business framing (why leadership funds it)

| Business goal | Metric / target |
|---|---|
| **Accelerated time to value** | Cycle time: **< 1 day** idea → production |
| **High data trust & governance** | Reliability: **99.9% uptime SLA** |
| **Cost & resource efficiency** | High compute savings + high user adoption |

- The platform investment is justified in business terms, not just eng
  convenience: faster delivery, contractual reliability, lower spend, wider
  adoption.
- Note the tension: 99.9% SLA (~8.8h downtime/yr) across *hundreds* of instances
  is exactly why the zero-downtime fleet-upgrade machinery matters.

### Architecture diagram

- They showed a full platform layout diagram. **TODO: upload the photo** (add to
  this repo, e.g. `img/as26-platform-architecture.jpg`, and link it here).
