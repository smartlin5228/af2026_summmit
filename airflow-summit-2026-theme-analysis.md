# Airflow Summit 2026 — theme analysis (whole program)

Based on the full 101-session program (`https://airflowsummit.org/sessions/2026/`).
Counts are approximate — many talks span themes.

---

## Ranked themes

### 1. AI & agents — the dominant theme (~30 sessions, ~1 in 3)

**(a) Airflow orchestrating AI / agent workloads:**
Agentic Pipelines on Airflow · Agor (agent collaboration layer) · Apache Airflow,
AI Agents and I · Dynamic graphs for agentic workflows · Build AI Pipelines with
AF3 · Orchestrating & Testing RAG Pipelines · Orchestrating GenAI & ML Pipelines ·
From Hours to Minutes: Local LLMs for Sensitive Data · Data Engineers Already
Solved Agentic AI's Reliability Problem · Graphs of AI Coding: Agentic MapReduce
with Airflow's New Sandboxes · Beyond Containers: agent isolation · Taming AI
Workloads · 850 Engineering Hours Back (Wix agentic platform) · community
discussion: Agentic Orchestration.

**(b) AI helping author / operate Airflow:**
Talk to your Dags: AI Assistant (AIP-101) · Airflow Autopilot (generate-verify-
refine) · Airflow as a Harness: The Workflow That Merged Itself · Spec-Driven
Development for DAGs · How We Taught an Agent Airflow (Otto) · Using agents to
create ML pipelines (DAG factory on MWAA serverless).

### 2. Self-healing / autonomous incident response (~8 — a subset of #1)

From Broken DAGs to Self-Healing Pipelines (×2, different talks) · Designing
Self-Healing Airflow Platforms: Autonomous DAG Recovery at Scale · Nobody Got
Paged: Agentic Incident Diagnosis · Stop Fighting Fires: Autonomous Incident
Response using AI Agents · The Self-Healing Pipeline: Error Classification
Eliminated 100% of Engineering Oversight · Airflow Already Knows the Root Cause.
→ **the loudest sub-theme.**

### 3. Platform / multi-tenancy / federation / scale (~13)

Airflow as a Platform (Capital One) · Multi-Team Airflow (AIP-67) · Triggers at
Datadog: Trigger Queues · Breaking the Monolith: Remote Execution for Multi-Team ·
Architecting the Center of Excellence: Federated Airflow at Scale · One Gateway,
Six Clusters (eBay) · Building a low-cost Airflow Platform for Small Teams · Stop
Being the DAG Bottleneck · Stabilizing LinkedIn Continuous Deployment · Scaling
for Capacity Forecasting at Amazon Prime Video · Scaling to 1,000 DAGs · From
Chaos to Control: Airflow Sprawl · Asset-Centric Federated Governance.

### 4. Migration / upgrade 2→3 (~6)

Enterprise-Grade Airflow Upgrade · Streamlining Your Airflow Upgrade · Migrating
at Scale — What the Docs Don't Tell You · Migrating Airflow 2→3 for Infra Ops at
Scale · From Airflow 2 to 3: Migrating 100+ DAGs.

### 5. Reliability / durability / performance (~8)

Resumable Task Execution (AIP-103/105) · Debugging the Undebuggable · Performance
Debugging: From Symptoms to Solutions · Optimising Airflow in Real-World
Deployments · Reliability Patterns from Large-Scale Distributed Systems · Anatomy
of a Task Instance · Airflow Callbacks Revamped · Deadlines for DAGs.

### 6. Assets / event-driven scheduling (~7)

Airflow 3.0 Asset Watchers: Cross-Domain Data Mesh · Asset Partitions · From Cron
to Assets: Drone Telemetry · Self-Service DAGs: Event-Driven at Lyft ·
Event-Driven Monitoring: Metadata to Kafka via CDC · Asset-Centric Federated
Governance · UX: Dags to Assets to Agents.

### 7. Spark integration (~6)

A Decade of Spark + Airflow · Spark Declarative Pipelines + Airflow 3 ·
Orchestration Decisions Impact Performance and Cost · Resumable Task Execution ·
Streaming + Batch with Kafka/Spark/K8s on GCP · community discussion: Spark.

### 8. DAG authoring abstraction / DX (~6)

Building Blocks, not Factories · The Rise of Abstraction: From YAML to Minecraft ·
Spec-Driven Development · Stop Being the DAG Bottleneck · Developer Velocity at
Scale: Production-Like Environments on K8s · Airflow in a Box (testing).

### 9. Polyglot / language SDKs (~4)

Toward a Polyglot Airflow · From JAR to DAG (Java SDK) · One Codebase, Many
Distributions · A SQL Query is Just a DAG.

### 10. Security / auth (~3)

Fixing the Token Authentication: Revocation, Scoping, Securing the Execution
Boundary · Securing Airflow with Keycloak (Keycloak Auth Manager) · Beyond
Containers (agent isolation).

### 11. MLOps (~5)

Orchestrating 100 ML Models · Taming the MLOps Zoo · From Experiments to
Production: Lightweight ML Platform · From Data Pipelines to Business Outcomes.

### 12. Creative / unusual use cases (~8, span themes)

Airflow 3 as a Dungeon Master · Airflow to the rescue: chemical emergencies ·
DAGs Move Robots (silicon validation labs) · AI endurance sports coach · Gleaming
the Cube (Rubik's Cube) · Healthcare Interoperability · Graph DB workloads with
TinkerPop · YAML to Minecraft.

---

## One-line summary

**2026 is the AI year.** ~1/3 of the program is agents-in-Airflow or
AI-authoring-Airflow, and "self-healing" is that theme applied to ops. Beneath
it, the perennial infra concerns continue — **multi-tenancy / platform** is the
biggest, with **migration**, **assets / event-driven scheduling**, and
**durability / reliability** close behind. **Polyglot** and **security/auth** are
smaller but clearly on the roadmap.

## What this means for us (platform team)

- The **agentic wave** will reach our tenants — plan the guardrail policy
  (`platform/`… agentic TODO) before they adopt, not after.
- **Self-healing / AI incident diagnosis** validates the OTel+OpenLineage+agent
  direction (`platform/observability-otel-openlineage.md`) — the whole industry
  is converging on it.
- **Multi-tenancy** being the #1 infra theme confirms we're not early or late —
  it's the right time to act (`platform/multi-tenancy-decision.md`).
- **Assets** are clearly the strategic direction for scheduling — reinforces
  moving tenants off `TriggerDagRunOperator`
  (`platform/cross-tenant-triggering-move-to-assets.md`).
