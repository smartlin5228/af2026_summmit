# AS26: Airflow Already Knows the Root Cause

**Event:** Airflow Summit 2026 · Day 3

> **Likely useful for our tenant teams** — a shared context/lineage layer they'd
> get "for free" on the managed platform. Ties to
> `as26-agentic-pipelines-on-airflow.md` (OTel observability, gather+diagnose
> callbacks) and `as26-multi-team-airflow.md`.

## Thesis

- Dashboards show **symptoms**. Lineage shows **relationships**. Neither explains
  **why** something happened.
- In the AI era, Airflow is **operational intelligence**, not just an
  orchestrator.
- **OpenLineage** (open standard for data lineage) emits rich structured context
  across Airflow, Spark, and other tools — including **failure logs and detailed
  execution metadata**.

## Recent improvements (available today, OOTB)

- **Run ID correlation** between Airflow entities out of the box — trace a run
  across DAG / task / asset / external system.
- **Hook-level lineage for SQL operators** — captures the actual **queries and
  query IDs** from *within* Python operators.
- **Human-in-the-loop metadata** — who approved what, and when.
- …and more — "the context layer is more powerful than ever."

## Talk structure

- What OpenLineage gives you OOTB **today**.
- What's coming next.
- How this foundation powers **AI-driven assistants / auditors / operational
  agents**.
- Demo: **Astro Observe Investigation Agent** — but the focus is the **context
  layer available to everyone**, not the product.

## My notes / observations

_(add during/after the talk)_

### Why this matters for us

- If we run the managed platform, enabling the OpenLineage provider centrally
  gives every tenant team lineage + failure context with **no per-team work**.
- Cross-team debugging: run-ID correlation + lineage could show a Team B failure
  caused by a Team A asset going stale — exactly the cross-team blind spot in
  the multi-team model.
- Feeds the "gather + diagnose" callback pattern and any future ops agent.
- **We already run OTel (metrics + traces) on Airflow 3.** OTel = "is something
  wrong / where did the wall-clock go". OpenLineage = "**why** did this run
  behave differently" (what data, which query, row counts, upstream freshness).
  The three layers together end the workflow-owner ↔ infra ping-pong: both teams
  (or an agent) look at the *same* run record.
- Full proposal + the free-metrics audit list:
  `platform/observability-otel-openlineage.md`.
