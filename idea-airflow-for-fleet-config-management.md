# Idea: Airflow for configuration management across the fleet?

**Status:** thought experiment, sparked at Airflow Summit 2026 (Capital One
"Airflow as a Platform" keynote). Not a proposal yet.

**Context:** we already use Airflow for machine lifecycle. Could we also use it
for config management across clusters / the company fleet?

## Verdict

**Airflow as the orchestrator, not the config engine.** Drive a real
desired-state tool with Airflow; don't reimplement one in DAGs.

## Why Airflow alone is the wrong tool for this

- **It's a workflow orchestrator, not a desired-state reconciler.** Config mgmt
  wants continuous convergence + self-healing; Airflow does batch runs on a
  schedule. Between runs, drift is invisible.
- **No native state model** (desired vs. observed, per target) — you'd build and
  store it yourself.
- **Push, not pull.** For a fleet you want each cluster reconciling itself from
  Git (survives control-plane outage, no fan-out blast radius). Airflow's model
  is one scheduler reaching out to N targets — a bad DAG run pushes bad config
  everywhere at once.
- **Scale shape.** Hundreds/thousands of targets as dynamic-mapped tasks →
  large serialized DAGs, parsing pressure (see
  `as26-optimising-airflow-real-world.md` Layer 1), scheduler becomes the SPOF
  for reconciliation cadence.
- **Idempotency is on you** in every task.
- Capital One deliberately splits this: **Argo CD** for the control plane,
  Airflow for data pipelines.

## Where Airflow genuinely fits (orchestration on top of a config tool)

- **Sequenced fleet rollouts** — canary → checks → wave 1 → wave 2, each stage
  gated, audit trail, one-click rollback. Airflow triggers Argo CD syncs /
  Ansible / Terraform; they do the convergence.
- **Cross-cutting coordinated migrations** — one-time "change X across the whole
  fleet in the right order, with approvals."
- **Periodic compliance scans + drift reporting** — cron DAG checks every
  cluster against policy, posts a report. Detection, not enforcement.
- **Non-k8s config with no GitOps story** — network gear, SaaS tenants, DB
  grants.

## The pattern

- **GitOps (Argo CD / Flux)** owns "what each cluster should look like" and
  continuously reconciles it.
- **Airflow** owns "roll this change across the fleet safely, in order, with
  gates + rollback" and "tell me where we've drifted."
- Airflow tasks **drive the reconciler**, they don't `ssh` in and edit files.

## Open questions if we pursue it

- Do our targets have a GitOps story already, or is it heterogeneous?
- How much of the need is *rollout orchestration* (Airflow fits) vs *steady-state
  enforcement* (it doesn't)?
- Who owns the desired-state repo(s) and approval flow?
