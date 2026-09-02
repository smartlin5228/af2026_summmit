# From Airflow 2 to Airflow 3: Migrating 100+ DAGs Without Downtime or Developer Burden

**Event:** Airflow Summit 2026

## Talk summary

Migrating a production Airflow deployment from version 2 to 3 without disrupting
hundreds of DAGs across multiple teams sounds scary (and it is). This talk shares
how the team migrated versions:

- Without a big-bang cutover
- Without weeks of cross-team change requests
- Without leaving pipelines in a broken state

## Key points covered

- **Compatibility layer** — code runs on both Airflow 2 and 3 during the migration.
- **AI tooling** — used to orchestrate 400+ DAG changes.
- **On-demand ephemeral environments** — full k8s deployments spun up per pull
  request, enabling experimentation and testing of all required changes.

## My notes / observations

- **No dedicated cluster for the migration.** Airflow 2 and 3 ran in place at the
  same time on the existing infra — not a separate parallel cluster per version.
- **Staging environment used for validation.** Per the speaker, staging had the
  same access/permissions as prod but ran against a *subset of data* — enough to
  validate DAG behavior on Airflow 3 without touching full prod data.

## Takeaways

- What they learned
- Where they failed
- What they would do better next time
