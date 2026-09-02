# AS26: Performance Debugging in Airflow — From Symptoms to Solutions

**Event:** Airflow Summit 2026 · Day 2

> Related: `as26-optimising-airflow-real-world.md` (Koti/Regani) covers similar
> ground from a fleet-operator angle — this one is more codebase / upstream-fix
> oriented.

## Session summary

Airflow slow? Memory spiking? Tasks queuing forever? In a distributed system,
the hard part is isolating *which* moving part — scheduler, database, DAG
processor, or DAG code. Practical techniques for isolating and fixing perf
problems, with real examples from the Airflow codebase.

## What's covered

- **Understanding Airflow's moving parts** — where bottlenecks typically hide:
  scheduler loop, DAG parsing, database queries.
- **Profiling techniques** — memory profiling, query analysis, and the metrics
  that actually matter.
- **Case study: DAG Processor OOM** — how a *single SQLAlchemy query* caused a
  memory explosion, and how it was fixed.
- **Testing your fixes** — setting up reproducible performance tests before and
  after.

## Takeaway

A toolkit for tackling Airflow performance issues — whether troubleshooting your
own deployment or contributing fixes upstream.

## My notes / observations

_(add during/after the talk)_
