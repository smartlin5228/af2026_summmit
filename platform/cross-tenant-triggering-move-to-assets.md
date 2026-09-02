# Cross-tenant DAG triggering: move from `TriggerDagRunOperator` to assets

**Status:** platform proposal / recipe. Not yet rolled out.
**Context:** multi-team is **not** enabled. Today, Tenant A triggers Tenant B's
DAG with `TriggerDagRunOperator`. We want to move cross-tenant coordination to
**asset-driven scheduling** and promote that pattern to tenants.

Background reading: `../learning/03-airflow-assets.md`,
`../as26-multi-team-airflow.md` (cross-team assets section).

---

## Why change

`TriggerDagRunOperator` is for **"I own a sub-workflow and I'm invoking it."**
For **peer / cross-tenant** coordination it's the wrong tool:

| Problem with `TriggerDagRunOperator` cross-tenant | Assets fix |
|---|---|
| A hardcodes B's `dag_id` → A coupled to B | A/B only share an asset name |
| A owns the fan-out → new consumer = change A | consumers self-subscribe |
| Dependency is invisible (no lineage, not in Assets graph) | shows in the Assets UI + OpenLineage |
| B can't cleanly have multiple producers | `schedule=a | b`, or many producers of one asset |
| A can accidentally trigger B with bad/`conf` payloads | asset events carry only metadata, not control |

Asset-driven scheduling is the **Airflow-recommended pattern** for this in 3.x.

---

## The change, per side

### Producer (Tenant A)

```python
from airflow.sdk import Asset

ORDERS = Asset("s3://lake/teamA/orders")     # URI = the real storage location

@task(outlets=[ORDERS])
def build_orders():
    ...  # writes the data; on task SUCCESS an asset event is emitted
```

- **Remove** the `TriggerDagRunOperator` task.
- Put `outlets=` on the task that **actually produces the data** (usually the
  last real task), not a trailing dummy.

### Consumer (Tenant B)

```python
from airflow.sdk import Asset

ORDERS = Asset("s3://lake/teamA/orders")     # same URI, declared independently

@dag(schedule=[ORDERS])                      # was: schedule=None
def downstream():
    ...
```

- Change `schedule` from `None` / cron to `[ORDERS]`.
- If B needs to know *what* changed: read `context["triggering_asset_events"]`
  or use `inlets=[ORDERS]` + `context["inlet_events"][ORDERS][-1].extra`.

---

## Migration recipe (no gap, no double-runs)

Coordinate across the two tenants. **Do not** just delete the trigger and hope.

1. **A: add `outlets`** (deploy). Asset events start flowing. Nothing consumes
   them yet — B is still triggered the old way. Zero behaviour change. Verify
   events appear in the **Assets** view.
2. **Same release, or the next: B switches `schedule` to `[ORDERS]` AND A removes
   the `TriggerDagRunOperator`.** Land these together → B goes straight from
   "triggered by A" to "scheduled by asset", no overlap.
   - If they *can't* land together, accept a short window where B has both and
     may double-run, and keep it to hours not days.
3. **Verify:** B's runs now show an asset event as the trigger
   (`Run type: asset triggered`). Remove any leftover trigger glue in A.

Roll back = revert step 2 (B back to `schedule=None`, A re-adds the trigger).

---

## Pre-flight checklist — check BEFORE migrating a given A→B pair

| A's `TriggerDagRunOperator` currently... | OK to move to assets? |
|---|---|
| just fires B when A finishes | ✅ clean swap |
| B should run when **A1 or A2** finishes | ✅ `schedule=a1 | a2` |
| passes `conf={...}` to B | ⚠️ **assets carry no run payload.** B must get the value from the asset event `extra` (A sets `outlet_events[ORDERS].extra = {...}`) or derive it itself. Migrate the payload first. |
| `wait_for_completion=True` (A blocks on B) | ❌ asset scheduling is fire-and-forget. If A truly needs B's result, this is a design smell for cross-tenant — resolve separately. |
| triggers B **conditionally** (sometimes skips) | ⚠️ asset events fire on task success. Options: emit the asset conditionally, or let B run every time and short-circuit early. |
| relies on cron catchup / backfill in B | ⚠️ asset-scheduled DAGs don't backfill like cron. Confirm B doesn't depend on it. |
| triggers B many times per A run (loop) | ⚠️ one asset event per successful task run; a loop of `TriggerDagRunOperator` doesn't map 1:1. Rethink. |

---

## Tenant-facing template

Give tenants this. Two files, two tenants, they only share the **URI string**.

**Producer DAG (`team_a/orders_dag.py`):**
```python
from airflow.sdk import DAG, Asset, task
import pendulum

# Name the asset by WHERE the data lives. This string is the contract.
ORDERS = Asset("s3://lake/team_a/orders")

with DAG("team_a_orders", schedule="@hourly", start_date=pendulum.datetime(2026, 1, 1)):
    @task(outlets=[ORDERS])
    def build_orders(**context):
        rows = write_orders_to_s3()
        # optional: expose metadata to consumers
        context["outlet_events"][ORDERS].extra = {"row_count": rows}
    build_orders()
```

**Consumer DAG (`team_b/enrich_dag.py`):**
```python
from airflow.sdk import DAG, Asset, task

ORDERS = Asset("s3://lake/team_a/orders")     # declared again, same string

with DAG("team_b_enrich", schedule=[ORDERS], start_date=None):
    @task(inlets=[ORDERS])
    def enrich(**context):
        last = context["inlet_events"][ORDERS][-1]
        print("orders updated, rows:", last.extra.get("row_count"))
        ...
    enrich()
```

### Naming rules for tenants

- **Use a URI that points at the real storage location** (`s3://…`, `gs://…`,
  `snowflake://acct/db/schema/table`). The name then *is* the location:
  self-documenting, collision-resistant.
- One producer per asset is the simple case. Multiple producers of the same
  asset is allowed but agree on the schema.
- Don't rename assets casually — it breaks every consumer silently (they just
  stop triggering).

---

## What we defer until multi-team

- **`AssetAccessControl`** (`producer_teams` / `consumer_teams`) — not needed now;
  without multi-team, any DAG can produce/consume any asset. When we enable
  multi-team, cross-team asset events go **off by default** and each pair needs
  explicit opt-in. Because tenants will already be naming assets by URI, adding
  the access-control wrapper later is mechanical.
- Per-team asset views / RBAC.

---

## Rollout plan (platform side)

1. Publish the tenant-facing template + naming rules in our docs.
2. Inventory existing `TriggerDagRunOperator` cross-tenant edges (grep DAG repos
   for `TriggerDagRunOperator`). Classify each against the pre-flight checklist.
3. Migrate the clean ones first (no `conf`, no `wait_for_completion`), pair by
   pair, using the 3-step recipe.
4. Handle `conf`-passing edges: move the payload into asset `extra` first, then
   migrate.
5. Leave `wait_for_completion=True` edges for a design review — those are
   cross-tenant synchronous coupling and probably shouldn't exist.
6. Add a lint / policy: new cross-tenant coordination must use assets, not
   `TriggerDagRunOperator`.
