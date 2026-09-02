# AS26: Agentic Pipelines on Airflow — From Thesis to Production

**Event:** Airflow Summit 2026 · Day 2

## Thesis

The industry treats **agents** and **pipelines** as opposing paradigms. The
speakers argue that's wrong: most agentic problem-solving, when you look at what
it actually does, **has pipeline structure** — gather data, process each
dimension independently, synthesize, evaluate.

The real question isn't "agents or pipelines?" but **where the LLM fits inside
the pipeline**, and what you gain by making each step explicit (auditability,
retries, observability, testing).

## What's covered

### First-class LLM support — AIP-99

An operator library giving Airflow native LLM support:

- Inference
- SQL generation
- Branching (LLM-driven)
- Schema validation
- Embedding

Backed by **PydanticAI**, with **20+ model providers** out of the box.

### The worked example

A real pipeline analyzing **5,856 survey responses**:

- **4 parallel LLM-generated queries**
- **DataFusion** execution
- A **synthesis** step

Shows exactly where the LLM *reasons* and where the pipeline *handles everything
else*.

### Fault tolerance — AIP-105

Fault-tolerant agentic systems need more than retry counts.

- **Pluggable retry policies** that classify failures **at the exception level**.
- Includes an **LLM-powered variant** that distinguishes a rate limit vs. an
  auth error vs. a transient network blip.
- **`LLMSchemaCheckOperator`** — validates upstream data *before the LLM ever
  sees it*.
- **DAG Result API** — a pipeline exposes a **semantic output**, turning a DAG
  into a **callable function for downstream agents**.

(All demoed live, not theoretical.)

### What's next

- **Persistent task state for agentic workflows that survive retries — AIP-103.**
- Path toward **dynamic execution graphs** that support **feedback loops** while
  preserving the **auditability** that makes pipelines worth building.

## My notes / observations

### Task state store (AIP-103) — resume from the failure point

- A **persistent per-task state store** the running task can write checkpoints
  into *during* execution — not just the final XCom.
- On retry, the task **reads back its last checkpoint and continues from there**
  instead of restarting from scratch.
- The point: an agent task might do 6 LLM calls, then fail on the 7th. Today the
  whole retry re-runs calls 1–6 → you pay those tokens again (and re-hit rate
  limits, re-do side effects). With the state store, calls 1–6's results are
  persisted, retry skips straight to call 7.
- Yes, that matches what you said — it stores *where* the request failed +
  the work done so far, so a retry doesn't repeat token usage.
- Makes retries cheap enough that "just retry" becomes a viable recovery
  strategy for long/expensive agent runs (pairs with AIP-105's smart retry
  classification — retry only the transient failures, and resume when you do).
- **API shape:** `task_state_store.set(key, value)` / `.get(key)` — the task
  calls `set` to write a checkpoint; everything executed before that call is now
  durable. On retry it reads the last checkpoint back. Explicit, in-task calls —
  the author decides the checkpoint boundaries. Matching `asset_state_store` for
  assets.
- **Demo:** first attempt was rigged to fail on purpose at **step 16**. Retry
  kicked in, **skipped steps 1–16**, and continued from **step 17** — because
  each step had written its checkpoint via the state store. Without it the retry
  would have re-run all 16.
- **Status: SHIPPED in Airflow 3.3.0** (correcting — this is not roadmap).
  AIP-103 = "Task State Management" / Task & Asset State Store.
  - State in the metadata DB by default, or a custom worker-side backend
    (`[workers] state_store_backend`); per-key retention + GC; optional
    `clear_on_success`; manageable via Core API + Execution API.
  - `ResumableJobMixin` is the flagship use — `SparkSubmitOperator` now uses it:
    if the worker dies mid-run, the retry **reconnects to the existing Spark
    job** instead of resubmitting. On by default.

### `@task.agent` with `durable=True` — automatic step-level replay

- `@task.agent` / `AgentOperator` (from the **`common-ai` provider**, AIP-99)
  adds guardrails on top of AIP-103: durable execution, HITL review, automatic
  tool logging.
- **`durable=True`** = **step-level caching of every model response and tool
  result** to ObjectStorage as the agent runs. You *don't* call
  `task_state_store.set()` yourself — the decorator checkpoints each step.
- On retry: cached steps **replay instantly** (no repeated LLM/tool calls), then
  it resumes live from the failure point. Cache **deleted on success**.
- So: `task_state_store` = manual primitive; `@task.agent(durable=True)` =
  batteries-included version for agent loops.

### Personal note — how the checkpoint/step replay actually works

(My own dig into the mechanism, not from the talk directly.)

- **A "step" = one model completion, or one tool call + its result.** A run is a
  sequence: `model → tool → tool → model → tool → … → final answer`.
- **Each step is wrapped when `durable=True`:**
  1. Before executing step *k*, compute a **deterministic key** (step index +
     usually a hash of its inputs: message history so far, tool name + args).
  2. Look it up in ObjectStorage:
     - **Hit** → return cached result, skip execution entirely (no LLM call, no
       tool run).
     - **Miss** → execute (call model / run tool), write result under that key,
       return it.
  3. The loop code is identical whether replaying or running live — it just
     receives step results, not knowing if they're cached or fresh.
- **On retry:** whole loop re-runs from the top; every previously-completed step
  is a cache hit and returns instantly ("blasts through" the history) until it
  reaches the failed step → miss → executes live → continues forward.
- **Why it's correct:** you're not re-running the model hoping for the same
  output — you cached the *output*, so the non-determinism is frozen. Only
  requirement: the loop's control flow is a pure function of
  (prompt, step results). Branching on `datetime.now()` / RNG could make replay
  diverge; some impls hash step inputs and treat a mismatch as a miss to catch
  that.
- **Side effects:** if a tool wrote to a DB on attempt 1 then the task crashed,
  replay does *not* re-run it — serves the cached return value. Usually what you
  want (no double-write), but attempt 1's external effect stands.
- **Lifecycle:** cache keyed per task-instance (shared across its retries),
  stored in ObjectStorage (completions/results can be big — don't bloat the
  metadata DB), deleted on success.
- **It's the Temporal / durable-execution pattern:** agent loop = "workflow",
  model/tool calls = "activities" replayed from an event log. PydanticAI's
  durable execution (which `common-ai` builds on) implements exactly this.

### 3rd demo — the word puzzle, and why durable helps here

- Toy: a Wordle-style game. Agent guesses → tool replies "right letter" /
  "right letter, right position" → agent iterates.
- It's inherently a **long multi-turn loop** (many LLM + tool calls). That's
  exactly the shape `durable=True` is for.
- When it fails partway (e.g. after N guesses), the retry **replays the N cached
  guess-steps instantly** — visibly blasts through the history — then makes
  *new* LLM calls from guess N+1.
- Why useful *here* specifically: each guess is an expensive model call, the
  loop has no natural idempotency, and restarting would waste every token spent
  narrowing the answer. The puzzle just makes replay-then-resume easy to *see*.

### Token economics

- **Context grows with every tool/model call.** Each turn re-sends the whole
  running transcript (prompt + all prior calls + all prior results).
- **Quadratic vs linear.** One big agent doing N steps in a single context:
  cost grows ~**quadratically** (step k re-reads all k−1 prior steps → total
  ~N²). N scoped agent calls, each with a fresh minimal context: ~**linear**.
- **Scoped agent calls are cheaper, faster, and more accurate** — smaller
  context = fewer tokens, less latency, and the model isn't distracted by
  irrelevant history. Split the work into narrow tasks instead of one
  mega-agent.
- **Model selection per step: reasoning vs evaluation.**
  - Reasoning / generation step → the strong (expensive) model.
  - Evaluation / validation / classification step → a cheaper model is usually
    fine (and better as an independent judge).
  - The pipeline structure lets you assign a different model per task — you
    can't do that inside one opaque agent.

### Governance

- **Agent controls via hooks** — lifecycle hooks around the agent (pre/post tool
  call, pre/post step) to enforce policy: approve, deny, log, redact, rate-limit
  what the agent does.
- **Scoped connections** — the agent task only gets the connections it needs,
  with least-privilege credentials — not the whole connection pool.
- **SQL generation constraints** — bound what LLM-generated SQL can do:
  allowed tables/columns, read-only, row limits, no DDL — validated before
  execution.
- **Sandbox / code-escape guards** — if the agent runs generated code, it runs
  in a sandbox with guards against escaping it (filesystem, network, syscalls).
  (Ties to the "Beyond Containers" talk — strong isolation for untrusted agent
  code.)

### Governance demo — deliberately rogue agent

- Setup: an "analyst" agent with **full access to the company warehouse**,
  answering questions via SQL tools.
- The prompt is intentionally reckless — e.g. *"do whatever the customer notes
  ask"* — i.e. a prompt-injection vector wired straight in (would never do this
  in real life).
- Guardrails on the SQL tool: `allowed_tables=[...]`, `allow_writes=False`.
- **What the demo showed:**
  - ✅ `DELETE FROM ...` (row deletion) → **blocked** by `allow_writes=False`.
  - ✅ query against an **off-limits table** → **blocked** by `allowed_tables`.
  - ❌ the **prompt injection itself** → **not blocked** — the agent still
    followed the injected instruction; it just couldn't do damage because the
    tool-level guardrails caught the dangerous *actions*.
- **Takeaway:** tool-level allow-lists / write-protection are the real safety
  net and they work. But you **cannot rely on the model not being injected** —
  defense has to be at the tool/permission layer, not the prompt layer. The
  injection "getting through" but being harmless is the whole point: assume the
  agent is compromised and constrain what it can *do*.

### Visibility & controls

- **Human in the loop (HITL)** — the agent can pause and wait for a person to
  approve / edit / reject before continuing (built into `@task.agent`). Gate the
  risky steps, not every step.
- **Agent identity** — the agent acts as a distinct identity (not the platform
  or a shared service account), so actions are attributable and permissions are
  its own.
- **UI visibility, retries, auditability** — every agent step (each LLM call,
  each tool call + result) shows up in the Airflow UI like any other task:
  inspectable, retryable, logged. The whole run is an audit trail — the thing
  you lose with an opaque agent, and the core argument for the pipeline framing.
- **OTel observability** — agent steps emit OpenTelemetry traces/metrics, so
  agent runs plug into the same observability stack as everything else (latency,
  token counts, failure rates per step).

### Multi-dimensional evaluation

- Evaluation is **part of the pipeline, not an afterthought** — explicit tasks,
  not a check tacked onto the generator.
- Evaluate across **multiple independent dimensions**:
  - **Correctness** — is the answer factually/logically right?
  - **Safety / policy** — does it violate any policy or safety rule?
  - **Relevance / usefulness** — does it actually answer what was asked?
- Flow:
  ```
  result → [correctness eval] ┐
           [safety eval]       ├→ aggregate evaluation ──pass──→ select / publish
           [relevance eval]   ┘                        └─fail──→ regenerate  OR  human review
  ```
- Each dimension can be its own model call (possibly different models). The
  **aggregate** step combines them into a pass/fail (or score + threshold).
- On fail: either loop back to **regenerate**, or route to **human review** —
  both are explicit branches in the DAG.

### Recovery, not just retry

- Blind retry against the same failing provider doesn't help if that provider is
  down. Recovery = **change what you do on the retry**.
- Flow:
  ```
  Airflow task → managed-agent toolset → primary managed agent
                                              │
                        runtime failure / provider unavailable
                                              ▼
                     recovery policy → alternate managed agent
                       (can be a DIFFERENT provider) → continue the task
  ```
- The **toolset abstraction** lets the recovery policy swap the backing agent
  (even cross-provider) transparently, and — combined with the state store /
  `durable=True` — the alternate agent **continues from the checkpoint**, not
  from scratch.
- This is AIP-105 territory: classify the failure → if it's "provider down",
  the right response isn't wait-and-retry, it's failover.

### What production agentic pipelines need — the summary (3 buckets)

**1. Production confidence**
- Durability (AIP-103 task/asset state store)
- Agent durability (`@task.agent(durable=True)` step replay)
- Governance (hooks, scoped connections, SQL constraints, sandbox, HITL, audit)
- Token economics (scoped calls, per-step model selection)
- Recovery / failover (classify failure → failover to alternate provider,
  resume from checkpoint)

**2. Heterogeneous intelligence**
- Hosted models (the 20+ providers via PydanticAI)
- Self-hosted models
- Access to existing systems (connections/hooks — the data side)

**3. Richer execution**
- Agents (multi-turn tool loops as tasks)
- Loops (feedback loops — regenerate-until-passes)
- Dynamic graphs (execution graph not fully known ahead of time; still on the
  roadmap, must preserve auditability)

### Closing framing

- **"The goal isn't just to make agents run"** — anyone can get an agent to
  *run*. The hard part (and the point of the whole talk) is making them run
  **reliably, cheaply, safely, and auditably** in production.
- My take: agrees with the general mood — the shared worry across the room is
  less "can we build agents" and more "can we *trust and operate* them." Putting
  agents inside pipelines is a bet that the orchestration discipline we already
  have is the answer to that fear.

### Why durability matters more for AI

A full retry of a normal deterministic task is cheap. For an AI task it isn't —
four reasons:

1. **Tokens are expensive** — re-running the completed LLM calls burns real
   money every retry.
2. **Time & SLAs** — agent loops are long; redoing 15 steps to get back to
   step 16 can blow the run's deadline.
3. **Non-determinism** — re-running an LLM step doesn't reproduce the same
   output, so a naive retry doesn't just cost more, it can land somewhere
   different / worse. Replaying the cached step keeps the run consistent.
4. **Secondary failures** — hammering the provider on every retry hits **rate
   limits** (and other cascading failures), turning one transient error into
   many.

→ so for AI, "resume from checkpoint" isn't an optimization, it's close to a
requirement for production.

- **"An AI system is more than a model."** The model is one component; the
  system also includes:
  - **Tools** the model calls
  - **Evaluation** (checking outputs, gating on quality)
  - **Human judgement** (review, approval, correction in the loop)
- **Orchestration is the critical piece** that ties those together — deciding
  what runs when, passing state between steps, handling failure, and keeping the
  whole thing auditable. That's the argument for Airflow: it's already the
  orchestration layer, so put the LLM step *inside* a pipeline rather than
  wrapping a pipeline inside an opaque agent.

### What it takes to run agents in production — the talk's agenda

These five are the topics the session is structured around today; each maps to a
concrete Airflow feature / AIP:

1. **Access to multiple sources** — agents need to reach many systems (DBs,
   APIs, files, warehouses). Connections/hooks already solve this in Airflow.
2. **Durable execution / intelligent recovery** — long agent runs must survive
   crashes and retries without losing progress or redoing expensive work
   (→ AIP-103 persistent task state; AIP-105 exception-level retry classification).
3. **Governance and evaluation** — policy on what agents can do, plus
   systematic output evaluation before results are trusted / propagated
   (→ `LLMSchemaCheckOperator`, eval steps as explicit tasks).
4. **Token economics** — cost per run has to be understood and bounded; know
   where tokens go, cache, choose model per step.
5. **Dynamic execution** — the graph isn't fully known ahead of time; need
   branching and (eventually) feedback loops while keeping auditability
   (→ dynamic execution graphs on the roadmap).

### "Meet users where they are" — Airflow as the bridge

Airflow sits between two ecosystems and connects them:

- **Existing data ecosystem** (where users already are):
  - Cloud platforms
  - Warehouses / data systems — e.g. Snowflake
  - Structured data
- **Expanding AI ecosystem** (where they're going):
  - Models
  - Agent frameworks / harnesses
  - Managed agents
  - Unstructured data

Airflow orchestration is the connective tissue — users don't have to leave their
data stack to adopt AI; the pipeline reaches into both sides.

- **Key point: the integration strategy doesn't change — the ecosystem it points
  at is expanding.** Same pattern (hooks / operators / connections wiring
  external systems into a DAG), just new targets on the AI side. Teams that
  already know how to integrate Snowflake into Airflow already know how to
  integrate an LLM provider or a managed agent — it's the same mechanism.

### The basic agentic pipeline shape

```
data / context  →  agent task  →  evaluation  →  result
                       (LLM reasons here)   (check / gate)
```

- Every step is an explicit Airflow task.
- **Orchestration** = wiring these together: dependencies, retries, state
  hand-off, failure handling, observability. The LLM only "thinks" in the agent
  task; everything else is deterministic pipeline.
- Scales by fan-out: `context → [agent task × N in parallel] → synthesis → eval`
  (the survey example: 4 parallel LLM queries → DataFusion → synthesis).

### The demo, concretely

Pipeline over the 5,856 survey responses:

1. **Agent generates SQL** — LLM writes the (4 parallel) queries against the
   survey dataset. LLM reasons here.
2. **Run the queries** — DataFusion executes them, produces result sets.
   - The survey CSV was **local**, so DataFusion runs SQL directly against the
     file — no warehouse, no load step. Convenient for the demo; DataFusion
     treats the CSV as a queryable table in-process.
3. **Synthesis** — combine the query outputs into a report/answer.
4. **Validator step** — a separate check catches errors in the report (bad
   numbers, schema mismatches, unsupported claims) *as an explicit task in the
   DAG*, not an afterthought.

So the LLM only does the "write the query" and "synthesize" thinking; execution
and validation are deterministic pipeline steps with their own retries/logging.

### Evaluation is the critical piece

- Evaluation exists for **quality** and **safety** — is the output correct, and
  is it safe to propagate downstream?
- It is a **separate model call** (its own task), not something bolted onto the
  agent task. The generator shouldn't grade its own work.
- Can (and often should) be a **different model** than the generator — e.g. a
  stronger/independent judge, or a cheaper safety classifier. Different model =
  less correlated failure, and you tune cost per role.
- Making it a distinct task means it gets its own retries, logging, and can gate
  the DAG (fail / branch) if the output doesn't pass — the pipeline enforces the
  quality bar, not the agent.

_(add more during/after the talk)_
