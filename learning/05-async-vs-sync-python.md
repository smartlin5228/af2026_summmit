# 05 — async vs sync in Python (the practical version)

**Why this matters:** the triggerer is one event loop running hundreds of trigger
coroutines. If you don't have the async model straight, you'll write a trigger
that blocks the whole thing. See `04-deferrable-operators-triggers-triggerer.md`.

---

## Sync (blocking) — the default

```python
data = requests.get(url).json()   # thread STOPS here until the response arrives
process(data)
```

`x = f()` — control does not come back until `f` finishes. If `f` is waiting on
the network for 2 seconds, your thread does **nothing** for 2 seconds.

For one task that's fine. The problem is **many** waits: 500 sensors each
sleeping 30s = either 500 threads (expensive) or they run one-after-another
(slow).

---

## Async — concurrency in ONE thread

**Concurrency ≠ parallelism.**
- **Parallelism** = things happen at the *same instant* (multiple CPU cores).
  `multiprocessing`, threads (for I/O).
- **Concurrency** = one worker *interleaves* many tasks, switching whenever one
  is waiting. That's async. One thread, no cores added.

Async wins when the work is **I/O-bound** — mostly *waiting* (network, disk, DB,
timers). While coroutine A waits for its HTTP response, the loop runs B, C, D.

### The pieces

```python
import asyncio

async def fetch(url):              # `async def` → a "coroutine function"
    async with httpx.AsyncClient() as c:
        r = await c.get(url)       # `await` → "suspend me until this is ready,
        return r.json()            #            let the loop run someone else"

async def main():
    # run three fetches CONCURRENTLY — total time ≈ the slowest one, not the sum
    a, b, c = await asyncio.gather(fetch(u1), fetch(u2), fetch(u3))

asyncio.run(main())                # starts the event loop, runs main to completion
```

- `async def f()` — defines a **coroutine function**. Calling `f()` returns a
  **coroutine object** that *hasn't run yet* — it runs when awaited or scheduled.
- `await x` — only valid inside `async def`. Means "pause here, hand control to
  the event loop, resume when `x` is done." You can `await` coroutines, Tasks,
  Futures.
- **The event loop** — a scheduler. It runs a coroutine until it hits `await` on
  something not-ready, parks it, runs the next ready one. Repeat.
- `asyncio.run(coro)` — the entry point from sync code: makes a loop, runs the
  coroutine, closes the loop.
- `asyncio.gather(*coros)` — run many concurrently, wait for all.
- `asyncio.create_task(coro)` — schedule a coroutine to run in the background;
  `await` the task later.

---

## The catch: cooperative, not preemptive

The loop **cannot interrupt** a running coroutine. A coroutine keeps the loop to
itself until it `await`s. So two ways to wreck it:

**1. A coroutine that never awaits (CPU work):**
```python
async def bad():
    total = sum(i*i for i in range(50_000_000))   # ❌ no await — loop frozen
    return total
```

**2. `await`-ing something that's secretly blocking / calling a sync blocker:**
```python
async def bad():
    time.sleep(5)                  # ❌ sync sleep — freezes the thread, not just this coroutine
    r = requests.get(url)          # ❌ sync HTTP — same
```

Both freeze **every other coroutine on that loop** for the duration.

### Fixes

| Situation | Fix |
|---|---|
| Sync I/O library, no async version | `result = await asyncio.to_thread(blocking_fn, arg)` — runs it on a thread pool, loop keeps going |
| Async version exists | use it: `httpx.AsyncClient`, `aiofiles`, `asyncpg`, `aioboto3`, `asyncio.sleep` |
| CPU-bound work | `await asyncio.to_thread(...)` helps a bit (GIL released for some numpy/hash ops); real fix is a process pool or don't do it here |

---

## "Colored functions" — async and sync don't mix freely

- **sync calling async:** need `asyncio.run(coro())` (or an already-running loop).
  You can't just call a coroutine and get its result.
- **async calling sync:** works syntactically, but a *blocking* sync call
  freezes the loop. Wrap blockers in `asyncio.to_thread`.
- This "colour" spreads: to `await` something, the caller must be `async`, and
  its caller must `await` it, ... up to `asyncio.run`.

---

## Analogy

One chef (one thread / one event loop) making several dishes:

- **sync:** start the pasta water, then **stand and stare at the pot** until it
  boils, then chop veg, then ... one dish finishes before the next starts.
- **async:** start the pasta water (`await boil`), and *while it heats* chop veg
  for dish 2, start the sauce for dish 3. Switch whenever something needs
  waiting. Still **one chef** — if a step needs actual chopping (CPU), everything
  else waits for that chopping.
- **`time.sleep()` in async:** the chef freezes, arms crossed, for 5 minutes.
- **blocking `requests.get()` in async:** the chef walks to the store and back;
  the kitchen sits idle.
- **threads / processes:** hire more chefs.

---

## When to use what

| Work is... | Use |
|---|---|
| Many concurrent **I/O waits** (HTTP, DB, sockets, timers) | **async** (or a thread pool) |
| **CPU-bound** (parsing, crunching, crypto, ML) | **processes** (`multiprocessing`, `ProcessPoolExecutor`) |
| A few blocking calls, simple script | just **sync** — async adds complexity for no gain |
| Mostly I/O but stuck with sync libraries | threads (`ThreadPoolExecutor`) — simpler than rewriting to async |

---

## Tie to Airflow

- **Triggerer** = one event loop watching hundreds of "is it done?" coroutines.
  Perfect async use case — it's almost pure waiting.
- A trigger doing `time.sleep`, `requests.get`, `psycopg2`, or `json.loads` on a
  big blob = the chef freezing. Every deferred task stalls.
- **Native async operators** (AS 3.2 talk) do the I/O with `asyncio` *on the
  worker, in-process* — good when you have one big batch of concurrent I/O and
  don't want the worker→triggerer→worker round trip per item.
- **Workers** are sync — a normal `@task` / operator runs blocking code and
  that's fine; it has its own process/slot.
