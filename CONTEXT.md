# System Design Prep — Context Handoff

Paste this at the start of a new thread to resume where we left off.

---

## Who I am and what this is for

I'm Omar. I'm interviewing with **Andercore** (Berlin). I passed the coding
interview and have a **system design interview** coming up. I'm weak at system
design, so I'm attacking it from three angles: understanding components (load
balancers, sharding, caching), understanding their business domain, and
practising the interview format itself.

The core method: **actually build the systems locally and break them with load
tests**, rather than reading about them. Repo: https://github.com/OmarAbdo/SDIA

Timeline: 1-2 weeks. Working directory: `D:\1 Software\System Design`

---

## About Andercore (researched, not guessed)

B2B **industrial trade platform** — steel, PV modules, batteries, inverters,
cables, transformers. Connects global suppliers to European buyers with
embedded logistics and financing. Founded 2021, Berlin. ~€62M raised, 7
European markets, triple-digit-million € turnover.

**Stack from their job ads:** Java 21 / Spring Boot 3, Python, **PostgreSQL**,
**Kinesis**, AWS, Terraform, Node.js/TypeScript.

**What their backend roles emphasise:** event-driven pipelines processing
high-throughput logistics data, real-time tracking, third-party LSP/ERP/WMS
integrations, complex supply-chain entity modelling (shipments, inventory
positions), and distributed-systems trade-offs around consistency and latency.

**Signal on difficulty level:** their live-coding round asked *find k nearest
items to a point*; I solved it with a custom Point class and a min-heap.
So: data-structure choice and complexity reasoning, not trivia.

---

## How I want to work

- **Egyptian Arabic** for explanations. English is fine for code and commit
  messages.
- **Short answers.** Long walls of text lose me — I'll ask when I want depth.
- **Build first, theory second.** Minimal preamble, get to running code.
- Don't assume prior knowledge when I ask what something is — start from zero.
- Explain the *why*, not the *what*. I annotate code with my own questions;
  that's how I learn.
- When I push back on a critique, engage with it rather than folding.

---

## What we built (3 systems, all in the repo)

### 01 — Rate-limited gateway (single instance)
Gateway in front of a deliberately hostile mock supplier API (fixed-window
limiter, variable latency, random failures).

Four defence layers, **cheapest first**: L1 cache → in-flight de-dup →
bounded queue → token bucket. Each sheds load before the next.

**Load test found a real bug:** fixed-interval polling (25ms) and fixed retry
backoff (200ms, no jitter) caused a **thundering herd** — queue depth
oscillated 0↔20 for 30s and never reached steady state. 63% failures, p95 4.98s.

**Also learned:** the first load test "passed" at 0.81% failures because it used
only 6 SKUs — the cache absorbed everything and the limiter was never tested.
Widening to 2000 SKUs exposed the real behaviour. *A passing test is evidence
about the test as much as the system.*

### 02 — Distributed gateway (3 instances + Redis + nginx)
Scaled out. What moved and what deliberately didn't:

| Component | Where | Why |
|---|---|---|
| Token bucket | **Redis + Lua** | Must be exact. Lua = atomic; check-then-set in app code is a race across instances |
| Queue | **stays local** | Approximate is fine; coordinating it puts a network hop on the fast-fail path |
| Timer | **deleted** | Tokens computed from elapsed time, not a `setInterval`. No drift, nothing to restart |
| L1 cache | local, **size-capped** | The original `Map` grew unbounded — a real memory leak |
| L2 cache | Redis | Catches what L1 misses |
| De-dup | stays local | Distributed locking costs more than the duplicate calls it prevents |
| LB | nginx L7, **consistent hash on :sku** | Restores L1 locality + de-dup. Trade-off: hotspots |

Jitter added everywhere (full jitter on token polling, exponential backoff +
jitter on 429 retry, TTL jitter on cache writes).

**Results vs 01:** failures 63% → 28%, p95 4.98s → 596ms, queue settled at 0-4
instead of oscillating. Zero 429s from the supplier. Cache absorbed 77% of 44k
requests.

**Key insight:** when the supplier cap changed from 5 → 500 req/s, *the star
component changed* — the cache became the system and the limiter became a
safety net. Design follows the numbers, not the pattern.

### 02b — Redis HA (Sentinel) + the investigation
Added 3 sentinels + 1 primary + 2 replicas to remove the Redis SPoF.

**Two real bugs found by killing the primary:**
1. Sentinels monitored by **hostname**. When the container died, Docker dropped
   the DNS name, sentinels couldn't resolve their master, and all three entered
   **tilt mode** — which *suspends failover*. HA disabled itself exactly when
   needed. Fixed with static IPs.
2. Gateways **hung** instead of failing open. node-redis queues commands while
   disconnected, so promises never settled and every `try/catch` was dead code.
   Fixed with `disableOfflineQueue` + a 500ms cap on every Redis call.

**Then the important part — I was wrong about the third "bug".**
I claimed "1 of 3 instances never follows the promotion" and produced *five*
diagnoses: DNS → offline queueing → missing periodic rediscovery → missed
pub/sub → connection age. Each got a fix. The symptom persisted.

Real cause: my health-check grep matched `"tokensRemaining":"..."` **with
quotes**, but the field serialises as `null` **without quotes** when the bucket
key is absent (60s TTL). Healthy instances scored as "stuck". And every probe
hit `gw-4001` — generalising from n=1.

Re-tested properly: **3 failover rounds × 8 instances = 24 observations, zero
wedged**, all recovered <10s. Sentinels promote in ~5-7s.

> **When a fix doesn't take, question the MEASUREMENT before adding another
> fix.** Four of five "fixes" modified working code.

### 03 — Shipment tracking (current system)
Closest to Andercore's actual domain. `carriers → Redpanda (Kafka API) →
consumer → Postgres → query API`.

**The problem:** carrier GPS feeds are out of order, duplicated, and late. A
14:00 ping arrives after the 15:00 ping because a device buffered while out of
coverage. The naive consumer overwrites on arrival order and the customer
watches their shipment **move backwards** — data no schema constraint catches,
because it isn't corrupt, just old.

**The fix is one line** in the UPSERT:
```sql
WHERE shipment_position.last_recorded_at < EXCLUDED.last_recorded_at
```
Predicate and write are one statement, evaluated under Postgres's row lock, so
two concurrent events for one shipment can't both win. Idempotency comes from
the carrier-supplied `event_id` being the primary key.

**The distinction that matters:** partitioning by `shipmentId` solves
**concurrency**; the predicate solves **lateness**. Two different problems,
commonly confused. That's *why* 20-way concurrency was safe — correctness never
rested on processing order.

**Perf lesson:** first consumer did **6 events/sec** while Postgres executed
each statement in **0.33ms** and sat idle at 8/100 connections — ~167ms per
event of pure round-trip waiting. Bounded concurrency (20) → ~45-86/sec.

**Verified on a feed 29.8% out of order** (avg lag 2968s, max 4008s):
no backwards movement, stored position == newest accepted event, no duplicate
event_ids, and `12295 received == accepted + stale + duplicates` reconciles
exactly. `verify.js` was written **before** the load test, deliberately — after
the rate-limiter session where a broken measurement manufactured a bug.

---

## Concepts covered (ask me, don't re-teach unprompted)

- **Token bucket vs fixed window** — boundary burst; why capacity 4 against a
  cap of 5 (clock drift)
- **Cache stampede / in-flight de-dup** — storing a *Promise* not a value is
  the trick
- **Bounded queue & load shedding** — unbounded queues defer and disguise
  failure; a fast 503 beats a 30s timeout
- **Thundering herd & jitter** — synchronised wake-ups; full jitter,
  exponential backoff
- **Redis Lua** — it's a DB, but scripts run single-threaded for *atomicity*,
  not for programming
- **Sentinel** — watchers not data; sdown (my opinion) vs odown (quorum
  agrees); quorum needs odd numbers; tilt mode
- **Sentinel vs Raft** — Raft agrees on *every write* (data path, sync,
  zero loss, slow); Sentinel agrees only on *who is primary* (control path,
  async, can lose writes, fast). Postgres has no built-in failover — Patroni
  does it, and Patroni uses **Raft underneath** (etcd/Consul)
- **fail-open vs fail-closed** — rule: component **protects** (auth, payments,
  fraud) → fail closed; component **optimises** (cache, rate limit) → fail open
- **Consistency levels** — three in one system: token bucket inside one Redis
  node is **linearizable**; primary→replica is **async** (acked writes can be
  *lost*, not merely delayed); L1 caches are **bounded staleness** (2s TTL)
- **Scaling vs distributing** — not synonyms. Scaling = handle more load;
  distributing = split across machines, and that's what breaks things
- **L4 vs L7 LB, consistent hashing, hotspots**
- **DNS round-robin vs Anycast** — not sub-IPs: either many IPs on one name, or
  one IP announced from many places
- **Distributed locks ≠ 2PC** — mutual exclusion vs cross-system atomicity;
  Redlock and the Kleppmann/antirez debate
- **Event time vs ingestion time** — the whole basis of system 03
- **Idempotency via natural key** — what makes at-least-once delivery survivable

Also relevant: I wrote **Crystal**, my own Raft implementation in Go
(github.com/OmarAbdo/crystal) — real Raft with ReadIndex, pre-vote, CheckQuorum,
exactly-once sessions. We concluded Raft was the *wrong* tool for a rate limiter
(coordination costing more than the resource it protects) and the right tool for
service discovery / leader election / config.

---

## Tooling

- **Excalidraw MCP** at `http://127.0.0.1:3000` — canvas has 3 diagrams stacked
  vertically (BEFORE single-instance / AFTER distributed / HA+Sentinel),
  separated by horizontal lines. Restart with:
  `PORT=3000 npx -y mcp-excalidraw-server start`
  **Note:** the canvas is in-memory — a machine restart wipes it and the
  diagrams must be redrawn.
- **k6 via Docker** for load tests (no local install):
  `MSYS_NO_PATHCONV=1 docker run --rm -i --add-host=host.docker.internal:host-gateway -v "$(pwd -W)/loadtest:/scripts" grafana/k6 run /scripts/break-it.js`
- **Git Bash quirk:** `pkill -f` can't see Windows process command lines. Kill
  by port via PowerShell `Get-NetTCPConnection` instead.
- Currently running: `st-postgres` (:5433), `st-redpanda` (:9092).

---

## Where we stopped

System 03 is built, verified, committed (`e0e01bc`), and pushed.

**Proposed next step:** scale system 03 to **multiple consumers** — this
surfaces consumer-group **rebalancing**, partition reassignment, and offset
management, which are common event-driven interview questions and which our
stack can demonstrate for real.

**Other open options:**
- Draw a 4th Excalidraw diagram for the shipment tracking system
- k6 load test on the read path (the query API under concurrent load)
- A full mock interview: you give requirements, I design, you pressure-test me
