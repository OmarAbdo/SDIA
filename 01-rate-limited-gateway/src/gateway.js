import express from "express";

/**
 * RATE-LIMITED CACHING GATEWAY
 * ============================
 *
 * Sits between our internal services and a third-party supplier pricing API
 * (supplier.js). Everything here exists to solve one problem:
 *
 *     Our demand is unbounded and bursty.
 *     The supplier's capacity is fixed at 5 req/s.
 *
 * You cannot make the supplier faster. So the gateway's job is to absorb the
 * mismatch — serve what it can from cache, meter what must go upstream, and
 * reject the rest quickly rather than letting it pile up.
 *
 * FOUR DEFENSE LAYERS, CHEAPEST FIRST
 * Each layer removes load before it reaches the next, which is why the order
 * matters as much as the mechanisms themselves:
 *
 *   1. CACHE          — already have the answer, return it (~1ms, no upstream call)
 *   2. IN-FLIGHT DEDUP— someone is already fetching this, wait on their result
 *   3. BOUNDED QUEUE  — capacity to wait for a token, else shed load (503)
 *   4. TOKEN BUCKET   — meter the calls that do go upstream, stay under 5 req/s
 *
 * The general principle — do the cheapest possible work to avoid the most
 * expensive work, and fail fast when you can't — is the reusable idea here.
 *
 * KNOWN BUG (found by load testing, documented at fetchWithRateLimit)
 * Fixed-interval polling and retry cause a thundering-herd effect under
 * sustained overload. Left in deliberately so the fix can be measured against
 * a real before/after. See the load test results in the sustained phase.
 */

const PORT = 4000;
const SUPPLIER_URL = "http://localhost:4001";

// ---------------------------------------------------------------------------
// LAYER 4: TOKEN BUCKET — outbound rate limiting
// ---------------------------------------------------------------------------

/**
 * Capacity 4 against the supplier's limit of 5, deliberately leaving headroom.
 *
 * Why not 5? Two clocks that never agree. Our refill timer and the supplier's
 * fixed window drift relative to each other, so running at exactly their limit
 * guarantees periodic overshoot into 429s. The ~20% margin buys tolerance for
 * that drift, for retries, and for the supplier's own boundary-burst flaw.
 *
 * Trading a little throughput for a lot of stability is usually right at an
 * integration boundary — and saying so explicitly is the kind of reasoning an
 * interviewer is listening for.
 */
const BUCKET_CAPACITY = 4;
const REFILL_PER_SEC = 4;

/**
 * TOKEN BUCKET vs FIXED WINDOW (the one in supplier.js)
 *
 * Tokens accrue continuously and are spent per request. Capacity caps how many
 * can be saved up, so a burst can spend the accumulated balance but the
 * long-run average still converges to the refill rate.
 *
 * Two properties the fixed window lacks:
 *   - No boundary burst. There is no reset instant to exploit.
 *   - Controlled burst tolerance. Idle time banks tokens, so a short spike
 *     after quiet is absorbed rather than rejected.
 *
 * Refilling in small increments every 100ms rather than a big chunk every
 * second matters: a once-per-second refill would release waiters in
 * synchronized batches, which is precisely the herding behavior documented
 * below. Smooth refill is a partial defense against it.
 */
let tokens = BUCKET_CAPACITY;
setInterval(() => {
  tokens = Math.min(BUCKET_CAPACITY, tokens + REFILL_PER_SEC / 10);
}, 100);

/**
 * Non-blocking token acquisition: take a token if one is available, otherwise
 * report failure and let the caller decide what to do (wait, shed, degrade).
 *
 * Safe without a lock only because Node runs this on a single thread with no
 * await inside — the check and the decrement cannot be interleaved. In Java or
 * Go this would need an atomic or a mutex, and in a multi-instance deployment
 * it needs to move into Redis (see the scaling note at the bottom of the file).
 */
function tryConsumeToken() {
  if (tokens >= 1) {
    tokens -= 1;
    return true;
  }
  return false;
}

// ---------------------------------------------------------------------------
// LAYER 1: CACHE-ASIDE
// ---------------------------------------------------------------------------

/**
 * Cache-aside (lazy loading): check cache → on miss, fetch → store → return.
 * The application owns the caching logic, as opposed to read-through where the
 * cache layer itself fetches on miss.
 *
 * WHY 2 SECONDS
 * TTL is a correctness/efficiency trade, and for pricing data it is a business
 * decision rather than a technical one. Too long and you quote a stale price —
 * in commodities that is real money lost on a real order. Too short and the
 * cache stops absorbing load. 2s is aggressive for a demo (it keeps cache
 * effects visible during a 30s test); a real quoting system would set this from
 * how fast the underlying commodity actually moves.
 *
 * The load-testing lesson attached to this constant: the first version of the
 * test used only 6 distinct SKUs, so nearly every request was a cache hit and
 * the rate limiter was never exercised at all. The system looked healthy
 * because the test was too easy — the cache was masking the code path under
 * test. Widening the key space to 2000 SKUs is what exposed the real behavior.
 */
const CACHE_TTL_MS = 2000;
const cache = new Map(); // sku -> { data, expiresAt }

// ---------------------------------------------------------------------------
// LAYER 2: IN-FLIGHT DE-DUPLICATION
// ---------------------------------------------------------------------------

/**
 * "In-flight" = received by the gateway, upstream call issued, no response yet.
 * "De-dup" = collapsing N concurrent requests for the same key into 1 upstream
 * call, with all N callers awaiting that single shared promise.
 *
 * THE PROBLEM IT SOLVES: CACHE STAMPEDE
 * A popular SKU's cache entry expires. In the milliseconds before the first
 * refetch completes, 50 more requests arrive for that same SKU. All 50 see a
 * cache miss. Without de-dup, all 50 call the supplier — for one identical
 * piece of data, against a 5 req/s budget. The gateway becomes the attacker.
 *
 * Storing a Promise rather than a value is the whole trick: the map entry is
 * created the instant the fetch starts, not when it finishes, so later arrivals
 * find something to wait on during the vulnerable window.
 *
 * Map gives key uniqueness — one entry per SKU by construction, which is
 * exactly the invariant de-dup depends on.
 *
 * Visible in the load test: the hot_key phase pushed 50 req/s at a single SKU
 * and the gateway served ~157 requests in a 2s window with the queue at zero
 * and tokens at full. Nearly all of that traffic never reached the supplier.
 */
const inFlight = new Map(); // sku -> Promise

// ---------------------------------------------------------------------------
// LAYER 3: BOUNDED QUEUE — load shedding
// ---------------------------------------------------------------------------

/**
 * Cap on concurrent requests waiting for a token. Past this, new requests are
 * rejected immediately with 503 rather than admitted to wait.
 *
 * WHY BOUNDED — THE CENTRAL IDEA
 * An unbounded queue does not prevent failure, it defers and disguises it.
 * Requests accumulate, latency grows without limit, memory fills with pending
 * connections, and clients time out anyway — except now the work is still being
 * done for responses nobody will read. The system dies slowly instead of
 * shedding cleanly.
 *
 * Bounded queue + fast rejection is the load-shedding pattern: under overload,
 * serve a subset well and reject the rest immediately. A fast 503 is genuinely
 * a better outcome than a 30s timeout — the client can retry, fail over, or
 * degrade, and it learns this in 1ms instead of 30s.
 *
 * Sizing: ~5 seconds of upstream capacity (4 req/s × 5s ≈ 20). Anything beyond
 * that is waiting longer than the timeout, so admitting it would be a lie.
 */
const MAX_QUEUE = 20;
let queueDepth = 0;

const app = express();

// ---------------------------------------------------------------------------
// OBSERVABILITY
// ---------------------------------------------------------------------------

/**
 * Rolling status-code counts, flushed to the log every 2s.
 *
 * 200 = served (from cache, dedup, or a real upstream call)
 * 502 = upstream failed (supplier error, or retries exhausted)
 * 503 = we shed the load (queue full) — our own protection working
 *
 * The 502/503 distinction is the one that matters operationally: 503 means the
 * gateway is deliberately protecting itself and is arguably healthy, while 502
 * means the supplier is failing. Same user-visible failure, completely
 * different response from whoever is on call.
 *
 * This middleware is registered BEFORE the routes on purpose. Express runs
 * middleware in registration order; registered after, it would never run for
 * requests the route already responded to, and the counters would stay empty.
 * (This was a real bug here — the first load test produced no logs at all, so
 * the run had to be thrown away and repeated. Instrument before you test.)
 */
let statusCounts = { 200: 0, 502: 0, 503: 0 };
app.use((req, res, next) => {
  res.on("finish", () => {
    statusCounts[res.statusCode] = (statusCounts[res.statusCode] || 0) + 1;
  });
  next();
});

// ---------------------------------------------------------------------------
// MAIN ROUTE — the four layers in cheapest-first order
// ---------------------------------------------------------------------------

app.get("/price/:sku", async (req, res) => {
  const sku = req.params.sku;
  const now = Date.now();

  // LAYER 1 — cache hit. ~1ms, no upstream call, no token spent.
  const cached = cache.get(sku);
  if (cached && cached.expiresAt > now) {
    return res.json({ ...cached.data, cached: true });
  }

  // LAYER 2 — someone is already fetching this exact SKU. Wait on their
  // result instead of starting a second identical upstream call.
  //
  // Note this is checked before the queue: a deduped request costs no token
  // and no queue slot, so rejecting it for queue pressure would shed load that
  // is effectively free to serve.
  if (inFlight.has(sku)) {
    try {
      const data = await inFlight.get(sku);
      return res.json({ ...data, cached: false, deduped: true });
    } catch {
      // The shared fetch failed, so every waiter fails together. Correct, but
      // worth naming: de-dup couples these requests' fates. One bad upstream
      // call now fails N clients rather than 1.
      return res.status(502).json({ error: "upstream failure" });
    }
  }

  // LAYER 3 — no capacity left to wait. Shed immediately.
  if (queueDepth >= MAX_QUEUE) {
    return res.status(503).json({ error: "gateway overloaded, try again later" });
  }

  // LAYER 4 — commit to a real upstream call.
  queueDepth++;
  const fetchPromise = fetchWithRateLimit(sku)
    .then((data) => {
      cache.set(sku, { data, expiresAt: Date.now() + CACHE_TTL_MS });
      return data;
    })
    .finally(() => {
      // finally, not then: these must run on failure too, or a single failed
      // fetch permanently leaks a queue slot and poisons the SKU's inFlight
      // entry so every future request for it waits on a dead promise. Slow
      // resource leaks under partial failure are a classic production bug and
      // a good thing to be able to point at in an interview.
      queueDepth--;
      inFlight.delete(sku);
    });

  // Published before awaiting, so concurrent arrivals can find it (Layer 2).
  inFlight.set(sku, fetchPromise);

  try {
    const data = await fetchPromise;
    res.json({ ...data, cached: false });
  } catch (err) {
    res.status(502).json({ error: "upstream failure", detail: String(err) });
  }
});

// ---------------------------------------------------------------------------
// UPSTREAM FETCH — token acquisition + retry
// ---------------------------------------------------------------------------

/**
 * Wait for a token, call the supplier, retry on 429.
 *
 * ⚠️ KNOWN BUG: THUNDERING HERD (deliberately left unfixed)
 *
 * Both delays below are fixed intervals with no randomization:
 *   - the token-wait poll (25ms)
 *   - the 429 retry backoff (200ms)
 *
 * Under sustained load, requests arrive together, block together, and then
 * wake together on the same tick — spending the few available tokens in
 * synchronized batches instead of spreading out. The bucket empties instantly,
 * the next wave collides the same way, and the cycle repeats.
 *
 * MEASURED EFFECT (k6, sustained phase: 20 req/s vs ~4 req/s capacity, 30s)
 *   - queue depth oscillated 0 ↔ 20 for the entire window, never settling
 *   - 63% overall failure rate; p95 latency 4.98s, max 6.21s
 *   - 502s scattered throughout, not just at the burst edges
 *
 * The important nuance: 5x overload SHOULD shed load — shedding is not the
 * bug. The bug is that the system never reaches a stable operating point. It
 * thrashes between idle and saturated rather than rejecting a steady,
 * proportional share. Graceful degradation is about the SHAPE of failure, not
 * just its rate.
 *
 * THE FIX: full jitter — randomize both delays (`Math.random() * interval`)
 * so waiters desynchronize and arrivals spread across ticks. This is the same
 * mechanism behind retry storms in large distributed systems, and why every
 * mature retry library ships jitter by default.
 *
 * Also missing here, worth knowing by name:
 *   - EXPONENTIAL BACKOFF: retry delay should grow (200ms → 400ms → 800ms).
 *     Fixed backoff retries a struggling upstream at constant pressure.
 *   - CIRCUIT BREAKER: after N consecutive failures, stop calling entirely for
 *     a cooldown. Retrying a dead supplier wastes tokens that working SKUs
 *     could have used, and delays recovery.
 */
async function fetchWithRateLimit(sku, retries = 3) {
  const waitStart = Date.now();

  // Spin-wait for a token. The 5s ceiling bounds how long a request can be
  // held before we give up — without it, a request could wait indefinitely
  // while the client has long since timed out.
  while (!tryConsumeToken()) {
    if (Date.now() - waitStart > 5000) {
      throw new Error("timed out waiting for rate limit token");
    }
    await sleep(25); // ← fixed interval: herding, see above
  }

  const res = await fetch(`${SUPPLIER_URL}/price/${sku}`);

  // 429 means our metering was too loose — clock drift, or the supplier's
  // boundary-burst flaw. Retrying is reasonable; retrying at a fixed delay,
  // in lockstep with every other retrying request, is not.
  if (res.status === 429 && retries > 0) {
    await sleep(200); // ← fixed backoff, no jitter: herding, see above
    return fetchWithRateLimit(sku, retries - 1);
  }

  // Note the asymmetry: 429 is retried, 500 is not. A 429 is a timing problem
  // that a short wait may resolve. A 500 signals the supplier is actually
  // broken, and immediate retries would spend scarce tokens on a request
  // likely to fail again. Knowing WHICH errors are worth retrying is the
  // substance of retry design.
  if (!res.ok) {
    throw new Error(`supplier returned ${res.status}`);
  }

  return res.json();
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// ---------------------------------------------------------------------------
// HEALTH
// ---------------------------------------------------------------------------

/**
 * Live internals, useful to poll during a load test.
 *
 *   tokens ≈ 0 and queueDepth = MAX_QUEUE → saturated, shedding load
 *   tokens ≈ full and queueDepth = 0      → idle, or cache absorbing everything
 *
 * Distinguishing those two idle cases is why cacheSize is reported. During the
 * first (broken) load test, cacheSize sitting at exactly 6 was the clue that
 * the test only used 6 SKUs and the limiter was never under real pressure.
 */
app.get("/health", (_req, res) => {
  res.json({ tokens: tokens.toFixed(2), queueDepth, cacheSize: cache.size });
});

setInterval(() => {
  const total = Object.values(statusCounts).reduce((a, b) => a + b, 0);
  if (total === 0) return;
  console.log(
    `[gateway] last 2s: ${JSON.stringify(statusCounts)} | tokens=${tokens.toFixed(1)} queue=${queueDepth} cache=${cache.size}`
  );
  statusCounts = { 200: 0, 502: 0, 503: 0 };
}, 2000);

app.listen(PORT, () => {
  console.log(`[gateway] listening on :${PORT}, proxying ${SUPPLIER_URL}`);
});

/**
 * ---------------------------------------------------------------------------
 * SCALING NOTE — what breaks with more than one gateway instance
 * ---------------------------------------------------------------------------
 *
 * Every mechanism above is in-process, which is the right call for one box and
 * wrong for a fleet. This is the natural interview follow-up ("now run three of
 * these"), so it's worth having the answer ready:
 *
 *   TOKEN BUCKET → breaks immediately and badly. Three instances each allowing
 *     4 req/s send 12 req/s at a supplier that permits 5. The limiter must
 *     become shared state — Redis with an atomic Lua script, since
 *     check-then-decrement across a network is a race otherwise.
 *
 *   CACHE → still correct, just less effective. Each instance keeps its own
 *     copy, so hit rate drops roughly by the instance count and upstream load
 *     rises accordingly. Fix: shared Redis cache, at the cost of a network hop
 *     per lookup and a new failure mode.
 *
 *   IN-FLIGHT DEDUP → degrades to per-instance. Three instances can each issue
 *     one call for the same SKU: better than 50, worse than 1. Distributed
 *     de-dup needs a lock, which usually costs more than the duplicate calls
 *     it saves — often correctly left alone.
 *
 *   QUEUE → per-instance, so effective total capacity is MAX_QUEUE × instances.
 *     Usually fine, as long as the sizing math accounts for it.
 *
 * The general shape: local state is fast and simple but wrong the moment you
 * scale horizontally; shared state is correct but adds latency, a dependency,
 * and a new thing that can fail. Naming that trade-off explicitly — rather than
 * jumping straight to "put it in Redis" — is the answer worth giving.
 */
