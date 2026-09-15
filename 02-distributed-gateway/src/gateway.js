import express from "express";
import { createSentinel } from "redis";
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { dirname, join } from "path";

/**
 * DISTRIBUTED GATEWAY
 * ===================
 * Same four layers as 01, but built to run as N instances behind a load
 * balancer. What changed and why:
 *
 *   TOKEN BUCKET  -> moved to Redis. MUST be exact: 3 instances x local
 *                    bucket = 3x the supplier's cap. Lua makes it atomic.
 *   TIMER         -> deleted. Tokens are computed from elapsed time inside
 *                    the Lua script. Nothing to drift or restart.
 *   L1 CACHE      -> stays local, but now SIZE-CAPPED. The Map in 01 grew
 *                    forever — a real memory leak.
 *   L2 CACHE      -> new, shared in Redis. Catches what L1 misses.
 *   QUEUE         -> stays local and per-instance. Approximate is fine here,
 *                    and coordinating it would put a network hop on the
 *                    fast-fail path, which defeats its purpose.
 *   DE-DUP        -> stays local. Distributed locking costs more than the
 *                    duplicate calls it would prevent. Mitigated instead by
 *                    hashing on :sku at the load balancer.
 *   JITTER        -> added everywhere. With N instances the thundering herd
 *                    gets worse, not better.
 */

const __dirname = dirname(fileURLToPath(import.meta.url));

const PORT = Number(process.env.PORT || 4000);
const INSTANCE_ID = process.env.INSTANCE_ID || `gw-${PORT}`;
const SUPPLIER_URL = process.env.SUPPLIER_URL || "http://localhost:4001";
/**
 * Sentinel endpoints, NOT a fixed primary address.
 *
 * Connecting to redis://localhost:6379 defeats the entire HA setup: when
 * Sentinel promoted a replica, the gateways kept dialling the dead primary's
 * address and reported the limiter as permanently unavailable. Redis failed
 * over correctly and the application never noticed.
 *
 * Pointing the client at the sentinels makes it ask "who is primary now?" on
 * every reconnect, so promotion is followed automatically. That indirection is
 * the whole reason Sentinel exists — a fixed address cannot survive failover.
 */
const SENTINELS = (process.env.REDIS_SENTINELS || "localhost:26379,localhost:26380,localhost:26381")
  .split(",")
  .map((hp) => {
    const [host, port] = hp.trim().split(":");
    return { host, port: Number(port) };
  });
const SENTINEL_NAME = process.env.REDIS_SENTINEL_NAME || "mymaster";

// Supplier now allows 500 req/s (realistic B2B contract). We claim 450 to
// leave headroom for clock drift between our refill math and their window.
const BUCKET_CAPACITY = Number(process.env.BUCKET_CAPACITY || 450);
const REFILL_PER_SEC = Number(process.env.REFILL_PER_SEC || 450);

// {supplier_a} hash tag forces both keys into the same Redis Cluster slot.
// Cross-slot Lua is rejected outright, so this is mandatory, not cosmetic.
const TOKENS_KEY = "ratelimit:{supplier_a}:tokens";
const REFILL_KEY = "ratelimit:{supplier_a}:last_refill";

const L1_TTL_MS = 2000;
const L2_TTL_MS = 10000;
const L1_MAX_ENTRIES = 5000; // the cap that 01 was missing
const MAX_QUEUE = 20;
const TOKEN_WAIT_MS = 5000;

const luaScript = readFileSync(join(__dirname, "ratelimit.lua"), "utf8");

/**
 * Distinguishes OUR load shedding from THEIR failure.
 *
 * Both used to throw a plain Error, so a request we rejected ourselves after
 * waiting 5s for a token was reported as 502 "upstream failure" — blaming the
 * supplier for our own backpressure. In the last load test that mislabelled
 * ~10k requests.
 *
 * Operationally this is the difference between "we are at capacity, scale up
 * or raise the limit" and "the supplier is down, page their integration
 * owner". Same user-visible failure, completely different response.
 */
class CapacityError extends Error {}

/**
 * disableOfflineQueue is the critical setting.
 *
 * By default node-redis QUEUES commands while disconnected and replays them on
 * reconnect, so a call made during an outage never settles — it just waits.
 * Every `try/catch` around a Redis call is then dead code: the promise does not
 * reject, it hangs, and the request hangs with it. That is how a Redis outage
 * turned into three fully wedged gateways that could not even serve /health.
 *
 * With the offline queue disabled, commands fail FAST while disconnected, the
 * catch blocks actually run, and the fail-open path becomes real instead of
 * theoretical.
 *
 * The lesson generalizes past Redis: a fallback you have never exercised is a
 * hypothesis, not a safety net. This one was written, reviewed, and wrong.
 */
/**
 * Sentinels report the Docker-internal address of whichever node is primary
 * (172.30.0.10/.11/.12), and those are unroutable from the host. Without a
 * translation the client discovers the right primary and then fails to reach
 * it — a failover that "works" and still leaves the app disconnected.
 *
 * In production this map does not exist: services share the network with Redis
 * and use the reported addresses directly. It is a local-dev artifact of
 * running the app outside Docker and Redis inside it.
 */
const NODE_ADDRESS_MAP = {
  "172.30.0.10:6379": { host: "localhost", port: 6379 },
  "172.30.0.11:6379": { host: "localhost", port: 6380 },
  "172.30.0.12:6379": { host: "localhost", port: 6381 },
};

const redis = createSentinel({
  name: SENTINEL_NAME,
  sentinelRootNodes: SENTINELS,
  nodeAddressMap: NODE_ADDRESS_MAP,

  /**
   * scanInterval is deliberately left at its default of 0 (no periodic scan).
   *
   * Setting it to 2000 looked like the fix for a client that did not follow a
   * failover, but an isolated test disproved that: a long-lived client sustained
   * 20 consecutive writes at 1-3ms across the whole window, and the scan fired
   * a MASTER_CHANGE event every 2s even though the master never changed —
   * rebuilding connections continuously for no benefit. It was churn, not
   * resilience, so it was reverted.
   *
   * The client follows promotions via sentinel push notifications; it does not
   * need polling to do so.
   */
  nodeClientOptions: {
    disableOfflineQueue: true,
    socket: {
      connectTimeout: 2000,
      reconnectStrategy: (retries) => Math.min(retries * 200, 3000),
    },
  },
});
redis.on("error", (e) => console.error(`[${INSTANCE_ID}] redis error`, e.message));
await redis.connect();

// Belt and braces: even with the offline queue disabled, a command issued to a
// socket that is mid-teardown can stall. This caps ANY Redis call so a slow
// dependency can never become an unbounded wait on our own request path.
const REDIS_OP_TIMEOUT_MS = Number(process.env.REDIS_OP_TIMEOUT_MS || 500);
function withTimeout(promise, ms = REDIS_OP_TIMEOUT_MS) {
  return Promise.race([
    promise,
    new Promise((_, rej) => setTimeout(() => rej(new Error("redis timeout")), ms)),
  ]);
}

// ---------------------------------------------------------------------------
// L1 CACHE — local, size-capped
// ---------------------------------------------------------------------------

/**
 * Bounded via JS Map insertion order: the first key returned by keys() is the
 * oldest inserted, so deleting it is FIFO eviction.
 *
 * FIFO vs LRU/LFU is a real trade. LRU would keep hot keys better, but needs
 * access-order bookkeeping. With a 2s TTL almost nothing survives long enough
 * for the difference to matter, so FIFO is the right amount of machinery.
 *
 * The important part is that a cap exists at all. Without one this Map grows
 * until the process dies — which is exactly the bug in 01.
 */
const l1 = new Map();

function l1Set(sku, data) {
  if (l1.size >= L1_MAX_ENTRIES) {
    l1.delete(l1.keys().next().value);
  }
  // TTL jitter (±20%) so keys created together do not expire together.
  // Without this, a burst of misses creates a synchronized wave of
  // expirations — a self-inflicted thundering herd.
  const jittered = L1_TTL_MS * (0.8 + Math.random() * 0.4);
  l1.set(sku, { data, expiresAt: Date.now() + jittered });
}

const inFlight = new Map();
let queueDepth = 0;

const app = express();

// `shed` counts 503s caused by token-wait timeout specifically, separate from
// 503s caused by a full queue. Both are our own backpressure, but they say
// different things: queue-full means we are saturated locally, shed means we
// waited on the shared limiter and gave up. Different fixes.
let stats = { 200: 0, 502: 0, 503: 0, l1: 0, l2: 0, upstream: 0, dedup: 0, shed: 0 };
app.use((req, res, next) => {
  res.on("finish", () => { stats[res.statusCode] = (stats[res.statusCode] || 0) + 1; });
  next();
});

app.get("/price/:sku", async (req, res) => {
  const sku = req.params.sku;

  // LAYER 1a — L1, local memory. ~1ms, no network at all.
  const hit = l1.get(sku);
  if (hit && hit.expiresAt > Date.now()) {
    stats.l1++;
    return res.json({ ...hit.data, cached: "l1", instance: INSTANCE_ID });
  }

  // LAYER 1b — L2, shared Redis. ~1ms network, but shared across instances,
  // so one instance's fetch warms the cache for all of them.
  try {
    const l2 = await withTimeout(redis.get(`price:${sku}`));
    if (l2) {
      const data = JSON.parse(l2);
      l1Set(sku, data);
      stats.l2++;
      return res.json({ ...data, cached: "l2", instance: INSTANCE_ID });
    }
  } catch { /* Redis down: degrade to upstream, do not fail the request */ }

  // LAYER 2 — de-dup, local only.
  if (inFlight.has(sku)) {
    try {
      const data = await inFlight.get(sku);
      stats.dedup++;
      return res.json({ ...data, cached: "dedup", instance: INSTANCE_ID });
    } catch (err) {
      // De-dup couples fates: waiters inherit the leader's failure, so they
      // must inherit its CLASSIFICATION too. Without this, one capacity
      // rejection answers the leader 503 and every waiter 502.
      if (err instanceof CapacityError) {
        stats.shed++;
        return res.status(503).json({ error: "gateway at capacity", instance: INSTANCE_ID });
      }
      return res.status(502).json({ error: "upstream failure" });
    }
  }

  // LAYER 3 — bounded queue, local. Shed before spending a token.
  if (queueDepth >= MAX_QUEUE) {
    return res.status(503).json({ error: "gateway overloaded", instance: INSTANCE_ID });
  }

  queueDepth++;
  const p = fetchWithRateLimit(sku)
    .then(async (data) => {
      l1Set(sku, data);
      try {
        await withTimeout(redis.set(`price:${sku}`, JSON.stringify(data), { PX: L2_TTL_MS }));
      } catch { /* L2 write is best-effort */ }
      return data;
    })
    .finally(() => { queueDepth--; inFlight.delete(sku); });

  inFlight.set(sku, p);

  try {
    const data = await p;
    stats.upstream++;
    res.json({ ...data, cached: "miss", instance: INSTANCE_ID });
  } catch (err) {
    if (err instanceof CapacityError) {
      stats.shed++;
      return res.status(503).json({ error: "gateway at capacity", instance: INSTANCE_ID });
    }
    res.status(502).json({ error: "upstream failure", detail: String(err) });
  }
});

// ---------------------------------------------------------------------------
// LAYER 4 — distributed token bucket
// ---------------------------------------------------------------------------

async function tryConsumeToken() {
  try {
    const granted = await withTimeout(redis.eval(luaScript, {
      keys: [TOKENS_KEY, REFILL_KEY],
      arguments: [String(BUCKET_CAPACITY), String(REFILL_PER_SEC), String(Date.now())],
    }));
    return granted === 1;
  } catch (e) {
    // FAIL OPEN: if Redis is unreachable, allow the call rather than taking
    // the whole system down. The supplier's own limiter becomes the backstop.
    // Fail-closed would turn a Redis blip into a full outage — worse.
    console.error(`[${INSTANCE_ID}] limiter unavailable, failing open:`, e.message);
    return true;
  }
}

async function fetchWithRateLimit(sku, retries = 3) {
  const start = Date.now();

  while (!(await tryConsumeToken())) {
    if (Date.now() - start > TOKEN_WAIT_MS) {
      // OUR limit, not theirs. Typed so the handler can answer 503.
      throw new CapacityError("timed out waiting for token");
    }
    // FULL JITTER on the poll. Fixed 25ms made every waiter wake on the same
    // tick and collide; random spreads them across the window.
    await sleep(Math.random() * 25);
  }

  const res = await fetch(`${SUPPLIER_URL}/price/${sku}`);

  if (res.status === 429 && retries > 0) {
    // EXPONENTIAL BACKOFF + JITTER: 200 -> 400 -> 800, each randomized.
    // Fixed backoff retries a struggling upstream at constant pressure and
    // keeps retries synchronized across instances.
    const attempt = 3 - retries;
    const base = 200 * Math.pow(2, attempt);
    await sleep(base * (0.5 + Math.random() * 0.5));
    return fetchWithRateLimit(sku, retries - 1);
  }

  if (!res.ok) throw new Error(`supplier returned ${res.status}`);
  return res.json();
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

app.get("/health", async (_req, res) => {
  let tokens = null;
  // /health must answer even when Redis is down — an unbounded call here is
  // what made all three gateways unreachable during the failover test.
  try { tokens = await withTimeout(redis.get(TOKENS_KEY)); } catch { tokens = "unavailable"; }
  res.json({ instance: INSTANCE_ID, queueDepth, l1Size: l1.size, tokensRemaining: tokens });
});

setInterval(() => {
  const total = stats[200] + stats[502] + stats[503];
  if (total === 0) return;
  console.log(
    `[${INSTANCE_ID}] 200=${stats[200]} 502=${stats[502]} 503=${stats[503]}(shed=${stats.shed}) | ` +
    `l1=${stats.l1} l2=${stats.l2} dedup=${stats.dedup} upstream=${stats.upstream} | ` +
    `queue=${queueDepth} l1size=${l1.size}`
  );
  stats = { 200: 0, 502: 0, 503: 0, l1: 0, l2: 0, upstream: 0, dedup: 0, shed: 0 };
}, 2000);

app.listen(PORT, () => {
  console.log(
    `[${INSTANCE_ID}] listening on :${PORT} -> ${SUPPLIER_URL}, ` +
    `redis via sentinels ${SENTINELS.map((s) => `${s.host}:${s.port}`).join(",")} (${SENTINEL_NAME})`
  );
});
