import http from "k6/http";
import { check } from "k6";
import { Counter, Trend } from "k6/metrics";

/**
 * LOAD TEST — find where the gateway actually breaks
 * =================================================
 *
 * The goal is not to confirm the gateway works. It is to find the point where
 * it stops working, and to characterize HOW it fails when it does.
 *
 * THE LESSON THIS FILE ALREADY TAUGHT US
 * The first version drew from only 6 SKUs. It reported a 0.81% failure rate and
 * looked like a clean pass. It wasn't — with 6 keys and a 2s TTL, nearly every
 * request was a cache hit, so the token bucket and queue were never meaningfully
 * exercised. The test was measuring the cache and reporting it as system health.
 *
 * Widening to 2000 SKUs made most requests cache misses, and the same gateway
 * immediately showed 63% failures and 5s p95 latency. Same code, same load
 * profile, completely different verdict.
 *
 * Generalize it: a passing load test is evidence about the TEST as much as the
 * system. Before trusting a green run, ask which code path it actually reached.
 * Here the tell was `cacheSize: 6` on the health endpoint.
 *
 * RUN
 *   docker run --rm -i --add-host=host.docker.internal:host-gateway \
 *     -v <abs-path>/loadtest:/scripts grafana/k6 run /scripts/break-it.js
 *
 * On Git Bash, prefix with MSYS_NO_PATHCONV=1 and use `pwd -W`, or the mount
 * path gets mangled into a Windows path k6 can't resolve inside the container.
 */

const GATEWAY = "http://host.docker.internal:4000";

/**
 * Custom metrics. The built-in http_req_failed collapses everything into one
 * rate, which is not enough — 502 and 503 mean opposite things here:
 *
 *   503 = the gateway shed load deliberately. Our protection working.
 *   502 = the supplier failed, or retries were exhausted. Upstream hurting.
 *
 * Counting them separately is what turns "63% failed" into a diagnosis. A run
 * that is mostly 503s is a capacity problem; mostly 502s is a dependency
 * problem. Same headline number, different fix.
 */
const overloadRejects = new Counter("overload_503");
const upstreamFailures = new Counter("upstream_502");
const latency = new Trend("gateway_latency", true);
const cacheHits = new Counter("cache_hit");
const cacheMisses = new Counter("cache_miss");

/**
 * Key-space width is the most important dial in this file.
 *
 * Cache hit rate is roughly a function of (pool size, TTL, request rate). With
 * 2000 SKUs at ~20 req/s and a 2s TTL, a repeat within any entry's lifetime is
 * rare, so traffic reaches the rate limiter — which is the component under
 * test. Shrinking this number turns the test back into a cache benchmark.
 */
const SKU_POOL_SIZE = 2000;
function randomSkuId() {
  return `SKU-${Math.floor(Math.random() * SKU_POOL_SIZE)}`;
}

/**
 * FIVE PHASES, EACH TARGETING A DIFFERENT FAILURE MODE
 *
 *   warmup    3 req/s   — below capacity. Baseline: what does healthy look like?
 *   burst    80 req/s   — 20x capacity, sudden. Tests the queue's shed path and,
 *                         critically, whether a spike causes lasting damage.
 *   sustained 20 req/s  — 5x capacity, held 30s. The most revealing phase: a
 *                         brief spike can be absorbed, but sustained pressure
 *                         forces the system to find a steady state. This is
 *                         where the thundering-herd bug showed up as queue
 *                         depth oscillating 0↔20 instead of settling.
 *   hot_key  50 req/s   — one SKU. Isolates cache + dedup from the limiter.
 *                         Should be absorbed almost entirely.
 *   cooldown  2 req/s   — back to light. Does it RECOVER, or is it wedged?
 *                         Recovery is a separate property from survival; a
 *                         system can pass every load phase and still never
 *                         return to normal once the load stops.
 *
 * constant-arrival-rate (not constant-vus) is the right executor: it holds the
 * REQUEST rate fixed regardless of how slow responses get. With fixed VUs,
 * slow responses would throttle the offered load automatically — the test would
 * quietly back off exactly when the system is struggling, which is the opposite
 * of what you want. Open-model load is what real traffic looks like: users
 * arrive at their own rate and do not politely slow down because you're busy.
 *
 * startTimes overlap slightly (burst at 12s runs into sustained at 25s) so the
 * system is entering sustained load while still recovering from the spike.
 */
export const options = {
  scenarios: {
    warmup: {
      executor: "constant-arrival-rate",
      rate: 3,
      timeUnit: "1s",
      duration: "10s",
      preAllocatedVUs: 10,
      startTime: "0s",
      exec: "randomSku",
    },
    burst: {
      executor: "constant-arrival-rate",
      rate: 80,
      timeUnit: "1s",
      duration: "10s",
      preAllocatedVUs: 100,
      startTime: "12s",
      exec: "randomSku",
    },
    sustained: {
      executor: "constant-arrival-rate",
      rate: 20,
      timeUnit: "1s",
      duration: "30s",
      preAllocatedVUs: 50,
      startTime: "25s",
      exec: "randomSku",
    },
    hot_key: {
      executor: "constant-arrival-rate",
      rate: 50,
      timeUnit: "1s",
      duration: "10s",
      preAllocatedVUs: 60,
      startTime: "58s",
      exec: "hotKey",
    },
    cooldown: {
      executor: "constant-arrival-rate",
      rate: 2,
      timeUnit: "1s",
      duration: "10s",
      preAllocatedVUs: 5,
      startTime: "70s",
      exec: "randomSku",
    },
  },

  /**
   * Thresholds are the pass/fail contract — they make k6 exit non-zero, which
   * is what lets a load test gate a deploy instead of just producing numbers
   * someone has to interpret.
   *
   * <50% is deliberately permissive. The system is driven far past capacity on
   * purpose, so some shedding is expected and correct. This is set to catch
   * catastrophic collapse, not to assert an SLO. A production threshold would
   * be much tighter and would run at realistic load — p95 latency and error
   * rate under EXPECTED traffic, not this.
   */
  thresholds: {
    http_req_failed: ["rate<0.5"],
  },
};

// Most phases: spread across the wide pool, so requests miss cache and reach
// the rate limiter.
export function randomSku() {
  hit(randomSkuId());
}

// hot_key phase: one SKU, maximum contention. Simulates the real pattern where
// a single popular item (a steel batch everyone is quoting at once) draws
// disproportionate traffic. Exercises dedup, not the limiter.
export function hotKey() {
  hit("HOT-SKU-1");
}

function hit(sku) {
  const res = http.get(`${GATEWAY}/price/${sku}`);
  latency.add(res.timings.duration);

  if (res.status === 503) overloadRejects.add(1);
  if (res.status === 502) upstreamFailures.add(1);

  // Only 200s carry a body worth inspecting. The gateway tags responses with
  // `cached` / `deduped`, which lets the test measure how much load each defense
  // layer absorbed rather than inferring it from aggregate numbers.
  if (res.status === 200) {
    const body = res.json();
    if (body.cached || body.deduped) cacheHits.add(1);
    else cacheMisses.add(1);
  }

  /**
   * Checks record correctness; thresholds decide pass/fail. A failed check does
   * not fail the run on its own.
   *
   * This one is permissive by design: 502 and 503 are ACCEPTABLE under
   * deliberate overload — a fast 503 is the system behaving correctly. What
   * would be unacceptable is a hang, a connection reset, or a 500 from an
   * unhandled exception in the gateway itself. So this asserts "failed in a way
   * we designed for," not "succeeded."
   */
  check(res, {
    "status is 200 or a known overload code": (r) =>
      [200, 502, 503].includes(r.status),
  });
}
