import http from "k6/http";
import { check } from "k6";
import { Counter, Trend } from "k6/metrics";

/**
 * Load test for the DISTRIBUTED gateway.
 *
 * Points at the LB (:4100), which fans out to 3 gateway instances. Rates are
 * scaled up from 01 because the supplier now allows 500 req/s instead of 5.
 *
 * What we are looking for, versus the 01 baseline (63% fail, p95 4.98s):
 *   - does the shared bucket actually hold 3 instances under the cap?
 *   - does queue depth settle instead of oscillating (the jitter fix)?
 *   - how much load do L1/L2 absorb before anything reaches the limiter?
 */

const TARGET = __ENV.TARGET || "http://host.docker.internal:4100";

const reject503 = new Counter("overload_503");
const fail502 = new Counter("upstream_502");
const latency = new Trend("gateway_latency", true);
const hitL1 = new Counter("cache_l1");
const hitL2 = new Counter("cache_l2");
const hitDedup = new Counter("dedup");
const missUpstream = new Counter("upstream_call");

const SKU_POOL_SIZE = 2000;

export const options = {
  scenarios: {
    warmup:    { executor: "constant-arrival-rate", rate: 100,  timeUnit: "1s", duration: "10s", preAllocatedVUs: 50,  startTime: "0s",  exec: "randomSku" },
    burst:     { executor: "constant-arrival-rate", rate: 2000, timeUnit: "1s", duration: "10s", preAllocatedVUs: 400, startTime: "12s", exec: "randomSku" },
    sustained: { executor: "constant-arrival-rate", rate: 800,  timeUnit: "1s", duration: "30s", preAllocatedVUs: 300, startTime: "25s", exec: "randomSku" },
    hot_key:   { executor: "constant-arrival-rate", rate: 1000, timeUnit: "1s", duration: "10s", preAllocatedVUs: 200, startTime: "58s", exec: "hotKey" },
    cooldown:  { executor: "constant-arrival-rate", rate: 50,   timeUnit: "1s", duration: "10s", preAllocatedVUs: 30,  startTime: "70s", exec: "randomSku" },
  },
  thresholds: { http_req_failed: ["rate<0.9"] },
};

export function randomSku() {
  hit(`SKU-${Math.floor(Math.random() * SKU_POOL_SIZE)}`);
}

export function hotKey() {
  hit("HOT-SKU-1");
}

function hit(sku) {
  const res = http.get(`${TARGET}/price/${sku}`);
  latency.add(res.timings.duration);

  if (res.status === 503) reject503.add(1);
  if (res.status === 502) fail502.add(1);
  if (res.status === 200) {
    const b = res.json();
    if (b.cached === "l1") hitL1.add(1);
    else if (b.cached === "l2") hitL2.add(1);
    else if (b.cached === "dedup") hitDedup.add(1);
    else missUpstream.add(1);
  }

  check(res, { "known status": (r) => [200, 502, 503].includes(r.status) });
}
