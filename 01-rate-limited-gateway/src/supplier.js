import express from "express";

/**
 * MOCK THIRD-PARTY SUPPLIER API
 * =============================
 *
 * Stands in for an external vendor's pricing endpoint — a steel mill, a PV
 * module distributor, a cable manufacturer. In Andercore's real architecture
 * there are many of these, all owned by other companies, all behaving badly
 * in their own particular way.
 *
 * WHY THIS EXISTS AS A SEPARATE SERVICE
 * The interesting engineering problems in B2B trade platforms live at the
 * integration boundary, not inside your own code. Your own services are fast,
 * reliable, and you control them. The vendor's API is slow, flaky, rate-limited,
 * and will cut you off without warning. So to practice realistic system design,
 * the upstream has to be genuinely hostile — otherwise you build a gateway that
 * only works against a cooperative backend, which teaches you nothing.
 *
 * THREE HOSTILE BEHAVIORS MODELED HERE
 *   1. A hard rate limit (429 when exceeded) — the vendor protecting itself
 *   2. Variable latency (50-400ms)          — network + their processing time
 *   3. Random failures (3% → 500)           — the everyday unreliability of
 *                                              someone else's infrastructure
 *
 * Every defense in gateway.js exists because of one of these three.
 */

const PORT = 4001;

/**
 * The vendor's contractual rate limit. Exceed it and you get 429s.
 *
 * This number is the single most important constant in the whole exercise:
 * it is the REAL capacity ceiling of the system. No amount of scaling on our
 * side changes it. The gateway can be infinitely fast and it still cannot
 * push more than 5 req/s through this door.
 *
 * This is the defining constraint of integration-heavy systems and a very
 * common system design interview theme: your throughput is set by the
 * slowest/most-restricted dependency, not by your own service's capacity.
 */
const SUPPLIER_RATE_LIMIT = 5; // requests/second
const WINDOW_MS = 1000;

/**
 * FIXED-WINDOW rate limiting — deliberately the naive algorithm.
 *
 * We track a window start time and a counter. When the clock passes the
 * window boundary, the counter resets to zero.
 *
 * Its well-known flaw is the BOUNDARY BURST: a client can send 5 requests at
 * t=0.999s and 5 more at t=1.001s — 10 requests in 2ms, double the intended
 * rate — because the reset is tied to wall-clock boundaries rather than to
 * each request's own history.
 *
 * Kept naive on purpose. Real vendors often implement exactly this, and the
 * gateway has to cope with a limiter that is itself imprecise. (Contrast with
 * the token bucket in gateway.js, which does not have this flaw — the two
 * files together give you both algorithms to compare in an interview.)
 */
let windowStart = Date.now();
let windowCount = 0;

const app = express();

app.get("/price/:sku", (req, res) => {
  const now = Date.now();

  // Reset the counter if we've crossed into a new window.
  if (now - windowStart >= WINDOW_MS) {
    windowStart = now;
    windowCount = 0;
  }
  windowCount++;

  // Over budget: reject immediately and cheaply.
  //
  // Note that the 429 is returned BEFORE the artificial latency below. This is
  // realistic and consequential: rejections are cheap and fast, successes are
  // slow. That asymmetry is why a client hammering a rate-limited API gets a
  // flood of instant errors rather than slow ones — and why naive retry logic
  // can spiral so fast, since failures return quickly enough to be retried
  // almost immediately.
  if (windowCount > SUPPLIER_RATE_LIMIT) {
    return res.status(429).json({ error: "supplier rate limit exceeded" });
  }

  // Variable latency: 50-400ms, the spread you'd see from a real cross-border
  // vendor call. The variance matters more than the average — it means you
  // cannot assume a stable per-request cost, so timeouts and queue sizing have
  // to be built around a distribution rather than a single number.
  const latency = 50 + Math.random() * 350;

  setTimeout(() => {
    // 3% random failure. Small enough to be invisible in light testing, large
    // enough to matter at volume — which is exactly how it behaves in
    // production, and why it must be simulated rather than assumed away.
    if (Math.random() < 0.03) {
      return res.status(500).json({ error: "supplier internal error" });
    }

    // Deterministic base price per SKU (so the same SKU is stably priced)
    // plus small jitter (so responses differ slightly, making it visible in
    // logs when a value came from cache versus a fresh upstream call).
    const base = 100 + (req.params.sku.charCodeAt(0) % 50);
    const jitter = (Math.random() - 0.5) * 4;

    res.json({
      sku: req.params.sku,
      price: Number((base + jitter).toFixed(2)),
      currency: "EUR",
      ts: new Date().toISOString(),
    });
  }, latency);
});

app.listen(PORT, () => {
  console.log(`[supplier] mock supplier API listening on :${PORT}`);
  console.log(`[supplier] hard limit: ${SUPPLIER_RATE_LIMIT} req/s`);
});
