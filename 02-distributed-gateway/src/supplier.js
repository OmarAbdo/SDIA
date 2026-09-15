import express from "express";

/**
 * MOCK SUPPLIER — realistic B2B contract tier.
 *
 * Raised from 5 req/s (01) to 500 req/s. That single number change moves the
 * whole design: at 5 req/s the rate limiter was the star, at 500 req/s with
 * ~3300 req/s of demand the CACHE is the star and the limiter is a safety net
 * for the remainder. Worth saying out loud in an interview — which component
 * matters is a function of the numbers, not of the pattern.
 */

const PORT = Number(process.env.PORT || 4001);
const SUPPLIER_RATE_LIMIT = Number(process.env.SUPPLIER_RATE_LIMIT || 500);
const WINDOW_MS = 1000;

let windowStart = Date.now();
let windowCount = 0;
let served = 0;
let rejected = 0;

const app = express();

app.get("/price/:sku", (req, res) => {
  const now = Date.now();
  if (now - windowStart >= WINDOW_MS) {
    windowStart = now;
    windowCount = 0;
  }
  windowCount++;

  if (windowCount > SUPPLIER_RATE_LIMIT) {
    rejected++;
    return res.status(429).json({ error: "supplier rate limit exceeded" });
  }

  const latency = 20 + Math.random() * 80;
  setTimeout(() => {
    if (Math.random() < 0.02) {
      return res.status(500).json({ error: "supplier internal error" });
    }
    served++;
    const base = 100 + (req.params.sku.charCodeAt(0) % 50);
    res.json({
      sku: req.params.sku,
      price: Number((base + (Math.random() - 0.5) * 4).toFixed(2)),
      currency: "EUR",
      ts: new Date().toISOString(),
    });
  }, latency);
});

setInterval(() => {
  if (served || rejected) {
    console.log(`[supplier] served=${served} rejected429=${rejected} (last 2s)`);
    served = 0; rejected = 0;
  }
}, 2000);

app.listen(PORT, () => {
  console.log(`[supplier] listening on :${PORT}, hard limit ${SUPPLIER_RATE_LIMIT} req/s`);
});
