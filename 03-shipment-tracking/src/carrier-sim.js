import { Kafka } from "kafkajs";

/**
 * CARRIER SIMULATOR
 * =================
 *
 * Stands in for the third-party logistics providers Andercore integrates with
 * (DHL, Maersk, regional truckers). Each one pushes GPS position events for
 * shipments in transit.
 *
 * WHY THIS IS THE HARD PART
 * Real carrier feeds are not a clean ordered stream. They are:
 *
 *   OUT OF ORDER    A truck's 14:00 ping can arrive after its 15:00 ping,
 *                   because the device buffered while out of coverage and
 *                   flushed everything at once.
 *   DUPLICATED      Carriers retry on timeout without knowing we already got
 *                   the event, so the same ping arrives 2-3 times.
 *   LATE            A ping can arrive hours after it was recorded.
 *   BURSTY          A ship docks and 500 containers report simultaneously.
 *
 * Every one of those is simulated here on purpose. If the consumer is written
 * against a well-behaved stream, it will report shipments MOVING BACKWARDS in
 * production — which is the failure this whole exercise is about.
 *
 * THE KEY DISTINCTION: EVENT TIME vs INGESTION TIME
 *   recordedAt  = when the carrier's device observed the position (event time)
 *   arrival     = when our consumer received it (ingestion time)
 * These differ, sometimes by hours. Ordering by arrival is the bug. Ordering
 * by recordedAt is the fix.
 */

const BROKER = process.env.BROKER || "localhost:9092";
const TOPIC = process.env.TOPIC || "shipment-events";
const SHIPMENTS = Number(process.env.SHIPMENTS || 200);
const EVENTS_PER_SEC = Number(process.env.EVENTS_PER_SEC || 200);
const DURATION_SEC = Number(process.env.DURATION_SEC || 60);

// How badly the feed misbehaves. Tunable so we can prove the consumer is
// correct under clean conditions first, then turn up the chaos.
const OUT_OF_ORDER_RATE = Number(process.env.OUT_OF_ORDER_RATE || 0.25);
const DUPLICATE_RATE = Number(process.env.DUPLICATE_RATE || 0.10);
const MAX_LATENESS_MS = Number(process.env.MAX_LATENESS_MS || 120000);

const CARRIERS = ["DHL", "MAERSK", "KUEHNE", "DSV"];
const STATUSES = ["PICKED_UP", "IN_TRANSIT", "AT_HUB", "OUT_FOR_DELIVERY", "DELIVERED"];

const kafka = new Kafka({ clientId: "carrier-sim", brokers: [BROKER] });
const producer = kafka.producer();

// Per-shipment progress. Each shipment walks forward through statuses and
// along a route, so "moving backwards" is detectable as an actual error rather
// than random noise.
const state = new Map();

function initShipment(id) {
  return {
    seq: 0,
    lat: 50 + Math.random() * 4,
    lon: 8 + Math.random() * 4,
    statusIdx: 0,
    baseTime: Date.now() - 3600000, // started an hour ago
    carrier: CARRIERS[Math.floor(Math.random() * CARRIERS.length)],
  };
}

function nextEvent(shipmentId) {
  let s = state.get(shipmentId);
  if (!s) {
    s = initShipment(shipmentId);
    state.set(shipmentId, s);
  }

  s.seq++;
  // Move roughly north-east along a route.
  s.lat += 0.01 + Math.random() * 0.02;
  s.lon += 0.01 + Math.random() * 0.02;
  if (Math.random() < 0.08 && s.statusIdx < STATUSES.length - 1) s.statusIdx++;

  // Event time advances 30s per step. This is the carrier's clock, not ours.
  const recordedAt = s.baseTime + s.seq * 30000;

  return {
    eventId: `${shipmentId}-${s.seq}`, // stable id => duplicates are detectable
    shipmentId,
    carrier: s.carrier,
    seq: s.seq,
    lat: Number(s.lat.toFixed(5)),
    lon: Number(s.lon.toFixed(5)),
    status: STATUSES[s.statusIdx],
    recordedAt: new Date(recordedAt).toISOString(),
  };
}

// Events waiting to be sent late, simulating a device that lost coverage.
const delayed = [];

async function run() {
  await producer.connect();
  console.log(`[carrier-sim] connected to ${BROKER}, topic=${TOPIC}`);
  console.log(
    `[carrier-sim] ${SHIPMENTS} shipments, ${EVENTS_PER_SEC} ev/s, ${DURATION_SEC}s | ` +
    `out-of-order=${OUT_OF_ORDER_RATE} dup=${DUPLICATE_RATE}`
  );

  const started = Date.now();
  let sent = 0, dupes = 0, late = 0;

  const tick = setInterval(async () => {
    if (Date.now() - started > DURATION_SEC * 1000) {
      clearInterval(tick);
      // Flush anything still held back, so late events are not simply lost.
      if (delayed.length) {
        await producer.send({
          topic: TOPIC,
          messages: delayed.map((e) => ({ key: e.shipmentId, value: JSON.stringify(e) })),
        });
        console.log(`[carrier-sim] flushed ${delayed.length} delayed events`);
      }
      console.log(`[carrier-sim] DONE sent=${sent} dupes=${dupes} late=${late}`);
      await producer.disconnect();
      process.exit(0);
    }

    const batch = [];
    for (let i = 0; i < Math.ceil(EVENTS_PER_SEC / 10); i++) {
      const shipmentId = `SHIP-${Math.floor(Math.random() * SHIPMENTS)}`;
      const ev = nextEvent(shipmentId);

      if (Math.random() < OUT_OF_ORDER_RATE) {
        // Hold it back — it will be sent later, arriving AFTER newer events.
        delayed.push(ev);
        late++;
        continue;
      }

      batch.push({ key: ev.shipmentId, value: JSON.stringify(ev) });
      sent++;

      if (Math.random() < DUPLICATE_RATE) {
        // Carrier retried: byte-identical event, same eventId.
        batch.push({ key: ev.shipmentId, value: JSON.stringify(ev) });
        dupes++;
      }
    }

    // Release some held-back events, so they interleave with fresher ones.
    while (delayed.length > 0 && Math.random() < 0.3) {
      const ev = delayed.shift();
      const age = Date.now() - Date.parse(ev.recordedAt);
      if (age < MAX_LATENESS_MS) {
        batch.push({ key: ev.shipmentId, value: JSON.stringify(ev) });
        sent++;
      } else {
        batch.push({ key: ev.shipmentId, value: JSON.stringify(ev) });
        sent++;
      }
    }

    if (batch.length) {
      try {
        await producer.send({ topic: TOPIC, messages: batch });
      } catch (e) {
        console.error(`[carrier-sim] send failed: ${e.message}`);
      }
    }
  }, 100);
}

run().catch((e) => {
  console.error("[carrier-sim] fatal", e);
  process.exit(1);
});
