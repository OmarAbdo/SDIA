import { Kafka } from "kafkajs";
import pg from "pg";

/**
 * SHIPMENT EVENT CONSUMER
 * =======================
 *
 * Reads carrier GPS events off the stream and maintains current position per
 * shipment. The entire difficulty is that the stream is not ordered, not
 * deduplicated, and not on time.
 *
 * THE CENTRAL BUG THIS AVOIDS
 * The naive consumer is:
 *
 *     UPDATE shipment_position SET lat=$1, lon=$2 WHERE shipment_id=$3
 *
 * Correct-looking, and wrong. A 14:00 ping that arrives after the 15:00 ping
 * overwrites it, and the customer watching the map sees their shipment jump
 * BACKWARDS. The data is not corrupt in any way a schema constraint would
 * catch — it is just old.
 *
 * THE FIX: compare EVENT TIME, not arrival order.
 *
 *     WHERE shipment_position.last_recorded_at < EXCLUDED.last_recorded_at
 *
 * An event only advances the row if the carrier recorded it later than
 * whatever we already accepted. Late arrivals are stored in history but do not
 * move the position. This is last-write-wins keyed on event time.
 *
 * WHY THIS IS SAFE UNDER CONCURRENCY
 * The comparison and the write are ONE statement, so Postgres evaluates the
 * predicate against the row it is about to modify while holding its lock. Two
 * consumers processing two events for the same shipment cannot interleave a
 * read-then-write and both win. Doing it as SELECT-then-UPDATE in application
 * code would reintroduce exactly the race the Lua script avoided in 02.
 *
 * ORDERING GUARANTEE FROM PARTITIONING
 * The producer keys every message by shipmentId, so all events for a shipment
 * land in the same partition and are consumed in arrival order by one consumer.
 * That gives per-shipment serialization for free. It does NOT give event-time
 * ordering — the carrier can still send us stale data — which is why the
 * predicate above is still required. Partitioning solves concurrency; the
 * predicate solves lateness. Two different problems, often confused.
 */

const BROKER = process.env.BROKER || "localhost:9092";
const TOPIC = process.env.TOPIC || "shipment-events";
const GROUP = process.env.GROUP || "tracking-consumer";
const PG_URL = process.env.PG_URL || "postgres://track:track@localhost:5433/tracking";
const CONSUMER_ID = process.env.CONSUMER_ID || `c-${process.pid}`;

const kafka = new Kafka({ clientId: CONSUMER_ID, brokers: [BROKER] });
const consumer = kafka.consumer({ groupId: GROUP });
// Pool must exceed the batch concurrency below, or the queue simply moves from
// the event loop into the pool and nothing gets faster.
const pool = new pg.Pool({ connectionString: PG_URL, max: 30 });

let received = 0, accepted = 0, stale = 0, dupes = 0, errors = 0;

/**
 * Two statements per event, deliberately:
 *
 * 1. INSERT into history with ON CONFLICT DO NOTHING. The carrier-supplied
 *    event_id is the primary key, so a retried event hits the conflict and is
 *    counted as a duplicate rather than processed twice. This is IDEMPOTENCY
 *    via a natural key — no coordination needed, and it works even if the same
 *    event is delivered by different consumers after a rebalance.
 *
 * 2. UPSERT the position, guarded by the event-time predicate.
 *
 * They are not wrapped in a transaction on purpose. If step 2 fails after step
 * 1 succeeds, we have recorded history without advancing position; the next
 * event for that shipment corrects it. Wrapping both would add lock contention
 * on the hot path to protect against a case that is self-healing.
 */
async function handleEvent(ev) {
  received++;

  const client = await pool.connect();
  try {
    const hist = await client.query(
      `INSERT INTO shipment_event
         (event_id, shipment_id, carrier, lat, lon, status, recorded_at, accepted)
       VALUES ($1,$2,$3,$4,$5,$6,$7,false)
       ON CONFLICT (event_id) DO NOTHING
       RETURNING event_id`,
      [ev.eventId, ev.shipmentId, ev.carrier, ev.lat, ev.lon, ev.status, ev.recordedAt]
    );

    if (hist.rowCount === 0) {
      dupes++;
      return; // already processed this exact event
    }

    const upsert = await client.query(
      `INSERT INTO shipment_position
         (shipment_id, carrier, lat, lon, status, last_recorded_at, updated_at, event_count)
       VALUES ($1,$2,$3,$4,$5,$6,now(),1)
       ON CONFLICT (shipment_id) DO UPDATE SET
         carrier          = EXCLUDED.carrier,
         lat              = EXCLUDED.lat,
         lon              = EXCLUDED.lon,
         status           = EXCLUDED.status,
         last_recorded_at = EXCLUDED.last_recorded_at,
         updated_at       = now(),
         event_count      = shipment_position.event_count + 1
       WHERE shipment_position.last_recorded_at < EXCLUDED.last_recorded_at
       RETURNING shipment_id`,
      [ev.shipmentId, ev.carrier, ev.lat, ev.lon, ev.status, ev.recordedAt]
    );

    if (upsert.rowCount > 0) {
      accepted++;
      // Third round-trip: flips the history row's accepted flag. Kept separate
      // rather than folded into the upsert because the upsert targets a
      // different table, and a CTE spanning both would still be two writes
      // under one lock — more coupling, no fewer round-trips. Left as-is; the
      // real cost was never the statement count, it was processing events one
      // at a time (see eachBatch).
      await client.query(
        `UPDATE shipment_event SET accepted = true WHERE event_id = $1`,
        [ev.eventId]
      );
    } else {
      // Arrived late: kept in history, did not move the position.
      stale++;
    }
  } finally {
    client.release();
  }
}

async function run() {
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
  console.log(`[${CONSUMER_ID}] consuming ${TOPIC} group=${GROUP}`);

  await consumer.run({
    eachBatchAutoResolve: true,
    eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
      /**
       * Events are processed with bounded concurrency, not one at a time.
       *
       * The first version awaited each event sequentially. Measured result:
       * 6 events/sec, while Postgres executed each statement in 0.33ms and sat
       * idle at 8 of 100 connections. ~167ms per event of pure round-trip
       * waiting, with a pool of 10 serving exactly one event at any moment.
       *
       * CONCURRENCY IS SAFE HERE, and the reason matters: the producer keys by
       * shipmentId, so every event for a given shipment is in ONE partition and
       * this batch is from ONE partition. Two events for the same shipment can
       * therefore be in flight together — which is exactly why correctness can
       * NOT rest on processing order. It rests on the event-time predicate in
       * the UPSERT, which is evaluated under Postgres's row lock. Ordering was
       * never what made this correct, so parallelising it changes nothing about
       * the guarantee.
       *
       * Offsets are still resolved in order after the batch completes, so a
       * crash mid-batch replays the whole batch. Replay is harmless because
       * event_id is the primary key: duplicates are suppressed, not double
       * counted. Idempotency is what makes at-least-once delivery survivable.
       */
      const CONCURRENCY = 20;
      for (let i = 0; i < batch.messages.length; i += CONCURRENCY) {
        const slice = batch.messages.slice(i, i + CONCURRENCY);
        await Promise.all(
          slice.map(async (msg) => {
            try {
              await handleEvent(JSON.parse(msg.value.toString()));
            } catch (e) {
              errors++;
              if (errors < 5) console.error(`[${CONSUMER_ID}] ${e.message}`);
            }
          })
        );
        for (const msg of slice) resolveOffset(msg.offset);
        await heartbeat();
      }
    },
  });
}

setInterval(async () => {
  if (received === 0) return;
  console.log(
    `[${CONSUMER_ID}] received=${received} accepted=${accepted} stale=${stale} dup=${dupes} err=${errors}`
  );
  try {
    await pool.query(
      `UPDATE ingest_stats SET total_received=$1, accepted=$2, stale_rejected=$3, duplicates=$4 WHERE id=1`,
      [received, accepted, stale, dupes]
    );
  } catch { /* stats are best-effort */ }
}, 3000);

run().catch((e) => {
  console.error(`[${CONSUMER_ID}] fatal`, e);
  process.exit(1);
});
