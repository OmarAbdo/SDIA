import pg from "pg";

/**
 * CORRECTNESS CHECKER
 * ===================
 *
 * Written BEFORE the load test, on purpose.
 *
 * The lesson from the rate limiter was that a passing test can be measuring the
 * wrong thing — there, a broken health-check grep manufactured a bug that did
 * not exist, and five fixes were aimed at it. So for this system the definition
 * of "correct" is written down first, as queries, rather than inferred later
 * from whether the throughput numbers looked good.
 *
 * Throughput proves nothing here. A consumer that drops every event is
 * infinitely fast. These are the properties that actually matter:
 *
 *   1. NO BACKWARDS MOVEMENT
 *      Every stored position must be the newest by event time. If any accepted
 *      event is older than the shipment's stored last_recorded_at, the
 *      event-time guard failed and customers saw a shipment reverse.
 *
 *   2. NO LOST UPDATES
 *      For each shipment, the stored position must equal the event with the
 *      maximum recorded_at among accepted events. If not, a concurrent write
 *      clobbered a newer one — the classic read-modify-write race.
 *
 *   3. DUPLICATES SUPPRESSED
 *      The carrier sends the same event_id more than once. History must hold
 *      exactly one row per event_id, otherwise idempotency is broken.
 *
 *   4. NOTHING SILENTLY DROPPED
 *      received == accepted + stale + duplicates. If the sum does not reconcile,
 *      events vanished somewhere and no latency graph would reveal it.
 *
 * Properties 1 and 2 are the ones a naive implementation fails, and they fail
 * INVISIBLY: the rows look plausible, the API returns 200, and nothing in the
 * infrastructure reports an error.
 */

const PG_URL = process.env.PG_URL || "postgres://track:track@localhost:5433/tracking";
const pool = new pg.Pool({ connectionString: PG_URL });

async function check() {
  let failures = 0;
  const line = (ok, label, detail) => {
    console.log(`  ${ok ? "PASS" : "FAIL"}  ${label}${detail ? " — " + detail : ""}`);
    if (!ok) failures++;
  };

  console.log("\n=== CORRECTNESS CHECKS ===\n");

  // 1. Any accepted event older than the shipment's stored position means the
  //    event-time predicate let a stale write through.
  const backwards = await pool.query(`
    SELECT e.shipment_id, count(*)::int AS bad
      FROM shipment_event e
      JOIN shipment_position p ON p.shipment_id = e.shipment_id
     WHERE e.accepted = true
       AND e.recorded_at > p.last_recorded_at
     GROUP BY e.shipment_id`);
  line(backwards.rowCount === 0, "no position moved backwards in event time",
    backwards.rowCount ? `${backwards.rowCount} shipments affected` : "");

  // 2. Stored position must match the newest accepted event exactly.
  const lost = await pool.query(`
    WITH newest AS (
      SELECT DISTINCT ON (shipment_id) shipment_id, recorded_at, lat, lon
        FROM shipment_event WHERE accepted = true
       ORDER BY shipment_id, recorded_at DESC
    )
    SELECT p.shipment_id, p.last_recorded_at, n.recorded_at
      FROM shipment_position p JOIN newest n ON n.shipment_id = p.shipment_id
     WHERE p.last_recorded_at <> n.recorded_at`);
  line(lost.rowCount === 0, "stored position == newest accepted event",
    lost.rowCount ? `${lost.rowCount} mismatches (lost update)` : "");

  // 3. event_id is the primary key, so a violation would have thrown on insert;
  //    this verifies the constraint is actually doing the dedup work.
  const dupRows = await pool.query(`
    SELECT event_id, count(*)::int c FROM shipment_event
     GROUP BY event_id HAVING count(*) > 1`);
  line(dupRows.rowCount === 0, "no duplicate event_id rows in history",
    dupRows.rowCount ? `${dupRows.rowCount} duplicated` : "");

  // 4. Reconcile the counters against reality.
  const s = (await pool.query(`SELECT * FROM ingest_stats WHERE id=1`)).rows[0];
  if (s) {
    const sum = Number(s.accepted) + Number(s.stale_rejected) + Number(s.duplicates);
    line(Number(s.total_received) === sum, "received == accepted + stale + duplicates",
      `${s.total_received} vs ${sum}`);
  }

  // Context, not pass/fail: how much of the feed arrived out of order at all.
  const summary = await pool.query(`
    SELECT count(*)::int AS events,
           count(*) FILTER (WHERE accepted)::int AS accepted,
           count(*) FILTER (WHERE NOT accepted)::int AS late,
           count(DISTINCT shipment_id)::int AS shipments,
           round(avg(EXTRACT(EPOCH FROM (ingested_at - recorded_at))))::int AS avg_lag_s,
           max(EXTRACT(EPOCH FROM (ingested_at - recorded_at)))::int AS max_lag_s
      FROM shipment_event`);
  const r = summary.rows[0];
  console.log(`\n=== FEED SUMMARY ===`);
  console.log(`  shipments      ${r.shipments}`);
  console.log(`  events         ${r.events}`);
  console.log(`  advanced pos   ${r.accepted}`);
  console.log(`  arrived late   ${r.late}  (${((r.late / Math.max(r.events,1)) * 100).toFixed(1)}% out of order)`);
  console.log(`  avg lag        ${r.avg_lag_s}s`);
  console.log(`  max lag        ${r.max_lag_s}s`);

  console.log(`\n${failures === 0 ? "ALL CHECKS PASSED" : failures + " CHECK(S) FAILED"}\n`);
  await pool.end();
  process.exit(failures === 0 ? 0 : 1);
}

check().catch((e) => { console.error(e); process.exit(2); });
