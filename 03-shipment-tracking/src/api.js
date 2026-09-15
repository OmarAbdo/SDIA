import express from "express";
import pg from "pg";

/**
 * QUERY API — "where is my shipment?"
 *
 * The read side of the system. Deliberately thin: all the difficulty lives in
 * the consumer's write path, and by the time a row reaches shipment_position it
 * is already the newest position by event time.
 *
 * Note the two timestamps returned together. They answer different questions
 * and support teams confuse them constantly:
 *
 *   recordedAt  the carrier's clock — when the truck was actually there
 *   updatedAt   our clock — when we learned about it
 *
 * A shipment showing recordedAt from 3 hours ago is not necessarily a broken
 * pipeline; it may be a truck in a tunnel. Surfacing both lets the caller tell
 * "we are behind" apart from "the carrier is silent", which is why staleness
 * is computed and returned rather than left implicit.
 */

const PORT = Number(process.env.PORT || 5000);
const PG_URL = process.env.PG_URL || "postgres://track:track@localhost:5433/tracking";

const pool = new pg.Pool({ connectionString: PG_URL, max: 10 });
const app = express();

app.get("/shipment/:id", async (req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT shipment_id, carrier, lat, lon, status,
              last_recorded_at, updated_at, event_count
         FROM shipment_position WHERE shipment_id = $1`,
      [req.params.id]
    );
    if (!rows.length) return res.status(404).json({ error: "unknown shipment" });

    const r = rows[0];
    res.json({
      shipmentId: r.shipment_id,
      carrier: r.carrier,
      position: { lat: Number(r.lat), lon: Number(r.lon) },
      status: r.status,
      recordedAt: r.last_recorded_at,
      updatedAt: r.updated_at,
      eventCount: r.event_count,
      stalenessSeconds: Math.round((Date.now() - new Date(r.last_recorded_at)) / 1000),
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Route history. Ordered by event time, not ingestion time, so a late arrival
// appears at its true position in the journey rather than at the end.
app.get("/shipment/:id/history", async (req, res) => {
  try {
    const { rows } = await pool.query(
      `SELECT event_id, lat, lon, status, recorded_at, ingested_at, accepted
         FROM shipment_event
        WHERE shipment_id = $1
        ORDER BY recorded_at ASC
        LIMIT 200`,
      [req.params.id]
    );
    res.json({
      shipmentId: req.params.id,
      events: rows.length,
      accepted: rows.filter((r) => r.accepted).length,
      lateArrivals: rows.filter((r) => !r.accepted).length,
      route: rows.map((r) => ({
        lat: Number(r.lat), lon: Number(r.lon), status: r.status,
        recordedAt: r.recorded_at, ingestedAt: r.ingested_at, accepted: r.accepted,
        // How far behind event time the ingestion was. This is the number that
        // tells you whether the pipeline is healthy.
        lagSeconds: Math.round((new Date(r.ingested_at) - new Date(r.recorded_at)) / 1000),
      })),
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get("/stats", async (_req, res) => {
  try {
    const stats = await pool.query(`SELECT * FROM ingest_stats WHERE id = 1`);
    const pos = await pool.query(`SELECT count(*)::int AS shipments FROM shipment_position`);
    const ev = await pool.query(`SELECT count(*)::int AS events FROM shipment_event`);
    res.json({ ...stats.rows[0], ...pos.rows[0], ...ev.rows[0] });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get("/health", (_req, res) => res.json({ ok: true }));

app.listen(PORT, () => console.log(`[api] listening on :${PORT}`));
