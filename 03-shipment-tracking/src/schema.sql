-- SHIPMENT TRACKING SCHEMA
-- ========================

-- Current position per shipment. One row per shipment, overwritten in place.
--
-- last_recorded_at is the load-bearing column: it stores the carrier's EVENT
-- TIME for the newest position we have accepted. Every incoming event is
-- compared against it, and anything older is rejected. Without this column
-- there is no way to tell a genuinely new position from a late-arriving old
-- one, because both look identical on arrival.
CREATE TABLE IF NOT EXISTS shipment_position (
  shipment_id       TEXT PRIMARY KEY,
  carrier           TEXT        NOT NULL,
  lat               NUMERIC(9,5) NOT NULL,
  lon               NUMERIC(9,5) NOT NULL,
  status            TEXT        NOT NULL,
  last_recorded_at  TIMESTAMPTZ NOT NULL,  -- carrier's clock (event time)
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(), -- our clock
  event_count       INT         NOT NULL DEFAULT 0
);

-- Full history, append-only. Useful for "show me the route" queries and for
-- auditing what we actually received versus what we accepted.
CREATE TABLE IF NOT EXISTS shipment_event (
  event_id     TEXT PRIMARY KEY,  -- carrier-supplied, stable across retries
  shipment_id  TEXT        NOT NULL,
  carrier      TEXT        NOT NULL,
  lat          NUMERIC(9,5) NOT NULL,
  lon          NUMERIC(9,5) NOT NULL,
  status       TEXT        NOT NULL,
  recorded_at  TIMESTAMPTZ NOT NULL,
  ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  accepted     BOOLEAN     NOT NULL  -- did it advance the position, or was it stale?
);

CREATE INDEX IF NOT EXISTS idx_event_shipment_time
  ON shipment_event (shipment_id, recorded_at DESC);

-- Counters for measuring correctness during load tests. A system that silently
-- drops or reorders events looks healthy unless you count explicitly.
CREATE TABLE IF NOT EXISTS ingest_stats (
  id              INT PRIMARY KEY DEFAULT 1,
  total_received  BIGINT NOT NULL DEFAULT 0,
  accepted        BIGINT NOT NULL DEFAULT 0,
  stale_rejected  BIGINT NOT NULL DEFAULT 0,  -- arrived late, older than current
  duplicates      BIGINT NOT NULL DEFAULT 0,  -- same event_id seen twice
  CONSTRAINT one_row CHECK (id = 1)
);

INSERT INTO ingest_stats (id) VALUES (1) ON CONFLICT DO NOTHING;
