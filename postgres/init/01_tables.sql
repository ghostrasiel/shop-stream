-- Landing zone for the M1 stream. Append-only on purpose.
--
-- event_id / order_id / payment_id are indexed and not unique. The Spark sink
-- is at-least-once: a micro-batch can be replayed after a crash that happens
-- between the commit and the checkpoint. A unique constraint would fail that
-- replay and stop the query. M2 adds the unique key and switches the writer
-- to an upsert (see spark/job/sinks.py).

CREATE TABLE events (
    id                  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id            TEXT NOT NULL,
    event_type          TEXT NOT NULL,
    event_time          TIMESTAMPTZ NOT NULL,
    user_id             TEXT NOT NULL,
    session_id          TEXT NOT NULL,
    device_type         TEXT,
    device_os           TEXT,
    device_app_version  TEXT,
    utm_source          TEXT,
    utm_medium          TEXT,
    utm_campaign        TEXT,
    page_url            TEXT,
    product_sku         TEXT,
    order_id            TEXT,
    order_total         NUMERIC(12, 2),
    payment_id          TEXT,
    payment_amount      NUMERIC(12, 2),
    payment_method      TEXT,
    kafka_partition     INTEGER NOT NULL,
    kafka_offset        BIGINT NOT NULL,
    schema_id           INTEGER,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT events_event_type_chk CHECK (
        event_type IN (
            'page_view',
            'add_to_cart',
            'order_created',
            'payment_completed'
        )
    )
);

COMMENT ON TABLE events IS
    'One flattened row per shop event. Nested payload lives in Mongo events_raw.';

CREATE INDEX events_event_id_idx ON events (event_id);
CREATE INDEX events_user_time_idx ON events (user_id, event_time);
CREATE INDEX events_type_time_idx ON events (event_type, event_time);
CREATE INDEX events_session_idx ON events (session_id);

CREATE TABLE orders (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id        TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    event_time      TIMESTAMPTZ NOT NULL,
    user_id         TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    currency        CHAR(3) NOT NULL,
    item_count      INTEGER NOT NULL,
    total_amount    NUMERIC(12, 2) NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE orders IS
    'Projection of order_created. Money lives here, not on the wide events row.';

CREATE INDEX orders_order_id_idx ON orders (order_id);
CREATE INDEX orders_event_id_idx ON orders (event_id);
CREATE INDEX orders_user_time_idx ON orders (user_id, event_time);

CREATE TABLE order_items (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id        TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    sku             TEXT NOT NULL,
    product_name    TEXT NOT NULL,
    category        TEXT,
    qty             INTEGER NOT NULL,
    unit_price      NUMERIC(12, 2) NOT NULL,
    CONSTRAINT order_items_qty_chk CHECK (qty > 0)
);

COMMENT ON TABLE order_items IS
    'Line items for an order_created event. sku points at Mongo products.';

CREATE INDEX order_items_order_id_idx ON order_items (order_id);
CREATE INDEX order_items_sku_idx ON order_items (sku);

CREATE TABLE payments (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    payment_id      TEXT NOT NULL,
    order_id        TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    event_time      TIMESTAMPTZ NOT NULL,
    user_id         TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    amount          NUMERIC(12, 2) NOT NULL,
    method          TEXT NOT NULL,
    status          TEXT NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE payments IS
    'Projection of payment_completed. GMV in M2 should sum this table, not events.';

CREATE INDEX payments_payment_id_idx ON payments (payment_id);
CREATE INDEX payments_order_id_idx ON payments (order_id);
CREATE INDEX payments_event_id_idx ON payments (event_id);
CREATE INDEX payments_user_time_idx ON payments (user_id, event_time);
