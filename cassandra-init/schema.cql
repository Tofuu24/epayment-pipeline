-- Runs once per `docker compose up` (cassandra-init service). IF NOT EXISTS makes it safe to re-run.
-- All timestamps are stored in UTC; cqlsh shows them as +0000 (10:00 Manila = 02:00+0000).

CREATE KEYSPACE IF NOT EXISTS payment_pipeline
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Written by Spark Job A (registry sync from Kafka)
CREATE TABLE IF NOT EXISTS payment_pipeline.institution_registry (
    institution_code text PRIMARY KEY,
    institution_name text,
    rail_eligibility set<text>,
    bank_code        text,
    active           boolean
);

-- Written by Spark Job B. One row per payment intent: its routing decision.
-- Single-partition lookup by reference_id.
CREATE TABLE IF NOT EXISTS payment_pipeline.transaction_lifecycle_by_reference (
    reference_id                 text PRIMARY KEY,
    source_institution_code      text,
    destination_institution_code text,
    amount                       bigint,     -- centavos
    currency                     text,
    created_at                   timestamp,
    status                       text,       -- ROUTED | REJECTED
    rail                         text,       -- INSTAPAY | PESONET (null when rejected)
    routing_reason               text,
    rejection_reason             text,
    batch_window                 timestamp,  -- PESONet clearing window (null for InstaPay)
    settlement_due               timestamp,
    metadata_json                text,
    kafka_partition              int,
    kafka_offset                 bigint,
    processed_at                 timestamp
);

-- Written by Spark Job B. Routed transfers per (rail, sending institution),
-- newest settlement deadline first: "what is BPI waiting on in PESONet?"
CREATE TABLE IF NOT EXISTS payment_pipeline.settlement_monitoring_by_institution (
    rail                         text,
    source_institution_code      text,
    settlement_due               timestamp,
    reference_id                 text,
    destination_institution_code text,
    amount                       bigint,
    batch_window                 timestamp,
    created_at                   timestamp,
    settlement_status            text,       -- AWAITING_SETTLEMENT until a settlement event arrives
    PRIMARY KEY ((rail, source_institution_code), settlement_due, reference_id)
) WITH CLUSTERING ORDER BY (settlement_due DESC, reference_id ASC);

-- Written by Spark Job B. Messages that couldn't be keyed (not JSON, or no
-- reference_id), kept by Kafka position so nothing is silently dropped.
CREATE TABLE IF NOT EXISTS payment_pipeline.invalid_intent_events (
    kafka_topic     text,
    kafka_partition int,
    kafka_offset    bigint,
    raw_value       text,
    reason          text,
    received_at     timestamp,
    PRIMARY KEY ((kafka_topic), kafka_partition, kafka_offset)
);
