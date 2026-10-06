-- Runs once per `docker compose up` (cassandra-init service). IF NOT EXISTS makes it safe to re-run.
-- All timestamps are stored in UTC; cqlsh shows them as +0000 (10:00 Manila = 02:00+0000).
--
-- Column ownership (so jobs never overwrite each other's data):
--   Job A writes institution_registry.
--   Job B writes the routing decision columns, risk_flags, invalid_intent_events, and payment-dlq.
--   Job C writes the settlement_* / turnaround / webhook columns, settlement_alerts_by_rail
--   and webhook_events_by_reference.

CREATE KEYSPACE IF NOT EXISTS payment_pipeline
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

-- Written by Spark Job A (registry sync from Kafka)
CREATE TABLE IF NOT EXISTS payment_pipeline.institution_registry (
    institution_code text PRIMARY KEY,
    institution_name text,
    rail_eligibility set<text>,
    bank_code        text,
    active           boolean
);

-- One row per payment intent. Single-partition lookup by reference_id.
CREATE TABLE IF NOT EXISTS payment_pipeline.transaction_lifecycle_by_reference (
    reference_id                 text PRIMARY KEY,
    -- Job B: the routing decision (written once per reference_id)
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
    processed_at                 timestamp,
    -- Job B: ledger provenance. schema_version lets consumers/migrations tell which
    -- writer shape produced the row; ingested_at is when Job B committed it.
    schema_version               int,
    ingested_at                  timestamp,
    -- Job B: fraud/risk flags (null for clean payments; comma-separated for flagged ones)
    risk_flags                   text,       -- e.g. HIGH_VALUE,SUSPICIOUS_AMOUNT
    -- Job C: settlement tracking (null until Job C sees the routed transfer)
    settlement_status            text,       -- AWAITING_SETTLEMENT | SETTLED | SETTLED_LATE | FAILED | STUCK
    settled_at                   timestamp,
    turnaround_ms                bigint,     -- settled_at - created_at
    settlement_failure_reason    text,
    settlement_updated_at        timestamp,
    last_webhook_event_type      text,       -- payment.paid | payment.failed
    last_webhook_at              timestamp
);

-- Routed transfers per (rail, sending institution), newest settlement deadline first:
-- "what is BPI waiting on in PESONet?"  Job B creates the row, Job C fills in the status.
CREATE TABLE IF NOT EXISTS payment_pipeline.settlement_monitoring_by_institution (
    rail                         text,
    source_institution_code      text,
    settlement_due               timestamp,
    reference_id                 text,
    destination_institution_code text,
    amount                       bigint,
    batch_window                 timestamp,
    created_at                   timestamp,
    settlement_status            text,       -- written by Job C
    settled_at                   timestamp,  -- written by Job C
    turnaround_ms                bigint,     -- written by Job C
    PRIMARY KEY ((rail, source_institution_code), settlement_due, reference_id)
) WITH CLUSTERING ORDER BY (settlement_due DESC, reference_id ASC);

-- Written by Spark Job B. Messages that couldn't be processed as new intents, kept by Kafka
-- position so nothing is silently dropped. reason: UNPARSEABLE_OR_MISSING_REFERENCE_ID |
-- DUPLICATE_REFERENCE_ID
CREATE TABLE IF NOT EXISTS payment_pipeline.invalid_intent_events (
    kafka_topic     text,
    kafka_partition int,
    kafka_offset    bigint,
    raw_value       text,
    reason          text,
    received_at     timestamp,
    PRIMARY KEY ((kafka_topic), kafka_partition, kafka_offset)
);

-- Written by Spark Job C. Transfers that went STUCK, FAILED or SETTLED_LATE, newest first.
CREATE TABLE IF NOT EXISTS payment_pipeline.settlement_alerts_by_rail (
    rail                         text,
    status_changed_at            timestamp,
    reference_id                 text,
    alert_type                   text,       -- STUCK | FAILED | SETTLED_LATE
    source_institution_code      text,
    destination_institution_code text,
    amount                       bigint,
    created_at                   timestamp,
    settlement_due               timestamp,
    failure_reason               text,
    risk_flags                   text,       -- carried from Job B, e.g. HIGH_VALUE,RAPID_REPEAT
    PRIMARY KEY ((rail), status_changed_at, reference_id, alert_type)
) WITH CLUSTERING ORDER BY (status_changed_at DESC, reference_id ASC, alert_type ASC);

-- Written by Spark Job C. One row per unique webhook event (duplicates removed in Spark).
CREATE TABLE IF NOT EXISTS payment_pipeline.webhook_events_by_reference (
    reference_id  text,
    event_id      text,
    event_type    text,       -- payment.paid | payment.failed
    payment_id    text,
    amount        bigint,
    event_created_at timestamp,
    received_at   timestamp,
    PRIMARY KEY ((reference_id), event_id)
);

-- Upgrade path for stacks created before Job C: add the new columns to existing tables.
-- (IF NOT EXISTS on ALTER ... ADD needs Cassandra 4.1+; this stack runs 5.0.)
ALTER TABLE payment_pipeline.transaction_lifecycle_by_reference ADD IF NOT EXISTS settlement_status text;
ALTER TABLE payment_pipeline.transaction_lifecycle_by_reference ADD IF NOT EXISTS settled_at timestamp;
ALTER TABLE payment_pipeline.transaction_lifecycle_by_reference ADD IF NOT EXISTS turnaround_ms bigint;
ALTER TABLE payment_pipeline.transaction_lifecycle_by_reference ADD IF NOT EXISTS settlement_failure_reason text;
ALTER TABLE payment_pipeline.transaction_lifecycle_by_reference ADD IF NOT EXISTS settlement_updated_at timestamp;
ALTER TABLE payment_pipeline.transaction_lifecycle_by_reference ADD IF NOT EXISTS last_webhook_event_type text;
ALTER TABLE payment_pipeline.transaction_lifecycle_by_reference ADD IF NOT EXISTS last_webhook_at timestamp;
ALTER TABLE payment_pipeline.settlement_monitoring_by_institution ADD IF NOT EXISTS settled_at timestamp;
ALTER TABLE payment_pipeline.settlement_monitoring_by_institution ADD IF NOT EXISTS turnaround_ms bigint;
-- Addition 2 (fraud/risk): risk_flags column for Job B
ALTER TABLE payment_pipeline.transaction_lifecycle_by_reference ADD IF NOT EXISTS risk_flags text;
-- risk_flags carried through Job C onto the settlement alerts table.
ALTER TABLE payment_pipeline.settlement_alerts_by_rail ADD IF NOT EXISTS risk_flags text;
-- Ledger provenance columns (Job B).
ALTER TABLE payment_pipeline.transaction_lifecycle_by_reference ADD IF NOT EXISTS schema_version int;
ALTER TABLE payment_pipeline.transaction_lifecycle_by_reference ADD IF NOT EXISTS ingested_at timestamp;

-- NEW: Append-only Event-Sourced Ledger
-- This table records every state transition as an immutable event instead of mutating the current state.
CREATE TABLE IF NOT EXISTS payment_pipeline.ledger_events (
    reference_id text,
    event_id text,
    event_type text,
    event_timestamp timestamp,
    payload_json text,
    PRIMARY KEY ((reference_id), event_id)
);
