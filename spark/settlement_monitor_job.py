# Spark Job C: settlement & SLA monitoring.
#
#   rail-routing-events  (from Job B) ─┐
#                                      ├─► per-transfer state machine ─► Cassandra
#   settlement-events    (from rail)  ─┘     (watermarked, event-time timeouts)
#
#   webhook-events (PayMongo-style) ─► dropDuplicatesWithinWatermark ─► Cassandra
#
# Run from WSL (needs pandas + pyarrow in the Python that Spark uses, see SPARK_GUIDE):
#   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 settlement_monitor_job.py
#
# Why a state machine and not a plain stream-stream join: every transfer has its own
# deadline (InstaPay: seconds; PESONet: the next clearing window, up to ~3 days away
# over a weekend). A watermarked outer join only takes one fixed time bound, so a bound
# wide enough for PESONet would delay InstaPay stuck-detection by days. Instead, both
# streams are unioned, keyed by reference_id, and joined inside applyInPandasWithState,
# which sets a per-transfer event-time timeout at settlement_due + grace.
#
# Statuses written to settlement_status:
#   AWAITING_SETTLEMENT  routed, no confirmation yet
#   SETTLED              confirmed on or before settlement_due + grace
#   SETTLED_LATE         confirmed after the deadline (including after being flagged STUCK)
#   FAILED               the rail reported a failure
#   STUCK                no confirmation by settlement_due + grace (watermark passed it)
# Make PySpark use a modern cloudpickle before anything serializes a Python
# function (this job's state machine runs inside applyInPandasWithState). The fix
# lives in one shared module instead of a per-job, hardcoded-path monkeypatch.
import _cloudpickle_compat  # noqa: F401  (import for its import-time side effect)

import json
import math
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, LongType, StringType, StructField, StructType,
                               TimestampType)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", os.path.expanduser("~/checkpoints/settlement_monitor"))
METRICS_FILE = os.getenv("METRICS_FILE", "")
SHUFFLE_PARTITIONS = os.getenv("SHUFFLE_PARTITIONS", "8")

KEYSPACE = "payment_pipeline"
ROUTING_TOPIC = "rail-routing-events"
SETTLEMENT_TOPIC = "settlement-events"
WEBHOOK_TOPIC = "webhook-events"
# Malformed inputs (unparseable JSON / missing required fields) go here instead of
# being silently filtered out, mirroring Job B's payment-dlq.
DLQ_TOPIC = os.getenv("SETTLEMENT_DLQ_TOPIC", "settlement-dlq")

# How far behind the newest event time Spark waits for stragglers before acting.
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "2 minutes")
# Extra time after settlement_due before a transfer counts as STUCK (team-chosen demo values).
INSTAPAY_GRACE_SECONDS = int(os.getenv("INSTAPAY_GRACE_SECONDS", "30"))
PESONET_GRACE_SECONDS = int(os.getenv("PESONET_GRACE_SECONDS", "3600"))
# How long (event time) to remember a finished or stuck transfer, to recognise
# repeated or late confirmations. Also how long an unmatched settlement waits.
STATE_RETENTION_SECONDS = int(os.getenv("STATE_RETENTION_SECONDS", str(24 * 3600)))
# PayMongo retries webhook deliveries; repeats of the same event id within this
# window are dropped.
WEBHOOK_DEDUP_WINDOW = os.getenv("WEBHOOK_DEDUP_WINDOW", "1 hour")

ALERT_STATUSES = ("STUCK", "FAILED", "SETTLED_LATE")

from settlement_udfs import track_settlement, OUTPUT_FIELDS, STATE_FIELDS

# Input schemas are defined inside the parse_* functions (local to each) rather than
# as module globals, to keep what gets captured by closures small and obvious.

# Unified event fed to the state machine (one schema for both input streams).
EVENT_COLUMNS = ["reference_id", "kind", "event_time", "event_ms", "rail",
                 "source_institution_code", "destination_institution_code", "amount",
                 "created_ms", "due_ms", "settle_status", "failure_reason", "risk_flags"]

STATE_SCHEMA = "routed STRING, rail STRING, src STRING, dst STRING, amount LONG, created_ms LONG, due_ms LONG, status STRING, settled_ms LONG, failure STRING, changed_ms LONG, pend_status STRING, pend_ms LONG, pend_failure STRING, risk_flags STRING"
STATE_FIELDS = ["routed", "rail", "src", "dst", "amount", "created_ms", "due_ms", "status", "settled_ms", "failure", "changed_ms", "pend_status", "pend_ms", "pend_failure", "risk_flags"]

OUTPUT_SCHEMA = "reference_id STRING, rail STRING, source_institution_code STRING, destination_institution_code STRING, amount LONG, created_ms LONG, due_ms LONG, settlement_status STRING, settled_ms LONG, turnaround_ms LONG, failure_reason STRING, status_changed_ms LONG, risk_flags STRING"
OUTPUT_FIELDS = ["reference_id", "rail", "source_institution_code", "destination_institution_code", "amount", "created_ms", "due_ms", "settlement_status", "settled_ms", "turnaround_ms", "failure_reason", "status_changed_ms", "risk_flags"]


# UDFs moved to settlement_udfs.py
# ---- Stream construction (takes DataFrames with a string `value` column) ------------
def _ms(col):
    return F.unix_millis(col)


def parse_routing(raw):
    ROUTING_SCHEMA = (StructType()
        .add("reference_id", StringType()).add("status", StringType()).add("rail", StringType())
        .add("source_institution_code", StringType()).add("destination_institution_code", StringType())
        .add("amount", LongType()).add("created_at", TimestampType())
        .add("settlement_due", TimestampType()).add("risk_flags", StringType()))
    r = raw.select(F.from_json(F.col("value").cast("string"), ROUTING_SCHEMA).alias("r")).select("r.*")
    r = r.filter((F.col("status") == "ROUTED") & F.col("reference_id").isNotNull()
                 & F.col("created_at").isNotNull() & F.col("settlement_due").isNotNull())
    return r.select(
        "reference_id", F.lit("ROUTED").alias("kind"),
        F.col("created_at").alias("event_time"), _ms("created_at").alias("event_ms"),
        "rail", "source_institution_code", "destination_institution_code", "amount",
        _ms("created_at").alias("created_ms"), _ms("settlement_due").alias("due_ms"),
        F.lit(None).cast("string").alias("settle_status"),
        F.lit(None).cast("string").alias("failure_reason"),
        "risk_flags")


def parse_settlements(raw):
    SETTLEMENT_SCHEMA = (StructType()
        .add("settlement_id", StringType()).add("reference_id", StringType())
        .add("rail", StringType()).add("status", StringType())          # SETTLED | FAILED
        .add("failure_reason", StringType()).add("settled_at", TimestampType()))
    s = raw.select(F.from_json(F.col("value").cast("string"), SETTLEMENT_SCHEMA).alias("s")).select("s.*")
    s = s.filter(F.col("reference_id").isNotNull() & F.col("settled_at").isNotNull()
                 & F.col("status").isin("SETTLED", "FAILED"))
    return s.select(
        "reference_id", F.lit("SETTLEMENT").alias("kind"),
        F.col("settled_at").alias("event_time"), _ms("settled_at").alias("event_ms"),
        "rail", F.lit(None).cast("string").alias("source_institution_code"),
        F.lit(None).cast("string").alias("destination_institution_code"),
        F.lit(None).cast("long").alias("amount"),
        F.lit(None).cast("long").alias("created_ms"), F.lit(None).cast("long").alias("due_ms"),
        F.col("status").alias("settle_status"), "failure_reason",
        F.lit(None).cast("string").alias("risk_flags"))


def settlement_status_stream(routing_events, settlement_events):
    events = routing_events.unionByName(settlement_events).withWatermark("event_time", WATERMARK_DELAY)
    return (events.groupBy("reference_id")
            .applyInPandasWithState(track_settlement, OUTPUT_SCHEMA, STATE_SCHEMA,
                                    "update", "EventTimeTimeout"))


def parse_webhooks(raw):
    _PAYMENT = (StructType()
        .add("id", StringType())
        .add("attributes", StructType()
             .add("amount", LongType()).add("currency", StringType()).add("status", StringType())
             .add("metadata", StructType().add("reference_id", StringType()))))
    WEBHOOK_SCHEMA = StructType().add("data", StructType()
        .add("id", StringType())
        .add("type", StringType())
        .add("attributes", StructType()
             .add("type", StringType())
             .add("livemode", BooleanType())
             .add("data", _PAYMENT)
             .add("created_at", LongType())))        # unix seconds, as in PayMongo
    w = raw.select(F.from_json(F.col("value").cast("string"), WEBHOOK_SCHEMA).alias("w"))
    return (w.select(
        F.col("w.data.id").alias("event_id"),
        F.col("w.data.attributes.type").alias("event_type"),
        F.col("w.data.attributes.data.id").alias("payment_id"),
        F.col("w.data.attributes.data.attributes.amount").alias("amount"),
        F.col("w.data.attributes.data.attributes.metadata.reference_id").alias("reference_id"),
        F.timestamp_seconds(F.col("w.data.attributes.created_at")).alias("event_created_at"))
        .filter(F.col("event_id").isNotNull() & F.col("reference_id").isNotNull()
                & F.col("event_created_at").isNotNull()))


def dedupe_webhooks(webhooks):
    """PayMongo retries a delivery until it is acknowledged, so the same event id can
    arrive several times. Keep the first; Spark forgets an id once the watermark has
    moved WEBHOOK_DEDUP_WINDOW past it."""
    return (webhooks.withWatermark("event_created_at", WEBHOOK_DEDUP_WINDOW)
            .dropDuplicatesWithinWatermark(["event_id"]))


# ---- Dead-letter queue -----------------------------------------------------------
# Each input stream filters out messages it can't use (bad JSON, missing required
# fields). These guards capture exactly those messages and publish the raw payload to
# the DLQ so nothing is dropped silently. They are additive, separate streaming queries
# that never touch the working state-machine path. "Valid but not applicable" messages
# (e.g. a REJECTED routing event, which the state machine intentionally ignores) are NOT
# dead-lettered — only genuinely malformed ones are.

def _dlq_value(source_topic, reason, raw_col):
    return F.to_json(F.struct(
        F.lit(source_topic).alias("source_topic"),
        F.lit(reason).alias("failure_reason"),
        F.current_timestamp().cast("string").alias("failed_at"),
        raw_col.alias("original_payload")))


def routing_dlq(raw):
    schema = (StructType().add("reference_id", StringType()).add("status", StringType())
              .add("created_at", TimestampType()).add("settlement_due", TimestampType()))
    rv = F.col("value").cast("string")
    j = F.from_json(rv, schema)
    bad = raw.select(rv.alias("raw_value"), j.alias("j")).filter(
        F.col("j").isNull()
        | F.col("j.reference_id").isNull()
        # A ROUTED event that can't be tracked (no timestamps) is malformed; other
        # statuses legitimately omit them, so don't dead-letter those.
        | ((F.col("j.status") == "ROUTED")
           & (F.col("j.created_at").isNull() | F.col("j.settlement_due").isNull())))
    return bad.select(_dlq_value(ROUTING_TOPIC, "MALFORMED_ROUTING_EVENT",
                                 F.col("raw_value")).alias("value"))


def settlement_dlq(raw):
    schema = (StructType().add("reference_id", StringType()).add("status", StringType())
              .add("settled_at", TimestampType()))
    rv = F.col("value").cast("string")
    j = F.from_json(rv, schema)
    bad = raw.select(rv.alias("raw_value"), j.alias("j")).filter(
        F.col("j").isNull()
        | F.col("j.reference_id").isNull()
        | F.col("j.settled_at").isNull()
        | ~F.col("j.status").isin("SETTLED", "FAILED"))
    return bad.select(_dlq_value(SETTLEMENT_TOPIC, "MALFORMED_SETTLEMENT_EVENT",
                                 F.col("raw_value")).alias("value"))


def webhook_dlq(raw):
    schema = StructType().add("data", StructType()
        .add("id", StringType())
        .add("attributes", StructType()
             .add("created_at", LongType())
             .add("data", StructType().add("attributes", StructType()
                  .add("metadata", StructType().add("reference_id", StringType()))))))
    rv = F.col("value").cast("string")
    j = F.from_json(rv, schema)
    bad = raw.select(rv.alias("raw_value"), j.alias("j")).filter(
        F.col("j").isNull()
        | F.col("j.data.id").isNull()
        | F.col("j.data.attributes.created_at").isNull()
        | F.col("j.data.attributes.data.attributes.metadata.reference_id").isNull())
    return bad.select(_dlq_value(WEBHOOK_TOPIC, "MALFORMED_WEBHOOK_EVENT",
                                 F.col("raw_value")).alias("value"))


# ---- Sinks -----------------------------------------------------------------------
def write_cassandra(df, table):
    (df.write.format("org.apache.spark.sql.cassandra")
       .options(keyspace=KEYSPACE, table=table).mode("append").save())


def with_timestamps(df):
    ts = lambda c: F.when(F.col(c).isNotNull(), F.timestamp_millis(F.col(c)))
    return (df.withColumn("created_at", ts("created_ms"))
              .withColumn("settlement_due", ts("due_ms"))
              .withColumn("settled_at", ts("settled_ms"))
              .withColumn("status_changed_at", ts("status_changed_ms")))


def write_status_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    df = with_timestamps(batch_df).persist()
    try:
        write_cassandra(df.select(
            "reference_id", "settlement_status", "settled_at", "turnaround_ms",
            F.col("failure_reason").alias("settlement_failure_reason"),
            F.col("status_changed_at").alias("settlement_updated_at")),
            "transaction_lifecycle_by_reference")
        write_cassandra(df.select(
            "rail", "source_institution_code", "settlement_due", "reference_id",
            "settlement_status", "settled_at", "turnaround_ms"),
            "settlement_monitoring_by_institution")
        write_cassandra(df.filter(F.col("settlement_status").isin(*ALERT_STATUSES)).select(
            "rail", "status_changed_at", "reference_id",
            F.col("settlement_status").alias("alert_type"),
            "source_institution_code", "destination_institution_code", "amount",
            "created_at", "settlement_due", "failure_reason", "risk_flags"),
            "settlement_alerts_by_rail")

        counts = df.groupBy("settlement_status").count().orderBy("settlement_status").collect()
        tat = (df.filter(F.col("turnaround_ms").isNotNull()).groupBy("rail")
                 .agg(F.round(F.avg("turnaround_ms") / 1000, 1).alias("avg_s")).orderBy("rail").collect())
        msg = ", ".join(f"{r.settlement_status}: {r['count']}" for r in counts)
        if tat:
            msg += " | avg turnaround " + ", ".join(f"{r.rail} {r.avg_s}s" for r in tat)
        print(f"[settlement batch {batch_id}] {msg}", flush=True)
    finally:
        df.unpersist()


def write_webhook_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    df = batch_df.withColumn("received_at", F.current_timestamp()).persist()
    try:
        write_cassandra(df.select("reference_id", "event_id", "event_type", "payment_id",
                                  "amount", "event_created_at", "received_at"),
                        "webhook_events_by_reference")
        # Latest webhook per transfer onto the ledger row (one row per key per write).
        latest = (df.groupBy("reference_id")
                    .agg(F.max_by("event_type", "event_created_at").alias("last_webhook_event_type"),
                         F.max("event_created_at").alias("last_webhook_at")))
        write_cassandra(latest, "transaction_lifecycle_by_reference")
        print(f"[webhook batch {batch_id}] {df.count()} unique deliveries", flush=True)
    finally:
        df.unpersist()


def run_with_metrics(queries, metrics_file):
    if not metrics_file:
        queries[0].awaitTermination()
        return
    written = set()
    with open(metrics_file, "a") as f:
        while all(q.isActive for q in queries):
            for q in queries:
                for p in q.recentProgress:
                    k = (p["name"], p["batchId"])
                    if k not in written:
                        written.add(k)
                        f.write(json.dumps(p) + "\n")
            f.flush()
            time.sleep(2)
    queries[0].sparkSession.streams.awaitAnyTermination()


def kafka_stream(spark, topic):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load())


def main():
    spark = (SparkSession.builder.appName("SettlementMonitor")
             .config("spark.cassandra.connection.host", CASSANDRA_HOST)
             .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
             .config("spark.sql.session.timeZone", "Asia/Manila")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    status = settlement_status_stream(parse_routing(kafka_stream(spark, ROUTING_TOPIC)),
                                      parse_settlements(kafka_stream(spark, SETTLEMENT_TOPIC)))
    q1 = (status.writeStream.queryName("settlement_tracking")
          .outputMode("update")          # required by applyInPandasWithState(..., "update")
          .foreachBatch(write_status_batch)
          .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "tracking"))
          .start())

    webhooks = dedupe_webhooks(parse_webhooks(kafka_stream(spark, WEBHOOK_TOPIC)))
    q2 = (webhooks.writeStream.queryName("webhook_dedup")
          .foreachBatch(write_webhook_batch)
          .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "webhooks"))
          .start())

    # Dead-letter malformed inputs on each source topic (additive; independent of the
    # working path above). A bug here can't corrupt the ledger — the query just fails
    # on its own and the state-machine queries keep running.
    dlq_specs = [("routing_dlq", routing_dlq, ROUTING_TOPIC),
                 ("settlement_dlq", settlement_dlq, SETTLEMENT_TOPIC),
                 ("webhook_dlq", webhook_dlq, WEBHOOK_TOPIC)]
    dlq_queries = []
    for name, fn, topic in dlq_specs:
        q = (fn(kafka_stream(spark, topic)).writeStream.queryName(name)
             .format("kafka")
             .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
             .option("topic", DLQ_TOPIC)
             .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, name))
             .start())
        dlq_queries.append(q)

    run_with_metrics([q1, q2, *dlq_queries], METRICS_FILE)


if __name__ == "__main__":
    main()
