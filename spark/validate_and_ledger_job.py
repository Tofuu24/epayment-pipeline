# Spark Job B: validate payment intents against the live institution registry,
# route each one to InstaPay or PESONet, assign PESONet clearing windows, write
# the ledger to Cassandra, and publish each decision to rail-routing-events for Job C.
#
# Run from WSL, with the Compose stack up and Job A running in another tab:
#   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 validate_and_ledger_job.py
#
# Input (topic payment-intent-events), one JSON object per message:
#   {"reference_id": "PAY-001", "source_institution_code": "BANK_BPI",
#    "destination_institution_code": "EMI_GCASH", "amount": 150000, "currency": "PHP",
#    "created_at": "2026-09-25T09:30:00+08:00", "metadata": {"channel": "mobile"}}
# amount is in centavos (PayMongo convention): 150000 = PHP 1,500.00.
# The producer does NOT choose the rail; this job does.
#
# Each reference_id is decided exactly once:
#   - the first message for a reference_id (lowest Kafka position) is routed;
#   - later messages with the same reference_id go to invalid_intent_events as
#     DUPLICATE_REFERENCE_ID (the ledger row is never overwritten);
#   - re-reading a message that was already decided (same Kafka position, e.g. after a
#     checkpoint reset) is a replay and is skipped, so the original decision stands.
import json
import os
import time

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructType

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", os.path.expanduser("~/checkpoints/validate_ledger"))
METRICS_FILE = os.getenv("METRICS_FILE", "")          # optional: JSON-lines of query progress
SHUFFLE_PARTITIONS = os.getenv("SHUFFLE_PARTITIONS", "8")

KEYSPACE = "payment_pipeline"
INTENT_TOPIC = "payment-intent-events"
ROUTING_TOPIC = "rail-routing-events"

# ---- Routing rules (from the proposal; change here, nowhere else) -------------
INSTAPAY_CAP_CENTAVOS = 5_000_000      # PHP 50,000.00 per transaction, inclusive
PESONET_WINDOW_HOURS = [10, 13, 16]    # daily clearing windows, Asia/Manila time
# Team-chosen demo target for "InstaPay should settle within N seconds".
# This is NOT a BSP figure; it only drives the settlement_due column.
INSTAPAY_SLA_SECONDS = int(os.getenv("INSTAPAY_SLA_SECONDS", "30"))
# If InstaPay can't be used for a small payment (one side isn't an InstaPay
# participant, e.g. Landbank here), send it via PESONet instead of rejecting it.
ALLOW_PESONET_FALLBACK = os.getenv("ALLOW_PESONET_FALLBACK", "true").lower() == "true"

INTENT_SCHEMA = (StructType()
    .add("reference_id", StringType())
    .add("source_institution_code", StringType())
    .add("destination_institution_code", StringType())
    .add("amount", LongType())
    .add("currency", StringType())
    .add("created_at", StringType())
    .add("metadata", StringType()))    # nested object is kept as its raw JSON text

ROUTING_EVENT_FIELDS = [
    "reference_id", "status", "rail", "routing_reason", "rejection_reason",
    "source_institution_code", "destination_institution_code", "amount", "currency",
    "created_at", "batch_window", "settlement_due", "decided_at"]


def _at_hour(date_sql, hour):
    """SQL for <date> at <hour>:00:00 in the session time zone (Asia/Manila)."""
    return f"make_timestamp(year({date_sql}), month({date_sql}), day({date_sql}), {hour}, 0, 0)"


def _next_window_sql(ts):
    """Next PESONet window strictly after ts (a transfer arriving exactly at 10:00
    has missed the 10:00 batch), ignoring weekends."""
    d = f"to_date({ts})"
    cases = " ".join(f"WHEN {ts} < {_at_hour(d, h)} THEN {_at_hour(d, h)}"
                     for h in PESONET_WINDOW_HOURS)
    return f"CASE {cases} ELSE {_at_hour(f'date_add({d}, 1)', PESONET_WINDOW_HOURS[0])} END"


def _skip_weekend_sql(w):
    """PESONet clears on banking days only: a Saturday or Sunday window moves to
    Monday's first window. (Philippine public holidays are not modeled.)"""
    first = PESONET_WINDOW_HOURS[0]
    d = f"to_date({w})"
    return (f"CASE dayofweek({w}) "            # 1 = Sunday, 7 = Saturday
            f"WHEN 7 THEN {_at_hour(f'date_add({d}, 2)', first)} "
            f"WHEN 1 THEN {_at_hour(f'date_add({d}, 1)', first)} "
            f"ELSE {w} END")


def parse_intents(raw):
    """raw: DataFrame with kafka_topic, kafka_partition, kafka_offset, raw_value."""
    return (raw.withColumn("i", F.from_json("raw_value", INTENT_SCHEMA))
               .select("kafka_topic", "kafka_partition", "kafka_offset", "raw_value", "i.*"))


def has_reference_id():
    return F.col("reference_id").isNotNull() & (F.trim(F.col("reference_id")) != "")


def classify_intents(keyed, existing):
    """Split keyed intents into (new, duplicates). No I/O, so it can be unit-tested.

    keyed:    parsed intents that have a reference_id (one micro-batch)
    existing: ledger rows (reference_id, kafka_partition, kafka_offset) for any of
              those reference_ids that were already decided

    new:        first occurrence of a reference_id never decided before
    duplicates: a later occurrence, in this batch or after an earlier decision
    Replays (same reference_id AND same Kafka position as the ledger row) are in neither.
    """
    w = Window.partitionBy("reference_id").orderBy("kafka_partition", "kafka_offset")
    ranked = keyed.withColumn("_rn", F.row_number().over(w))
    in_batch_dups = ranked.filter("_rn > 1").drop("_rn")
    firsts = ranked.filter("_rn = 1").drop("_rn")

    ex = existing.select("reference_id",
                         F.col("kafka_partition").alias("_ex_partition"),
                         F.col("kafka_offset").alias("_ex_offset"))
    j = firsts.join(ex, "reference_id", "left")
    never_seen = F.col("_ex_offset").isNull()
    replay = (F.col("_ex_partition") == F.col("kafka_partition")) & \
             (F.col("_ex_offset") == F.col("kafka_offset"))
    new = j.filter(never_seen).drop("_ex_partition", "_ex_offset")
    earlier_dups = j.filter(~never_seen & ~replay).drop("_ex_partition", "_ex_offset")
    return new, in_batch_dups.unionByName(earlier_dups)


def route(intents, registry):
    """Pure routing logic: intents (parsed, one row per reference_id) + registry
    snapshot -> one decision row per intent. No I/O, so it can be unit-tested."""
    reg = registry.select("institution_code", "active", "rail_eligibility")
    src = reg.select(F.col("institution_code").alias("src_code"),
                     F.col("active").alias("src_active"),
                     F.col("rail_eligibility").alias("src_rails"))
    dst = reg.select(F.col("institution_code").alias("dst_code"),
                     F.col("active").alias("dst_active"),
                     F.col("rail_eligibility").alias("dst_rails"))

    j = (intents
         .withColumn("created_ts", F.col("created_at").cast("timestamp"))
         .join(F.broadcast(src), F.col("source_institution_code") == F.col("src_code"), "left")
         .join(F.broadcast(dst), F.col("destination_institution_code") == F.col("dst_code"), "left"))

    def supports(rails, rail):
        return F.coalesce(F.array_contains(F.col(rails), rail), F.lit(False))

    amount = F.col("amount")
    within_cap = amount <= INSTAPAY_CAP_CENTAVOS
    both_instapay = supports("src_rails", "INSTAPAY") & supports("dst_rails", "INSTAPAY")
    both_pesonet = supports("src_rails", "PESONET") & supports("dst_rails", "PESONET")
    fallback = within_cap & both_pesonet & F.lit(ALLOW_PESONET_FALLBACK)

    candidate_rail = (F.when(within_cap & both_instapay, "INSTAPAY")
                       .when(~within_cap & both_pesonet, "PESONET")
                       .when(fallback, "PESONET"))
    candidate_reason = (F.when(within_cap & both_instapay, "WITHIN_INSTAPAY_CAP")
                         .when(~within_cap & both_pesonet, "ABOVE_INSTAPAY_CAP")
                         .when(fallback, "INSTAPAY_NOT_SUPPORTED_BY_PARTICIPANT"))

    # First matching rule wins, so the order here is the order of checks.
    rejection = (
        F.when(F.col("source_institution_code").isNull() | F.col("destination_institution_code").isNull()
               | amount.isNull() | F.col("currency").isNull() | F.col("created_ts").isNull(),
               "INVALID_PAYLOAD")
         .when(amount <= 0, "INVALID_AMOUNT")
         .when(F.col("currency") != "PHP", "UNSUPPORTED_CURRENCY")
         .when(F.col("src_code").isNull(), "UNKNOWN_SOURCE")
         .when(~F.coalesce(F.col("src_active"), F.lit(False)), "INACTIVE_SOURCE")
         .when(F.col("dst_code").isNull(), "UNKNOWN_DESTINATION")
         .when(~F.coalesce(F.col("dst_active"), F.lit(False)), "INACTIVE_DESTINATION")
         .when(candidate_rail.isNull(), "NO_ELIGIBLE_RAIL"))

    ok = F.col("rejection_reason").isNull()
    return (j
        .withColumn("rejection_reason", rejection)
        .withColumn("status", F.when(ok, "ROUTED").otherwise("REJECTED"))
        .withColumn("rail", F.when(ok, candidate_rail))
        .withColumn("routing_reason", F.when(ok, candidate_reason))
        .withColumn("next_window", F.expr(_next_window_sql("created_ts")))
        .withColumn("batch_window", F.when(F.col("rail") == "PESONET",
                                           F.expr(_skip_weekend_sql("next_window"))))
        .withColumn("settlement_due",
                    F.when(F.col("rail") == "INSTAPAY",
                           F.expr(f"created_ts + INTERVAL {INSTAPAY_SLA_SECONDS} SECONDS"))
                     .otherwise(F.col("batch_window")))
        .drop("next_window"))


def routing_events(decided):
    """Kafka records (key, value) for rail-routing-events, one per decision.
    Timestamps are ISO 8601 with the Manila offset, e.g. 2026-09-25T10:00:00.000+08:00."""
    rec = decided.select(*[F.col("created_ts").alias("created_at") if c == "created_at"
                           else F.col("processed_at").alias("decided_at") if c == "decided_at"
                           else F.col(c) for c in ROUTING_EVENT_FIELDS])
    return rec.select(F.col("reference_id").alias("key"),
                      F.to_json(F.struct(*ROUTING_EVENT_FIELDS)).alias("value"))


def write_cassandra(df, table):
    (df.write.format("org.apache.spark.sql.cassandra")
       .options(keyspace=KEYSPACE, table=table)
       .mode("append").save())


def read_cassandra(spark, table):
    return (spark.read.format("org.apache.spark.sql.cassandra")
            .options(keyspace=KEYSPACE, table=table).load())


def publish_kafka(df, topic):
    (df.write.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
       .option("topic", topic).save())


def invalid_rows(df, reason, now):
    return df.select("kafka_topic", "kafka_partition", "kafka_offset", "raw_value",
                     F.lit(reason).alias("reason"), now.alias("received_at"))


def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    batch_df.persist()
    try:
        spark = batch_df.sparkSession
        now = F.current_timestamp()

        # Messages we can't key (not JSON, or no reference_id) are kept, not dropped.
        unkeyed = batch_df.filter(~has_reference_id())
        keyed = batch_df.filter(has_reference_id())

        # Ledger rows already written for these reference_ids. With the connector's
        # CassandraSparkExtensions this join becomes a direct per-key lookup instead of
        # a full table scan.
        ledger = (read_cassandra(spark, "transaction_lifecycle_by_reference")
                  .select("reference_id", "kafka_partition", "kafka_offset"))
        existing = ledger.join(keyed.select("reference_id").distinct(), "reference_id")
        new, duplicates = classify_intents(keyed, existing)

        write_cassandra(invalid_rows(unkeyed, "UNPARSEABLE_OR_MISSING_REFERENCE_ID", now)
                        .unionByName(invalid_rows(duplicates, "DUPLICATE_REFERENCE_ID", now)),
                        "invalid_intent_events")

        # Fresh registry snapshot every micro-batch, so a MongoDB change (via Job A)
        # affects validation immediately, without restarting this job.
        registry = read_cassandra(spark, "institution_registry")

        decided = route(new, registry).withColumn("processed_at", now).persist()

        # Order matters: the ledger row is written LAST. If the job crashes part-way,
        # the batch is re-run; no ledger row means the intent is still "new", so it is
        # decided and published again (Job C ignores a repeated routing event).
        write_cassandra(decided.filter(F.col("status") == "ROUTED").select(
            "rail", "source_institution_code", "settlement_due", "reference_id",
            "destination_institution_code", "amount", "batch_window",
            F.col("created_ts").alias("created_at")),
            "settlement_monitoring_by_institution")

        publish_kafka(routing_events(decided), ROUTING_TOPIC)

        write_cassandra(decided.select(
            "reference_id", "source_institution_code", "destination_institution_code",
            "amount", "currency", F.col("created_ts").alias("created_at"),
            "status", "rail", "routing_reason", "rejection_reason",
            "batch_window", "settlement_due", F.col("metadata").alias("metadata_json"),
            "kafka_partition", "kafka_offset", "processed_at"),
            "transaction_lifecycle_by_reference")

        summary = decided.groupBy("status", F.coalesce("rail", "rejection_reason").alias("detail")) \
                         .count().orderBy("status", "detail").collect()
        parts = [f"{r.status}/{r.detail}: {r['count']}" for r in summary]
        n_dup, n_unkeyed = duplicates.count(), unkeyed.count()
        if n_dup:
            parts.append(f"DUPLICATE: {n_dup}")
        if n_unkeyed:
            parts.append(f"UNKEYED: {n_unkeyed}")
        print(f"[batch {batch_id}] " + (", ".join(parts) or "replayed only, nothing new"), flush=True)
        decided.unpersist()
    finally:
        batch_df.unpersist()


def run_with_metrics(query, metrics_file):
    """Wait for the query; if metrics_file is set, append each batch's progress
    (rows/sec, batch duration) as one JSON line for the benchmark scripts."""
    if not metrics_file:
        query.awaitTermination()
        return
    written = set()
    with open(metrics_file, "a") as f:
        while query.isActive:
            for p in query.recentProgress:
                if p["batchId"] not in written:
                    written.add(p["batchId"])
                    f.write(json.dumps(p) + "\n")
            f.flush()
            time.sleep(2)
    query.awaitTermination()


if __name__ == "__main__":
    spark = (SparkSession.builder.appName("ValidateAndLedger")
             .config("spark.cassandra.connection.host", CASSANDRA_HOST)
             # Turns joins on a Cassandra partition key into direct per-key lookups.
             .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
             .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
             # Batch windows are Manila wall-clock times; created_at values without
             # an offset are read as Manila time too.
             .config("spark.sql.session.timeZone", "Asia/Manila")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", INTENT_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
        .select(F.col("topic").alias("kafka_topic"),
                F.col("partition").alias("kafka_partition"),
                F.col("offset").alias("kafka_offset"),
                F.col("value").cast("string").alias("raw_value")))

    query = (parse_intents(raw).writeStream.queryName("validate_and_ledger")
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start())
    run_with_metrics(query, METRICS_FILE)
