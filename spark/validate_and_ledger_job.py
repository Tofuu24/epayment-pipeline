# Spark Job B: validate payment intents against the live institution registry,
# route each one to InstaPay or PESONet, assign PESONet clearing windows, write
# the ledger to Cassandra, and publish each routed transfer to rail-routing-events.
#
# Run from WSL, with the Compose stack up and Job A running in another tab:
#   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 validate_and_ledger_job.py
#
# Input (topic payment-intent-events), one PayMongo-style Payment Intent per message:
#   {"id": "PAY-001", "amount": 150000, "currency": "PHP", "status": "processing",
#    "payment_method_allowed": ["dob"], "created_at": "2026-09-25T09:30:00+08:00",
#    "metadata": {"source_institution_code": "BANK_BPI",
#                 "destination_institution_code": "EMI_GCASH", "channel": "mobile"}}
# amount is in centavos (PayMongo convention): 150000 = PHP 1,500.00.
# Our custom routing fields live inside PayMongo's metadata object, never at the top level.
# The producer does NOT choose the rail; this job does.
#
# Output (topic rail-routing-events), one message per ROUTED intent, keyed by
# reference_id; format in SPARK_GUIDE.md section 5.
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, MapType, StringType, StructType

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", os.path.expanduser("~/checkpoints/validate_ledger"))

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

# Only the fields routing needs. status and payment_method_allowed are part of
# the message (PayMongo shape) but don't affect routing, so they aren't parsed.
INTENT_SCHEMA = (StructType()
    .add("id", StringType())
    .add("amount", LongType())
    .add("currency", StringType())
    .add("created_at", StringType())
    # PayMongo metadata is flat key -> string; non-string values arrive as their JSON text
    .add("metadata", MapType(StringType(), StringType())))

# Timestamps in rail-routing-events: ISO 8601 with offset, e.g. 2026-09-25T10:00:00.000+08:00
ROUTING_JSON_OPTIONS = {"timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
                        "ignoreNullFields": "false"}   # batch_window: null for InstaPay, not missing


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


def write_cassandra(df, table):
    (df.write.format("org.apache.spark.sql.cassandra")
       .options(keyspace=KEYSPACE, table=table)
       .mode("append").save())


def publish_routing_events(routed):
    """One message per routed transfer on rail-routing-events, keyed by reference_id.
    At-least-once: a replayed micro-batch publishes again, so consumers must
    treat a repeated reference_id as a duplicate."""
    event = F.struct(
        "reference_id", "source_institution_code", "destination_institution_code",
        "amount", "currency",
        F.col("rail").alias("rail_selected"), "routing_reason",
        F.lit(INSTAPAY_CAP_CENTAVOS).alias("threshold_applied"),
        F.col("created_ts").alias("created_at"),
        "batch_window", "settlement_due",
        F.col("processed_at").alias("routing_timestamp"))
    (routed.select(F.col("reference_id").alias("key"),
                   F.to_json(event, ROUTING_JSON_OPTIONS).alias("value"))
        .write.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", ROUTING_TOPIC)
        .save())


def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    batch_df.persist()
    try:
        now = F.current_timestamp()
        has_ref = F.col("reference_id").isNotNull() & (F.trim(F.col("reference_id")) != "")

        # Messages we can't key (not JSON, or no id) are kept, not dropped.
        unkeyed = batch_df.filter(~has_ref)
        write_cassandra(unkeyed.select(
            "kafka_topic", "kafka_partition", "kafka_offset", "raw_value",
            F.lit("UNPARSEABLE_OR_MISSING_ID").alias("reason"),
            now.alias("received_at")), "invalid_intent_events")

        # Fresh registry snapshot every micro-batch, so a MongoDB change (via Job A)
        # affects validation immediately, without restarting this job.
        registry = (spark.read.format("org.apache.spark.sql.cassandra")
                    .options(keyspace=KEYSPACE, table="institution_registry").load())

        intents = batch_df.filter(has_ref).dropDuplicates(["reference_id"])
        decided = route(intents, registry).withColumn("processed_at", now).persist()

        # Writes are upserts by primary key, so a replayed micro-batch (e.g. after a
        # crash) rewrites the same rows instead of creating duplicates.
        write_cassandra(decided.select(
            "reference_id", "source_institution_code", "destination_institution_code",
            "amount", "currency", F.col("created_ts").alias("created_at"),
            "status", "rail", "routing_reason", "rejection_reason",
            "batch_window", "settlement_due", F.col("metadata").alias("metadata_json"),
            "kafka_partition", "kafka_offset", "processed_at"),
            "transaction_lifecycle_by_reference")

        routed = decided.filter(F.col("status") == "ROUTED")
        write_cassandra(routed.select(
            "rail", "source_institution_code", "settlement_due", "reference_id",
            "destination_institution_code", "amount", "batch_window",
            F.col("created_ts").alias("created_at"),
            F.lit("AWAITING_SETTLEMENT").alias("settlement_status")),
            "settlement_monitoring_by_institution")

        # Published after the Cassandra writes, so the ledger row already exists
        # by the time anything downstream sees the routing event.
        publish_routing_events(routed)

        summary = decided.groupBy("status", F.coalesce("rail", "rejection_reason").alias("detail")) \
                         .count().orderBy("status", "detail").collect()
        print(f"[batch {batch_id}] " + ", ".join(f"{r.status}/{r.detail}: {r['count']}" for r in summary)
              + (f", UNKEYED: {unkeyed.count()}" if not unkeyed.isEmpty() else ""))
        decided.unpersist()
    finally:
        batch_df.unpersist()


if __name__ == "__main__":
    spark = (SparkSession.builder.appName("ValidateAndLedger")
             .config("spark.cassandra.connection.host", CASSANDRA_HOST)
             # Batch windows are Manila wall-clock times; created_at values without
             # an offset are read as Manila time too.
             .config("spark.sql.session.timeZone", "Asia/Manila")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    intents_stream = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", INTENT_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
        .select(F.col("topic").alias("kafka_topic"),
                F.col("partition").alias("kafka_partition"),
                F.col("offset").alias("kafka_offset"),
                F.col("value").cast("string").alias("raw_value"))
        .withColumn("i", F.from_json("raw_value", INTENT_SCHEMA))
        # Flatten to the column names route() works with; the routing fields
        # come out of metadata, the rest are PayMongo's own top-level fields.
        .select("kafka_topic", "kafka_partition", "kafka_offset", "raw_value",
                F.col("i.id").alias("reference_id"),
                F.col("i.metadata")["source_institution_code"].alias("source_institution_code"),
                F.col("i.metadata")["destination_institution_code"].alias("destination_institution_code"),
                "i.amount", "i.currency", "i.created_at",
                F.to_json("i.metadata").alias("metadata")))

    (intents_stream.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start()
        .awaitTermination())
