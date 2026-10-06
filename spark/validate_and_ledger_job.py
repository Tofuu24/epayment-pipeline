# Spark Job B: validate payment intents against the live institution registry,
# route each one to InstaPay or PESONet, assign PESONet clearing windows, write
# the ledger to Cassandra, and publish each decision to rail-routing-events for Job C.
#
# Run from WSL, with the Compose stack up and Job A running in another tab:
#   spark-submit --jars <jars> validate_and_ledger_job.py
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
#
# NEW in this version:
#   Addition 1: Philippine public holiday calendar (Nager.Date API, fetched once at startup).
#               HOLIDAY_SOURCE env var: "nager" (default) or "none".
#               PESONet batch_window skips both weekends AND holidays.
#   Addition 2: Rule-based fraud/risk check (HIGH_VALUE, RAPID_REPEAT, SUSPICIOUS_AMOUNT,
#               BLOCKED). Blocked payments are rejected and published to payment-dlq.
#               BLOCKED_INSTITUTIONS env var: comma-separated institution codes (default empty).
#   Addition 3: Dead-letter queue. Blocked and unexpected batch errors go to payment-dlq.

import json
import os
import time
import datetime
import urllib.request

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructType, TimestampType

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", os.path.expanduser("~/checkpoints/validate_ledger"))
METRICS_FILE = os.getenv("METRICS_FILE", "")          # optional: JSON-lines of query progress
SHUFFLE_PARTITIONS = os.getenv("SHUFFLE_PARTITIONS", "8")

KEYSPACE = "payment_pipeline"
INTENT_TOPIC = "payment-intent-events"
ROUTING_TOPIC = "rail-routing-events"
DLQ_TOPIC = "payment-dlq"

# ---- Routing rules (from the proposal; change here, nowhere else) -------------
INSTAPAY_CAP_CENTAVOS = 5_000_000      # PHP 50,000.00 per transaction, inclusive
PESONET_WINDOW_HOURS = [10, 13, 16]    # daily clearing windows, Asia/Manila time
# Team-chosen demo target for "InstaPay should settle within N seconds".
# This is NOT a BSP figure; it only drives the settlement_due column.
INSTAPAY_SLA_SECONDS = int(os.getenv("INSTAPAY_SLA_SECONDS", "30"))
# If InstaPay can't be used for a small payment (one side isn't an InstaPay
# participant, e.g. Landbank here), send it via PESONet instead of rejecting it.
ALLOW_PESONET_FALLBACK = os.getenv("ALLOW_PESONET_FALLBACK", "true").lower() == "true"

# ---- Fraud/risk rules --------------------------------------------------------
# PHP 200,000.00 = 20,000,000 centavos
HIGH_VALUE_THRESHOLD_CENTAVOS = 20_000_000
# Payments suspicious close to the InstaPay cap (structuring detection)
# PHP 49,000.01 to PHP 50,000.00 = 4,900,001 to 5,000,000 centavos
SUSPICIOUS_AMOUNT_LOW_CENTAVOS  = 4_900_001
SUSPICIOUS_AMOUNT_HIGH_CENTAVOS = 5_000_000
# Max times the same src->dst pair can appear in one micro-batch before flagging
RAPID_REPEAT_THRESHOLD = 3
# Comma-separated institution codes that are unconditionally blocked (no default)
_blocked_raw = os.getenv("BLOCKED_INSTITUTIONS", "")
BLOCKED_INSTITUTIONS: set = {c.strip() for c in _blocked_raw.split(",") if c.strip()}

# ---- Holiday calendar --------------------------------------------------------
# Fetched once at startup; broadcast to executors inside process_batch.
HOLIDAY_SOURCE = os.getenv("HOLIDAY_SOURCE", "nager").lower()
MANILA_TZ = datetime.timezone(datetime.timedelta(hours=8))


def _fetch_nager_holidays(year: int) -> set:
    """Return a set of datetime.date objects for Philippine public holidays in year."""
    url = f"https://date.nager.at/api/v3/PublicHolidays/{year}/PH"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        return {datetime.date.fromisoformat(entry["date"]) for entry in data}
    except Exception as exc:
        print(f"[holiday-fetch] WARNING: could not fetch holidays for {year}: {exc}. "
              "Falling back to empty set.", flush=True)
        return set()


def _load_holiday_set() -> set:
    """Load Philippine holiday calendar. Never raises — returns empty set on any failure."""
    if HOLIDAY_SOURCE == "none":
        print("[holiday-fetch] HOLIDAY_SOURCE=none, skipping holiday calendar.", flush=True)
        return set()
    today = datetime.date.today()
    holidays: set = set()
    for year in (today.year, today.year + 1):
        holidays |= _fetch_nager_holidays(year)
    print(f"[holiday-fetch] Loaded {len(holidays)} Philippine holiday dates "
          f"for {today.year}/{today.year + 1}.", flush=True)
    return holidays


# Loaded once when the module is imported (i.e. once per Job B process).
_HOLIDAY_SET: set = _load_holiday_set()


# ---- Holiday-aware PESONet window UDF ----------------------------------------

def _next_pesonet_window_py(ts_value, holidays_broadcast):
    """
    Python implementation of the PESONet window logic.
    ts_value: a Python datetime (Manila TZ expected from Spark's session TZ)
    Returns: datetime at the next valid PESONet business window (not weekend, not holiday).

    Algorithm:
      1. Find the next clearing window (10, 13, or 16) strictly after ts_value on its date.
         If past all windows today, move to 10:00 the following calendar day.
      2. Loop: while candidate date is a weekend OR a holiday, advance by one calendar day
         at PESONET_WINDOW_HOURS[0]:00.
      3. Return the final candidate.
    """
    if ts_value is None:
        return None

    holidays = holidays_broadcast  # plain Python set passed via UDF closure

    # Ensure we have a tz-aware Manila datetime
    if ts_value.tzinfo is None:
        ts = ts_value.replace(tzinfo=MANILA_TZ)
    else:
        ts = ts_value.astimezone(MANILA_TZ)

    ts_date = ts.date()

    # Step 1: find the first window strictly after ts on ts_date
    candidate = None
    for h in PESONET_WINDOW_HOURS:
        window_dt = datetime.datetime(ts_date.year, ts_date.month, ts_date.day, h, 0, 0,
                                      tzinfo=MANILA_TZ)
        if ts < window_dt:
            candidate = window_dt
            break

    # Past all windows today → first window tomorrow
    if candidate is None:
        next_day = ts_date + datetime.timedelta(days=1)
        candidate = datetime.datetime(next_day.year, next_day.month, next_day.day,
                                      PESONET_WINDOW_HOURS[0], 0, 0, tzinfo=MANILA_TZ)

    # Step 2: skip weekends (Mon=0 ... Sun=6 in Python's weekday()) and holidays
    while True:
        wd = candidate.weekday()   # 5=Saturday, 6=Sunday
        if wd in (5, 6) or candidate.date() in holidays:
            next_day = candidate.date() + datetime.timedelta(days=1)
            candidate = datetime.datetime(next_day.year, next_day.month, next_day.day,
                                          PESONET_WINDOW_HOURS[0], 0, 0, tzinfo=MANILA_TZ)
        else:
            break

    return candidate


def _make_pesonet_window_udf(holiday_set: set):
    """
    Returns a Spark UDF that accepts a timestamp and returns the next valid
    PESONet business window, skipping weekends and Philippine public holidays.
    The holiday_set is captured as a closure (serialised with the UDF).
    """
    # Freeze the set into a frozenset so it serialises cleanly.
    frozen = frozenset(holiday_set)

    def _udf_fn(ts):
        return _next_pesonet_window_py(ts, frozen)

    return F.udf(_udf_fn, TimestampType())


# ---- SQL helpers (still used for _next_window_sql; _skip_weekend_sql is replaced) ----

def _at_hour(date_sql, hour):
    """SQL for <date> at <hour>:00:00 in the session time zone (Asia/Manila)."""
    return f"make_timestamp(year({date_sql}), month({date_sql}), day({date_sql}), {hour}, 0, 0)"


def _next_window_sql(ts):
    """Next PESONet window strictly after ts (a transfer arriving exactly at 10:00
    has missed the 10:00 batch), ignoring weekends/holidays (the UDF handles those)."""
    d = f"to_date({ts})"
    cases = " ".join(f"WHEN {ts} < {_at_hour(d, h)} THEN {_at_hour(d, h)}"
                     for h in PESONET_WINDOW_HOURS)
    return f"CASE {cases} ELSE {_at_hour(f'date_add({d}, 1)', PESONET_WINDOW_HOURS[0])} END"


# ---- Kafka / Cassandra schemas -----------------------------------------------

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
    "created_at", "batch_window", "settlement_due", "decided_at", "risk_flags"]


# ---- Pure functions (no I/O; unit-testable) ----------------------------------

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


def evaluate_fraud_risk(intents):
    """
    Add a `risk_flags` column (comma-separated text) and a `blocked` boolean column.
    All logic is deterministic and pure — no I/O.

    Rules:
      HIGH_VALUE:         amount > PHP 200,000 (20,000,000 centavos) — flag, still route.
      SUSPICIOUS_AMOUNT:  PHP 49,000.01 – PHP 50,000.00 (4,900,001–5,000,000 centavos)
                          — flag, still route.
      RAPID_REPEAT:       same src→dst pair appears more than RAPID_REPEAT_THRESHOLD times
                          in this micro-batch; 4th+ occurrence is flagged, still routes.
      BLOCKED:            source OR destination institution code is in BLOCKED_INSTITUTIONS
                          — rejected, published to DLQ.
    """
    # Count how many times each src→dst pair appears in this batch to detect RAPID_REPEAT.
    pair_w = Window.partitionBy("source_institution_code", "destination_institution_code") \
                   .orderBy("kafka_partition", "kafka_offset")
    df = intents.withColumn("_pair_rn", F.row_number().over(pair_w))

    # Build individual flag columns (boolean), then combine into a comma-separated string.
    high_value = F.col("amount") > HIGH_VALUE_THRESHOLD_CENTAVOS
    suspicious  = (F.col("amount") >= SUSPICIOUS_AMOUNT_LOW_CENTAVOS) & \
                  (F.col("amount") <= SUSPICIOUS_AMOUNT_HIGH_CENTAVOS)
    rapid_repeat = F.col("_pair_rn") > RAPID_REPEAT_THRESHOLD

    # BLOCKED: check both src and dst against the broadcast blocklist
    if BLOCKED_INSTITUTIONS:
        blocked_lit = F.array(*[F.lit(c) for c in BLOCKED_INSTITUTIONS])
        is_blocked = (F.array_contains(blocked_lit, F.col("source_institution_code")) |
                      F.array_contains(blocked_lit, F.col("destination_institution_code")))
    else:
        is_blocked = F.lit(False)

    # Assemble risk_flags as a comma-separated string (null when no flags)
    flags_array = F.filter(
        F.array(
            F.when(high_value,    F.lit("HIGH_VALUE")),
            F.when(suspicious,    F.lit("SUSPICIOUS_AMOUNT")),
            F.when(rapid_repeat,  F.lit("RAPID_REPEAT")),
        ),
        lambda x: x.isNotNull()
    )
    risk_flags_col = F.when(F.size(flags_array) > 0, F.array_join(flags_array, ","))

    return (df
            .withColumn("risk_flags", risk_flags_col)
            .withColumn("blocked", is_blocked)
            .drop("_pair_rn"))


def route(intents, registry, pesonet_window_udf):
    """Pure routing logic: intents (parsed, one row per reference_id) + registry
    snapshot -> one decision row per intent. No I/O, so it can be unit-tested.

    pesonet_window_udf: a Spark UDF(timestamp) -> timestamp that skips weekends + holidays.
    """
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

    # Holiday-aware PESONet window: use the UDF for the weekend+holiday skip step.
    # _next_window_sql gives us the raw next clearing slot (may land on a weekend/holiday);
    # pesonet_window_udf advances past any non-business days.
    return (j
        .withColumn("rejection_reason", rejection)
        .withColumn("status", F.when(ok, "ROUTED").otherwise("REJECTED"))
        .withColumn("rail", F.when(ok, candidate_rail))
        .withColumn("routing_reason", F.when(ok, candidate_reason))
        .withColumn("next_window", F.expr(_next_window_sql("created_ts")))
        .withColumn("batch_window", F.when(F.col("rail") == "PESONET",
                                           pesonet_window_udf(F.col("next_window"))))
        .withColumn("settlement_due",
                    F.when(F.col("rail") == "INSTAPAY",
                           F.expr(f"created_ts + INTERVAL {INSTAPAY_SLA_SECONDS} SECONDS"))
                     .otherwise(F.col("batch_window")))
        .drop("next_window"))


def routing_events(decided):
    """Kafka records (key, value) for rail-routing-events, one per decision.
    Timestamps are ISO 8601 with the Manila offset, e.g. 2026-09-25T10:00:00.000+08:00.
    Includes risk_flags so Job C and downstream consumers can see them."""
    rec = decided.select(*[F.col("created_ts").alias("created_at") if c == "created_at"
                           else F.col("processed_at").alias("decided_at") if c == "decided_at"
                           else F.col(c) for c in ROUTING_EVENT_FIELDS])
    return rec.select(F.col("reference_id").alias("key"),
                      F.to_json(F.struct(*ROUTING_EVENT_FIELDS)).alias("value"))


def dlq_rows(df, failure_reason, now_col):
    """Format rows for the payment-dlq Kafka topic."""
    return df.select(
        F.col("reference_id").alias("key"),
        F.to_json(F.struct(
            F.col("reference_id"),
            F.col("kafka_topic").alias("original_topic"),
            F.lit(failure_reason).alias("failure_reason"),
            now_col.alias("failed_at"),
            F.col("raw_value").alias("original_payload"),
        )).alias("value")
    )


# ---- I/O helpers -------------------------------------------------------------

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


# ---- Core batch handler ------------------------------------------------------

# Module-level UDF, built once per process using the holiday set loaded at import time.
_PESONET_WINDOW_UDF = _make_pesonet_window_udf(_HOLIDAY_SET)


def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    batch_df.persist()
    try:
        spark = batch_df.sparkSession
        now = F.current_timestamp()

        # Step 1 & 2: Parse and split unkeyed messages.
        # Messages we can't key (not JSON, or no reference_id) are kept, not dropped.
        unkeyed = batch_df.filter(~has_reference_id())
        keyed   = batch_df.filter(has_reference_id())

        # Step 3: Detect cross-batch duplicates via Cassandra ledger lookup.
        # With CassandraSparkExtensions this join becomes a direct per-key lookup
        # instead of a full table scan.
        ledger   = (read_cassandra(spark, "transaction_lifecycle_by_reference")
                    .select("reference_id", "kafka_partition", "kafka_offset"))
        existing = ledger.join(keyed.select("reference_id").distinct(), "reference_id")
        new, duplicates = classify_intents(keyed, existing)

        write_cassandra(invalid_rows(unkeyed, "UNPARSEABLE_OR_MISSING_REFERENCE_ID", now)
                        .unionByName(invalid_rows(duplicates, "DUPLICATE_REFERENCE_ID", now)),
                        "invalid_intent_events")

        # Step 4: Run fraud/risk evaluation on new valid intents only.
        screened = evaluate_fraud_risk(new)

        # Step 5: Split blocked vs. routable.
        blocked  = screened.filter(F.col("blocked")).drop("blocked")
        routable = screened.filter(~F.col("blocked")).drop("blocked")

        # Blocked payments → REJECTED / BLOCKED_INSTITUTION, DLQ, invalid_intent_events.
        # They must NOT appear on rail-routing-events.
        if not blocked.rdd.isEmpty():
            blocked_decided = (blocked
                .withColumn("status",           F.lit("REJECTED"))
                .withColumn("rejection_reason", F.lit("BLOCKED_INSTITUTION"))
                .withColumn("rail",             F.lit(None).cast(StringType()))
                .withColumn("routing_reason",   F.lit(None).cast(StringType()))
                .withColumn("batch_window",     F.lit(None).cast(TimestampType()))
                .withColumn("settlement_due",   F.lit(None).cast(TimestampType()))
                .withColumn("processed_at",     now))
            publish_kafka(dlq_rows(blocked_decided, "BLOCKED_INSTITUTION", now), DLQ_TOPIC)
            write_cassandra(invalid_rows(blocked, "BLOCKED_INSTITUTION", now),
                            "invalid_intent_events")

        # Step 6: Route the non-blocked intents.
        # Fresh registry snapshot every micro-batch so a MongoDB change (via Job A)
        # affects validation immediately, without restarting this job.
        registry = read_cassandra(spark, "institution_registry")
        decided  = route(routable, registry, _PESONET_WINDOW_UDF) \
                       .withColumn("processed_at", now).persist()

        # Step 7: Publish to rail-routing-events (includes risk_flags).
        # Order matters: ledger row is written LAST. If the job crashes part-way,
        # the batch is re-run; no ledger row means the intent is still "new", so it is
        # decided and published again (Job C ignores a repeated routing event).
        write_cassandra(decided.filter(F.col("status") == "ROUTED").select(
            "rail", "source_institution_code", "settlement_due", "reference_id",
            "destination_institution_code", "amount", "batch_window",
            F.col("created_ts").alias("created_at")),
            "settlement_monitoring_by_institution")

        publish_kafka(routing_events(decided), ROUTING_TOPIC)

        # Step 8: Write to Cassandra ledger (includes risk_flags column).
        write_cassandra(decided.select(
            "reference_id", "source_institution_code", "destination_institution_code",
            "amount", "currency", F.col("created_ts").alias("created_at"),
            "status", "rail", "routing_reason", "rejection_reason",
            "batch_window", "settlement_due", F.col("metadata").alias("metadata_json"),
            "kafka_partition", "kafka_offset", "processed_at", "risk_flags"),
            "transaction_lifecycle_by_reference")

        # Summary log line
        summary = decided.groupBy("status", F.coalesce("rail", "rejection_reason").alias("detail")) \
                         .count().orderBy("status", "detail").collect()
        parts = [f"{r.status}/{r.detail}: {r['count']}" for r in summary]
        n_dup, n_unkeyed = duplicates.count(), unkeyed.count()
        n_blocked = blocked.count()
        if n_dup:
            parts.append(f"DUPLICATE: {n_dup}")
        if n_unkeyed:
            parts.append(f"UNKEYED: {n_unkeyed}")
        if n_blocked:
            parts.append(f"BLOCKED: {n_blocked}")
        # Count flagged (risk_flags not null) among routed payments
        n_flagged = decided.filter(F.col("risk_flags").isNotNull()).count()
        if n_flagged:
            parts.append(f"RISK_FLAGGED: {n_flagged}")
        print(f"[batch {batch_id}] " + (", ".join(parts) or "replayed only, nothing new"),
              flush=True)
        decided.unpersist()

    except Exception as exc:
        # Unexpected batch-level failure: publish all in-flight rows to the DLQ,
        # then re-raise so Spark retries the batch via its normal retry mechanism.
        # DLQ publishing is at-least-once; do NOT assume it is exactly-once.
        try:
            now_lit = F.current_timestamp()
            publish_kafka(dlq_rows(batch_df, str(exc), now_lit), DLQ_TOPIC)
            print(f"[batch {batch_id}] UNEXPECTED ERROR — published {batch_df.count()} rows "
                  f"to {DLQ_TOPIC}. Re-raising.", flush=True)
        except Exception as dlq_exc:
            print(f"[batch {batch_id}] Could not publish to DLQ: {dlq_exc}", flush=True)
        raise

    finally:
        batch_df.unpersist()


# ---- Metrics helper ----------------------------------------------------------

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


# ---- Entry point -------------------------------------------------------------

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
