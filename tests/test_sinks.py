"""Run Job B's and Job C's foreachBatch functions with Cassandra and Kafka replaced by
in-memory fakes, and check every column written exists in cassandra-init/schema.cql."""
import json
import os
import re

import pytest

import settlement_monitor_job as C
import validate_and_ledger_job as J

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def schema_columns():
    """table -> set of columns, from CREATE TABLE and ALTER TABLE ... ADD in schema.cql."""
    cql = open(os.path.join(ROOT, "cassandra-init", "schema.cql")).read()
    cql = re.sub(r"--[^\n]*", "", cql)
    cols = {}
    for table, body in re.findall(r"CREATE TABLE IF NOT EXISTS payment_pipeline\.(\w+)\s*\((.*?)\)\s*(?:WITH|;)", cql, re.S):
        cols[table] = {part.split()[0] for part in re.split(r",\s*\n", body)
                       if part.strip() and not part.strip().startswith("PRIMARY")}
    for table, col in re.findall(r"ALTER TABLE payment_pipeline\.(\w+) ADD IF NOT EXISTS (\w+)", cql):
        cols[table].add(col)
    return cols


class FakeSinks:
    def __init__(self, tables=None):
        self.writes, self.kafka, self.tables = {}, {}, tables or {}

    def write(self, df, table):
        self.writes.setdefault(table, []).extend(r.asDict() for r in df.collect())

    def read(self, spark, table):
        return self.tables[table]

    def publish(self, df, topic):
        self.kafka.setdefault(topic, []).extend(r.asDict() for r in df.collect())


def assert_columns_exist(writes):
    cols = schema_columns()
    for table, rows in writes.items():
        assert table in cols, table
        for r in rows:
            missing = set(r) - cols[table]
            assert not missing, f"{table}: {missing}"


@pytest.fixture
def job_b(spark, registry, monkeypatch):
    ledger = spark.createDataFrame([("PAY-OLD", 0, 0)],
                                   "reference_id string, kafka_partition int, kafka_offset long")
    sinks = FakeSinks({"institution_registry": registry, "transaction_lifecycle_by_reference": ledger})
    monkeypatch.setattr(J, "write_cassandra", sinks.write)
    monkeypatch.setattr(J, "read_cassandra", sinks.read)
    monkeypatch.setattr(J, "publish_kafka", sinks.publish)
    return sinks


def test_job_b_process_batch(spark, job_b):
    lines = open(os.path.join(ROOT, "test_intents.jsonl")).read().splitlines()
    lines.append(lines[0])                                                  # in-batch duplicate
    lines.append(json.dumps({"reference_id": "PAY-OLD", "amount": 1}))      # decided earlier
    raw = spark.createDataFrame([("payment-intent-events", 0, i + 1, l) for i, l in enumerate(lines)],
                                "kafka_topic string, kafka_partition int, kafka_offset long, raw_value string")
    J.process_batch(J.parse_intents(raw), 0)

    w = job_b.writes
    assert_columns_exist(w)
    assert len(w["transaction_lifecycle_by_reference"]) == 12
    assert len(w["settlement_monitoring_by_institution"]) == 7              # the ROUTED ones
    reasons = sorted(r["reason"] for r in w["invalid_intent_events"])
    assert reasons == ["DUPLICATE_REFERENCE_ID"] * 2 + ["UNPARSEABLE_OR_MISSING_REFERENCE_ID"] * 2
    # Job B never touches Job C's columns (no race between the two jobs).
    assert not any("settlement_status" in r for r in w["transaction_lifecycle_by_reference"])
    assert not any("settlement_status" in r for r in w["settlement_monitoring_by_institution"])
    assert len(job_b.kafka[J.ROUTING_TOPIC]) == 12


def test_job_b_replay_writes_nothing_new(spark, job_b, registry):
    line = open(os.path.join(ROOT, "test_intents.jsonl")).readline().strip()
    job_b.tables["transaction_lifecycle_by_reference"] = spark.createDataFrame(
        [("PAY-001", 0, 5)], "reference_id string, kafka_partition int, kafka_offset long")
    raw = spark.createDataFrame([("payment-intent-events", 0, 5, line)],
                                "kafka_topic string, kafka_partition int, kafka_offset long, raw_value string")
    J.process_batch(J.parse_intents(raw), 0)
    assert all(len(v) == 0 for v in job_b.writes.values())


def test_job_c_status_writes(spark, monkeypatch):
    sinks = FakeSinks()
    monkeypatch.setattr(C, "write_cassandra", sinks.write)
    t = 1_790_000_000_000
    rows = [("A", "INSTAPAY", "BANK_BPI", "EMI_GCASH", 100, t, t + 30_000, "SETTLED", t + 5000, 5000, None, t + 5000),
            ("B", "INSTAPAY", "BANK_BPI", "EMI_GCASH", 100, t, t + 30_000, "STUCK", None, None, None, t + 60_000),
            ("D", "PESONET", "BANK_BPI", "BANK_UNIONBANK", 100, t, t + 3_600_000, "FAILED", t + 9000, 9000, "AML_HOLD", t + 9000)]
    df = spark.createDataFrame(rows, C.OUTPUT_SCHEMA)
    C.write_status_batch(df, 0)
    assert_columns_exist(sinks.writes)
    assert len(sinks.writes["transaction_lifecycle_by_reference"]) == 3
    assert len(sinks.writes["settlement_monitoring_by_institution"]) == 3
    assert sorted(r["alert_type"] for r in sinks.writes["settlement_alerts_by_rail"]) == ["FAILED", "STUCK"]
    a = [r for r in sinks.writes["transaction_lifecycle_by_reference"] if r["reference_id"] == "A"][0]
    assert (a["settled_at"] - a["settlement_updated_at"]).total_seconds() == 0


def test_job_c_webhook_writes(spark, monkeypatch):
    sinks = FakeSinks()
    monkeypatch.setattr(C, "write_cassandra", sinks.write)
    lines = open(os.path.join(ROOT, "test_webhook_events.jsonl")).read().splitlines()
    parsed = C.parse_webhooks(spark.createDataFrame([(l,) for l in lines], "value string"))
    C.write_webhook_batch(parsed.dropDuplicates(["event_id"]), 0)
    assert_columns_exist(sinks.writes)
    assert len(sinks.writes["webhook_events_by_reference"]) == 3
    ledger = {r["reference_id"]: r for r in sinks.writes["transaction_lifecycle_by_reference"]}
    assert ledger["PAY-003"]["last_webhook_event_type"] == "payment.failed"
