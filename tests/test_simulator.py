"""Simulator logic without Kafka, plus a Kafka-free run of simulator -> Job B -> simulator -> Job C."""
import json
import os
import random
import sys
from datetime import datetime, timedelta

from pyspark.sql import functions as F

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "simulator"))
import simulator as S                     # noqa: E402
import settlement_monitor_job as C       # noqa: E402
import validate_and_ledger_job as J      # noqa: E402

START = datetime(2026, 9, 25, 9, 0, tzinfo=S.MANILA)


def test_intents_are_well_formed():
    f = S.IntentFactory(random.Random(1), "T", invalid_pct=0, dup_pct=0)
    vals = [json.loads(f.next(START)[1]) for _ in range(500)]
    assert len({v["reference_id"] for v in vals}) == 500
    assert all(v["source_institution_code"] != v["destination_institution_code"] for v in vals)
    assert all(v["amount"] > 0 and v["currency"] == "PHP" for v in vals)
    assert any(v["amount"] <= 5_000_000 for v in vals) and any(v["amount"] > 5_000_000 for v in vals)


def test_invalid_and_duplicate_mix():
    f = S.IntentFactory(random.Random(2), "T", invalid_pct=0.5, dup_pct=0.2)
    out = [f.next(START) for _ in range(400)]
    assert any(v == "this is not json" for _, v in out)
    keys = [k for k, _ in out if k]
    assert len(keys) > len(set(keys))          # some re-sent reference_ids


def routed_event(ref, rail, created, due):
    return {"reference_id": ref, "status": "ROUTED", "rail": rail, "amount": 100000,
            "currency": "PHP", "created_at": S.iso(created), "settlement_due": S.iso(due)}


def test_rail_outcomes_and_timing():
    r = S.RailSimulator(random.Random(3), late_pct=0, missing_pct=0, failed_pct=0, dup_webhook_pct=0)
    r.on_routing_event(routed_event("A", "INSTAPAY", START, START + timedelta(seconds=30)))
    window = datetime(2026, 9, 25, 10, 0, tzinfo=S.MANILA)
    r.on_routing_event(routed_event("B", "PESONET", START, window))
    r.on_routing_event(routed_event("A", "INSTAPAY", START, START))      # repeat: ignored
    assert r.pop_due(START) == []
    early = r.pop_due(START + timedelta(seconds=15))
    assert {(t, k) for t, k, _ in early} == {(S.SETTLEMENT_TOPIC, "A"), (S.WEBHOOK_TOPIC, "A")}
    assert r.pop_due(window) == []                                      # PESONet settles after its window
    later = r.pop_due(window + timedelta(minutes=20))
    settle = json.loads([v for t, _, v in later if t == S.SETTLEMENT_TOPIC][0])
    assert settle["reference_id"] == "B" and datetime.fromisoformat(settle["settled_at"]) > window


def test_missing_and_duplicate_webhooks():
    r = S.RailSimulator(random.Random(4), late_pct=0, missing_pct=1.0, failed_pct=0)
    r.on_routing_event(routed_event("M", "INSTAPAY", START, START))
    assert r.queue == [] and r.stats["missing"] == 1
    r = S.RailSimulator(random.Random(4), late_pct=0, missing_pct=0, failed_pct=0, dup_webhook_pct=1.0)
    r.on_routing_event(routed_event("D", "INSTAPAY", START, START))
    hooks = [v for t, _, v in r.pop_due(START + timedelta(hours=1)) if t == S.WEBHOOK_TOPIC]
    assert len(hooks) >= 2 and len({json.loads(h)["data"]["id"] for h in hooks}) == 1


def test_end_to_end_without_kafka(spark, registry):
    """Simulator intents -> Job B route() -> routing events -> rail simulator ->
    settlement events parse in Job C. Checks every message format lines up."""
    f = S.IntentFactory(random.Random(5), "E2E", invalid_pct=0, dup_pct=0)
    lines = [f.next(START + timedelta(minutes=i))[1] for i in range(200)]
    raw = spark.createDataFrame([("payment-intent-events", 0, i, l) for i, l in enumerate(lines)],
                                "kafka_topic string, kafka_partition int, kafka_offset long, raw_value string")
    decided = J.route(J.parse_intents(raw), registry).withColumn("processed_at", F.current_timestamp())
    statuses = {r.status for r in decided.collect()}
    assert statuses == {"ROUTED", "REJECTED"}     # random pairs hit NO_ELIGIBLE_RAIL sometimes

    rails = S.RailSimulator(random.Random(6), late_pct=0, missing_pct=0, failed_pct=0)
    events = [json.loads(r.value) for r in J.routing_events(decided).collect()]
    for ev in events:
        rails.on_routing_event(ev)
    n_routed = sum(e["status"] == "ROUTED" for e in events)
    assert rails.stats["routed"] == n_routed

    out = rails.pop_due(START + timedelta(days=5))
    settle = spark.createDataFrame([(v,) for t, _, v in out if t == S.SETTLEMENT_TOPIC], "value string")
    hooks = spark.createDataFrame([(v,) for t, _, v in out if t == S.WEBHOOK_TOPIC], "value string")
    assert C.parse_settlements(settle).count() == n_routed
    paid = C.parse_webhooks(hooks).filter(F.col("event_type") == "payment.paid")
    assert paid.select("event_id").distinct().count() == n_routed     # retries share an id
    routed = spark.createDataFrame([(r.value,) for r in J.routing_events(decided).collect()], "value string")
    assert C.parse_routing(routed).count() == n_routed
