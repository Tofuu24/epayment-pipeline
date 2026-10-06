"""Job B routing rules and PESONet windows (spark/validate_and_ledger_job.py: route)."""
import json
import os

import pytest
from pyspark.sql import functions as F

import validate_and_ledger_job as J

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def intents_df(spark, lines):
    raw = spark.createDataFrame(
        [("payment-intent-events", 0, i, l) for i, l in enumerate(lines)],
        "kafka_topic string, kafka_partition int, kafka_offset long, raw_value string")
    return J.parse_intents(raw)


def decide(spark, registry, lines):
    parsed = intents_df(spark, lines).filter(J.has_reference_id())
    rows = J.route(parsed, registry).collect()
    return {r.reference_id: r for r in rows}


def intent(ref, amount=100000, src="BANK_BPI", dst="EMI_GCASH", currency="PHP",
           created_at="2026-09-25T11:00:00+08:00", **extra):
    d = {"reference_id": ref, "source_institution_code": src,
         "destination_institution_code": dst, "amount": amount, "currency": currency,
         "created_at": created_at, "metadata": {}}
    d.update(extra)
    return json.dumps(d)


def manila(ts):
    return ts.strftime("%a %Y-%m-%d %H:%M") if ts else None


# The expected table from SPARK_GUIDE section 6.
EXPECTED = {
    "PAY-001": ("ROUTED", "INSTAPAY", None, None),
    "PAY-002": ("ROUTED", "PESONET", None, "Fri 2026-09-25 10:00"),
    "PAY-003": ("ROUTED", "PESONET", None, "Fri 2026-09-25 16:00"),
    "PAY-004": ("ROUTED", "PESONET", None, "Mon 2026-09-28 10:00"),
    "PAY-005": ("ROUTED", "PESONET", None, "Fri 2026-09-25 13:00"),
    "PAY-006": ("REJECTED", None, "NO_ELIGIBLE_RAIL", None),
    "PAY-007": ("REJECTED", None, "UNKNOWN_DESTINATION", None),
    "PAY-008": ("ROUTED", "INSTAPAY", None, None),
    "PAY-009": ("REJECTED", None, "NO_ELIGIBLE_RAIL", None),
    "PAY-010": ("REJECTED", None, "INVALID_AMOUNT", None),
    "PAY-011": ("REJECTED", None, "UNSUPPORTED_CURRENCY", None),
    "PAY-012": ("ROUTED", "PESONET", None, "Fri 2026-09-25 13:00"),
}


def test_test_intents_file_matches_guide(spark, registry):
    lines = open(os.path.join(ROOT, "test_intents.jsonl")).read().splitlines()
    got = decide(spark, registry, lines)
    assert set(got) == set(EXPECTED)
    for ref, (status, rail, rejection, window) in EXPECTED.items():
        r = got[ref]
        assert (r.status, r.rail, r.rejection_reason, manila(r.batch_window)) == \
               (status, rail, rejection, window), ref


def test_unkeyable_messages_are_not_routed(spark):
    lines = open(os.path.join(ROOT, "test_intents.jsonl")).read().splitlines()
    unkeyed = intents_df(spark, lines).filter(~J.has_reference_id()).count()
    assert unkeyed == 2


@pytest.mark.parametrize("amount,expected_rail", [
    (5_000_000, "INSTAPAY"),     # exactly PHP 50,000: cap is inclusive
    (5_000_001, "PESONET"),
])
def test_cap_boundary(spark, registry, amount, expected_rail):
    r = decide(spark, registry, [intent("X", amount=amount, dst="BANK_UNIONBANK")])["X"]
    assert r.rail == expected_rail


@pytest.mark.parametrize("created_at,window", [
    ("2026-09-25T09:59:59+08:00", "Fri 2026-09-25 10:00"),
    ("2026-09-25T10:00:00+08:00", "Fri 2026-09-25 13:00"),   # exactly at a window: missed it
    ("2026-09-25T15:59:00+08:00", "Fri 2026-09-25 16:00"),
    ("2026-09-24T16:00:00+08:00", "Fri 2026-09-25 10:00"),   # after last window: next day
    ("2026-09-26T09:00:00+08:00", "Mon 2026-09-28 10:00"),   # Saturday
    ("2026-09-27T18:00:00+08:00", "Mon 2026-09-28 10:00"),   # Sunday evening
    ("2026-09-25T01:59:00Z", "Fri 2026-09-25 10:00"),        # UTC input = 09:59 Manila
    ("2026-09-25T09:00:00", "Fri 2026-09-25 10:00"),         # no offset: read as Manila
])
def test_pesonet_windows(spark, registry, created_at, window):
    r = decide(spark, registry, [intent("W", amount=9_000_000, dst="BANK_UNIONBANK",
                                        created_at=created_at)])["W"]
    assert manila(r.batch_window) == window
    assert r.settlement_due == r.batch_window


def test_instapay_settlement_due_is_created_plus_sla(spark, registry):
    r = decide(spark, registry, [intent("I")])["I"]
    assert (r.settlement_due - r.created_ts).total_seconds() == J.INSTAPAY_SLA_SECONDS
    assert r.batch_window is None


@pytest.mark.parametrize("line,reason", [
    (intent("A", amount=1500.50), "INVALID_PAYLOAD"),
    (intent("A", amount="150000"), "INVALID_PAYLOAD"),
    (intent("A", created_at="yesterday"), "INVALID_PAYLOAD"),
    (json.dumps({"reference_id": "A", "amount": 100}), "INVALID_PAYLOAD"),
    (intent("A", amount=0), "INVALID_AMOUNT"),
    (intent("A", currency="php"), "UNSUPPORTED_CURRENCY"),
    (intent("A", src="NOPE"), "UNKNOWN_SOURCE"),
    (intent("A", src="BANK_CLOSED"), "INACTIVE_SOURCE"),
    (intent("A", dst="BANK_CLOSED"), "INACTIVE_DESTINATION"),
])
def test_rejections(spark, registry, line, reason):
    r = decide(spark, registry, [line])["A"]
    assert (r.status, r.rejection_reason, r.rail) == ("REJECTED", reason, None)


def test_nested_metadata_kept_as_json(spark, registry):
    r = decide(spark, registry, [intent("M", metadata={"channel": "mobile", "tags": [1, 2]})])["M"]
    assert json.loads(r.metadata) == {"channel": "mobile", "tags": [1, 2]}


def test_routing_event_shape(spark, registry):
    parsed = intents_df(spark, [intent("E", amount=9_000_000, dst="BANK_UNIONBANK")])
    decided = J.route(parsed, registry).withColumn("processed_at", F.current_timestamp())
    rec = J.routing_events(decided).collect()[0]
    assert rec.key == "E"
    v = json.loads(rec.value)
    assert v["status"] == "ROUTED" and v["rail"] == "PESONET"
    assert v["batch_window"].startswith("2026-09-25T13:00:00") and v["batch_window"].endswith("+08:00")
    assert set(v) <= set(J.ROUTING_EVENT_FIELDS)
