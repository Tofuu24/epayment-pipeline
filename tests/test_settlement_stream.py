"""Job C end to end in Spark Structured Streaming: file source stands in for Kafka
(both give a string `value` column), foreachBatch collects the output."""
import json
import os
import uuid

import pytest
from pyspark.sql import functions as F

import settlement_monitor_job as C


class Collector:
    def __init__(self):
        self.rows = []

    def __call__(self, df, batch_id):
        self.rows += [r.asDict() for r in df.collect()]

    def latest(self):
        out = {}
        for r in self.rows:
            out[r.get("reference_id")] = r
        return out


def push(d, name, records):
    with open(os.path.join(d, name + ".json"), "w") as f:
        f.write("\n".join(json.dumps(r) for r in records) + "\n")


def route_ev(ref, rail, created, due, status="ROUTED"):
    return {"reference_id": ref, "status": status, "rail": rail,
            "source_institution_code": "BANK_BPI", "destination_institution_code": "EMI_GCASH",
            "amount": 100000, "created_at": created, "settlement_due": due}


def settle_ev(ref, at, status="SETTLED", reason=None):
    return {"settlement_id": "stl_" + ref, "reference_id": ref, "rail": "INSTAPAY",
            "status": status, "failure_reason": reason, "settled_at": at}


@pytest.fixture
def dirs(tmp_path):
    r, s = tmp_path / "routing", tmp_path / "settle"
    r.mkdir(); s.mkdir()
    return str(r), str(s), str(tmp_path / "ckpt")


def test_settled_stuck_failed_late(spark, dirs):
    rdir, sdir, ckpt = dirs
    out = Collector()
    status = C.settlement_status_stream(C.parse_routing(spark.readStream.text(rdir)),
                                        C.parse_settlements(spark.readStream.text(sdir)))
    q = (status.writeStream.foreachBatch(out).outputMode("update")
         .option("checkpointLocation", ckpt).start())
    try:
        # 11:00 Manila: three InstaPay transfers (due +30s) and one rejected decision.
        push(rdir, "b1", [
            route_ev("OK", "INSTAPAY", "2026-09-25T11:00:00+08:00", "2026-09-25T11:00:30+08:00"),
            route_ev("STUCK", "INSTAPAY", "2026-09-25T11:00:00+08:00", "2026-09-25T11:00:30+08:00"),
            route_ev("FAIL", "INSTAPAY", "2026-09-25T11:00:00+08:00", "2026-09-25T11:00:30+08:00"),
            route_ev("NOPE", None, "2026-09-25T11:00:00+08:00", None, status="REJECTED"),
        ])
        q.processAllAvailable()
        assert {r["reference_id"]: r["settlement_status"] for r in out.rows} == {
            "OK": "AWAITING_SETTLEMENT", "STUCK": "AWAITING_SETTLEMENT", "FAIL": "AWAITING_SETTLEMENT"}

        push(sdir, "s1", [settle_ev("OK", "2026-09-25T11:00:07+08:00"),
                          settle_ev("FAIL", "2026-09-25T11:00:05+08:00", "FAILED", "ACCOUNT_CLOSED")])
        q.processAllAvailable()
        assert out.latest()["OK"]["settlement_status"] == "SETTLED"
        assert out.latest()["OK"]["turnaround_ms"] == 7000
        assert out.latest()["FAIL"]["failure_reason"] == "ACCOUNT_CLOSED"

        # Later traffic moves the watermark (2 min delay) past STUCK's deadline (11:01:00).
        push(rdir, "b2", [route_ev("LATER", "INSTAPAY", "2026-09-25T11:10:00+08:00",
                                   "2026-09-25T11:10:30+08:00")])
        q.processAllAvailable()
        push(rdir, "b3", [route_ev("LATER2", "INSTAPAY", "2026-09-25T11:10:05+08:00",
                                   "2026-09-25T11:10:35+08:00")])
        q.processAllAvailable()
        stuck = out.latest()["STUCK"]
        assert stuck["settlement_status"] == "STUCK"
        assert stuck["status_changed_ms"] == stuck["due_ms"] + C.INSTAPAY_GRACE_SECONDS * 1000

        # The confirmation finally arrives (still within the watermark delay of 11:10:05).
        push(sdir, "s2", [settle_ev("STUCK", "2026-09-25T11:09:00+08:00"),
                          settle_ev("OK", "2026-09-25T11:09:00+08:00")])      # repeat: ignored
        q.processAllAvailable()
        assert out.latest()["STUCK"]["settlement_status"] == "SETTLED_LATE"
        assert out.latest()["OK"]["settled_ms"] == out.latest()["OK"]["created_ms"] + 7000
        assert "NOPE" not in out.latest()
    finally:
        q.stop()


def test_webhook_duplicates_dropped(spark, tmp_path):
    wdir = tmp_path / "webhooks"; wdir.mkdir()

    def wh(evt, ref, t):
        return {"data": {"id": evt, "type": "event", "attributes": {
            "type": "payment.paid", "livemode": False, "created_at": t,
            "data": {"id": "pay_" + ref, "type": "payment", "attributes": {
                "amount": 100000, "currency": "PHP", "status": "paid",
                "metadata": {"reference_id": ref}}}}}}

    out = Collector()
    q = (C.dedupe_webhooks(C.parse_webhooks(spark.readStream.text(str(wdir))))
         .writeStream.foreachBatch(out).option("checkpointLocation", str(tmp_path / "ck")).start())
    try:
        t = 1_790_000_000
        push(str(wdir), "a", [wh("evt_1", "A", t), wh("evt_1", "A", t), wh("evt_2", "B", t + 1)])
        q.processAllAvailable()
        push(str(wdir), "b", [wh("evt_1", "A", t + 30), wh("evt_3", "C", t + 40)])   # retry of evt_1
        q.processAllAvailable()
        assert sorted(r["event_id"] for r in out.rows) == ["evt_1", "evt_2", "evt_3"]
        assert {r["event_id"]: r["reference_id"] for r in out.rows}["evt_3"] == "C"
    finally:
        q.stop()


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# The demo in SPARK_GUIDE section 6b: what Job C should report for the test files.
DEMO_EXPECTED = {
    "PAY-001": "SETTLED", "PAY-002": "SETTLED", "PAY-003": "FAILED", "PAY-004": "SETTLED",
    "PAY-005": "SETTLED_LATE", "PAY-008": "STUCK", "PAY-012": "STUCK",
}


def test_demo_files(spark, registry, tmp_path, monkeypatch):
    """The test intents span Fri 09:30-16:45 but arrive in one burst, so the demo needs a
    wide watermark (SPARK_GUIDE: WATERMARK_DELAY="12 hours"). With the 2-minute default the
    watermark would already be at 16:43 and the 09:30 settlements would be dropped as late."""
    import validate_and_ledger_job as J
    monkeypatch.setattr(C, "WATERMARK_DELAY", "12 hours")
    rdir, sdir, wdir = tmp_path / "r", tmp_path / "s", tmp_path / "w"
    for d in (rdir, sdir, wdir):
        d.mkdir()
    lines = open(os.path.join(ROOT, "test_intents.jsonl")).read().splitlines()
    raw = spark.createDataFrame([("payment-intent-events", 0, i, l) for i, l in enumerate(lines)],
                                "kafka_topic string, kafka_partition int, kafka_offset long, raw_value string")
    parsed = J.parse_intents(raw).filter(J.has_reference_id())
    decided = J.route(parsed, registry).withColumn("processed_at", F.current_timestamp())
    with open(rdir / "routing.json", "w") as f:
        f.write("\n".join(r.value for r in J.routing_events(decided).collect()) + "\n")

    out = Collector()
    status = C.settlement_status_stream(C.parse_routing(spark.readStream.text(str(rdir))),
                                        C.parse_settlements(spark.readStream.text(str(sdir))))
    q = (status.writeStream.foreachBatch(out).outputMode("update")
         .option("checkpointLocation", str(tmp_path / "c1")).start())
    hooks = Collector()
    q2 = (C.dedupe_webhooks(C.parse_webhooks(spark.readStream.text(str(wdir))))
          .writeStream.foreachBatch(hooks).option("checkpointLocation", str(tmp_path / "c2")).start())
    try:
        q.processAllAvailable()
        with open(sdir / "s.json", "w") as f:
            f.write(open(os.path.join(ROOT, "test_settlement_events.jsonl")).read())
        q.processAllAvailable()
        q.processAllAvailable()
        got = {k: v["settlement_status"] for k, v in out.latest().items()}
        assert got == DEMO_EXPECTED
        assert out.latest()["PAY-001"]["turnaround_ms"] == 5000

        with open(wdir / "w.json", "w") as f:
            f.write(open(os.path.join(ROOT, "test_webhook_events.jsonl")).read())
        q2.processAllAvailable()
        assert sorted(r["event_id"] for r in hooks.rows) == ["evt_test_001", "evt_test_002", "evt_test_003"]
    finally:
        q.stop()
        q2.stop()
