"""Synthetic traffic for the pipeline: payment intents in, settlements and webhooks out.

  simulator ──payment-intent-events──► Job B ──rail-routing-events──► simulator (acts as the rails)
      │                                                                   │
      └──────────────── settlement-events, webhook-events ◄───────────────┘

The simulator plays two roles in one process:
  * participating institutions: sends payment intents at --rate per second;
  * the clearing rails + PayMongo: reads Job B's routing decisions and, for each ROUTED
    transfer, later sends a settlement confirmation (or a failure, a late one, or none
    at all, so Job C has STUCK transfers to find) and a PayMongo-style webhook, which is
    sometimes delivered more than once, like PayMongo's retries.

Simulated clock: all event timestamps use a clock that runs --speed times faster than
real time, so PESONet windows hours away pass in minutes during a demo. With --speed 60,
one real minute is one simulated hour. Job C's WATERMARK_DELAY must cover the pipeline's
real lag in simulated time (about 15 minutes at --speed 60).

Install:  pip install confluent-kafka
Examples:
  python simulator.py --rate 20 --speed 60 --duration 600            # demo
  python simulator.py --rate 2000 --speed 1 --duration 120 --no-rails  # throughput benchmark
  python simulator.py --dry-run 50 > intents.jsonl                   # no Kafka, intents only
"""
import argparse
import heapq
import itertools
import json
import random
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

MANILA = timezone(timedelta(hours=8))

INTENT_TOPIC = "payment-intent-events"
ROUTING_TOPIC = "rail-routing-events"
SETTLEMENT_TOPIC = "settlement-events"
WEBHOOK_TOPIC = "webhook-events"

# Same institutions as mongo-init/seed.js
INSTITUTIONS = ["BANK_UNIONBANK", "BANK_BPI", "BANK_LANDBANK", "EMI_GCASH", "EMI_MAYA"]
CHANNELS = ["mobile", "web", "branch", "api"]
FAILURE_REASONS = ["ACCOUNT_CLOSED", "INVALID_ACCOUNT", "BENEFICIARY_BANK_TIMEOUT", "AML_HOLD"]


def iso(dt):
    return dt.astimezone(MANILA).isoformat(timespec="milliseconds")


class SimClock:
    def __init__(self, start, speed, real=time.monotonic):
        self.start, self.speed, self.real = start, speed, real
        self.t0 = real()

    def now(self):
        return self.start + timedelta(seconds=(self.real() - self.t0) * self.speed)


class IntentFactory:
    """Payment intents shaped like the PayMongo-based schema in SPARK_GUIDE section 5."""

    def __init__(self, rng, run_id, invalid_pct=0.0, dup_pct=0.0):
        self.rng, self.run_id = rng, run_id
        self.invalid_pct, self.dup_pct = invalid_pct, dup_pct
        self.seq = itertools.count(1)
        self.sent = []          # recent valid intents, for duplicate re-sends

    def amount(self):
        r = self.rng.random()
        if r < 0.60:
            return self.rng.randint(100_00, 50_000_00)            # PHP 100 - 50,000
        if r < 0.70:
            return self.rng.choice([50_000_00, 50_000_01, 49_999_99])  # around the cap
        return self.rng.randint(50_000_01, 500_000_00)            # PHP 50k - 500k

    def next(self, sim_now):
        """Returns (key, value) for payment-intent-events."""
        rng = self.rng
        if self.sent and rng.random() < self.dup_pct:
            key, value = rng.choice(self.sent)                     # re-sent duplicate
            return key, value
        ref = f"PAY-{self.run_id}-{next(self.seq):07d}"
        src, dst = rng.sample(INSTITUTIONS, 2)
        d = {"reference_id": ref, "source_institution_code": src,
             "destination_institution_code": dst, "amount": self.amount(), "currency": "PHP",
             "created_at": iso(sim_now), "metadata": {"channel": rng.choice(CHANNELS)}}
        if rng.random() < self.invalid_pct:
            kind = rng.choice(["currency", "amount", "unknown", "no_ref", "not_json"])
            if kind == "not_json":
                return None, "this is not json"
            if kind == "no_ref":
                del d["reference_id"]
                return None, json.dumps(d)
            if kind == "currency":
                d["currency"] = "USD"
            elif kind == "amount":
                d["amount"] = -rng.randint(1, 10000)
            else:
                d["destination_institution_code"] = "BANK_XYZ"
        value = json.dumps(d)
        self.sent.append((ref, value))
        if len(self.sent) > 1000:
            self.sent.pop(0)
        return ref, value


class RailSimulator:
    """Turns ROUTED decisions into future settlement and webhook events (simulated time)."""

    def __init__(self, rng, late_pct=0.05, missing_pct=0.03, failed_pct=0.02,
                 dup_webhook_pct=0.10, instapay_grace_s=30, pesonet_grace_s=3600):
        self.rng = rng
        self.late_pct, self.missing_pct, self.failed_pct = late_pct, missing_pct, failed_pct
        self.dup_webhook_pct = dup_webhook_pct
        self.instapay_grace_s, self.pesonet_grace_s = instapay_grace_s, pesonet_grace_s
        self.queue = []                   # heap of (due datetime, seq, topic, key, value)
        self.seq = itertools.count()
        self.seen = set()                 # Job B may republish a decision after a retry
        self.stats = {"routed": 0, "settled": 0, "failed": 0, "late": 0, "missing": 0, "webhooks": 0}

    def _push(self, at, topic, key, value):
        heapq.heappush(self.queue, (at, next(self.seq), topic, key, value))

    def on_routing_event(self, ev):
        if ev.get("status") != "ROUTED" or ev["reference_id"] in self.seen:
            return
        self.seen.add(ev["reference_id"])
        self.stats["routed"] += 1
        rng, rail = self.rng, ev["rail"]
        created = datetime.fromisoformat(ev["created_at"])
        due = datetime.fromisoformat(ev["settlement_due"])
        base = created if rail == "INSTAPAY" else due     # PESONet settles in its window
        r = rng.random()
        if r < self.missing_pct:
            self.stats["missing"] += 1
            return                                         # never confirmed -> STUCK
        if r < self.missing_pct + self.failed_pct:
            status, outcome = "FAILED", "failed"
            at = base + timedelta(seconds=rng.uniform(1, 5) if rail == "INSTAPAY" else rng.uniform(60, 900))
        elif r < self.missing_pct + self.failed_pct + self.late_pct:
            status, outcome = "SETTLED", "late"
            grace = self.instapay_grace_s if rail == "INSTAPAY" else self.pesonet_grace_s
            extra = rng.uniform(30, 300) if rail == "INSTAPAY" else rng.uniform(600, 7200)
            at = due + timedelta(seconds=grace + extra)
        else:
            status, outcome = "SETTLED", "settled"
            at = base + timedelta(seconds=rng.uniform(1, 10) if rail == "INSTAPAY" else rng.uniform(60, 900))
        self.stats[outcome] += 1
        ref = ev["reference_id"]
        settlement = {"settlement_id": "stl_" + uuid.uuid4().hex[:24], "reference_id": ref,
                      "rail": rail, "status": status,
                      "failure_reason": rng.choice(FAILURE_REASONS) if status == "FAILED" else None,
                      "settled_at": iso(at)}
        self._push(at, SETTLEMENT_TOPIC, ref, json.dumps(settlement))
        self._schedule_webhook(ev, status, at + timedelta(seconds=rng.uniform(1, 3)))

    def _schedule_webhook(self, ev, status, at):
        """PayMongo-style event object (see EVENT_FORMATS.md):
        data.attributes.type is the event type, data.attributes.data the payment."""
        evt_id = "evt_" + uuid.uuid4().hex[:24]
        paid = status == "SETTLED"
        body = {"data": {"id": evt_id, "type": "event", "attributes": {
            "type": "payment.paid" if paid else "payment.failed", "livemode": False,
            "data": {"id": "pay_" + uuid.uuid4().hex[:24], "type": "payment", "attributes": {
                "amount": ev["amount"], "currency": ev.get("currency") or "PHP",
                "status": "paid" if paid else "failed",
                "metadata": {"reference_id": ev["reference_id"]}}},
            "created_at": int(at.timestamp()), "updated_at": int(at.timestamp())}}}
        value = json.dumps(body)
        self._push(at, WEBHOOK_TOPIC, ev["reference_id"], value)
        self.stats["webhooks"] += 1
        if self.rng.random() < self.dup_webhook_pct:        # retried delivery, same event id
            for _ in range(self.rng.randint(1, 3)):
                self._push(at + timedelta(seconds=self.rng.uniform(5, 120)),
                           WEBHOOK_TOPIC, ev["reference_id"], value)

    def pop_due(self, sim_now):
        out = []
        while self.queue and self.queue[0][0] <= sim_now:
            _, _, topic, key, value = heapq.heappop(self.queue)
            out.append((topic, key, value))
        return out


def run(args):
    from confluent_kafka import Consumer, Producer, TopicPartition

    rng = random.Random(args.seed)
    run_id = args.run_id or uuid.uuid4().hex[:6].upper()
    start = datetime.fromisoformat(args.start) if args.start else datetime.now(MANILA)
    if start.tzinfo is None:
        start = start.replace(tzinfo=MANILA)
    clock = SimClock(start, args.speed)
    intents = IntentFactory(rng, run_id, args.invalid_pct, args.dup_intent_pct)
    rails = RailSimulator(rng, args.late_pct, args.missing_pct, args.failed_pct,
                          args.dup_webhook_pct, args.instapay_grace, args.pesonet_grace)
    producer = Producer({"bootstrap.servers": args.bootstrap, "linger.ms": 20,
                         "queue.buffering.max.messages": 1_000_000})
    run_started_ms = int(time.time() * 1000)

    consumer = None
    if not args.no_rails:
        consumer = Consumer({"bootstrap.servers": args.bootstrap, "group.id": f"simulator-{run_id}",
                             "enable.auto.commit": False, "auto.offset.reset": "latest"})

        def on_assign(c, partitions):
            # Start from this run's start time, so decisions published while the
            # consumer was still joining aren't missed.
            ts = [TopicPartition(p.topic, p.partition, run_started_ms) for p in partitions]
            c.assign(c.offsets_for_times(ts, timeout=10))
        consumer.subscribe([ROUTING_TOPIC], on_assign=on_assign)

    print(f"run {run_id}: rate={args.rate}/s speed={args.speed}x duration={args.duration}s "
          f"sim start={iso(start)}", file=sys.stderr)
    sent = {"intents": 0, SETTLEMENT_TOPIC: 0, WEBHOOK_TOPIC: 0}
    t0 = time.monotonic()
    last_report = t0
    prefix = f"PAY-{run_id}-"
    try:
        while True:
            now = time.monotonic()
            elapsed = now - t0
            producing = elapsed < args.duration
            if not producing and (args.no_rails or elapsed > args.duration + args.drain):
                break
            if producing:
                target = int(elapsed * args.rate)
                while sent["intents"] < target:
                    key, value = intents.next(clock.now())
                    producer.produce(INTENT_TOPIC, value=value, key=key)
                    sent["intents"] += 1
                    if sent["intents"] % 10000 == 0:
                        producer.poll(0)
            if consumer is not None:
                for msg in consumer.consume(num_messages=500, timeout=0.01):
                    if msg.error() or msg.value() is None:
                        continue
                    try:
                        ev = json.loads(msg.value())
                    except ValueError:
                        continue
                    if str(ev.get("reference_id", "")).startswith(prefix):
                        rails.on_routing_event(ev)
                for topic, key, value in rails.pop_due(clock.now()):
                    producer.produce(topic, value=value, key=key)
                    sent[topic] += 1
            producer.poll(0)
            if now - last_report >= 5:
                last_report = now
                print(f"[{elapsed:6.0f}s] sim {clock.now().strftime('%a %H:%M')} | "
                      f"intents {sent['intents']} | routed seen {rails.stats['routed']} | "
                      f"settlements {sent[SETTLEMENT_TOPIC]} | webhooks {sent[WEBHOOK_TOPIC]} | "
                      f"pending {len(rails.queue)}", file=sys.stderr)
            if consumer is None:
                time.sleep(0.005)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(30)
        if consumer is not None:
            consumer.close()
    print(f"done: sent {sent}; rail outcomes {rails.stats}", file=sys.stderr)


def dry_run(args):
    rng = random.Random(args.seed)
    start = datetime.fromisoformat(args.start) if args.start else datetime.now(MANILA)
    if start.tzinfo is None:
        start = start.replace(tzinfo=MANILA)
    f = IntentFactory(rng, args.run_id or "DRY", args.invalid_pct, args.dup_intent_pct)
    for i in range(args.dry_run):
        print(f.next(start + timedelta(seconds=i / max(args.rate, 1) * args.speed))[1])


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--rate", type=float, default=10, help="payment intents per real second")
    p.add_argument("--duration", type=float, default=300, help="real seconds to send intents")
    p.add_argument("--drain", type=float, default=120,
                   help="real seconds to keep sending settlements/webhooks after intents stop")
    p.add_argument("--speed", type=float, default=60, help="simulated seconds per real second")
    p.add_argument("--start", help="simulated start time, ISO 8601 (default: now, Manila)")
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--run-id", help="prefix for reference_ids (default: random)")
    p.add_argument("--invalid-pct", type=float, default=0.02, help="share of invalid intents")
    p.add_argument("--dup-intent-pct", type=float, default=0.01, help="share of re-sent intents")
    p.add_argument("--late-pct", type=float, default=0.05, help="settled after the deadline")
    p.add_argument("--missing-pct", type=float, default=0.03, help="never confirmed (STUCK)")
    p.add_argument("--failed-pct", type=float, default=0.02, help="rail reports FAILED")
    p.add_argument("--dup-webhook-pct", type=float, default=0.10, help="webhooks delivered 2-4 times")
    p.add_argument("--instapay-grace", type=float, default=30, help="match Job C INSTAPAY_GRACE_SECONDS")
    p.add_argument("--pesonet-grace", type=float, default=3600, help="match Job C PESONET_GRACE_SECONDS")
    p.add_argument("--no-rails", action="store_true",
                   help="only send intents (no settlements/webhooks); for throughput benchmarks")
    p.add_argument("--dry-run", type=int, metavar="N", help="print N intents to stdout; no Kafka")
    return p.parse_args(argv)


if __name__ == "__main__":
    a = parse_args()
    dry_run(a) if a.dry_run else run(a)
