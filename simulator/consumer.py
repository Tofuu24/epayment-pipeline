"""Settlement simulator: the synthetic InstaPay/PESONet rail and PayMongo webhook side.

Consumes rail-routing-events (written by Job B) and, for every routed transfer,
produces what the rail and the gateway would send back:

  settlement-events  PESONet: "pending" right away, then "settled"/"failed" at its batch
                     InstaPay: "settled"/"failed" a few seconds later
  webhook-events     payment.paid / payment.failed, delivered at least once

with the proposal's (v1 section 6.5) defects mixed in, per transfer:

  duplicate_webhook  the same webhook (same id) delivered 2-3 times
  missing_webhook    settlement arrives, the webhook never does
  stuck              neither a final settlement nor a webhook ever arrives
  out_of_order       the webhook arrives before the settlement event
  late               InstaPay settles after its settlement_due (SLA breach)

It learns the rail from Job B's decision; it never routes anything itself.

    pip install -r simulator/requirements.txt
    python simulator/consumer.py                 # demo speed: PESONet settles 30 s after routing
    python simulator/consumer.py --real-windows  # PESONet settles at its real batch_window
    python simulator/consumer.py --no-defects    # clean run

Pending settlements are held in memory: anything scheduled when you stop this
script is never sent, and those transfers will look stuck. Message formats:
SPARK_GUIDE.md section 7.
"""
import argparse
import heapq
import itertools
import json
import random
import string
import sys
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaConsumer, KafkaProducer

ROUTING_TOPIC = "rail-routing-events"
SETTLEMENT_TOPIC = "settlement-events"
WEBHOOK_TOPIC = "webhook-events"
MANILA = timezone(timedelta(hours=8))
ID_CHARS = string.ascii_letters + string.digits


def iso(epoch):
    return datetime.fromtimestamp(epoch, MANILA).isoformat(timespec="milliseconds")


def epoch(iso_text):
    return datetime.fromisoformat(iso_text).timestamp() if iso_text else None


class Simulator:
    def __init__(self, args, producer):
        self.args = args
        self.producer = producer
        self.rng = random.Random(args.seed)
        self.queue = []                 # (fire_at_epoch, seq, action)
        self.seq = itertools.count()
        self.seen = set()               # reference_ids already handled (routing events are at-least-once)
        self.stats = {}

    # ---- plumbing -----------------------------------------------------------
    def new_id(self, prefix):
        return prefix + "".join(self.rng.choices(ID_CHARS, k=24))

    def at(self, fire_at, action):
        heapq.heappush(self.queue, (fire_at, next(self.seq), action))

    def run_due(self):
        now = time.time()
        while self.queue and self.queue[0][0] <= now:
            heapq.heappop(self.queue)[2]()

    def seconds_to_next(self):
        return max(0.0, self.queue[0][0] - time.time()) if self.queue else 1.0

    def count(self, key):
        self.stats[key] = self.stats.get(key, 0) + 1

    def send(self, topic, key, event, note):
        self.producer.send(topic, key=key.encode(), value=json.dumps(event).encode()) \
            .add_errback(lambda exc: print(f"  !! send to {topic} failed: {exc}", file=sys.stderr))
        print(f"{datetime.now(MANILA):%H:%M:%S}  {topic:<17} {key:<27} {note}")

    # ---- events -------------------------------------------------------------
    def settlement(self, r, status):
        """Builds the event when it fires, so settlement_timestamp is the real send time."""
        def action():
            self.send(SETTLEMENT_TOPIC, r["reference_id"], {
                "id": self.new_id("st_"),
                "reference_id": r["reference_id"],
                "rail": r["rail_selected"],
                "settlement_status": status,
                "settlement_timestamp": iso(time.time()),
                "batch_window": r.get("batch_window"),
                "source_institution_code": r.get("source_institution_code"),
                "destination_institution_code": r.get("destination_institution_code"),
                "amount": r.get("amount"),
            }, f"{r['rail_selected']} {status}")
            self.count(f"settlement_{status}")
        return action

    def webhook(self, r, event_id, event_type, attempt):
        def action():
            self.send(WEBHOOK_TOPIC, r["reference_id"], {
                "id": event_id,                 # same id on every redelivery
                "type": event_type,
                "data": {
                    "reference_id": r["reference_id"],
                    "amount": r.get("amount"),
                    "currency": r.get("currency"),
                    "rail": r["rail_selected"],
                },
                "delivery_attempt": attempt,
                "received_timestamp": iso(time.time()),
            }, f"{event_type} attempt {attempt}")
            self.count("webhook_deliveries")
        return action

    # ---- one routed transfer ------------------------------------------------
    def pick_defect(self):
        a = self.args
        if a.no_defects:
            return None
        r = self.rng.random()
        for name, rate in [("stuck", a.stuck_rate), ("missing_webhook", a.missing_webhook_rate),
                           ("duplicate_webhook", a.duplicate_webhook_rate),
                           ("out_of_order", a.out_of_order_rate), ("late", a.late_rate)]:
            if r < rate:
                return name
            r -= rate
        return None

    def handle(self, r):
        ref, rail = r.get("reference_id"), r.get("rail_selected")
        if not ref or rail not in ("INSTAPAY", "PESONET"):
            print(f"  skipped routing event without reference_id/rail: {r}", file=sys.stderr)
            return
        if ref in self.seen:
            self.count("duplicate_routing_events_ignored")
            return
        self.seen.add(ref)
        self.count(f"routed_{rail}")

        now = time.time()
        defect = self.pick_defect()
        if defect == "late" and rail != "INSTAPAY":
            defect = None               # "late" is the InstaPay SLA breach; PESONet waits for its window anyway
        if defect:
            self.count(f"defect_{defect}")
        failed = not self.args.no_defects and self.rng.random() < self.args.fail_rate
        status = "failed" if failed else "settled"

        if rail == "INSTAPAY":
            due = epoch(r.get("settlement_due")) or now
            settle_at = (max(now, due) + self.rng.uniform(5, 30) if defect == "late"
                         else now + self.rng.uniform(1, 10))
        else:
            self.at(now + self.rng.uniform(0.5, 2), self.settlement(r, "pending"))
            window = epoch(r.get("batch_window")) or now
            settle_at = (max(now, window) + self.rng.uniform(0, 120) if self.args.real_windows
                         else now + self.args.pesonet_delay)

        print(f"{datetime.now(MANILA):%H:%M:%S}  {'routed':<17} {ref:<27} {rail}, "
              f"final settlement in {settle_at - now:,.0f}s" + (f"   <- defect: {defect}" if defect else ""))

        if defect == "stuck":
            return
        webhook_at = settle_at + self.rng.uniform(0.5, 3)
        if defect == "out_of_order":
            settle_at, webhook_at = webhook_at + self.rng.uniform(2, 5), settle_at
        self.at(settle_at, self.settlement(r, status))

        if defect == "missing_webhook":
            return
        event_id = self.new_id("evt_")
        event_type = "payment.failed" if failed else "payment.paid"
        self.at(webhook_at, self.webhook(r, event_id, event_type, 1))
        if defect == "duplicate_webhook":
            for attempt in range(2, 2 + self.rng.randint(1, 2)):
                webhook_at += self.rng.uniform(2, 10)
                self.at(webhook_at, self.webhook(r, event_id, event_type, attempt))


def main():
    p = argparse.ArgumentParser(description="Simulate InstaPay/PESONet settlement and PayMongo webhooks "
                                            "for every transfer Job B routes.")
    p.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    p.add_argument("--group", default="settlement-simulator", help="Kafka consumer group id")
    p.add_argument("--from-latest", action="store_true",
                   help="for a group with no saved position: skip routing events published before now")
    p.add_argument("--real-windows", action="store_true",
                   help="settle PESONet at its real batch_window instead of after --pesonet-delay")
    p.add_argument("--pesonet-delay", type=float, default=30.0,
                   help="demo mode: seconds from routing to PESONet settlement (default 30)")
    p.add_argument("--no-defects", action="store_true", help="no failures and no injected defects")
    p.add_argument("--fail-rate", type=float, default=0.03, help="share settled as failed (default 0.03)")
    p.add_argument("--stuck-rate", type=float, default=0.03)
    p.add_argument("--missing-webhook-rate", type=float, default=0.05)
    p.add_argument("--duplicate-webhook-rate", type=float, default=0.10)
    p.add_argument("--out-of-order-rate", type=float, default=0.05)
    p.add_argument("--late-rate", type=float, default=0.05, help="InstaPay SLA breaches (default 0.05)")
    p.add_argument("--seed", type=int, help="random seed, for a repeatable run")
    args = p.parse_args()

    consumer = KafkaConsumer(ROUTING_TOPIC, bootstrap_servers=args.bootstrap, group_id=args.group,
                             auto_offset_reset="latest" if args.from_latest else "earliest")
    producer = KafkaProducer(bootstrap_servers=args.bootstrap, acks="all", linger_ms=5)
    sim = Simulator(args, producer)
    print(f"waiting for {ROUTING_TOPIC} (group {args.group}); Ctrl+C to stop", file=sys.stderr)
    try:
        while True:
            batches = consumer.poll(timeout_ms=int(min(1.0, sim.seconds_to_next()) * 1000))
            for records in batches.values():
                for rec in records:
                    try:
                        sim.handle(json.loads(rec.value))
                    except ValueError:
                        print(f"  skipped non-JSON routing event at offset {rec.offset}", file=sys.stderr)
            sim.run_due()
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        consumer.close()
        print(f"stats: {', '.join(f'{k}={v}' for k, v in sorted(sim.stats.items()))}", file=sys.stderr)
        if sim.queue:
            print(f"{len(sim.queue)} scheduled events were not sent; those transfers will look stuck.",
                  file=sys.stderr)


if __name__ == "__main__":
    main()
