"""Payment-intent producer: the synthetic merchant/bank side of the pipeline.

Sends PayMongo-style Payment Intents to payment-intent-events at a steady rate,
with a small share of deliberately bad or awkward messages (proposal v1 section
6.5) so Job B's rejection paths get exercised. It never picks the rail; Job B does.

    pip install -r simulator/requirements.txt
    python simulator/producer.py                          # 2 intents/s until Ctrl+C
    python simulator/producer.py --rate 10 --count 200    # 200 intents, then stop
    python simulator/producer.py --count 5 --dry-run      # print, don't send

Message format: SPARK_GUIDE.md section 5.
"""
import argparse
import json
import math
import random
import string
import sys
import time
from collections import deque
from datetime import datetime, timedelta, timezone

TOPIC = "payment-intent-events"
MANILA = timezone(timedelta(hours=8))   # no DST in the Philippines; avoids needing tzdata on Windows
INSTAPAY_CAP_CENTAVOS = 5_000_000       # only used to aim amounts around the cap; Job B applies it

# Mirrors mongo-init/seed.js. payment_method_allowed uses PayMongo payment method
# types; the registry's bank_code prefix (dob..., brankas...) says which one.
INSTITUTIONS = {
    "BANK_UNIONBANK": ["brankas"],
    "BANK_BPI":       ["dob"],
    "BANK_LANDBANK":  ["brankas"],
    "EMI_GCASH":      ["gcash"],
    "EMI_MAYA":       ["paymaya"],
}
CHANNELS = ["mobile", "web", "api", "branch"]
ID_CHARS = string.ascii_letters + string.digits

# What each injected defect is, and how Job B should treat it.
DEFECTS = {
    "duplicate":           "an earlier intent sent again, same id (Job B keeps one row)",
    "late":                "created_at 10-120 s in the past, i.e. arrives out of order (still routed)",
    "unknown_destination": "destination BANK_XYZ (REJECTED / UNKNOWN_DESTINATION)",
    "missing_source":      "no metadata.source_institution_code (REJECTED / INVALID_PAYLOAD)",
    "negative_amount":     "amount < 0 (REJECTED / INVALID_AMOUNT)",
    "wrong_currency":      "currency USD (REJECTED / UNSUPPORTED_CURRENCY)",
    "missing_id":          "no id (goes to invalid_intent_events)",
    "not_json":            "not JSON at all (goes to invalid_intent_events)",
}


def random_amount(rng):
    """Centavos. Mostly InstaPay-sized, some PESONet-sized, a few exactly at the cap."""
    r = rng.random()
    if r < 0.70:    # PHP 100 - 50,000, log-uniform so small transfers dominate
        return int(10 ** rng.uniform(4, math.log10(INSTAPAY_CAP_CENTAVOS)))
    if r < 0.95:    # PHP 50,000.01 - 1,000,000
        return int(10 ** rng.uniform(math.log10(INSTAPAY_CAP_CENTAVOS + 1), 8))
    return rng.choice([INSTAPAY_CAP_CENTAVOS, INSTAPAY_CAP_CENTAVOS + 1])


def make_intent(rng, now):
    src, dst = rng.sample(list(INSTITUTIONS), 2)
    return {
        "id": "pi_" + "".join(rng.choices(ID_CHARS, k=24)),
        "amount": random_amount(rng),
        "currency": "PHP",
        "status": "processing",
        "payment_method_allowed": INSTITUTIONS[src],
        "created_at": now.isoformat(timespec="seconds"),
        # Custom routing fields go inside PayMongo's metadata, never at the top level.
        "metadata": {
            "source_institution_code": src,
            "destination_institution_code": dst,
            "channel": rng.choice(CHANNELS),
        },
    }


def apply_defect(rng, kind, intent, recent):
    """Returns (key, value_text) for the damaged message."""
    if kind == "duplicate":
        intent = rng.choice(recent)
    elif kind == "late":
        created = datetime.fromisoformat(intent["created_at"])
        intent["created_at"] = (created - timedelta(seconds=rng.randint(10, 120))).isoformat()
    elif kind == "unknown_destination":
        intent["metadata"]["destination_institution_code"] = "BANK_XYZ"
    elif kind == "missing_source":
        del intent["metadata"]["source_institution_code"]
    elif kind == "negative_amount":
        intent["amount"] = -intent["amount"]
    elif kind == "wrong_currency":
        intent["currency"] = "USD"
    elif kind == "missing_id":
        del intent["id"]
    elif kind == "not_json":
        return None, "this is not json"
    return intent.get("id"), json.dumps(intent)


def describe(intent_text):
    try:
        i = json.loads(intent_text)
        md = i.get("metadata", {})
        return (f"{i.get('id', '(no id)'):<27} {md.get('source_institution_code', '?'):>14} -> "
                f"{md.get('destination_institution_code', '?'):<14} "
                f"{i.get('currency', '?')} {i.get('amount', 0) / 100:>14,.2f}")
    except ValueError:
        return intent_text


def main():
    p = argparse.ArgumentParser(description="Send synthetic PayMongo-style payment intents to Kafka.")
    p.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    p.add_argument("--rate", type=float, default=2.0, help="intents per second (default 2)")
    p.add_argument("--count", type=int, default=0, help="stop after N messages (default 0 = run until Ctrl+C)")
    p.add_argument("--defect-rate", type=float, default=0.10,
                   help="share of messages with an injected defect, 0-1 (default 0.10)")
    p.add_argument("--seed", type=int, help="random seed, for a repeatable run")
    p.add_argument("--dry-run", action="store_true", help="print the JSON lines instead of sending them")
    args = p.parse_args()

    rng = random.Random(args.seed)
    producer = None
    if not args.dry_run:
        from kafka import KafkaProducer     # imported here so --dry-run works without Kafka
        producer = KafkaProducer(bootstrap_servers=args.bootstrap, acks="all", linger_ms=5)

    def on_error(exc):
        print(f"  !! send failed: {exc}", file=sys.stderr)

    recent = deque(maxlen=50)   # candidates for the "duplicate" defect
    sent, defects = 0, {}
    interval = 1.0 / args.rate
    next_at = time.monotonic()
    try:
        while args.count == 0 or sent < args.count:
            intent = make_intent(rng, datetime.now(MANILA))
            kind = rng.choice(list(DEFECTS)) if rng.random() < args.defect_rate else None
            if kind == "duplicate" and not recent:
                kind = None     # nothing to duplicate yet
            if kind:
                key, value = apply_defect(rng, kind, intent, list(recent))
                defects[kind] = defects.get(kind, 0) + 1
            else:
                key, value = intent["id"], json.dumps(intent)
                recent.append(intent)

            if args.dry_run:
                print(value)
            else:
                producer.send(TOPIC, key=key.encode() if key else None,
                              value=value.encode()).add_errback(on_error)
                print(f"[{sent + 1:>5}] {describe(value)}" + (f"   <- defect: {kind}" if kind else ""))
            sent += 1

            next_at += interval
            time.sleep(max(0.0, next_at - time.monotonic()))
    except KeyboardInterrupt:
        pass
    finally:
        if producer:
            producer.flush()
            producer.close()
    print(f"sent {sent} messages to {TOPIC}"
          + (f"; defects: {', '.join(f'{k}={v}' for k, v in sorted(defects.items()))}" if defects else ""),
          file=sys.stderr)


if __name__ == "__main__":
    main()
