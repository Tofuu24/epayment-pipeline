"""Time the single-partition lookups the schema was designed for, and (optionally) the
end-to-end decision latency, against the running stack.

  pip install cassandra-driver
  python benchmark/cassandra_lookup_bench.py --samples 2000

Lookups timed (prepared statements, one partition each):
  1. transaction_lifecycle_by_reference WHERE reference_id = ?
  2. settlement_monitoring_by_institution WHERE rail = ? AND source_institution_code = ? LIMIT 50
--latency also reports processed_at - created_at for sampled intents; only meaningful
for runs where the simulator used --speed 1 (real time).
"""
import argparse
import random
import statistics
import time

from cassandra.cluster import Cluster

RAILS = ["INSTAPAY", "PESONET"]
INSTITUTIONS = ["BANK_UNIONBANK", "BANK_BPI", "BANK_LANDBANK", "EMI_GCASH", "EMI_MAYA"]


def pct(values, p):
    s = sorted(values)
    return s[min(len(s) - 1, int(round(p / 100 * (len(s) - 1))))]


def report(name, ms):
    print(f"{name:<45} n={len(ms):<6} mean={statistics.mean(ms):7.2f}ms  p50={pct(ms, 50):7.2f}  "
          f"p95={pct(ms, 95):7.2f}  p99={pct(ms, 99):7.2f}  max={max(ms):7.2f}")


def timed(session, stmt, params):
    t = time.perf_counter()
    rows = list(session.execute(stmt, params))
    return (time.perf_counter() - t) * 1000, rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--samples", type=int, default=1000)
    ap.add_argument("--latency", action="store_true")
    a = ap.parse_args()

    session = Cluster([a.host]).connect("payment_pipeline")
    refs = [r.reference_id for r in session.execute(
        f"SELECT reference_id FROM transaction_lifecycle_by_reference LIMIT {a.samples * 5}")]
    if not refs:
        raise SystemExit("transaction_lifecycle_by_reference is empty; run the simulator and Job B first")
    total = session.execute("SELECT count(*) FROM transaction_lifecycle_by_reference").one()[0]
    print(f"ledger rows: {total}")

    by_ref = session.prepare("SELECT * FROM transaction_lifecycle_by_reference WHERE reference_id = ?")
    by_inst = session.prepare("SELECT * FROM settlement_monitoring_by_institution "
                              "WHERE rail = ? AND source_institution_code = ? LIMIT 50")
    for _ in range(50):                                   # warm-up
        session.execute(by_ref, [random.choice(refs)])

    ref_ms, latencies = [], []
    for _ in range(a.samples):
        ms, rows = timed(session, by_ref, [random.choice(refs)])
        ref_ms.append(ms)
        if a.latency and rows and rows[0].processed_at and rows[0].created_at:
            latencies.append((rows[0].processed_at - rows[0].created_at).total_seconds() * 1000)
    inst_ms = [timed(session, by_inst, [random.choice(RAILS), random.choice(INSTITUTIONS)])[0]
               for _ in range(a.samples)]

    report("lookup by reference_id", ref_ms)
    report("lookup by (rail, source_institution) LIMIT 50", inst_ms)
    if latencies:
        report("decision latency (processed_at - created_at)", latencies)


if __name__ == "__main__":
    main()
