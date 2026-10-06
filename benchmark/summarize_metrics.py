"""Summarise the METRICS_FILE written by Job B / Job C (one Spark progress JSON per line).

  python benchmark/summarize_metrics.py ~/metrics/jobB_rate1000_p3.jsonl [more files...]

Prints one row per file and query: batches, input rows, average input and processing
rates, and batch duration percentiles. Empty batches are skipped. Add --csv for CSV.
"""
import argparse
import json
import statistics


def pct(values, p):
    if not values:
        return 0
    s = sorted(values)
    return s[min(len(s) - 1, int(round(p / 100 * (len(s) - 1))))]


def summarize(path):
    by_query = {}
    for line in open(path):
        line = line.strip()
        if not line:
            continue
        p = json.loads(line)
        if p.get("numInputRows", 0) == 0:
            continue
        by_query.setdefault(p.get("name") or "query", []).append(p)
    rows = []
    for name, ps in by_query.items():
        durations = [p["durationMs"].get("triggerExecution", 0) for p in ps]
        rows.append({
            "file": path, "query": name, "batches": len(ps),
            "input_rows": sum(p["numInputRows"] for p in ps),
            "avg_input_rows_per_s": round(statistics.mean(p.get("inputRowsPerSecond") or 0 for p in ps), 1),
            "avg_processed_rows_per_s": round(statistics.mean(p.get("processedRowsPerSecond") or 0 for p in ps), 1),
            "batch_ms_p50": pct(durations, 50), "batch_ms_p95": pct(durations, 95),
            "batch_ms_max": max(durations),
        })
    return rows


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="+")
    ap.add_argument("--csv", action="store_true")
    a = ap.parse_args()
    rows = [r for f in a.files for r in summarize(f)]
    if not rows:
        raise SystemExit("no non-empty batches found")
    cols = list(rows[0])
    if a.csv:
        print(",".join(cols))
        for r in rows:
            print(",".join(str(r[c]) for c in cols))
    else:
        print("| " + " | ".join(cols) + " |")
        print("|" + "---|" * len(cols))
        for r in rows:
            print("| " + " | ".join(str(r[c]) for c in cols) + " |")
