import math
import os

INSTAPAY_GRACE_SECONDS = int(os.getenv("INSTAPAY_GRACE_SECONDS", "30"))
PESONET_GRACE_SECONDS = int(os.getenv("PESONET_GRACE_SECONDS", "3600"))
TERMINAL = {"SETTLED", "SETTLED_LATE", "FAILED"}
LONG_FIELDS = {"event_ms", "amount", "created_ms", "due_ms"}
STATE_FIELDS = ["routed", "rail", "src", "dst", "amount", "created_ms", "due_ms", "status", "settled_ms", "failure", "changed_ms", "pend_status", "pend_ms", "pend_failure"]
OUTPUT_FIELDS = ["reference_id", "rail", "source_institution_code", "destination_institution_code", "amount", "created_ms", "due_ms", "settlement_status", "settled_ms", "turnaround_ms", "failure_reason", "status_changed_ms"]

def grace_ms(rail):
    return 1000 * (INSTAPAY_GRACE_SECONDS if rail == "INSTAPAY" else PESONET_GRACE_SECONDS)

def deadline_ms(s):
    return s["due_ms"] + grace_ms(s["rail"])

def _resolve(s, status, settled_ms, failure):
    if status == "FAILED":
        s["status"], s["failure"] = "FAILED", failure
    else:
        s["status"] = "SETTLED_LATE" if settled_ms > deadline_ms(s) else "SETTLED"
    s["settled_ms"], s["changed_ms"] = settled_ms, settled_ms

def next_timeout_ms(s, watermark):
    if s["status"] in TERMINAL or s["status"] == "STUCK":
        return s["changed_ms"] + 1000 * int(os.getenv("STATE_RETENTION_SECONDS", str(24 * 3600)))
    if s["due_ms"] is None:
        # If we only got a settlement event (pend_status) but no routing event yet,
        # we don't know the SLA deadline. Keep it alive for 1 hour from watermark.
        return watermark + 3600000
    return deadline_ms(s)

def apply_events(state, events):
    s = dict(state) if state else {f: None for f in STATE_FIELDS}
    old_status = s.get("status")

    for e in sorted(events, key=lambda x: x["event_ms"]):
        if e["kind"] == "ROUTED":
            s["routed"], s["rail"], s["src"], s["dst"] = "Y", e["rail"], e["source_institution_code"], e["destination_institution_code"]
            s["amount"], s["created_ms"], s["due_ms"] = e["amount"], e["created_ms"], e["due_ms"]
            if s["status"] is None:
                s["status"], s["changed_ms"] = "AWAITING_SETTLEMENT", e["event_ms"]

            if s["pend_status"] is not None:
                _resolve(s, s["pend_status"], s["pend_ms"], s["pend_failure"])
                s["pend_status"], s["pend_ms"], s["pend_failure"] = None, None, None

        elif e["kind"] == "SETTLEMENT":
            if s["status"] in TERMINAL:
                continue
            if s["routed"] != "Y":
                if s["pend_status"] is None or s["pend_status"] == "FAILED":
                    s["pend_status"], s["pend_ms"], s["pend_failure"] = e["settle_status"], e["event_ms"], e["failure_reason"]
            else:
                _resolve(s, e["settle_status"], e["event_ms"], e["failure_reason"])
    return s, s.get("status") != old_status

def on_timeout(s):
    if s["status"] not in TERMINAL and s["status"] != "STUCK" and s["routed"] == "Y":
        s["status"], s["changed_ms"] = "STUCK", deadline_ms(s)
        return s, True
    return None, False

def _clean(name, v):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    if hasattr(v, "item"):
        v = v.item()
    return int(v) if name in LONG_FIELDS else v

def output_row(reference_id, s):
    turnaround = s["settled_ms"] - s["created_ms"] if s["settled_ms"] and s["created_ms"] else None
    return (reference_id, s["rail"], s["src"], s["dst"], s["amount"], s["created_ms"], s["due_ms"],
            s["status"], s["settled_ms"], turnaround, s["failure"], s["changed_ms"])

def track_settlement(key, pdf_iter, state):
    import pandas as pd
    reference_id = key[0]
    current = dict(zip(STATE_FIELDS, state.get)) if state.exists else None
    watermark = state.getCurrentWatermarkMs()

    if state.hasTimedOut:
        new, changed = on_timeout(current)
    else:
        rows = [{k: _clean(k, v) for k, v in r.items()} for pdf in pdf_iter for r in pdf.to_dict("records")]
        new, changed = apply_events(current, rows)

    if new is None:
        state.remove()
    else:
        state.update(tuple(new[f] for f in STATE_FIELDS))
        state.setTimeoutTimestamp(max(next_timeout_ms(new, watermark), watermark + 1))

    if changed and new is not None and new["routed"] == "Y":
        yield pd.DataFrame([output_row(reference_id, new)], columns=OUTPUT_FIELDS)
