"""Job C state machine, pure Python (no Spark)."""
import settlement_monitor_job as C

T0 = 1_800_000_000_000          # arbitrary epoch ms
S = 1000


def routed(ref="R", rail="INSTAPAY", at=T0, due=None):
    due = due if due is not None else at + 30 * S
    return {"kind": "ROUTED", "reference_id": ref, "event_ms": at, "rail": rail,
            "source_institution_code": "BANK_BPI", "destination_institution_code": "EMI_GCASH",
            "amount": 100000, "created_ms": at, "due_ms": due,
            "settle_status": None, "failure_reason": None}


def settled(at, status="SETTLED", reason=None):
    return {"kind": "SETTLEMENT", "reference_id": "R", "event_ms": at, "rail": "INSTAPAY",
            "source_institution_code": None, "destination_institution_code": None,
            "amount": None, "created_ms": None, "due_ms": None,
            "settle_status": status, "failure_reason": reason}


def test_routed_then_settled_on_time():
    s, ch = C.apply_events(None, [routed()])
    assert ch and s["status"] == "AWAITING_SETTLEMENT"
    assert C.next_timeout_ms(s, T0) == T0 + 30 * S + C.INSTAPAY_GRACE_SECONDS * S
    s, ch = C.apply_events(s, [settled(T0 + 5 * S)])
    assert ch and s["status"] == "SETTLED"
    assert C.output_row("R", s)["turnaround_ms"] == 5 * S


def test_both_events_in_one_batch():
    s, ch = C.apply_events(None, [settled(T0 + 4 * S), routed()])   # order in batch doesn't matter
    assert s["status"] == "SETTLED" and C.output_row("R", s)["turnaround_ms"] == 4 * S


def test_settlement_before_routing_event_waits():
    s, ch = C.apply_events(None, [settled(T0 + 4 * S)])
    assert not ch and s["routed"] is None and s["pend_status"] == "SETTLED"
    s, ch = C.apply_events(s, [routed()])
    assert ch and s["status"] == "SETTLED"


def test_settled_after_deadline_is_late():
    s, _ = C.apply_events(None, [routed()])
    s, _ = C.apply_events(s, [settled(T0 + 30 * S + C.INSTAPAY_GRACE_SECONDS * S + 1)])
    assert s["status"] == "SETTLED_LATE"


def test_no_confirmation_becomes_stuck_then_late():
    s, _ = C.apply_events(None, [routed()])
    s, ch = C.on_timeout(s)
    assert ch and s["status"] == "STUCK"
    assert s["changed_ms"] == C.deadline_ms(s)
    s, ch = C.apply_events(s, [settled(T0 + 600 * S)])
    assert ch and s["status"] == "SETTLED_LATE"


def test_failure_is_recorded():
    s, _ = C.apply_events(None, [routed()])
    s, _ = C.apply_events(s, [settled(T0 + 2 * S, "FAILED", "ACCOUNT_CLOSED")])
    assert s["status"] == "FAILED" and s["failure"] == "ACCOUNT_CLOSED"


def test_repeated_events_are_ignored():
    s, _ = C.apply_events(None, [routed(), settled(T0 + 2 * S)])
    s2, ch = C.apply_events(s, [routed(), settled(T0 + 9 * S, "FAILED")])
    assert not ch and s2["status"] == "SETTLED" and s2["settled_ms"] == T0 + 2 * S


def test_finished_transfer_is_forgotten_after_retention():
    s, _ = C.apply_events(None, [routed(), settled(T0 + 2 * S)])
    assert C.next_timeout_ms(s, T0) == C.deadline_ms(s) + C.STATE_RETENTION_SECONDS * S
    assert C.on_timeout(s) == (None, False)


def test_pesonet_uses_its_own_grace():
    s, _ = C.apply_events(None, [routed(rail="PESONET", due=T0 + 3600 * S)])
    assert C.next_timeout_ms(s, T0) == T0 + 3600 * S + C.PESONET_GRACE_SECONDS * S


def test_clean_converts_pandas_values():
    import numpy as np
    assert C._clean("amount", float("nan")) is None
    assert C._clean("event_ms", np.float64(1.7e12)) == 1_700_000_000_000
    assert isinstance(C._clean("event_ms", np.int64(5)), int)
