# Notes for the Final Report

Material for the report's implementation, discussion and limitations sections. It maps each
proposal objective to what was built, records the design decisions and the places where the
implementation differs from the proposal, and lists what still needs verifying.

## 1. Objectives → implementation

| Objective (proposal §V.B) | Where it is implemented | Evidence |
| ------------------------- | ----------------------- | -------- |
| 1. Ingestion modeling across partitioned Kafka topics, PayMongo-based payloads | `simulator/simulator.py`; topics with `TOPIC_PARTITIONS` (default 3), keyed by `reference_id`; `EVENT_FORMATS.md` | `tests/test_simulator.py` |
| 2. ₱50,000 InstaPay cap and PESONet 10 AM / 1 PM / 4 PM windows | Job B `route()` | `tests/test_routing.py` (cap boundary, every window case, weekends, time zones) |
| 3. SLA and stuck/unconfirmed detection with stateful, watermarked processing | Job C settlement tracking (`applyInPandasWithState`, event-time timeouts) | `tests/test_settlement_state.py`, `tests/test_settlement_stream.py` |
| 3. (§IV.B item 4) Filter duplicate webhook deliveries | Job C `dedupe_webhooks` (`dropDuplicatesWithinWatermark`) | `test_webhook_duplicates_dropped`, demo files |
| (§IV.A) Duplicate detection | Job B `classify_intents`: first occurrence wins, later ones logged as `DUPLICATE_REFERENCE_ID`, replays skipped | `tests/test_intent_classification.py`, `tests/test_sinks.py` |
| 4. Cassandra schemas for high-write logging and single-partition lookups, benchmarked | `cassandra-init/schema.cql`; `benchmark/` | `benchmark/BENCHMARK.md` (results to be filled in) |
| 5. Grounding in PayMongo conventions | Centavo integers, ISO currency, nested metadata, PayMongo event objects with retried deliveries | `EVENT_FORMATS.md` |
| 6. Scope boundary | No ML, no live transfers, no real bank data; all data synthetic | — |

## 2. Design decisions worth explaining

**Registry as change data capture (not in the proposal).** Institution eligibility lives in
MongoDB and streams through Kafka Connect into Cassandra (Job A). Job B re-reads it every
micro-batch, so deactivating an institution affects routing within seconds without restarting
anything. This shows CDC as a second ingestion pattern next to event ingestion.

**Each payment decided exactly once.** Kafka and Spark give at-least-once delivery, and senders
retry. Job B therefore treats the ledger as the source of truth: the first Kafka message for a
`reference_id` is decided; later ones are recorded as duplicates, never re-decided; re-reading
an already-decided message (same Kafka position) is a replay and is skipped. The ledger row is
written after the other outputs, so a crash mid-batch simply causes the batch to be redone.

**Column ownership between jobs.** Job B writes the routing columns, Job C the settlement
columns of the same rows. Neither job writes the other's columns, so the order in which they
reach Cassandra doesn't matter (Cassandra has no cross-job transactions).

**State machine instead of a stream-stream join (deviation from §V.B.3).** The proposal says
"stateful watermarked stream joins". A Spark stream-stream outer join needs one fixed time
bound between the two sides, and its unmatched rows are only emitted once the watermark passes
that bound. Deadlines here range from 60 seconds (InstaPay with grace) to about three days
(a PESONet transfer made on Friday evening clears Monday 10:00). One bound wide enough for
PESONet would delay InstaPay stuck detection by days. Job C therefore joins the two streams
inside `applyInPandasWithState`, grouped by `reference_id`, and gives each transfer its own
event-time timeout at `settlement_due + grace`. It is still a stateful, watermarked join of two
asynchronous streams; the difference is per-key deadlines. Suggested wording: "stateful
watermarked stream join implemented with per-key event-time timeouts".

**Watermark delay trade-off.** A short delay detects stuck transfers sooner, but confirmations
that arrive more than the delay behind the newest event are dropped as late, and the transfer
stays STUCK. A long delay tolerates disorder but delays detection. The demo with fixed test files
needs 12 hours (a whole day of events arrives in one burst); live simulated traffic works with
minutes. Good material for the discussion section.

**Simulated clock.** The simulator's clock runs `--speed` times faster than real time, so
PESONet windows hours apart pass in minutes during a demo. All event-time logic in Spark uses
the events' own timestamps, so it behaves the same at any speed. Benchmarks use `--speed 1`.

## 3. Grounding: what is sourced and what is a parameter

| Item | Value used | Status |
| ---- | ---------- | ------ |
| InstaPay per-transaction cap | ₱50,000, inclusive | From the proposal. **Verify** the exact circular and whether the cap is inclusive. |
| PESONet clearing windows | 10:00, 13:00, 16:00 Manila, banking days | From the proposal. **Verify** against the cited circulars; holidays not modeled. |
| InstaPay settlement SLA | 30 s (`INSTAPAY_SLA_SECONDS`) | **Team-chosen demo parameter**, not a BSP figure. Label it as such unless a source is found. |
| Stuck grace periods | 30 s InstaPay, 1 h PESONet | **Team-chosen** parameters. |
| PayMongo payload conventions | centavos, ISO currency, metadata, `payment.paid`/`payment.failed`, event ids reused on retry | **Verify** field names against PayMongo's current docs before citing. |
| Rail eligibility of the 5 institutions | `mongo-init/seed.js` | Illustrative; Landbank as PESONet-only and the e-wallets as InstaPay-only are modeling choices to exercise every rule. |
| Simulated outcome rates | 2% failed, 5% late, 3% unconfirmed, 10% duplicate webhooks | Arbitrary test mix, configurable. |

The BSP circular numbers in the proposal (980, 1033, 1196, 1238) are cited there; the code
doesn't depend on anything beyond the cap and windows above. Check each circular says what the
report attributes to it.

## 4. Limitations

- Single-node everything (one Kafka broker, one Cassandra node with replication factor 1,
  Spark local mode); benchmark numbers are relative, not production capacity.
- Philippine public holidays are not modeled for PESONet windows.
- Deleting an institution in MongoDB produces no event; deactivate with `active: false`.
- Job B validates against the registry as it is when the intent is processed, not as it was
  at `created_at`.
- Job B depends on Job A being up; there is no staleness check on the registry.
- A confirmation arriving later than `WATERMARK_DELAY` behind the newest event is dropped; the
  transfer stays STUCK.
- The watermark only advances while events arrive; with no traffic, nothing times out.
- If Job B crashes between writing a settlement-monitoring row and the ledger row, and the
  registry changes before the batch is redone, the redone decision can differ, leaving one
  stale monitoring row. The window is a single micro-batch.
- Validation is strict about types (string or fractional amounts and lowercase currency are
  rejected), by design.
- All data is synthetic; no real accounts or funds (as the proposal requires).

## 5. Still to do by the team

- Run SPARK_GUIDE section 6 (including 6b) on the real stack after pulling these changes.
- Run the benchmarks and fill in the tables in `benchmark/BENCHMARK.md`.
- Verify the items marked **Verify** in section 3.
- Get approval from whoever maintains `docker-compose.yml` for the partition change.
