# Event Formats

Every topic carries one JSON object per Kafka message, as UTF-8 text. Messages on the four
payment topics are keyed by `reference_id`, so all events for one transfer land on the same
partition and stay in order.

Conventions, following PayMongo's public API:
- **Amounts are integers in centavos** (`150000` = PHP 1,500.00). Never decimals or strings.
- **Currency** is an ISO 4217 code; only `PHP` is accepted.
- **Timestamps** are ISO 8601 with an offset (`2026-09-25T09:30:00+08:00`), except inside
  PayMongo webhook objects, which use Unix seconds, as PayMongo does.
- **`metadata`** is a free-form nested object, kept as raw JSON text in the ledger.

| Topic | Producer | Consumers |
| ----- | -------- | --------- |
| `institution-registry-events` | Kafka Connect (MongoDB) | Job A |
| `payment-intent-events` | institutions (simulator / test file) | Job B |
| `rail-routing-events` | Job B | Job C, simulator |
| `settlement-events` | clearing rails (simulator / test file) | Job C |
| `webhook-events` | PayMongo (simulator / test file) | Job C |

## institution-registry-events

The full current MongoDB document; see README "Message format".

## payment-intent-events

```json
{"reference_id": "PAY-001", "source_institution_code": "BANK_BPI",
 "destination_institution_code": "EMI_GCASH", "amount": 150000, "currency": "PHP",
 "created_at": "2026-09-25T09:30:00+08:00", "metadata": {"channel": "mobile"}}
```

All fields except `metadata` are required. The producer never chooses the rail.

## rail-routing-events

One message per decided intent, written by Job B. Fields that are null are omitted.

```json
{"reference_id": "PAY-002", "status": "ROUTED", "rail": "PESONET",
 "routing_reason": "ABOVE_INSTAPAY_CAP", "source_institution_code": "BANK_UNIONBANK",
 "destination_institution_code": "BANK_BPI", "amount": 7500000, "currency": "PHP",
 "created_at": "2026-09-25T09:30:00.000+08:00", "batch_window": "2026-09-25T10:00:00.000+08:00",
 "settlement_due": "2026-09-25T10:00:00.000+08:00", "decided_at": "2026-09-25T09:30:02.000+08:00"}

{"reference_id": "PAY-007", "status": "REJECTED", "rejection_reason": "UNKNOWN_DESTINATION",
 "source_institution_code": "BANK_BPI", "destination_institution_code": "BANK_XYZ",
 "amount": 100000, "currency": "PHP", "created_at": "2026-09-25T11:00:00.000+08:00",
 "decided_at": "2026-09-25T09:30:02.000+08:00"}
```

Duplicates (`DUPLICATE_REFERENCE_ID`) and unkeyable messages are not published here.

## settlement-events

The clearing rail's confirmation for one routed transfer.

```json
{"settlement_id": "stl_test_pay001", "reference_id": "PAY-001", "rail": "INSTAPAY",
 "status": "SETTLED", "failure_reason": null, "settled_at": "2026-09-25T09:30:05+08:00"}

{"settlement_id": "stl_test_pay003", "reference_id": "PAY-003", "rail": "PESONET",
 "status": "FAILED", "failure_reason": "ACCOUNT_CLOSED", "settled_at": "2026-09-25T16:05:00+08:00"}
```

- `status` is `SETTLED` or `FAILED`; anything else is ignored.
- `settled_at` is the event time Job C uses for its watermark and turnaround.
- The simulator's failure reasons are `ACCOUNT_CLOSED`, `INVALID_ACCOUNT`,
  `BENEFICIARY_BANK_TIMEOUT` and `AML_HOLD` (illustrative, not an official code list).

## webhook-events

A PayMongo event object: `data.attributes.type` is the event type and `data.attributes.data` is
the resource the event is about (a payment), which carries our `reference_id` in its `metadata`.

```json
{"data": {"id": "evt_test_001", "type": "event", "attributes": {
  "type": "payment.paid", "livemode": false,
  "data": {"id": "pay_test_pay001", "type": "payment", "attributes": {
    "amount": 150000, "currency": "PHP", "status": "paid",
    "metadata": {"reference_id": "PAY-001"}}},
  "created_at": 1790299807, "updated_at": 1790299807}}}
```

- Event types used: `payment.paid` (settled) and `payment.failed` (failed).
- A retried delivery is the **same message with the same event id** (`data.id`). Job C keeps the
  first and drops repeats within `WEBHOOK_DEDUP_WINDOW`.
- Only the fields above are read. The team should check the exact shape against PayMongo's
  current webhook documentation before citing it in the report.
