# Spark Guide — Running Job A and Job B Against the Compose Stack

This is for whoever owns the Spark side. The infrastructure (MongoDB, Kafka, Kafka Connect,
Cassandra) comes from `docker compose up -d --build` (see [README.md](README.md)). You only
install and run Spark.

**Tested combination** (Job A and Job B verified end to end on Windows 11 + WSL: a MongoDB
registry change reaches Cassandra and changes Job B's validation within seconds, no restart):

| Component | Version |
| --------- | ------- |
| Spark | 3.5.7 (Scala 2.12, Java 17, Python 3) |
| Kafka source package | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7` |
| Cassandra connector | `com.datastax.spark:spark-cassandra-connector_2.12:3.5.1` |

> **Use Spark 3.5.7, not 4.x.** Connector 3.5.1 is built for Spark 3.5 only. If you have Spark 4
> installed on Windows, leave it alone and install 3.5.7 in WSL as below; the two don't interfere.
> Typing `spark-submit` in PowerShell will still run your Windows copy, so always run the jobs from WSL.

## 1. What the stack gives you

| Thing | Where | Notes |
| ----- | ----- | ----- |
| Kafka | `localhost:9092` | |
| Topic `institution-registry-events` | Kafka | One message per insert/update in MongoDB, value = the full document as JSON. Starts with the 5 seeded institutions. Read by Job A. |
| Topic `payment-intent-events` | Kafka | Payment intents (format in section 5). Written by `simulator/producer.py` or `test_intents.jsonl`; read by Job B. |
| Topic `rail-routing-events` | Kafka | One message per routed transfer (format in section 5). Written by Job B; read by `simulator/consumer.py` and Job C. |
| Topics `settlement-events`, `webhook-events` | Kafka | Settlement results and PayMongo-style webhooks (format in section 7). Written by `simulator/consumer.py`; to be read by Job C (settlement/SLA monitoring). |
| Cassandra | `127.0.0.1:9042` | Keyspace `payment_pipeline` with 4 tables (section 5). |

A message on `institution-registry-events` looks like:

```json
{"_id": "6ab5...", "institution_code": "BANK_BPI", "active": true, "bank_code": "dobbpi",
 "institution_name": "BPI", "rail_eligibility": ["INSTAPAY", "PESONET"]}
```

It's the plain document, not a change-stream envelope. Deletes in MongoDB produce **no** message;
deactivate an institution with `active: false` instead.

The topic is an **event log, not a table**: every change appends a new line, so an institution
you've edited three times appears three times. The current state lives in Cassandra's
`institution_registry` (always one row per institution), which Job A maintains.

## 2. Install Spark 3.5.7 in WSL

WSL avoids the `winutils.exe` / `HADOOP_HOME` setup that native Windows Spark needs.
In an **Ubuntu** terminal:

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk-headless python3 netcat-openbsd

cd ~
wget https://archive.apache.org/dist/spark/spark-3.5.7/spark-3.5.7-bin-hadoop3.tgz
tar xzf spark-3.5.7-bin-hadoop3.tgz
echo 'export SPARK_HOME=~/spark-3.5.7-bin-hadoop3' >> ~/.bashrc
echo 'export PATH=$SPARK_HOME/bin:$PATH' >> ~/.bashrc
echo 'export SPARK_LOCAL_IP=127.0.0.1' >> ~/.bashrc    # silences a harmless hostname warning
source ~/.bashrc

spark-submit --version     # should say version 3.5.7, Scala 2.12
```

## 3. Make WSL see the Docker ports on `localhost`

By default (NAT mode), `localhost` inside WSL is **not** Windows' localhost, so Spark can't
reach Kafka or Cassandra. Switch WSL to mirrored networking (Windows 11 22H2+).

In PowerShell:

```powershell
notepad $env:USERPROFILE\.wslconfig
```

Put this in the file and save:

```ini
[wsl2]
networkingMode=mirrored
```

Then restart WSL. This also restarts Docker Desktop, so bring the stack back up afterwards:

```powershell
wsl --shutdown
# wait until Docker Desktop shows the engine running, then:
docker compose up -d
```

Reopen Ubuntu and check both ports:

```bash
nc -zv localhost 9092     # Connection to localhost 9092 port [tcp/*] succeeded!
nc -zv localhost 9042
```

**Make sure no other Kafka is running in WSL.** If you ever ran Kafka natively
(`kafka-server-start.sh`), stop it. Otherwise `localhost:9092` hits that broker instead of the
Docker one, and you'll connect fine but see **0 messages**. Check with:

```bash
ss -ltnp | grep 9092      # should show NO java process; Docker's port doesn't appear here
```

## 4. Run the jobs: one Ubuntu tab each

Both jobs run at the same time, **each in its own Ubuntu tab**, and stay running. Job B depends
on Job A: if Job A stops, Job B keeps validating against a stale registry.

Both use the same `--packages`. The first run downloads them (about 75 MB, cached in `~/.ivy2`).

**Tab 1: Job A (registry sync)**

```bash
cd /mnt/c/path/to/epayment-pipeline/spark
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  registry_sync_job.py
```

It prints one line per batch, e.g. `[registry batch 3] EMI_GCASH=inactive`, so you can see
it's alive. If a batch contains several events for the same institution (e.g. after a restart),
it keeps only the newest one by Kafka offset, so an older event never overwrites a newer one.

**Tab 2: Job B (validate, route, ledger)**

```bash
cd /mnt/c/path/to/epayment-pipeline/spark
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  validate_and_ledger_job.py
```

It prints one summary line per batch, e.g. `[batch 0] REJECTED/NO_ELIGIBLE_RAIL: 2, ROUTED/INSTAPAY: 2, ...`.

Both jobs read `startingOffsets=earliest` on their first run and then resume from their checkpoint
(section 8). `KAFKA_BOOTSTRAP_SERVERS`, `CASSANDRA_HOST` and `CHECKPOINT_DIR` can be overridden
with environment variables; the defaults already match the stack.

**Verify Job A:**

```bash
docker exec cassandra cqlsh -e "SELECT institution_code, active, rail_eligibility FROM payment_pipeline.institution_registry"
```

You should see 5 rows. Change something in MongoDB and re-run the SELECT; it updates within seconds:

```bash
docker exec mongodb mongosh --quiet payment_metadata --eval "db.institution_registry.updateOne({institution_code:'EMI_MAYA'},{\$set:{active:false}})"
```

(In PowerShell, write `` `$set `` instead of `\$set`.) Set it back to `true` afterwards.

## 5. What Job B does

### Input: `payment-intent-events`

One PayMongo-style Payment Intent per message:

```json
{"id": "PAY-001", "amount": 150000, "currency": "PHP", "status": "processing",
 "payment_method_allowed": ["dob"], "created_at": "2026-09-25T09:30:00+08:00",
 "metadata": {"source_institution_code": "BANK_BPI",
              "destination_institution_code": "EMI_GCASH", "channel": "mobile"}}
```

| Field | Source | Used by Job B |
| ----- | ------ | ------------- |
| `id` | PayMongo Payment Intent `id`. Becomes `reference_id` everywhere downstream. | Key |
| `amount` | PayMongo: **centavos** (`150000` = PHP 1,500.00) | Yes |
| `currency` | PayMongo | Yes (must be `PHP`) |
| `status` | PayMongo Payment Intent status; the simulator sends `processing` | No |
| `payment_method_allowed` | PayMongo payment method types (`dob`, `brankas`, `gcash`, `paymaya`) | No |
| `created_at` | ISO 8601 with `+08:00` (without an offset it's read as Manila time) | Yes (batch window, InstaPay due time) |
| `metadata.source_institution_code` | **Custom**, nested in PayMongo's `metadata` extension field | Yes |
| `metadata.destination_institution_code` | **Custom**, nested in `metadata` | Yes |
| `metadata.*` (anything else, e.g. `channel`) | Custom, free-form | Stored in `metadata_json` |

- Our custom fields go **inside `metadata`**, never at the top level (proposal v1 section 2, v2 slide 10).
  An intent with `source_institution_code` at the top level is `REJECTED / INVALID_PAYLOAD`.
- The producer does **not** choose the rail. Job B does.

### Rules, in the order they're checked (first match wins)

| Check | Result |
| ----- | ------ |
| A required field is missing or unparseable | `REJECTED / INVALID_PAYLOAD` |
| `amount <= 0` | `REJECTED / INVALID_AMOUNT` |
| `currency` isn't `PHP` | `REJECTED / UNSUPPORTED_CURRENCY` |
| Source not in the registry / inactive | `REJECTED / UNKNOWN_SOURCE` / `INACTIVE_SOURCE` |
| Destination not in the registry / inactive | `REJECTED / UNKNOWN_DESTINATION` / `INACTIVE_DESTINATION` |
| Amount <= PHP 50,000 and both sides support InstaPay | `ROUTED / INSTAPAY` (`WITHIN_INSTAPAY_CAP`) |
| Amount > PHP 50,000 and both sides support PESONet | `ROUTED / PESONET` (`ABOVE_INSTAPAY_CAP`) |
| Amount <= PHP 50,000, InstaPay not possible, both support PESONet | `ROUTED / PESONET` (`INSTAPAY_NOT_SUPPORTED_BY_PARTICIPANT`) |
| Anything else | `REJECTED / NO_ELIGIBLE_RAIL` |

- The PHP 50,000 cap is **inclusive** (exactly 5,000,000 centavos stays on InstaPay).
- The PESONet fallback (third ROUTED row) is a team decision. To reject those instead, start
  Job B with `ALLOW_PESONET_FALLBACK=false`.
- The registry is **re-read from Cassandra on every micro-batch**, so a MongoDB change affects
  validation within seconds, with no restart.
- Messages that aren't JSON or have no `id` can't be keyed, so they go to
  `invalid_intent_events` (by Kafka offset) instead of being dropped.

### PESONet batch windows (Asia/Manila time)

- Assigned to the next of **10:00, 13:00, 16:00** *strictly after* `created_at`. A transfer
  arriving exactly at 10:00 has missed the 10:00 batch and goes to 13:00.
- After 16:00, it goes to the next day's 10:00.
- A window on Saturday or Sunday moves to **Monday 10:00**.
- **Not modeled:** Philippine public holidays.

### `settlement_due`

- PESONet: the batch window.
- InstaPay: `created_at` + `INSTAPAY_SLA_SECONDS` (default 30). This is a **team-chosen demo
  target, not a BSP figure**; override with the env var.

### Output tables

| Table | Key | Holds |
| ----- | --- | ----- |
| `transaction_lifecycle_by_reference` | `reference_id` | Every keyed intent and its decision (status, rail, reasons, window, due time) |
| `settlement_monitoring_by_institution` | `(rail, source_institution_code)`, then `settlement_due` DESC | Routed transfers per sending institution, latest deadline first |
| `invalid_intent_events` | `kafka_topic`, then partition and offset | Messages that couldn't be keyed |

**Timestamps display in UTC** in cqlsh (`+0000`). Add 8 hours for Manila time:
`02:00` = 10 AM, `05:00` = 1 PM, `08:00` = 4 PM.

### Output topic: `rail-routing-events`

After the Cassandra writes, Job B publishes one message per **ROUTED** intent (rejections are
only in Cassandra). Key = `reference_id`. Timestamps are ISO 8601 in Manila time:

```json
{"reference_id": "PAY-002", "source_institution_code": "BANK_UNIONBANK",
 "destination_institution_code": "BANK_BPI", "amount": 7500000, "currency": "PHP",
 "rail_selected": "PESONET", "routing_reason": "ABOVE_INSTAPAY_CAP", "threshold_applied": 5000000,
 "created_at": "2026-09-25T09:30:00.000+08:00", "batch_window": "2026-09-25T10:00:00.000+08:00",
 "settlement_due": "2026-09-25T10:00:00.000+08:00", "routing_timestamp": "2026-09-30T14:02:11.412+08:00"}
```

- `rail_selected`: `INSTAPAY` or `PESONET`. `threshold_applied`: the InstaPay cap in centavos.
- `batch_window` is `null` for InstaPay (always present, never missing).
- **At-least-once:** if a micro-batch is replayed (crash, deleted checkpoint), its routing events are
  published again. Anything reading this topic must treat a repeated `reference_id` as a duplicate.

**Known limitation:** re-sending an intent with an `id` that already exists makes Job B
decide it again and overwrite the earlier row. Always use a new `id` when testing.
Proper duplicate handling is planned for Job C.

## 6. Test with `test_intents.jsonl`

With both jobs running, from PowerShell in the project folder:

```powershell
Get-Content test_intents.jsonl | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:19092 --topic payment-intent-events
```

Then:

```powershell
docker exec cassandra cqlsh -e "SELECT reference_id, status, rail, routing_reason, rejection_reason, batch_window FROM payment_pipeline.transaction_lifecycle_by_reference"
```

Expected (12 rows, plus 2 rows in `invalid_intent_events`):

| Intent | What it tests | Expected | `batch_window` (UTC → Manila) |
| ------ | ------------- | -------- | ----------------------------- |
| PAY-001 | PHP 1,500 BPI → GCash | ROUTED INSTAPAY | — |
| PAY-002 | PHP 75,000 at 09:30 | ROUTED PESONET | 02:00 → 10:00 |
| PAY-003 | PHP 120,000 at 14:20 | ROUTED PESONET | 08:00 → 16:00 |
| PAY-004 | Friday 16:45 | ROUTED PESONET | Mon 02:00 → **Mon 10:00** |
| PAY-005 | PHP 2,000 to Landbank (PESONet only) | ROUTED PESONET, fallback | 05:00 → 13:00 |
| PAY-006 | PHP 80,000 to GCash (InstaPay only) | REJECTED NO_ELIGIBLE_RAIL | — |
| PAY-007 | Unknown bank `BANK_XYZ` | REJECTED UNKNOWN_DESTINATION | — |
| PAY-008 | Exactly PHP 50,000 | ROUTED INSTAPAY | — |
| PAY-009 | PHP 50,000.01, e-wallet to e-wallet | REJECTED NO_ELIGIBLE_RAIL | — |
| PAY-010 | Negative amount | REJECTED INVALID_AMOUNT | — |
| PAY-011 | USD | REJECTED UNSUPPORTED_CURRENCY | — |
| PAY-012 | Arrives exactly at 10:00 | ROUTED PESONET | 05:00 → **13:00** |
| (no `id`), `this is not json` | Unkeyable messages | in `invalid_intent_events` | — |

The 7 ROUTED intents also appear on `rail-routing-events`:

```powershell
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic rail-routing-events --from-beginning --timeout-ms 10000
```

A per-institution lookup (the single-partition query from the proposal):

```powershell
docker exec cassandra cqlsh -e "SELECT reference_id, amount, settlement_due FROM payment_pipeline.settlement_monitoring_by_institution WHERE rail='PESONET' AND source_institution_code='BANK_BPI'"
```

### Live registry demo

Shows a MongoDB change altering validation with no restart:

1. Deactivate GCash (PowerShell):
   ```powershell
   docker exec mongodb mongosh --quiet payment_metadata --eval "db.institution_registry.updateOne({institution_code:'EMI_GCASH'},{`$set:{active:false}})"
   ```
2. Wait for Job A's tab to print `EMI_GCASH=inactive`.
3. Send an intent to GCash with a **new** `id`:
   ```powershell
   '{"id": "PAY-100", "amount": 100000, "currency": "PHP", "status": "processing", "payment_method_allowed": ["dob"], "created_at": "2026-09-25T11:00:00+08:00", "metadata": {"source_institution_code": "BANK_BPI", "destination_institution_code": "EMI_GCASH"}}' | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:19092 --topic payment-intent-events
   ```
4. It comes back `REJECTED / INACTIVE_DESTINATION`; PAY-001 (decided earlier) stays ROUTED.
5. Set GCash back to `active:true`.

## 7. Live traffic: `simulator/producer.py` and `simulator/consumer.py`

`test_intents.jsonl` is a fixed rule check. For continuous traffic, and for anything on the
settlement and webhook topics, use the two simulator scripts. They're plain Python (no Spark) and
run from PowerShell or WSL, against `localhost:9092`:

```bash
pip install -r simulator/requirements.txt
```

```
producer.py ──► payment-intent-events ──► Job B ──► rail-routing-events
                                                          │
consumer.py ◄─────────────────────────────────────────────┘
   ├──► settlement-events
   └──► webhook-events ──► (Job C, next)
```

With Job A and Job B running, open two more terminals:

**Terminal 3: the settlement simulator (start it first)**

```bash
python simulator/consumer.py
```

**Terminal 4: the intent producer**

```bash
python simulator/producer.py                  # 2 intents/s until Ctrl+C
python simulator/producer.py --rate 10 --count 200
python simulator/producer.py --count 5 --dry-run   # just print 5 intents
```

Every run of either script takes `--seed N` for repeatable output; `--help` lists all options.

### Producer

Sends intents in the section 5 format with `created_at` = now, from the 5 seeded institutions.
About 70% are InstaPay-sized, 25% above the cap, and 5% exactly PHP 50,000.00 or 50,000.01.
`--defect-rate` (default 0.10) mixes in one of these per damaged message:

| Defect | What Job B does with it |
| ------ | ----------------------- |
| `duplicate` (earlier intent re-sent, same `id`) | Keeps one ledger row; publishes the routing event again |
| `late` (`created_at` 10–120 s in the past) | Routes it normally (out-of-order arrival) |
| `unknown_destination`, `missing_source`, `negative_amount`, `wrong_currency` | `REJECTED` with the matching reason |
| `missing_id`, `not_json` | `invalid_intent_events` |

### Consumer (settlement simulator)

Reads `rail-routing-events` (consumer group `settlement-simulator`) and plays the rail and
PayMongo for every routed transfer. It never routes anything itself; it uses Job B's decision.

| Rail | What it sends |
| ---- | ------------- |
| InstaPay | `settled` (or `failed`, 3%) 1–10 s after routing, then a `payment.paid` / `payment.failed` webhook |
| PESONet | `pending` right away; `settled`/`failed` **30 s later** (`--pesonet-delay`), then the webhook |
| PESONet with `--real-windows` | `pending` right away; `settled`/`failed` at the real `batch_window` (can be days away) |

Real windows are hours apart, too slow for a demo, so the default is the 30 s demo delay.
It also injects these defects (proposal v1 section 6.5); `--no-defects` turns them and failures off:

| Defect | Rate flag (default) | Effect |
| ------ | ------------------- | ------ |
| `duplicate_webhook` | `--duplicate-webhook-rate` (0.10) | Same webhook `id` delivered 2–3 times, `delivery_attempt` 1, 2, 3 |
| `missing_webhook` | `--missing-webhook-rate` (0.05) | Settlement arrives, webhook never does |
| `stuck` | `--stuck-rate` (0.03) | No final settlement and no webhook (PESONet still gets `pending`) |
| `out_of_order` | `--out-of-order-rate` (0.05) | Webhook arrives before the settlement event |
| `late` | `--late-rate` (0.05) | InstaPay settles 5–30 s **after** `settlement_due` (SLA breach) |

- It skips a `reference_id` it has already handled, since routing events are at-least-once.
- On first start it reads `rail-routing-events` from the beginning, so it also settles anything Job B
  routed earlier (e.g. the test intents). `--from-latest` skips those; it only applies to a group
  with no saved position (use a new `--group` name to start over).
- Scheduled events are **in memory**. Stop the script and anything still scheduled is never sent;
  those transfers will look stuck. It prints how many on exit.

### `settlement-events` format

Key = `reference_id`. One `pending` (PESONet only) and one final event per transfer:

```json
{"id": "st_q3N0...", "reference_id": "pi_TJ7i...", "rail": "PESONET", "settlement_status": "settled",
 "settlement_timestamp": "2026-09-30T14:02:41.118+08:00", "batch_window": "2026-09-30T16:00:00.000+08:00",
 "source_institution_code": "BANK_BPI", "destination_institution_code": "BANK_UNIONBANK", "amount": 7500000}
```

- `settlement_status`: `pending`, `settled` or `failed` (proposal v1 section 6.3).
- `settlement_timestamp` is when the event was sent. In demo mode a PESONet transfer settles before
  its `batch_window`; that's the 30 s shortcut, not a bug.

### `webhook-events` format

Key = `reference_id`. Shaped on PayMongo's webhook event, flattened as in proposal v1 section 6.4:

```json
{"id": "evt_8KfL...", "type": "payment.paid",
 "data": {"reference_id": "pi_TJ7i...", "amount": 289874, "currency": "PHP", "rail": "INSTAPAY"},
 "delivery_attempt": 1, "received_timestamp": "2026-09-30T14:02:19.530+08:00"}
```

- `type`: `payment.paid` or `payment.failed`.
- Redeliveries keep the **same `id`** with a higher `delivery_attempt`, so `id` is the de-duplication key.

## 8. Checkpoints and resets

Each job stores its Kafka read position in a checkpoint folder:
`~/checkpoints/registry_sync` (Job A) and `~/checkpoints/validate_ledger` (Job B).
They're in your home folder, not `/tmp`, because WSL can clear `/tmp` on restart.

- Stop a job with Ctrl+C and start it again: it resumes where it left off. That's normal.
- **After `docker compose down -v`** (the stack is wiped and topics recreated empty), delete the
  checkpoints before restarting the jobs, or Spark fails on offsets that no longer exist:
  ```bash
  rm -rf ~/checkpoints/registry_sync ~/checkpoints/validate_ledger
  ```
- **After pulling a changed job script**, delete that job's checkpoint too. Deleting Job A's is
  always safe (it replays the topic and keeps the latest state). Deleting Job B's makes it
  reprocess every intent: the Cassandra rows are the same (writes are upserts by key), but every
  routed intent is published to `rail-routing-events` again, and `consumer.py` will ignore
  the ones it has already seen only if it has been running since then.
- **The intent format changed** (fields moved into `metadata`, `reference_id` became `id`). Old
  flat-format messages still on `payment-intent-events` can't be keyed any more. For a clean start:
  ```bash
  docker compose down -v && docker compose up -d --build     # PowerShell: run the two separately
  rm -rf ~/checkpoints/registry_sync ~/checkpoints/validate_ledger   # in WSL
  ```

## 9. Troubleshooting

| Symptom | Cause / fix |
| ------- | ----------- |
| Registry change reaches Kafka but Cassandra doesn't update | Job A isn't running. Check its tab; restart it. It catches up from its checkpoint. |
| Intent sent but no row appears in Cassandra | Job B isn't running. Check its tab; restart it. It processes the waiting messages. |
| Job B accepts an intent for an institution you just deactivated | Job A was down, so Cassandra still has the old state. Check Job A's tab. |
| Job runs but Cassandra stays empty; console consumer shows 0 messages | Another Kafka is on port 9092 in WSL (section 3). Stop it. |
| `Connection refused` to `localhost:9092` or `9042` | WSL is in NAT mode (section 3), or the stack isn't up (`docker compose ps -a`). |
| `Failed to find data source: kafka` / `org.apache.spark.sql.cassandra` | Missing `--packages`, or versions don't match your Spark (section 2). |
| `NoSuchMethodError` / `ClassNotFoundException` mentioning Cassandra | You're running Spark 4.x. Run from WSL with Spark 3.5.7. |
| `Partition ... offset ... is out of range` / data loss error | Stack was reset with `down -v`; delete checkpoints (section 8). |
| All columns `null` in Cassandra | Field names in the JSON don't match the schema; compare with the console consumer output. |
| Every intent is `REJECTED / INVALID_PAYLOAD`, or lands in `invalid_intent_events` | Old flat format (`reference_id`, `source_institution_code` at the top level). Use `id` and put the institution codes inside `metadata` (section 5). |
| `consumer.py` prints nothing | Job B isn't publishing: check it's running the current script and that intents are `ROUTED`. Check the topic with the console consumer (section 6). |
| `NoBrokersAvailable` from a simulator script | The stack isn't up, or (in WSL) networking isn't mirrored (section 3). |
| Batch windows look 8 hours off | They aren't: cqlsh shows UTC. Add 8 hours for Manila time. |
