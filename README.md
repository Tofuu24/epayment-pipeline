# E-Payment Pipeline — Local Stack

MongoDB → Kafka Connect → Kafka → Spark → Cassandra, all infrastructure in Docker Compose.
Spark runs **outside** Compose, on your own machine; see [SPARK_GUIDE.md](SPARK_GUIDE.md).

```
MongoDB (institution_registry)
   │  change stream
   ▼
Kafka Connect (MongoDB source connector, standalone mode)
   │
   ▼
Kafka topic: institution-registry-events      Kafka topic: payment-intent-events
   │                                               │
   │                                               │
   ▼                                               ▼
Spark Job A (registry sync)            Spark Job B (validate, route, ledger)
   │                                        ▲      │
   ▼                                        │      ├──► Cassandra: transaction_lifecycle_by_reference,
Cassandra: institution_registry ────────────┘      │               settlement_monitoring_by_institution,
          (Job B re-reads it every micro-batch)    │               invalid_intent_events
                                                   ▼
                                     Kafka topic: rail-routing-events
                                                   │
                                                   ▼
                                     simulator/consumer.py (settlement simulator)
                                                   │
                                                   ▼
                                     Kafka topics: settlement-events, webhook-events  ──► Job C (next)
```

`payment-intent-events` is fed by `simulator/producer.py` (live traffic) or `test_intents.jsonl`
(fixed rule check). See [SPARK_GUIDE.md](SPARK_GUIDE.md) sections 5-7.

> **Don't edit `docker-compose.yml` unless you're the person who maintains it.**
> This file is known to work. If something breaks, report it (with `docker compose ps -a`
> and `docker compose logs <service>` output) rather than patching it locally.

## Prerequisites

- **Docker Desktop** running (Windows or macOS), or Docker Engine + Compose v2 on Linux.
- Give Docker at least **4 GB RAM** (Cassandra alone uses about 1.5 GB).
- **Ports 9092, 9042 and 27018 must be free.** If you previously installed Kafka natively
  (e.g. in WSL), stop that broker and its Kafka Connect worker first, or `localhost:9092`
  will point at the wrong Kafka.

## Start it

```bash
docker compose up -d --build
```

The first run pulls images and downloads the MongoDB connector JAR (a few minutes). After
that, it takes about **30–60 seconds** to become ready. Check with:

```bash
docker compose ps -a
```

Expected when ready:

| Service          | Expected status          | What it does                                              |
| ---------------- | ------------------------ | --------------------------------------------------------- |
| `mongodb`        | Up (healthy)             | MongoDB 8.0, single-node replica set `rs0`                |
| `mongo-init`     | **Exited (0)**           | Initiates the replica set and seeds the 5 institutions     |
| `kafka`          | Up (healthy)             | Kafka 4.2.1, KRaft mode, single broker                     |
| `kafka-init`     | **Exited (0)**           | Creates the 5 topics (`institution-registry-events`, `payment-intent-events`, `rail-routing-events`, `settlement-events`, `webhook-events`) |
| `kafka-connect`  | Up                       | Streams MongoDB changes into Kafka                         |
| `cassandra`      | Up (healthy)             | Cassandra 5.0                                              |
| `cassandra-init` | **Exited (0)**           | Creates keyspace `payment_pipeline` and its 4 tables       |

The `-init` services are one-shot jobs, so **Exited (0) means success**. Any other exit
code means that step failed; check `docker compose logs <service>`.

## Verify the CDC pipeline

**1. The 5 seeded institutions are on the topic:**

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic institution-registry-events --from-beginning --timeout-ms 10000
```

You should see 5 JSON documents (UnionBank, BPI, Landbank, GCash, Maya). The consumer exits after
10 seconds with a `TimeoutException`; that's expected.

**2. Live changes flow through.** Leave this running in one terminal:

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --topic institution-registry-events
```

and in another terminal (PowerShell: escape the `$` as `` `$set ``):

```bash
docker exec mongodb mongosh --quiet payment_metadata --eval "db.institution_registry.updateOne({institution_code:'BANK_BPI'},{\$set:{active:false}})"
```

A new BPI document with `"active": false` appears within a couple of seconds.

## Message format

Each message on `institution-registry-events` is the **whole current document**, as plain JSON:

```json
{"_id": "6ab5...", "institution_code": "BANK_BPI", "active": true, "bank_code": "dobbpi",
 "institution_name": "BPI", "rail_eligibility": ["INSTAPAY", "PESONET"]}
```

- Inserts and updates both produce the full document (not a diff, not the change-stream envelope).
- **Deletes produce no message.** To remove an institution, set `active: false` instead.

## Connecting from your machine

| What                    | Address                                                   |
| ----------------------- | --------------------------------------------------------- |
| Kafka (Spark, clients)  | `localhost:9092`                                          |
| Cassandra (Spark, CQL)  | `127.0.0.1:9042`                                          |
| MongoDB (mongosh/Compass) | `mongodb://localhost:27018/?directConnection=true`      |

Notes:
- MongoDB is on **27018**, not 27017, so it can't clash with a native MongoDB install.
- Use `directConnection=true` for MongoDB from the host. `?replicaSet=rs0` won't work from outside
  Docker because the replica set member is named `mongodb:27017`, which only resolves inside Compose.
- Inside Compose, services use `kafka:19092`, `mongodb:27017` and `cassandra:9042`.

Handy shells without installing anything:

```bash
docker exec -it mongodb mongosh payment_metadata
docker exec -it cassandra cqlsh -k payment_pipeline
```

## Common tasks

| Task | Command |
| ---- | ------- |
| Stop (keep data) | `docker compose down` |
| Stop and **wipe everything** (fresh reseed on next up) | `docker compose down -v` |
| Connector logs | `docker compose logs -f kafka-connect` |
| Connector status | `docker exec kafka-connect curl -s localhost:8083/connectors/mongo-institution-source/status` |
| After editing `connect-config/*.properties` | `docker compose restart kafka-connect` |
| After adding tables to `cassandra-init/schema.cql` | `docker compose up -d cassandra-init` |
| After adding topics to `kafka-init` in `docker-compose.yml` | `docker compose up -d kafka-init` |
| Install the simulator's Python dependency | `pip install -r simulator/requirements.txt` |
| Live intents / settlements (see SPARK_GUIDE.md section 7) | `python simulator/producer.py` / `python simulator/consumer.py` |
| Send the test payment intents (PowerShell, project folder) | `Get-Content test_intents.jsonl \| docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:19092 --topic payment-intent-events` |

> After `docker compose down -v`, Kafka's topics are recreated empty. Anyone running Spark must
> also delete their Spark checkpoint folders (`rm -rf ~/checkpoints/*` in WSL), or Spark will fail
> on offsets that no longer exist.

## Project layout

```
docker-compose.yml                       all services
Dockerfile.connect                       Kafka 4.2.1 image + MongoDB connector 3.1.0 (-all jar)
connect-config/connect-standalone.properties   Connect worker settings
connect-config/mongo-source-connector.properties  the source connector (topic map, copy_existing, output format)
mongo-init/seed.js                       replica set init + 5 institutions (idempotent)
cassandra-init/schema.cql                keyspace + tables (idempotent)
spark/registry_sync_job.py               Spark Job A: registry sync, Kafka -> Cassandra
spark/validate_and_ledger_job.py         Spark Job B: validate, route, assign PESONet windows, ledger
simulator/producer.py                    live PayMongo-style payment intents -> payment-intent-events
simulator/consumer.py                    settlement simulator: rail-routing-events -> settlement-events, webhook-events
simulator/requirements.txt               kafka-python, for the two scripts above
test_intents.jsonl                       14 test payment intents covering every routing rule
SPARK_GUIDE.md                           how to run the Spark jobs against this stack
```

## Troubleshooting

- **`kafka-connect` keeps restarting:** `docker compose logs kafka-connect | grep -i error`. Most often
  MongoDB isn't a replica set yet; check that `mongo-init` exited with 0.
- **`port is already allocated`:** something else is on 9092, 9042 or 27018. Stop it, then `docker compose up -d`.
- **Nothing on the topic after `down -v` / `up`:** wait about 30s. The connector re-copies the collection
  on a fresh start.
- **Cassandra `unhealthy`:** it's usually short on memory. Give Docker Desktop more RAM.
- **`container name "/kafka" is already in use`:** an old container from earlier experiments has the
  same name. List them with `docker ps -a`, remove the old one with `docker rm -f kafka`, then
  `docker compose up -d --build` again.
