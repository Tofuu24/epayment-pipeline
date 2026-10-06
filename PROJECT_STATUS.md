# Distributed Streaming Architecture for Philippine E-Payment Rail Routing & Settlement Monitoring

## What This Project Is

A real-time CDC (Change Data Capture) pipeline that monitors Philippine e-payment institution data and validates payment routing decisions using BSP InstaPay/PESONet rules and PayMongo's API model.

**Team:** Bauza, Castillo, Chan, Conejos, Santos

---

## Architecture

```
MongoDB (source of truth)
   │
   │  Change Stream (CDC)
   ▼
Kafka Connect (MongoSourceConnector)
   │
   │  Produces to topic: institution-registry-events
   ▼
Apache Kafka (KRaft, single broker)
   │
   ├──► Spark Job A: Registry Sync
   │       Reads institution change events → upserts into Cassandra
   │
   └──► Spark Job B: Payment Validation
           Reads payment intents → looks up institution registry in Cassandra
           → validates rail routing (InstaPay vs PESONet) → writes results
   │
   ▼
Apache Cassandra (query-optimized store)
```

### Technology Versions

| Component        | Version     | Notes                                      |
|------------------|-------------|--------------------------------------------|
| MongoDB          | 8.3         | Native Windows service, replica set `rs0`  |
| Apache Kafka     | 4.2.1       | KRaft mode (no ZooKeeper)                  |
| Kafka Connect    | 4.2.1       | Standalone mode                            |
| MongoDB Connector| 3.1.0       | `mongo-kafka-connect-3.1.0-all.jar`        |
| Apache Spark     | TBD         | PySpark, Structured Streaming              |
| Apache Cassandra | TBD         | Docker container                           |

---

## Current Status

### ✅ Completed

**MongoDB (Step 1–2)**
- Installed MongoDB 8.3 natively on Windows via MSI
- Configured as single-node replica set (`rs0`) — required for Change Streams
- Seeded `payment_metadata.institution_registry` with 5 institutions:
  - `BANK_UNIONBANK`, `BANK_BPI`, `BANK_LANDBANK`, `EMI_GCASH`, `EMI_MAYA`
- Each document contains: `institution_code`, `institution_name`, `institution_type`, `supported_rails`, `max_transaction_amount`, `settlement_schedule`, `active` flag

**Kafka Broker (Step 3)**
- Installed Kafka 4.2.1 on Windows, running from WSL (`.sh` scripts)
- KRaft storage formatted with `--standalone` flag
- Broker starts successfully on `localhost:9092`
- Pivoted from native Windows `.bat` to WSL `.sh` due to `wmic` deprecation on Windows 24H2+

**Kafka Connect + MongoDB Source Connector (Step 4)**
- Downloaded `mongo-kafka-connect-3.1.0-all.jar` (uber JAR with all dependencies)
- Configured standalone Connect with:
  - `startup.mode=copy_existing` — replays existing documents on first run
  - `topic.namespace.map` — routes `payment_metadata.institution_registry` → `institution-registry-events`
  - `schemas.enable=false` on both key and value converters
- WSL mirrored networking enabled (`.wslconfig` with `networkingMode=mirrored`) so WSL processes can reach Windows MongoDB on `localhost:27017`
- Connector starts, connects to MongoDB, copies all 5 seeded documents, and enters live watch mode

**CDC Verification (Step 5)**
- Connector successfully copied 5 existing documents into `institution-registry-events` topic
- Live change detection confirmed — updates in `mongosh` appear in Kafka consumer within seconds

### 🔧 In Progress

**Docker Compose for Team Sharing**
- Building a single `docker-compose.yml` that bundles:
  - MongoDB (replica set, auto-seeded)
  - Kafka (KRaft, single broker)
  - Kafka Connect (with MongoDB connector baked in)
  - Cassandra
- Goal: any teammate runs `docker compose up` and has the full pipeline running without manual setup
- Eliminates dependency on one person's laptop being online

### ⏳ Not Yet Started

**Cassandra Setup (Step 7)**
- Schema design for `institution_registry` table
- Schema design for payment validation results table
- Will run as a Docker container, exposed on `localhost:9042`

**Spark Job A — Registry Sync (Step 8)**
- PySpark Structured Streaming job
- Reads from `institution-registry-events` Kafka topic
- Parses MongoDB CDC envelope → extracts `fullDocument`
- Upserts into Cassandra `institution_registry` table
- Uses `foreachBatch` with `outputMode("append")`

**Spark Job B — Payment Validation (Step 9)**
- PySpark Structured Streaming job
- Reads from a `payment-intents` Kafka topic (to be created)
- Looks up institution in Cassandra to validate:
  - Is the institution active?
  - Does it support the requested rail (InstaPay / PESONet)?
  - Is the amount within `max_transaction_amount`?
- Writes validation results (approved/rejected + reason) to Cassandra

**Spark Environment**
- Recommended: install Spark on WSL to avoid `winutils.exe` headache
- Spark package versions must match Kafka 4.2.1 (not the 3.5.3 in earlier drafts)
- Needs `spark-sql-kafka` and `spark-cassandra-connector` packages

---

## Key Lessons Learned So Far

1. **Change Streams require a replica set.** Even a single-node MongoDB needs `rs.initiate()` before Kafka Connect can open a change stream.

2. **`startup.mode=copy_existing` is essential.** Without it, the connector only sees changes made *after* it starts — the 5 seeded documents would be invisible.

3. **`topic.namespace.map` over `topic.prefix`.** Using `topic.prefix` produces topic names like `prefix.db.collection`, which is hard to work with. The namespace map gives a clean, explicit topic name.

4. **Use the `-all.jar` (uber JAR).** The plain `mongo-kafka-connect` JAR is missing its transitive dependencies and fails at runtime.

5. **WSL mirrored networking solves cross-boundary localhost access.** Without `networkingMode=mirrored` in `.wslconfig`, WSL processes can't reach Windows services on `localhost`.

6. **Kafka 4.2.1 is KRaft-only.** No ZooKeeper. The `kafka-storage.sh format` command requires `--standalone` for a single-node setup.

7. **Windows `.bat` scripts may fail on 24H2+.** The `wmic` command is deprecated. Use WSL `.sh` scripts instead.

---

## Repository Structure (Planned)

```
epayment-rail-routing/
├── docker-compose.yml
├── Dockerfile.connect
├── mongo-init/
│   └── seed.js
├── connect-config/
│   └── mongo-source-connector.properties
├── spark-jobs/
│   ├── registry_sync.py        # Job A
│   └── payment_validation.py   # Job B
├── cassandra-schema/
│   └── init.cql
├── PROJECT_STATUS.md
└── README.md
```

---

## How to Run (Once Docker Compose Is Ready)

```bash
# 1. Start the full pipeline
docker compose up -d

# 2. Verify CDC is flowing (should show 5 events)
docker exec -it kafka \
  kafka-console-consumer.sh --topic institution-registry-events \
  --from-beginning --bootstrap-server localhost:9092

# 3. Run Spark Job A (from WSL or local Spark install)
spark-submit --packages <kafka-and-cassandra-packages> spark-jobs/registry_sync.py

# 4. Run Spark Job B
spark-submit --packages <kafka-and-cassandra-packages> spark-jobs/payment_validation.py
```

---

*Last updated: September 25, 2026*
