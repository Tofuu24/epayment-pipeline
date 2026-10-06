# Distributed Streaming Architecture for Philippine E-Payment Rail Routing & Settlement Monitoring

## What This Project Is

A real-time CDC (Change Data Capture) pipeline that monitors Philippine e-payment institution data and validates payment routing decisions using BSP InstaPay/PESONet rules and PayMongo's API model, as well as tracking settlement SLAs.

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
   ├──► Spark Job B: Payment Validation
   │       Reads payment-intent-events → looks up institution registry in Cassandra
   │       → validates rail routing (InstaPay vs PESONet)
   │       → writes results to Cassandra and rail-routing-events topic
   │
   └──► Spark Job C: Settlement Monitoring
           Reads rail-routing-events and settlement-events
           → stateful stream-stream join to match routes with settlements
           → enforces SLAs (turnaround time)
           → writes final settlement status to Cassandra and webhook-events
   │
   ▼
Apache Cassandra (query-optimized store)
```

### Technology Versions

| Component        | Version     | Notes                                      |
|------------------|-------------|--------------------------------------------|
| MongoDB          | 8.3         | Dockerized, replica set `rs0`              |
| Apache Kafka     | 4.2.1       | Dockerized, KRaft mode (no ZooKeeper)      |
| Kafka Connect    | 4.2.1       | Dockerized, standalone mode                |
| MongoDB Connector| 3.1.0       | `mongo-kafka-connect-3.1.0-all.jar`        |
| Apache Spark     | 3.5.7       | PySpark, Structured Streaming (local WSL)  |
| Apache Cassandra | 4.1.3       | Dockerized                                 |

---

## Current Status

### ✅ Completed: Phase 1-6 (Core Infrastructure & Routing)

- **Dockerized Infrastructure**: Complete Docker Compose stack running Kafka, Kafka Connect, Cassandra, and MongoDB (as a replica set).
- **MongoDB CDC**: Kafka Connect correctly captures institution metadata changes and publishes to `institution-registry-events`.
- **Spark Job A (Registry Sync)**: Consumes CDC events and successfully upserts institution state into Cassandra's `institution_registry` table in real-time.
- **Spark Job B (Payment Validation)**: Consumes `payment-intent-events`, performs validations against Cassandra's institution registry, makes routing decisions (InstaPay/PESONet), and publishes to `rail-routing-events`. Validated to properly handle PESONet batch windows and real-time CDC state changes (e.g., rejecting payments if an institution goes offline).

### ✅ Completed: Phase 7 (Job C - Settlement Monitoring & Integration)

- **Job C Integration**: Successfully integrated Job C (`settlement_monitor_job.py`) into the pipeline.
- **Stateful Stream-Stream Join**: Job C consumes from both `rail-routing-events` and `settlement-events`, utilizing a custom PySpark stateful `apply_events` function to track SLA deadlines.
- **Out-of-Order Event Handling**: Discovered and fixed a bug in `settlement_udfs.py` where a `SETTLED` event arriving before a `ROUTED` event (due to network latency or startup delays) would crash the state machine due to a `NoneType` calculation.
- **Validation**: Full end-to-end flow validated with a `MERGE-002` test intent. The pipeline successfully ingested the intent, routed it via Job B, matched it with a `SETTLED` event in Job C, and successfully logged the turnaround SLA (`SETTLED_LATE`) to Cassandra's `transaction_lifecycle_by_reference` table.

---

## Key Achievements

The three core technologies now each serve a defensible, distinct role in the architecture:
1. **Kafka**: Serves as the central event backbone, connecting all decoupled stages (CDC → Routing → Settlement) rather than just acting as a naive input pipe.
2. **Spark**: Performs genuine stateful stream processing (cross-batch deduplication, stream-stream joins, time-bound SLA monitoring) rather than just copying data.
3. **Cassandra**: Serves tailored query patterns that justify its partition-key design (institution lookups, transaction lifecycle tracking).

---

*Last updated: October 2026*
