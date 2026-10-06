# AI Handoff & Project Context

## What This Project Is
This is a real-time Change Data Capture (CDC) pipeline designed for monitoring Philippine e-payment institution data and validating payment routing decisions using BSP InstaPay/PESONet rules and PayMongo's API model.

The architecture flows as follows:
`MongoDB (Source of Truth) -> Kafka Connect -> Apache Kafka -> Apache Spark -> Apache Cassandra`

## What We Have Built So Far
1. **MongoDB**: Installed as a replica set (`rs0`) and seeded with 5 mock institutions (banks and e-wallets like BPI, GCash, Maya) with metadata such as supported rails and active status.
2. **Apache Kafka & Connect**: Set up Kafka in KRaft mode. Kafka Connect is running with the MongoDB Source Connector (`copy_existing` enabled).
3. **CDC Ingestion**: Kafka Connect is actively capturing live document changes from MongoDB and publishing the full JSON documents into the `institution-registry-events` Kafka topic.
4. **Docker Compose**: The team has successfully unified MongoDB, Kafka, Kafka Connect, and Cassandra into a single `docker-compose.yml` for reproducible local environments.
5. **Cassandra Schema Drafted**: The keyspace (`payment_pipeline`) and schemas for the tables have been drafted in `cassandra-init/schema.cql` but wait to be fully leveraged by Spark.

---

## AI Handoff Prompt

*Copy and paste the text below into Claude, ChatGPT, or any other AI assistant to get them up to speed instantly.*

***

**System Context:**
Act as a Senior Data Engineer. We are building a real-time CDC pipeline for e-payment routing in the Philippines. I need you to take the wheel and help me implement the missing pieces for our Spark and Cassandra integration.

**Architecture:**
- **Source**: MongoDB (v8.0) replica set (`rs0`).
- **Message Broker**: Kafka (v4.2.1) running locally on port 9092. Topic `institution-registry-events` has live CDC JSON data from Mongo. A second topic `payment-intent-events` will receive test payment transactions.
- **Processing**: Apache Spark (v3.5.7). Must be run via PySpark Structured Streaming.
- **Sink**: Apache Cassandra (v5.0) running on port 9042 with a keyspace `payment_pipeline`.

**Current Status:**
The upstream ingestion (MongoDB -> Kafka Connect -> Kafka) is 100% complete and working via Docker Compose. The topics exist, and Cassandra is initialized with empty tables.

**What Needs To Be Done (Your Task):**

1. **Spark Job A (Registry Sync)**
   - Write a PySpark Structured Streaming job in Python (`spark/registry_sync_job.py`).
   - It must read from the Kafka topic `institution-registry-events` (`startingOffsets=earliest`).
   - Extract the `fullDocument` JSON (which contains `institution_code`, `active`, `rail_eligibility`, etc.).
   - Upsert the latest state of each institution into Cassandra's `institution_registry` table. Since Kafka acts as an append-only log, the Spark job must ensure it keeps the latest offset per institution. Use `foreachBatch` with `outputMode("append")`.

2. **Spark Job B (Payment Validation & Routing)**
   - Write a second PySpark job (`spark/validate_and_ledger_job.py`).
   - It reads payment intents from the Kafka topic `payment-intent-events`.
   - For every micro-batch, it must query Cassandra's `institution_registry` to check if the source and destination banks are active and which rails (InstaPay/PESONet) they support.
   - Apply routing rules (e.g., if amount <= PHP 50,000 and both support InstaPay -> Route to InstaPay. If > 50,000 -> Route to PESONet).
   - Calculate the settlement window (PESONet has specific batch windows: 10:00, 13:00, 16:00 Manila time).
   - Write the outcome (ROUTED or REJECTED with reasons) into Cassandra's `transaction_lifecycle_by_reference` table.

**Constraints:**
- Use PySpark 3.5.7 compatibility (using `spark-sql-kafka-0-10_2.12:3.5.7` and `spark-cassandra-connector_2.12:3.5.1`).
- Provide production-ready, well-commented Python scripts. 

Where should we start? Do you want to tackle Spark Job A first?
