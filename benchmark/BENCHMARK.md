# Benchmarking (Objective 4)

What we measure, with the stack and Jobs A–B (and C for the full run) running in WSL:

1. **Throughput and batch latency** of Job B (and C) at increasing input rates: Spark's own
   per-batch progress (input rows/s, processed rows/s, batch duration).
2. **Effect of partitions**: the same rates with 1, 3 and 6 topic partitions.
3. **Cassandra single-partition lookups**: latency of the two access patterns the schema was
   designed for, once the tables hold a realistic number of rows.
4. **Cassandra write latency** as Cassandra itself reports it.
5. Optionally **end-to-end decision latency** (`processed_at − created_at`) at `--speed 1`.

Keep the machine otherwise idle, and write down its CPU, RAM and Docker memory limit: the
numbers are only meaningful relative to each other on the same machine.

## One run

In WSL, with the venv active (SPARK_GUIDE section 2), from the project folder:

```bash
P=3          # partitions for this run
RATE=1000    # intents per second

# 1. Clean slate with P partitions (stop Jobs B and C first; Job A can keep running)
./benchmark/reset_topics.sh $P

# 2. Start Job B with metrics, using at least P cores (tab 2)
mkdir -p ~/metrics
cd spark && METRICS_FILE=~/metrics/jobB_r${RATE}_p${P}.jsonl spark-submit --master "local[$P]" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  validate_and_ledger_job.py

# 3. Send 2 minutes of traffic in real time, intents only (tab 4)
python simulator/simulator.py --rate $RATE --speed 1 --duration 120 --no-rails

# 4. Let Job B catch up (its batch lines stop showing new rows), then stop it with Ctrl+C
python benchmark/summarize_metrics.py ~/metrics/jobB_r${RATE}_p${P}.jsonl
```

**Repeat** for `RATE` in 100, 500, 1000, 2000, 5000 and `P` in 1, 3, 6 (use `local[1]` and
`local[6]` accordingly). If the simulator can't keep up at the highest rate, its progress line
shows fewer intents than `rate × seconds`; run two simulators side by side.

**Reading the result:** the pipeline keeps up at a rate if `avg_processed_rows_per_s` stays at
or above `avg_input_rows_per_s` and `batch_ms_p95` doesn't keep growing. The highest such rate is
the sustainable throughput for that partition count.

## Full pipeline run (Job C)

Same as above but start Job C too (`METRICS_FILE=~/metrics/jobC_r${RATE}_p${P}.jsonl`) and run
the simulator **without** `--no-rails`, with `--speed 1`. `summarize_metrics.py` shows two Job C
queries: `settlement_tracking` and `webhook_dedup`.

## Cassandra lookups and writes

After a large run (e.g. 1000/s for 10 minutes ≈ 600k ledger rows):

```bash
python benchmark/cassandra_lookup_bench.py --samples 2000 --latency
docker exec cassandra nodetool tablehistograms payment_pipeline transaction_lifecycle_by_reference
docker exec cassandra nodetool tablehistograms payment_pipeline settlement_monitoring_by_institution
```

`tablehistograms` prints read and write latency percentiles (microseconds) as Cassandra measured
them. `--latency` is only meaningful when the simulator ran with `--speed 1`.

## Results template

| Partitions | Rate (intents/s) | Avg processed rows/s | Batch p50 (ms) | Batch p95 (ms) | Kept up? |
| ---------- | ---------------- | -------------------- | -------------- | -------------- | -------- |
| 1 | 100 | | | | |
| 1 | 1000 | | | | |
| 3 | 1000 | | | | |
| 3 | 5000 | | | | |
| 6 | 5000 | | | | |

| Lookup | Rows in table | p50 (ms) | p95 (ms) | p99 (ms) |
| ------ | ------------- | -------- | -------- | -------- |
| by `reference_id` | | | | |
| by `(rail, source_institution_code)` LIMIT 50 | | | | |

## Caveats to state in the report

- Everything runs on one laptop: one Kafka broker, one Cassandra node (replication factor 1)
  and Spark in local mode. The results show relative behaviour (partitions, rates, schema
  access patterns), not production capacity.
- Kafka, Cassandra and Spark compete for the same CPU and memory.
- Job B reads the registry and looks up existing reference_ids every micro-batch, so batch
  duration has a fixed floor even for small batches.
