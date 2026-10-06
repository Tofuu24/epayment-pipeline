#!/usr/bin/env bash
# Recreate the pipeline topics with N partitions and clear the pipeline's data, for a
# clean benchmark run. Run from WSL (or any bash with docker) in the project folder:
#   ./benchmark/reset_topics.sh 3
# Keeps the institution registry (MongoDB, its topic, and Cassandra's institution_registry),
# so Job A can keep running. Stop Jobs B and C first; this deletes their checkpoints.
set -euo pipefail
N="${1:?usage: reset_topics.sh <partitions>}"
TOPICS="payment-intent-events rail-routing-events settlement-events webhook-events"
KT="docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092"

for t in $TOPICS; do
  $KT --delete --if-exists --topic "$t"
done
sleep 3   # topic deletion is asynchronous
for t in $TOPICS; do
  $KT --create --topic "$t" --partitions "$N" --replication-factor 1
done
$KT --describe --topic payment-intent-events | head -1

for tbl in transaction_lifecycle_by_reference settlement_monitoring_by_institution \
           invalid_intent_events settlement_alerts_by_rail webhook_events_by_reference; do
  docker exec cassandra cqlsh -e "TRUNCATE payment_pipeline.$tbl"
done

rm -rf ~/checkpoints/validate_ledger ~/checkpoints/settlement_monitor
echo "Topics recreated with $N partition(s); pipeline tables truncated; Job B/C checkpoints removed."
echo "Start Job B (and C) again with SPARK master local[$N] or more, e.g. spark-submit --master local[$N] ..."
