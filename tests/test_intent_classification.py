"""Job B duplicate / replay handling (classify_intents)."""
import json

import validate_and_ledger_job as J


def keyed(spark, rows):
    raw = spark.createDataFrame(
        [("payment-intent-events", p, o, json.dumps({"reference_id": ref, "amount": 1}))
         for ref, p, o in rows],
        "kafka_topic string, kafka_partition int, kafka_offset long, raw_value string")
    return J.parse_intents(raw).filter(J.has_reference_id())


def existing(spark, rows):
    return spark.createDataFrame(rows, "reference_id string, kafka_partition int, kafka_offset long")


def refs(df):
    return sorted((r.reference_id, r.kafka_offset) for r in df.collect())


def test_new_intents_pass_through(spark):
    new, dups = J.classify_intents(keyed(spark, [("A", 0, 1), ("B", 0, 2)]), existing(spark, []))
    assert refs(new) == [("A", 1), ("B", 2)] and refs(dups) == []


def test_duplicate_within_batch_keeps_earliest_offset(spark):
    new, dups = J.classify_intents(keyed(spark, [("A", 0, 9), ("A", 0, 3), ("A", 0, 5)]),
                                   existing(spark, []))
    assert refs(new) == [("A", 3)]
    assert refs(dups) == [("A", 5), ("A", 9)]


def test_resent_reference_id_is_a_duplicate_not_a_redecision(spark):
    new, dups = J.classify_intents(keyed(spark, [("A", 0, 20)]), existing(spark, [("A", 0, 3)]))
    assert refs(new) == [] and refs(dups) == [("A", 20)]


def test_replay_of_same_message_is_skipped(spark):
    # Same reference_id at the same Kafka position as the ledger row: checkpoint reset.
    new, dups = J.classify_intents(keyed(spark, [("A", 0, 3)]), existing(spark, [("A", 0, 3)]))
    assert refs(new) == [] and refs(dups) == []


def test_mixed_batch(spark):
    batch = keyed(spark, [("OLD", 0, 1), ("OLD", 0, 7), ("NEW", 0, 8), ("NEW", 0, 9)])
    new, dups = J.classify_intents(batch, existing(spark, [("OLD", 0, 1)]))
    assert refs(new) == [("NEW", 8)]
    assert refs(dups) == [("NEW", 9), ("OLD", 7)]
