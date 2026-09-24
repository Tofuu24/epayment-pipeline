# Spark Job A: keep Cassandra's institution_registry in sync with the
# institution-registry-events topic (fed by MongoDB via Kafka Connect).
#
# Run from WSL, with the Compose stack up:
#   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 registry_sync_job.py
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BooleanType, StringType, StructType

# Defaults match the Compose stack as seen from the host (Windows or WSL).
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "127.0.0.1")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", os.path.expanduser("~/checkpoints/registry_sync"))

# Each Kafka message is the plain MongoDB document, e.g.
# {"_id": "...", "institution_code": "BANK_BPI", "institution_name": "BPI",
#  "rail_eligibility": ["INSTAPAY", "PESONET"], "bank_code": "dobbpi", "active": true}
SCHEMA = (StructType()
    .add("institution_code", StringType())
    .add("institution_name", StringType())
    .add("rail_eligibility", ArrayType(StringType()))
    .add("bank_code", StringType())
    .add("active", BooleanType()))


def latest_per_institution(df):
    """Keep only the newest event per institution_code in this micro-batch.

    A batch can hold several events for one institution (e.g. active:false then
    active:true after a restart). Cassandra doesn't apply rows from one write in
    order, so without this an older event could overwrite a newer one. Kafka
    offsets are ordered within a partition, and the topic has one partition."""
    w = Window.partitionBy("institution_code").orderBy(
        F.col("kafka_partition").desc(), F.col("kafka_offset").desc())
    return (df.withColumn("rn", F.row_number().over(w))
              .filter("rn = 1")
              .drop("rn", "kafka_partition", "kafka_offset"))


def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    latest = latest_per_institution(batch_df).persist()
    (latest.write.format("org.apache.spark.sql.cassandra")
        .options(keyspace="payment_pipeline", table="institution_registry")
        .mode("append").save())
    changes = ", ".join(f"{r.institution_code}={'active' if r.active else 'inactive'}"
                        for r in latest.select("institution_code", "active").collect())
    print(f"[registry batch {batch_id}] {changes}", flush=True)
    latest.unpersist()


if __name__ == "__main__":
    spark = (SparkSession.builder.appName("RegistrySync")
             .config("spark.cassandra.connection.host", CASSANDRA_HOST)
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    parsed = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", "institution-registry-events")
        .option("startingOffsets", "earliest")
        .load()
        .select(F.col("partition").alias("kafka_partition"),
                F.col("offset").alias("kafka_offset"),
                F.from_json(F.col("value").cast("string"), SCHEMA).alias("data"))
        .select("kafka_partition", "kafka_offset", "data.*")
        .filter(F.col("institution_code").isNotNull()))

    (parsed.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .start()
        .awaitTermination())
