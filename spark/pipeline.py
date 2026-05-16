"""
Spark Structured Streaming Pipeline
────────────────────────────────────
Stage 1: events.raw   → parse JSON, validate schema
Stage 2: events.cleaned → deduplicate, null checks, enrich
Stage 3: events.curated → aggregate windowed metrics
Sink A:  Delta Lake (long-term storage)
Sink B:  PostgreSQL (live dashboard feed)
"""

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
log = logging.getLogger("spark-pipeline")

# ── Config ────────────────────────────────────────────────────────────────
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "streaming_db")
PG_USER = os.getenv("POSTGRES_USER", "pipeline")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "pipeline123")
DELTA_PATH = os.getenv("DELTA_PATH", "/opt/delta-lake")
CHECKPOINT_DIR = f"{DELTA_PATH}/checkpoints"

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS = {"user": PG_USER, "password": PG_PASS, "driver": "org.postgresql.Driver"}

# ── Schemas ───────────────────────────────────────────────────────────────
CLICKSTREAM_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("timestamp", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("action", StringType()),
    StructField("page", StringType()),
    StructField("device", StringType()),
    StructField("browser", StringType()),
    StructField("country", StringType()),
    StructField("response_time_ms", IntegerType()),
    StructField("revenue", DoubleType()),
])

SENSOR_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("timestamp", StringType()),
    StructField("sensor_id", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("pressure", DoubleType()),
    StructField("vibration", DoubleType()),
    StructField("is_anomaly", BooleanType()),
    StructField("location", StringType()),
])

LOG_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("timestamp", StringType()),
    StructField("level", StringType()),
    StructField("service", StringType()),
    StructField("message", StringType()),
    StructField("trace_id", StringType()),
    StructField("status_code", IntegerType()),
])

RAW_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("timestamp", StringType()),
    # Clickstream fields
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("action", StringType()),
    StructField("page", StringType()),
    StructField("device", StringType()),
    StructField("browser", StringType()),
    StructField("country", StringType()),
    StructField("response_time_ms", IntegerType()),
    StructField("revenue", DoubleType()),
    # Sensor fields
    StructField("sensor_id", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("pressure", DoubleType()),
    StructField("vibration", DoubleType()),
    StructField("is_anomaly", BooleanType()),
    StructField("location", StringType()),
    # Log fields
    StructField("level", StringType()),
    StructField("service", StringType()),
    StructField("message", StringType()),
    StructField("trace_id", StringType()),
    StructField("status_code", IntegerType()),
])


def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("RealTimeStreamingPipeline")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "io.delta:delta-core_2.12:2.4.0,"
                "org.postgresql:postgresql:42.7.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# ── Stage 1: Read raw Kafka stream ────────────────────────────────────────
def read_raw_stream(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", "events.raw")
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 1000)
        .load()
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_json"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .select(
            F.col("kafka_key"),
            F.col("kafka_timestamp"),
            F.from_json(F.col("raw_json"), RAW_SCHEMA).alias("data"),
        )
        .select("kafka_key", "kafka_timestamp", "data.*")
    )


# ── Stage 2: Clean & validate ─────────────────────────────────────────────
def clean_stream(raw_df):
    return (
        raw_df
        # Parse timestamp
        .withColumn("event_ts", F.to_timestamp(F.col("timestamp")))
        # Drop rows missing critical fields
        .filter(F.col("event_id").isNotNull() & F.col("event_type").isNotNull())
        # Clamp out-of-range sensor values
        .withColumn(
            "temperature",
            F.when(F.col("event_type") == "sensor",
                   F.greatest(F.lit(-50.0), F.least(F.lit(150.0), F.col("temperature")))
                   ).otherwise(F.col("temperature"))
        )
        # Null-safe revenue
        .withColumn("revenue", F.coalesce(F.col("revenue"), F.lit(0.0)))
        # Add processing metadata
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("pipeline_version", F.lit("1.0.0"))
        # Flag anomalous response times
        .withColumn(
            "is_slow_request",
            F.when((F.col("event_type") == "clickstream") & (F.col("response_time_ms") > 1500),
                   F.lit(True)).otherwise(F.lit(False))
        )
        .dropDuplicates(["event_id"])
    )


# ── Sink helpers ──────────────────────────────────────────────────────────
def write_to_postgres(batch_df, batch_id: int, table: str):
    """Write a micro-batch to Postgres via JDBC."""
    try:
        row_count = batch_df.count()
        if row_count == 0:
            return
        batch_df.write.jdbc(
            url=JDBC_URL,
            table=table,
            mode="append",
            properties=JDBC_PROPS,
        )
        log.info(f"Batch {batch_id}: wrote {row_count} rows → {table}")
    except Exception as e:
        log.error(f"Batch {batch_id}: failed writing to {table}: {e}")


def write_clickstream_agg(batch_df, batch_id: int):
    """Aggregate clickstream events per page/device and upsert to Postgres."""
    agg = (
        batch_df
        .filter(F.col("event_type") == "clickstream")
        .groupBy(
            F.window("event_ts", "1 minute").alias("window"),
            "page", "device", "country"
        )
        .agg(
            F.count("*").alias("event_count"),
            F.avg("response_time_ms").alias("avg_response_ms"),
            F.sum("revenue").alias("total_revenue"),
            F.countDistinct("user_id").alias("unique_users"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
        .withColumn("batch_id", F.lit(batch_id))
    )
    write_to_postgres(agg, batch_id, "clickstream_metrics")


def write_sensor_agg(batch_df, batch_id: int):
    """Aggregate sensor readings per location."""
    agg = (
        batch_df
        .filter(F.col("event_type") == "sensor")
        .groupBy(
            F.window("event_ts", "1 minute").alias("window"),
            "location"
        )
        .agg(
            F.avg("temperature").alias("avg_temperature"),
            F.max("temperature").alias("max_temperature"),
            F.avg("humidity").alias("avg_humidity"),
            F.avg("vibration").alias("avg_vibration"),
            F.sum(F.col("is_anomaly").cast("int")).alias("anomaly_count"),
            F.count("*").alias("reading_count"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
        .withColumn("batch_id", F.lit(batch_id))
    )
    write_to_postgres(agg, batch_id, "sensor_metrics")


def write_log_agg(batch_df, batch_id: int):
    """Aggregate log events by service and level."""
    agg = (
        batch_df
        .filter(F.col("event_type") == "app_log")
        .groupBy(
            F.window("event_ts", "1 minute").alias("window"),
            "service", "level"
        )
        .agg(
            F.count("*").alias("log_count"),
            F.avg(F.col("status_code").cast("double")).alias("avg_status_code"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
        .withColumn("batch_id", F.lit(batch_id))
    )
    write_to_postgres(agg, batch_id, "log_metrics")


def process_batch(batch_df, batch_id: int):
    """Master batch processor — called by foreachBatch."""
    batch_df.cache()
    total = batch_df.count()
    log.info(f"Processing batch {batch_id} with {total} events")

    # Raw events → Postgres (last 24h rolling window kept via PG partition)
    cols = [
        "event_id", "event_type", "event_ts", "user_id", "sensor_id",
        "action", "page", "device", "country", "revenue",
        "temperature", "humidity", "vibration", "is_anomaly", "location",
        "level", "service", "status_code", "response_time_ms",
        "is_slow_request", "processed_at",
    ]
    write_to_postgres(batch_df.select(cols), batch_id, "events_cleaned")

    # Aggregations
    write_clickstream_agg(batch_df, batch_id)
    write_sensor_agg(batch_df, batch_id)
    write_log_agg(batch_df, batch_id)

    batch_df.unpersist()


def main():
    log.info("Initialising Spark session…")
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    log.info("Reading raw stream from Kafka…")
    raw_df = read_raw_stream(spark)
    cleaned_df = clean_stream(raw_df)

    # ── Write to Delta Lake (fault-tolerant storage) ──────────────────────
    (
        cleaned_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/delta")
        .option("path", f"{DELTA_PATH}/events")
        .partitionBy("event_type")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # ── Write to Postgres via foreachBatch ────────────────────────────────
    (
        cleaned_df.writeStream
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/postgres")
        .trigger(processingTime="10 seconds")
        .foreachBatch(process_batch)
        .start()
    )

    log.info("Pipeline running. Awaiting termination…")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
