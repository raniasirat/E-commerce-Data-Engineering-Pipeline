
"""
Spark Structured Streaming :
Kafka (orders) -> JSON structuré -> Parquet dans SILO (API S3).
"""

import os

os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"

import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DecimalType,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

KAFKA_TOPIC = "orders"
KAFKA_CONNECTOR = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9"
HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.3.4"
AWS_SDK = "com.amazonaws:aws-java-sdk-bundle:1.12.262"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://127.0.0.1:9000")
MINIO_BUCKET = "ecommerce-data"
OUTPUT_PATH = f"s3a://{MINIO_BUCKET}/orders/"

CHECKPOINT_DIR = (
    PROJECT_ROOT
    / "data"
    / "checkpoints"
    / "kafka_spark_streaming"
    / "orders_parquet_minio"
)

EVENT_SCHEMA = StructType([
    StructField("schema_version", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", TimestampType(), True),
    StructField("order_approved_at", TimestampType(), True),
    StructField("order_delivered_carrier_date", TimestampType(), True),
    StructField("order_delivered_customer_date", TimestampType(), True),
    StructField("order_estimated_delivery_date", TimestampType(), True),
    StructField("order_amount", DecimalType(12, 2), True),
    StructField("currency", StringType(), True),
])


def main():
    minio_access_key = os.getenv("MINIO_ROOT_USER")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD")
    if not minio_access_key or not minio_secret_key:
        raise RuntimeError(
            "Set MINIO_ROOT_USER and MINIO_ROOT_PASSWORD in .env or the process environment."
        )
    parser = argparse.ArgumentParser(
        description="Kafka vers Parquet dans SILO avec Spark."
    )
    parser.add_argument(
        "--bootstrap-server",
        default="127.0.0.1:9092",
        help="Adresse du broker Kafka.",
    )
    args = parser.parse_args()

    spark = None
    query = None

    try:
        print("Initialisation de Spark vers SILO...", flush=True)

        packages = ",".join([
            KAFKA_CONNECTOR,
            HADOOP_AWS,
            AWS_SDK,
        ])

        spark = (
            SparkSession.builder
            .appName("OrdersKafkaToParquetSILO")
            .master("local[2]")
            .config("spark.jars.packages", packages)
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config(
                "spark.hadoop.fs.s3a.endpoint",
                MINIO_ENDPOINT,
            )
            .config(
                "spark.hadoop.fs.s3a.access.key",
                minio_access_key,
            )
            .config(
                "spark.hadoop.fs.s3a.secret.key",
                minio_secret_key,
            )
            .config(
                "spark.hadoop.fs.s3a.path.style.access",
                "true",
            )
            .config(
                "spark.hadoop.fs.s3a.connection.ssl.enabled",
                "false",
            )
            .config(
                "spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem",
            )
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        print(f"Spark : {spark.version}", flush=True)
        print(f"Kafka : {args.bootstrap_server}", flush=True)
        print(f"Topic : {KAFKA_TOPIC}", flush=True)
        print(f"Destination : {OUTPUT_PATH}", flush=True)
        print(f"Checkpoint : {CHECKPOINT_DIR}", flush=True)

        kafka_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_server)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", "100")
            .load()
        )

        parsed_stream = kafka_stream.select(
            from_json(
                col("value").cast("string"),
                EVENT_SCHEMA,
            ).alias("event"),
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("raw_value"),
            col("topic"),
            col("partition"),
            col("offset"),
        )

        output_stream = (
            parsed_stream
            .select(
                "event.*",
                "kafka_key",
                "raw_value",
                "topic",
                "partition",
                "offset",
            )
            .withColumn("ingestion_timestamp", current_timestamp())
        )

        query = (
            output_stream.writeStream
            .format("parquet")
            .outputMode("append")
            .option("path", OUTPUT_PATH)
            .option("checkpointLocation", str(CHECKPOINT_DIR))
            .trigger(processingTime="10 seconds")
            .start()
        )

        print(
            "Streaming actif : Kafka -> Parquet -> SILO. "
            "Arrêt avec Ctrl+C.",
            flush=True,
        )

        query.awaitTermination()

    except KeyboardInterrupt:
        print("\nArrêt demandé par l'utilisateur.")

    except Exception:
        logging.exception("Erreur pendant Kafka -> Parquet -> SILO.")
        raise

    finally:
        if query is not None and query.isActive:
            query.stop()

        if spark is not None:
            spark.stop()
            print("Session Spark arrêtée.", flush=True)


if __name__ == "__main__":
    main()
