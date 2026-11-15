
"""
Spark Structured Streaming : lecture des événements Kafka du topic orders.

Ce script est indépendant du pipeline Spark Batch.
"""

# IMPORTANT : limiter les threads avant d'importer PySpark
import os

os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"

import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession


# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

CHECKPOINT_DIR = (
    PROJECT_ROOT
    / "data"
    / "checkpoints"
    / "kafka_spark_streaming"
    / "orders_console"
)

KAFKA_TOPIC = "orders"

KAFKA_CONNECTOR = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9"
)

DEFAULT_BOOTSTRAP_SERVER = "127.0.0.1:9092"


# --------------------------------------------------
# MAIN
# --------------------------------------------------

def main():

    parser = argparse.ArgumentParser(
        description="Lecture Kafka avec Spark Structured Streaming."
    )

    parser.add_argument(
        "--bootstrap-server",
        default=DEFAULT_BOOTSTRAP_SERVER,
        help="Adresse du broker Kafka."
    )

    args = parser.parse_args()

    spark = None

    try:

        print("Initialisation de Spark Structured Streaming...")

        spark = (
            SparkSession.builder
            .appName("OrdersKafkaStructuredStreaming")
            .master("local[2]")
            .config("spark.jars.packages", KAFKA_CONNECTOR)
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        print(f"Version Spark : {spark.version}")
        print(f"Broker Kafka : {args.bootstrap_server}")
        print(f"Topic Kafka : {KAFKA_TOPIC}")
        print(f"Checkpoint : {CHECKPOINT_DIR}")

        # --------------------------------------------------
        # LECTURE KAFKA
        # --------------------------------------------------

        kafka_stream = (
            spark.readStream
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                args.bootstrap_server
            )
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", "100")
            .load()
        )

        # --------------------------------------------------
        # EXTRACTION DES DONNEES
        # --------------------------------------------------

        events = kafka_stream.selectExpr(
            "CAST(key AS STRING) AS key",
            "CAST(value AS STRING) AS value",
            "topic",
            "partition",
            "offset"
        )

        # --------------------------------------------------
        # SORTIE CONSOLE
        # --------------------------------------------------

        query = (
            events.writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", "false")
            .option(
                "checkpointLocation",
                str(CHECKPOINT_DIR)
            )
            .trigger(processingTime="5 seconds")
            .start()
        )

        print("\nStreaming Kafka démarré avec succès.")
        print("En attente des événements...")
        print("Pour arrêter : CTRL + C\n")

        query.awaitTermination()

    except KeyboardInterrupt:

        print("\nArrêt demandé par l'utilisateur.")

    except Exception:

        logging.exception(
            "Erreur pendant l'exécution de Spark Streaming."
        )
        raise

    finally:

        if spark is not None:
            spark.stop()
            print("Session Spark arrêtée.")


if __name__ == "__main__":
    main()
