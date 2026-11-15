# E-commerce Data Platform — Olist

A data-engineering project for preparing, processing, and analysing the public
Brazilian Olist e-commerce dataset. It combines independent batch and event
streaming paths and publishes a dbt dimensional model for analytics.

## What is implemented

- Pandas batch ETL: Olist CSV files in `data/raw/` are standardized, cleaned,
  deduplicated by configured business keys, and written as Parquet under
  `data/processed/`.
- A manually triggered Airflow DAG runs the file check, Pandas ETL, Spark batch
  pipeline, and output validation in that order.
- A separate Kafka producer publishes validated `order.snapshot` messages to
  the `orders` topic. Spark Structured Streaming parses those messages and
  writes Parquet to the S3-compatible SILO/MinIO service.
- A PostgreSQL staging loader and dbt models build an analytics star schema
  and three aggregate tables.
- The Power BI report file is in `powerbi/`.

The Airflow DAG does **not** orchestrate the Kafka-to-MinIO stream, the
PostgreSQL loader, or dbt. These are separate execution paths.

## Technology

| Area | Technology in the project |
|---|---|
| Batch processing | Python, Pandas, PySpark, Parquet |
| Orchestration | Apache Airflow |
| Event transport | Apache Kafka 4 in KRaft mode |
| Streaming | Spark Structured Streaming |
| Object storage | SILO/MinIO-compatible S3 API |
| Warehouse | PostgreSQL |
| Transformations | dbt Core with the PostgreSQL adapter |
| Reporting | Power BI Desktop (`.pbix`) |

The Compose stack also defines Redis for Airflow's Celery executor. The custom
Airflow image is based on Airflow 3.4 with Python 3.11 and installs a Java
runtime for Spark.

## Prerequisites

- Windows with Docker Desktop and Docker Compose.
- Python compatible with the code and packages in `requirements.txt`.
- Java when running Spark locally; the custom Airflow image includes a Java
  runtime.
- The nine Olist CSV source files in `data/raw/`.
- dbt Core and the PostgreSQL adapter in the environment used to run dbt. dbt
  is not pinned in the root `requirements.txt`.

## Local setup (Windows CMD)

Run these commands from the repository root:

```cmd
py -3.11 -m venv venv
call venv\Scripts\activate.bat
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
copy .env.example .env
```

Populate the local `.env` with the required environment-specific settings.
Keep that file private; do not commit it. The example file contains no usable
credentials. Configure the dbt profile separately and keep its credentials
private as well.

Place the source CSVs in `data/raw/`. Their expected filenames and table keys
are defined in `src/processing/config.py`.

## Starting the local services

From the repository root, in CMD:

```cmd
docker compose up -d --build
docker compose ps
```

The Compose file exposes Kafka on port `9092`, the Airflow API/UI on port
`8081`, SILO/MinIO on ports `9000` and `9001`, and its PostgreSQL service on
host port `5433`. Airflow is available at `http://localhost:8081`; use the
locally configured Airflow web-account credentials.

The Kafka topic is not created by the batch DAG. Create it when needed:

```cmd
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic orders --partitions 1 --replication-factor 1
```

## Running the pipelines

### Batch path

The Airflow DAG `olist_batch_pipeline` has no schedule and is intended to be
triggered manually in the Airflow UI. Its tasks are:

```text
check_files -> ingest_data -> transform_data -> validate_output
```

For local execution of individual steps:

```cmd
python -m src.utils.check_files
python -m src.ingestion.etl_pipeline
python -m src.processing.olist_spark_pipeline
python -m src.utils.validate_output
```

The separate PostgreSQL loader is:

```cmd
python -m src.processing.load_parquet_to_postgres
```

Review that script and its target before running it: it truncates each target
staging table before inserting the processed Parquet rows. It is not a task in
the current Airflow DAG.

### Kafka and streaming path

With the services running and the `orders` topic available, run the producer
and Spark streaming application from separate CMD windows:

```cmd
python -m src.ingestion.kafka_producer
```

```cmd
python -m src.processing.kafka_spark_to_minio
```

The producer validates its event payload against
`schemas/order-event.schema.json`. The Spark application writes under
`s3a://ecommerce-data/orders/` and uses the local checkpoint under
`data/checkpoints/kafka_spark_streaming/orders_parquet_minio`. The checkpoint
is part of the stream's progress state; do not delete it to force a replay.

`src/processing/kafka_spark_streaming.py` is a separate console-output
Structured Streaming utility. It is not the MinIO sink.

### dbt and Power BI

Run dbt commands from the actual project directory, `ecommerce_dbt`:

```cmd
cd /d ecommerce_dbt
dbt debug
dbt parse
dbt run
dbt test
dbt docs generate
dbt docs serve --port 8080
```

The Power BI report file is
[`powerbi/dashbord_ecommerce.pbix`](powerbi/dashbord_ecommerce.pbix). Its
visual metadata references the `analytics.agg_*` tables; see
[`docs/powerbi-dashboard.md`](docs/powerbi-dashboard.md).

## Recorded results and data-quality notes

The existing warehouse documentation records 1,289,091 rows across the nine
staging tables and these batch aggregates: 24 order months, 112,650 order
items, 98,666 distinct orders, `revenue` of 13,591,643.70, and freight of
2,251,909.54 (source-currency amounts).

A separate read-only scan of the Kafka-to-MinIO output found 781 readable
Parquet files and 78,056 rows with one consistent schema. It also found one
invalid JSON value and seven repeated `event_id`/`order_id` groups. These
streaming anomalies have **not** been corrected. See
[`docs/data-quality.md`](docs/data-quality.md) for scope and caveats.

The project records a dbt test run with 59 passes, 0 warnings, and 0 errors.
That result does not validate the separate Kafka-to-MinIO stream.

## Repository map

```text
data/raw/                 Olist CSV inputs
data/processed/           Pandas Parquet and rejected duplicate rows
data/processed/spark/     Spark batch Parquet and aggregates
src/ingestion/            Batch ETL, Kafka producer, Kafka consumer
src/processing/           Cleaning, batch Spark, streaming, PostgreSQL loader
schemas/                  Kafka order-event JSON Schema
dags/                     Airflow batch DAG
ecommerce_dbt/            dbt sources, models, tests, and project config
powerbi/                  Power BI report
docs/                     Technical and project documentation
```

See [`docs/architecture.md`](docs/architecture.md),
[`docs/data-pipeline.md`](docs/data-pipeline.md),
[`docs/data-quality.md`](docs/data-quality.md), and
[`docs/powerbi-dashboard.md`](docs/powerbi-dashboard.md) for details.
