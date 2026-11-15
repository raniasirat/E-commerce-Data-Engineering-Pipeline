# Data Quality and Validation

This page records validation results already observed in the project. It does
not claim that the anomalies below have been repaired, and no pipeline or dbt
tests were run to create this documentation.

## Kafka-to-MinIO Parquet scan

The read-only scan covered Parquet objects in bucket `ecommerce-data`, prefix
`orders/`.

| Check | Observed result | Status |
|---|---:|---|
| Parquet files listed | 781 | PASS |
| Files read successfully | 781 / 781 | PASS |
| Zero-byte or zero-row Parquet files | 0 | PASS |
| Rows read | 78,056 | PASS |
| Schema variants | 1 across all 781 files | PASS |
| Required columns missing / incompatible types | 0 | PASS |
| Invalid JSON values in `raw_value` | 1 | WARN |
| Repeated `event_id` groups | 7 groups; 41 excess rows | WARN |
| Repeated `order_id` groups | 7 groups; 41 excess rows | WARN |
| Duplicate `(topic, partition, offset)` coordinates | 0 | PASS |

The repeated groups have identical business payloads except for
`event_timestamp`. They are distinct Kafka records, not duplicate Kafka
coordinates. The invalid JSON value was retained in `raw_value`; its parsed
event fields are null. No order identifiers or payload samples are reproduced
here.

There were also 102 non-Parquet objects under the prefix during the scan. They
were excluded from Parquet row counts and were not classified as business
data.

## dbt checks

The project records a dbt test run with **59 PASS, 0 WARN, and 0 ERROR**. The
result is a previously recorded run, not a rerun for this documentation.
Custom SQL tests cover:

- uniqueness of `(order_id, order_item_id)` in `fact_order_items`;
- non-negative `price` and `freight_value`;
- `total_item_amount = price + freight_value`.

These warehouse tests validate dbt relations. They do not validate the
separate Kafka-to-MinIO stream and did not detect the streaming JSON and
duplicate-snapshot observations above.

## Recorded batch and warehouse results

The existing warehouse documentation records the following results:

| Metric | Recorded value |
|---|---:|
| Rows across the nine staging tables | 1,289,091 |
| Order months | 24 |
| Order-item rows | 112,650 |
| Distinct orders with items | 98,666 |
| `revenue` (`SUM(price)`) | 13,591,643.70 |
| `freight` (`SUM(freight_value)`) | 2,251,909.54 |
| `total_revenue` (`SUM(total_item_amount)`) | 15,843,553.24 |

These are recorded batch/warehouse results and should not be confused with the
78,056 Kafka snapshot rows. The stream and dbt batch path are separate in the
current code.

## Known limitations and recommended follow-up

1. **Malformed streaming record:** Spark uses `from_json` without an explicit
   corrupt-record quarantine path. The raw value is retained, but a malformed
   payload can produce null parsed fields. A future change should count and
   quarantine invalid records or deliberately fail the stream according to an
   agreed policy.
2. **Repeated snapshots:** the producer uses an `event_id` derived from
   `order_id` and generates a new `event_timestamp` on each publication.
   Producer idempotence does not deduplicate separate producer executions.
   Decide whether the sink should upsert/deduplicate by event identity or
   whether every publication is meant to be a distinct event.
3. **No automatic repair:** the observed Parquet records were not deleted,
   rewritten, or deduplicated.
4. **Report freshness:** the PBIX contains an embedded data model. Its last
   refresh time and whether that model currently matches PostgreSQL were not
   established by static report inspection.

Treat the streaming outputs as requiring review before using them as a
deduplicated order-level KPI source.
