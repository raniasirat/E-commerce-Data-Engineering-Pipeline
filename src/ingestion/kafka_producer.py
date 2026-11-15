"""Publie des instantanés de commandes Olist dans Kafka."""

import argparse
import csv
import json
import logging
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from confluent_kafka import Producer
from jsonschema import Draft202012Validator, FormatChecker


PROJECT_ROOT = Path(__file__).resolve().parents[2]
ORDERS_PATH = PROJECT_ROOT / "data" / "raw" / "olist_orders_dataset.csv"
PAYMENTS_PATH = PROJECT_ROOT / "data" / "raw" / "olist_order_payments_dataset.csv"
SCHEMA_PATH = PROJECT_ROOT / "schemas" / "order-event.schema.json"
TOPIC = "orders"

ORDER_COLUMNS = {
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
}
OPTIONAL_DATE_COLUMNS = (
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
LOGGER = logging.getLogger("olist-kafka-producer")


def require_columns(reader, required_columns, path):
    actual_columns = set(reader.fieldnames or [])
    missing_columns = required_columns - actual_columns
    if missing_columns:
        names = ", ".join(sorted(missing_columns))
        raise ValueError(f"Colonnes manquantes dans {path} : {names}")


def load_payment_totals(path):
    totals = {}
    required_columns = {"order_id", "payment_value"}

    with path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        require_columns(reader, required_columns, path)

        for line_number, row in enumerate(reader, start=2):
            order_id = (row.get("order_id") or "").strip()
            raw_value = (row.get("payment_value") or "").strip()

            if not order_id:
                raise ValueError(
                    f"order_id vide dans {path}, ligne {line_number}"
                )
            if not raw_value:
                raise ValueError(
                    f"payment_value vide dans {path}, ligne {line_number}"
                )

            try:
                value = Decimal(raw_value)
            except InvalidOperation as exc:
                raise ValueError(
                    f"payment_value invalide dans {path}, ligne {line_number}"
                ) from exc

            if not value.is_finite() or value < 0:
                raise ValueError(
                    f"payment_value négatif ou non fini dans {path}, "
                    f"ligne {line_number}"
                )

            if value != value.quantize(Decimal("0.01")):
                raise ValueError(
                    f"payment_value avec plus de deux décimales dans {path}, "
                    f"ligne {line_number}"
                )

            totals[order_id] = totals.get(order_id, Decimal("0.00")) + value

    return totals


def load_validator(path):
    with path.open("r", encoding="utf-8") as schema_file:
        schema = json.load(schema_file, parse_float=Decimal)

    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=FormatChecker())


def build_event(row, payment_totals, validator):
    order_id = (row.get("order_id") or "").strip()
    customer_id = (row.get("customer_id") or "").strip()
    order_status = (row.get("order_status") or "").strip()
    purchase_timestamp = (row.get("order_purchase_timestamp") or "").strip()

    if not order_id or not customer_id or not order_status:
        raise ValueError(
            "order_id, customer_id et order_status doivent être renseignés"
        )
    if not purchase_timestamp:
        raise ValueError("order_purchase_timestamp doit être renseigné")

    payment_total = payment_totals.get(order_id)
    event = {
        "schema_version": "1.0",
        "event_id": f"olist:order.snapshot:v1:{order_id}",
        "event_type": "order.snapshot",
        "event_timestamp": (
            datetime.now(timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z")
        ),
        "order_id": order_id,
        "customer_id": customer_id,
        "order_status": order_status,
        "order_purchase_timestamp": purchase_timestamp,
        "order_approved_at": None,
        "order_delivered_carrier_date": None,
        "order_delivered_customer_date": None,
        "order_estimated_delivery_date": None,
        "order_amount": payment_total,
        "currency": "BRL",
    }

    for column in OPTIONAL_DATE_COLUMNS:
        value = (row.get(column) or "").strip()
        event[column] = value or None

    validator.validate(event)
    return event


def publish_orders(bootstrap_server, limit):
    payment_totals = load_payment_totals(PAYMENTS_PATH)
    validator = load_validator(SCHEMA_PATH)

    producer = Producer(
        {
            "bootstrap.servers": bootstrap_server,
            "client.id": "olist-orders-producer",
            "enable.idempotence": True,
            "acks": "all",
            "message.timeout.ms": 30000,
        }
    )

    delivery_errors = []
    delivery_count = 0

    def on_delivery(error, message):
        nonlocal delivery_count
        if error is not None:
            delivery_errors.append(str(error))
        else:
            delivery_count += 1

    queued_count = 0

    try:
        with ORDERS_PATH.open(
            "r", encoding="utf-8-sig", newline=""
        ) as csv_file:
            reader = csv.DictReader(csv_file)
            require_columns(reader, ORDER_COLUMNS, ORDERS_PATH)

            for line_number, row in enumerate(reader, start=2):
                if limit is not None and queued_count >= limit:
                    break

                try:
                    event = build_event(row, payment_totals, validator)
                except Exception as exc:
                    raise ValueError(
                        f"Commande invalide dans {ORDERS_PATH}, "
                        f"ligne {line_number}: {exc}"
                    ) from exc

                payload = json.dumps(
                    event,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    allow_nan=False,
                    default=float,
                ).encode("utf-8")

                while True:
                    try:
                        producer.produce(
                            TOPIC,
                            key=event["order_id"].encode("utf-8"),
                            value=payload,
                            callback=on_delivery,
                        )
                        queued_count += 1
                        break
                    except BufferError:
                        producer.poll(1)

                producer.poll(0)

    except Exception:
        producer.flush(10)
        raise

    remaining = producer.flush(30)
    if remaining:
        raise TimeoutError(
            f"{remaining} message(s) n'ont pas été confirmés par Kafka"
        )
    if delivery_errors:
        raise RuntimeError(
            f"{len(delivery_errors)} erreur(s) de livraison Kafka. "
            f"Première erreur : {delivery_errors[0]}"
        )

    LOGGER.info(
        "Publication terminée : %d message(s) envoyé(s), "
        "%d confirmation(s) reçue(s), topic=%s",
        queued_count,
        delivery_count,
        TOPIC,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Publie les commandes Olist dans Kafka."
    )
    parser.add_argument(
        "--bootstrap-server",
        default="127.0.0.1:9092",
        help="Adresse du broker Kafka (défaut : 127.0.0.1:9092).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limite facultative de commandes, utile pour le test.",
    )
    args = parser.parse_args()

    if args.limit is not None and args.limit <= 0:
        parser.error("--limit doit être un entier positif")

    try:
        publish_orders(args.bootstrap_server, args.limit)
    except Exception:
        LOGGER.exception("Échec du producteur Kafka")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())