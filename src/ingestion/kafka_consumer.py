
"""Lit et valide les événements de commandes depuis Kafka."""

import argparse
import json
import logging
import sys
from decimal import Decimal
from pathlib import Path

from confluent_kafka import Consumer, KafkaException
from jsonschema import Draft202012Validator, FormatChecker, ValidationError


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = PROJECT_ROOT / "schemas" / "order-event.schema.json"
TOPIC = "orders"
GROUP_ID = "olist-python-consumer-v1"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
LOGGER = logging.getLogger("olist-kafka-consumer")


def load_validator(path):
    with path.open("r", encoding="utf-8") as schema_file:
        schema = json.load(schema_file, parse_float=Decimal)

    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(
        schema,
        format_checker=FormatChecker(),
    )


def decode_and_validate(message, validator):
    value = message.value()
    if value is None:
        raise ValueError("Le message Kafka ne contient aucune valeur")

    try:
        event = json.loads(
            value.decode("utf-8"),
            parse_float=Decimal,
        )
        validator.validate(event)

    except (UnicodeDecodeError, json.JSONDecodeError, ValidationError) as exc:
        raise ValueError(f"Message JSON invalide : {exc}") from exc

    return event


def commit_message(consumer, message):
    committed_offsets = consumer.commit(
        message=message,
        asynchronous=False,
    )
    for committed_offset in committed_offsets or []:
        if committed_offset.error is not None:
            raise KafkaException(committed_offset.error)


def consume(bootstrap_server, max_messages, offset_reset):
    validator = load_validator(SCHEMA_PATH)
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_server,
            "client.id": "olist-python-consumer",
            "group.id": GROUP_ID,
            "enable.auto.commit": False,
            "auto.offset.reset": offset_reset,
        }
    )

    handled_count = 0
    valid_count = 0
    invalid_count = 0

    try:
        consumer.subscribe([TOPIC])
        LOGGER.info(
            "Consommateur demarre : topic=%s, group_id=%s, broker=%s. "
            "En attente de messages; Ctrl+C pour arreter.",
            TOPIC,
            GROUP_ID,
            bootstrap_server,
        )

        while max_messages is None or handled_count < max_messages:
            message = consumer.poll(1.0)

            if message is None:
                continue
            if message.error() is not None:
                raise KafkaException(message.error())

            try:
                event = decode_and_validate(message, validator)

            except ValueError as exc:
                LOGGER.error(
                    "Message invalide journalise puis ignore; "
                    "le message original reste dans Kafka : "
                    "topic=%s, partition=%s, offset=%s, cause=%s",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                    exc,
                )
                commit_message(consumer, message)
                invalid_count += 1
                handled_count += 1
                continue

            print(
                json.dumps(
                    event,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    default=float,
                ),
                flush=True,
            )

            commit_message(consumer, message)
            valid_count += 1
            handled_count += 1

    except KeyboardInterrupt:
        LOGGER.info("Arret demande par Ctrl+C.")

    finally:
        consumer.close()

    LOGGER.info(
        "Consommation terminee : %d message(s) JSON valide(s), "
        "%d message(s) invalide(s) journalise(s) et ignore(s), "
        "%d message(s) traite(s) au total, topic=%s, group_id=%s",
        valid_count,
        invalid_count,
        handled_count,
        TOPIC,
        GROUP_ID,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Consomme et valide les événements Kafka du topic orders."
    )
    parser.add_argument(
        "--bootstrap-server",
        default="127.0.0.1:9092",
        help="Adresse du broker Kafka (défaut : 127.0.0.1:9092).",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Nombre maximal de messages à traiter ; sans limite par défaut.",
    )
    parser.add_argument(
        "--offset-reset",
        choices=("earliest", "latest"),
        default="earliest",
        help=(
            "Position de départ si le groupe n'a pas encore d'offset validé."
        ),
    )
    args = parser.parse_args()

    if args.max_messages is not None and args.max_messages <= 0:
        parser.error("--max-messages doit être un entier positif")

    try:
        consume(
            args.bootstrap_server,
            args.max_messages,
            args.offset_reset,
        )
    except Exception:
        LOGGER.exception("Échec du consommateur Kafka")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
