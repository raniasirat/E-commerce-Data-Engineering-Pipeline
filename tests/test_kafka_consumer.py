import json
from types import SimpleNamespace

from src.ingestion import kafka_consumer


class FakeMessage:
    def __init__(self, value, offset):
        self._value = value
        self._offset = offset

    def value(self):
        return self._value

    def error(self):
        return None

    def topic(self):
        return "orders"

    def partition(self):
        return 0

    def offset(self):
        return self._offset


class FakeConsumer:
    def __init__(self, messages, actions):
        self.messages = iter(messages)
        self.actions = actions
        self.committed_offsets = []
        self.closed = False

    def subscribe(self, topics):
        assert topics == ["orders"]

    def poll(self, timeout):
        assert timeout == 1.0
        return next(self.messages, None)

    def commit(self, message, asynchronous):
        assert asynchronous is False
        self.actions.append(("commit", message.offset()))
        self.committed_offsets.append(message.offset())
        return [SimpleNamespace(error=None)]

    def close(self):
        self.closed = True


def valid_event():
    return {
        "schema_version": "1.0",
        "event_id": "olist:order.snapshot:v1:test-order",
        "event_type": "order.snapshot",
        "event_timestamp": "2026-11-14T20:00:00Z",
        "order_id": "test-order",
        "customer_id": "test-customer",
        "order_status": "delivered",
        "order_purchase_timestamp": "2017-10-02 10:56:33",
        "order_approved_at": None,
        "order_delivered_carrier_date": None,
        "order_delivered_customer_date": None,
        "order_estimated_delivery_date": None,
        "order_amount": 1.01,
        "currency": "BRL",
    }


def test_consume_logs_and_commits_invalid_message_then_continues(
    monkeypatch, capsys
):
    actions = []
    messages = [
        FakeMessage(b"order_id=TEST-001", 0),
        FakeMessage(json.dumps(valid_event()).encode("utf-8"), 1),
        FakeMessage(json.dumps(valid_event()).encode("utf-8"), 2),
    ]
    fake_consumer = FakeConsumer(messages, actions)
    monkeypatch.setattr(
        kafka_consumer, "Consumer", lambda _: fake_consumer
    )
    monkeypatch.setattr(
        kafka_consumer.LOGGER,
        "error",
        lambda *args: actions.append(("logged", args)),
    )

    kafka_consumer.consume("localhost:9092", 2, "earliest")

    assert actions[0][0] == "logged"
    assert "topic=%s, partition=%s, offset=%s, cause=%s" in actions[0][1][0]
    assert actions[0][1][1:4] == ("orders", 0, 0)
    assert "Message JSON invalide" in str(actions[0][1][4])
    assert actions[1:] == [("commit", 0), ("commit", 1)]
    assert fake_consumer.committed_offsets == [0, 1]
    assert fake_consumer.closed
    assert json.loads(capsys.readouterr().out) == valid_event()


def test_decode_and_validate_rejects_schema_invalid_json():
    message = FakeMessage(b'{"order_id":"incomplete"}', 7)
    validator = kafka_consumer.load_validator(kafka_consumer.SCHEMA_PATH)

    try:
        kafka_consumer.decode_and_validate(message, validator)
    except ValueError as exc:
        assert "Message JSON invalide" in str(exc)
    else:
        raise AssertionError("A schema-invalid event should be rejected")


def test_consume_stops_cleanly_on_keyboard_interrupt(monkeypatch):
    class InterruptingConsumer(FakeConsumer):
        def poll(self, _timeout):
            raise KeyboardInterrupt

    fake_consumer = InterruptingConsumer([], [])
    info_messages = []
    monkeypatch.setattr(
        kafka_consumer, "Consumer", lambda _: fake_consumer
    )
    monkeypatch.setattr(
        kafka_consumer.LOGGER,
        "info",
        lambda message, *_args: info_messages.append(message),
    )

    kafka_consumer.consume("localhost:9092", None, "earliest")

    assert fake_consumer.closed
    assert "Arret demande par Ctrl+C." in info_messages
