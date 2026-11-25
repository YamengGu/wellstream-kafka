"""Avro encoding with Schema Registry (a real one, or mock:// for tests and CI)."""
from __future__ import annotations

import datetime as dt
from functools import lru_cache
from importlib import resources

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from . import topics

# topic -> schema file
TOPIC_SCHEMAS = {
    topics.RAW: "raw_reading.avsc",
    topics.CLEAN: "clean_reading.avsc",
    topics.ALERTS: "drilling_event.avsc",
    topics.DLQ: "dlq_record.avsc",
}


def load_schema(filename: str) -> str:
    return resources.files("wellstream.schemas").joinpath(filename).read_text()


@lru_cache(maxsize=8)
def registry(url: str) -> SchemaRegistryClient:
    if url.startswith("mock://") and hasattr(SchemaRegistryClient, "new_client"):
        return SchemaRegistryClient.new_client({"url": url})
    return SchemaRegistryClient({"url": url})


def _epoch_ms(v):
    # fastavro returns timestamp-millis fields as aware datetimes; the pipeline works in epoch ms
    if isinstance(v, dt.datetime):
        return int(v.timestamp() * 1000)
    return v


class AvroCodec:
    """Serializer + deserializer for one topic's value schema."""

    def __init__(self, topic: str, registry_url: str):
        self.topic = topic
        sr = registry(registry_url)
        schema = load_schema(TOPIC_SCHEMAS[topic])
        self._ser = AvroSerializer(sr, schema, conf={"auto.register.schemas": True})
        self._de = AvroDeserializer(sr, schema)
        self._ctx = SerializationContext(topic, MessageField.VALUE)

    def encode(self, record: dict) -> bytes:
        return self._ser(record, self._ctx)

    def decode(self, payload: bytes) -> dict:
        rec = self._de(payload, self._ctx)
        return {k: _epoch_ms(v) for k, v in rec.items()}
