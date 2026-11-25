"""Kafka plumbing: replay producer, exactly-once transform loop, and the warehouse sink."""
from __future__ import annotations

import logging
import signal
import time
from collections.abc import Callable, Iterable

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from . import topics
from .serde import AvroCodec

log = logging.getLogger("wellstream")

# handler(record) -> iterable of (output_topic, key, record)
Handler = Callable[[dict], Iterable[tuple[str, str, dict]]]


class _Stop:
    def __init__(self) -> None:
        self.flag = False
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, self._set)
            except ValueError:  # not in the main thread (tests)
                pass

    def _set(self, *_):
        self.flag = True


def replay(bootstrap: str, registry_url: str, records: Iterable[dict], speedup: float = 0.0,
           topic: str = topics.RAW) -> int:
    """Publish raw readings. speedup=0 sends as fast as possible; 10 = ten times real time."""
    codec = AvroCodec(topic, registry_url)
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "acks": "all",
                         "compression.type": "lz4", "linger.ms": 20})
    sent, prev_ts, t0 = 0, None, time.monotonic()
    for rec in records:
        if speedup > 0 and prev_ts is not None:
            time.sleep(max((rec["ts"] - prev_ts) / 1000 / speedup, 0))
        prev_ts = rec["ts"]
        while True:
            try:
                producer.produce(topic, key=rec["wellbore_id"].encode(), value=codec.encode(rec))
                break
            except BufferError:  # local queue full: let the client drain it
                producer.poll(0.5)
        sent += 1
        if sent % 10000 == 0:
            producer.poll(0)
            log.info("replayed %d readings (%.0f/s)", sent, sent / (time.monotonic() - t0))
    producer.flush()
    return sent


def run_transform(bootstrap: str, registry_url: str, in_topic: str, group_id: str, transactional_id: str,
                  handler: Handler, out_topics: list[str], max_idle_s: float | None = None,
                  batch_size: int = 500) -> int:
    """Consume -> handle -> produce, committing input offsets inside the same Kafka transaction.

    Downstream consumers using isolation.level=read_committed see each input record's
    outputs exactly once, even if this process crashes and restarts mid-batch.
    """
    in_codec = AvroCodec(in_topic, registry_url)
    out_codecs = {t: AvroCodec(t, registry_url) for t in out_topics}
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": group_id, "enable.auto.commit": False,
                         "auto.offset.reset": "earliest", "isolation.level": "read_committed"})
    producer = Producer({"bootstrap.servers": bootstrap, "transactional.id": transactional_id,
                         "enable.idempotence": True, "compression.type": "lz4", "linger.ms": 20})
    producer.init_transactions()
    consumer.subscribe([in_topic])
    stop, processed, last_msg = _Stop(), 0, time.monotonic()
    try:
        while not stop.flag:
            msgs = consumer.consume(num_messages=batch_size, timeout=1.0)
            msgs = [m for m in msgs if not _is_eof(m)]
            if not msgs:
                if max_idle_s is not None and time.monotonic() - last_msg > max_idle_s:
                    break
                continue
            last_msg = time.monotonic()
            producer.begin_transaction()
            try:
                for m in msgs:
                    rec = in_codec.decode(m.value())
                    for topic, key, out in handler(rec):
                        producer.produce(topic, key=key.encode(), value=out_codecs[topic].encode(out))
                    producer.poll(0)
                producer.send_offsets_to_transaction(consumer.position(consumer.assignment()),
                                                     consumer.consumer_group_metadata())
                producer.commit_transaction()
            except Exception:
                # Nothing from this batch becomes visible. Exit so the supervisor restarts us from the
                # last committed offsets (in-memory detector state is rebuilt from the stream).
                producer.abort_transaction()
                raise
            processed += len(msgs)
    finally:
        consumer.close()
    return processed


def run_sink(bootstrap: str, registry_url: str, loader, group_id: str = "wellstream-sink",
             max_idle_s: float | None = None, batch_size: int = 2000) -> int:
    """Load clean readings and events into the warehouse.

    Offsets are committed only after the database transaction commits (at-least-once);
    the loader upserts on natural keys, so redelivered messages change nothing.
    """
    codecs = {t: AvroCodec(t, registry_url) for t in (topics.CLEAN, topics.ALERTS)}
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": group_id, "enable.auto.commit": False,
                         "auto.offset.reset": "earliest", "isolation.level": "read_committed"})
    consumer.subscribe(list(codecs))
    stop, written, last_msg = _Stop(), 0, time.monotonic()
    try:
        while not stop.flag:
            msgs = [m for m in consumer.consume(num_messages=batch_size, timeout=1.0) if not _is_eof(m)]
            if not msgs:
                if max_idle_s is not None and time.monotonic() - last_msg > max_idle_s:
                    break
                continue
            last_msg = time.monotonic()
            readings, events = [], []
            for m in msgs:
                rec = codecs[m.topic()].decode(m.value())
                (readings if m.topic() == topics.CLEAN else events).append(rec)
            loader.write(readings, events)
            consumer.commit(asynchronous=False)
            written += len(msgs)
    finally:
        consumer.close()
    return written


def _is_eof(m) -> bool:
    err = m.error()
    if err is None:
        return False
    if err.code() == KafkaError._PARTITION_EOF:
        return True
    raise KafkaException(err)
