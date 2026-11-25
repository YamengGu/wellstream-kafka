"""End-to-end test on a real Kafka broker (runs in CI).

WELLSTREAM_TEST_KAFKA=localhost:9092 enables it; add WELLSTREAM_TEST_PG_DSN to also run the sink.
"""
import os
import uuid

import pytest
from confluent_kafka import Consumer, TopicPartition
from conftest import reset_warehouse

from wellstream import topics

BOOTSTRAP = os.getenv("WELLSTREAM_TEST_KAFKA")
DSN = os.getenv("WELLSTREAM_TEST_PG_DSN")
REGISTRY = "mock://kafka-it"
pytestmark = [pytest.mark.kafka, pytest.mark.skipif(not BOOTSTRAP, reason="WELLSTREAM_TEST_KAFKA not set")]


def _end_offsets(topic):
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"probe-{uuid.uuid4()}"})
    md = c.list_topics(topic, timeout=10).topics[topic]
    total = 0
    for p in md.partitions:
        lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        total += hi - lo
    c.close()
    return total


def test_end_to_end(raw_excerpt):
    from wellstream.cli import _detector_handler, _validator_handler
    from wellstream.streams import replay, run_sink, run_transform

    topics.create_topics(BOOTSTRAP)
    run = uuid.uuid4().hex[:8]
    raw_before = _end_offsets(topics.RAW)

    assert replay(BOOTSTRAP, REGISTRY, raw_excerpt) == len(raw_excerpt)
    assert _end_offsets(topics.RAW) - raw_before == len(raw_excerpt)

    n = run_transform(BOOTSTRAP, REGISTRY, topics.RAW, f"it-validator-{run}", f"it-validator-{run}",
                      _validator_handler(), [topics.CLEAN, topics.DLQ], max_idle_s=15)
    assert n == _end_offsets(topics.RAW)
    n = run_transform(BOOTSTRAP, REGISTRY, topics.CLEAN, f"it-detector-{run}", f"it-detector-{run}",
                      _detector_handler(), [topics.ALERTS], max_idle_s=15)
    assert n >= len(raw_excerpt)

    if DSN:
        from wellstream.warehouse import Loader, connect, init_db
        conn = connect(DSN)
        reset_warehouse(conn)
        init_db(conn, os.path.join(os.path.dirname(__file__), "..", "reference"))
        run_sink(BOOTSTRAP, REGISTRY, Loader(conn), group_id=f"it-sink-{run}", max_idle_s=15)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM fact_drilling_reading")
            assert cur.fetchone()[0] == len(raw_excerpt)
            cur.execute("SELECT event_type, status FROM fact_drilling_event ORDER BY 1")
            assert cur.fetchall() == [("DOWNLINK", "CLOSED"), ("FROZEN_SENSOR", "CLOSED")]
