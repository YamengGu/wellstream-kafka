from wellstream import topics
from wellstream.detector import Detector
from wellstream.serde import AvroCodec
from wellstream.validator import Validator

REGISTRY = "mock://serde-test"


def test_all_topic_schemas_round_trip(raw_excerpt):
    v, d = Validator(), Detector()
    raw = raw_excerpt[100]
    _, clean = v.process(raw)
    _, dlq = v.process(raw)  # same ts again -> rejected
    events = []
    for r in raw_excerpt:
        _, c = Validator().process(r)
        events += d.process(c)
    samples = {topics.RAW: raw, topics.CLEAN: clean, topics.DLQ: dlq, topics.ALERTS: events[0]}
    for topic, rec in samples.items():
        codec = AvroCodec(topic, REGISTRY)
        payload = codec.encode(rec)
        assert payload[0] == 0  # Confluent wire format: magic byte + schema id
        back = codec.decode(payload)
        for k, val in rec.items():
            assert back[k] == val, (topic, k)


def test_raw_schema_accepts_missing_channels():
    codec = AvroCodec(topics.RAW, REGISTRY)
    rec = {"wellbore_id": "W", "ts": 1, "source": "TEST"}
    back = codec.decode(codec.encode(rec))
    assert back["spp_pa"] is None and back["rig_state_code"] is None
