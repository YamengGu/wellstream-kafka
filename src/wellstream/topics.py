"""Kafka topic catalogue: one place that defines names, partitioning and retention."""
from __future__ import annotations

import time
from dataclasses import dataclass

DAY_MS = 24 * 3600 * 1000


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    retention_ms: int | None  # None = keep forever (compacted topics)
    cleanup_policy: str = "delete"
    description: str = ""

    def config(self) -> dict[str, str]:
        cfg = {"cleanup.policy": self.cleanup_policy}
        if self.retention_ms is not None:
            cfg["retention.ms"] = str(self.retention_ms)
        return cfg


RAW = "drilling.sensor.raw.v1"
CLEAN = "drilling.sensor.clean.v1"
ALERTS = "drilling.event.v1"
DLQ = "drilling.sensor.dlq.v1"

# Every topic is keyed by wellbore_id, so all readings of one wellbore stay in order on one partition.
TOPICS = [
    TopicSpec(RAW, 6, 7 * DAY_MS, description="Rig sensor readings as logged (SI units)"),
    TopicSpec(CLEAN, 6, 30 * DAY_MS, description="Validated readings in field units with quality flags"),
    TopicSpec(ALERTS, 3, 90 * DAY_MS,
              description="Drilling events: downlinks, pressure loss, frozen sensors, torque spikes"),
    TopicSpec(DLQ, 3, 30 * DAY_MS, description="Readings rejected by the validator, with a reason"),
]


def create_topics(bootstrap: str, replication_factor: int = 1, timeout_s: float = 30.0) -> list[str]:
    """Create any missing topics and wait until every partition has a leader.

    Returns the names that were created.
    """
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=10).topics)
    todo = [
        NewTopic(t.name, num_partitions=t.partitions, replication_factor=replication_factor, config=t.config())
        for t in TOPICS if t.name not in existing
    ]
    if todo:
        for fut in admin.create_topics(todo).values():
            fut.result()  # raises if creation failed
    _wait_for_leaders(admin, [t.name for t in TOPICS], timeout_s)
    return [t.topic for t in todo]


def _wait_for_leaders(admin, names: list[str], timeout_s: float) -> None:
    """A new topic is usable only once each partition has an elected leader."""
    deadline = time.monotonic() + timeout_s
    while True:
        md = admin.list_topics(timeout=10).topics
        ready = all(
            n in md and md[n].error is None and md[n].partitions
            and all(p.leader >= 0 and p.error is None for p in md[n].partitions.values())
            for n in names
        )
        if ready:
            return
        if time.monotonic() > deadline:
            raise TimeoutError(f"topic partitions have no leader after {timeout_s}s: {names}")
        time.sleep(0.5)
