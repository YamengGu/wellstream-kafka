"""Kafka topic catalogue: one place that defines names, partitioning and retention."""
from __future__ import annotations

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


def create_topics(bootstrap: str, replication_factor: int = 1) -> list[str]:
    """Create any missing topics. Returns the names that were created."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=10).topics)
    todo = [
        NewTopic(t.name, num_partitions=t.partitions, replication_factor=replication_factor, config=t.config())
        for t in TOPICS if t.name not in existing
    ]
    if not todo:
        return []
    for fut in admin.create_topics(todo).values():
        fut.result()  # raises if creation failed
    return [t.topic for t in todo]
