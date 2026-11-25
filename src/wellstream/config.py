"""Runtime settings, read from environment variables (12-factor style)."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    bootstrap: str
    schema_registry_url: str
    pg_dsn: str
    data_dir: str

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            bootstrap=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
            # "mock://..." uses an in-process registry (tests, CI); otherwise a real Schema Registry
            schema_registry_url=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
            pg_dsn=os.getenv("WAREHOUSE_DSN", "postgresql://wellstream:wellstream@localhost:5432/wellstream"),
            data_dir=os.getenv("WELLSTREAM_DATA_DIR", "data"),
        )
