"""Warehouse tests against a real PostgreSQL. Set WELLSTREAM_TEST_PG_DSN to an empty, disposable database."""
import os
from pathlib import Path

import pytest
from conftest import reset_warehouse

from wellstream.detector import Detector
from wellstream.validator import Validator

DSN = os.getenv("WELLSTREAM_TEST_PG_DSN")
pytestmark = [pytest.mark.postgres, pytest.mark.skipif(not DSN, reason="WELLSTREAM_TEST_PG_DSN not set")]
REF = Path(__file__).parents[1] / "reference"


@pytest.fixture()
def conn():
    from wellstream.warehouse import connect, init_db
    c = connect(DSN)
    reset_warehouse(c)
    init_db(c, REF)
    yield c
    c.close()


def _pipeline(raw_excerpt):
    v, d = Validator(), Detector()
    readings, events = [], []
    for r in raw_excerpt:
        _, c = v.process(r)
        readings.append(c)
        events += d.process(c)
    return readings, events + d.flush()


def _count(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]


def test_load_is_idempotent(conn, raw_excerpt):
    from wellstream.warehouse import Loader
    readings, events = _pipeline(raw_excerpt)
    Loader(conn).write(readings, events)
    Loader(conn).write(readings, events)  # Kafka redelivery / replay
    assert _count(conn, "SELECT count(*) FROM fact_drilling_reading") == len(raw_excerpt)
    assert _count(conn, "SELECT count(*) FROM fact_drilling_event") == 2
    assert _count(conn, "SELECT count(*) FROM fact_drilling_event WHERE status = 'CLOSED'") == 2


def test_closed_event_is_not_reopened_by_a_late_open_message(conn, raw_excerpt):
    from wellstream.warehouse import Loader
    _, events = _pipeline(raw_excerpt)
    opened = next(e for e in events if e["status"] == "OPEN")
    closed = next(e for e in events if e["status"] == "CLOSED" and e["event_id"] == opened["event_id"])
    loader = Loader(conn)
    loader.write([], [closed])
    loader.write([], [opened])
    assert _count(conn, f"SELECT count(*) FROM fact_drilling_event WHERE event_id = '{opened['event_id']}' "
                        "AND status = 'CLOSED' AND end_ts IS NOT NULL") == 1


def test_scd2_wellbore_history(conn):
    with conn, conn.cursor() as cur:
        cur.execute("SELECT upsert_wellbore('NO 15/9-F-5', 'NO 15/9-F-5', 'Volve', 'MAIN', 'Equinor', NULL)")
        same = cur.fetchone()[0]
        cur.execute("SELECT upsert_wellbore('NO 15/9-F-5', 'NO 15/9-F-5', 'Volve', 'MAIN', 'Equinor', 'P&A')")
        new = cur.fetchone()[0]
    assert new != same
    assert _count(conn, "SELECT count(*) FROM dim_wellbore WHERE wellbore_id = 'NO 15/9-F-5'") == 2
    current = "SELECT count(*) FROM dim_wellbore WHERE wellbore_id = 'NO 15/9-F-5' AND is_current"
    assert _count(conn, current) == 1


def test_unknown_wellbore_gets_a_late_arriving_dimension_row(conn, raw_excerpt):
    from wellstream.warehouse import Loader
    readings, _ = _pipeline(raw_excerpt[:10])
    Loader(conn).write([dict(r, wellbore_id="NEW-1") for r in readings], [])
    assert _count(conn, "SELECT count(*) FROM dim_wellbore WHERE wellbore_id = 'NEW-1' AND is_current") == 1


def test_marts_and_dictionary(conn, raw_excerpt):
    from wellstream.warehouse import Loader, data_dictionary, refresh_marts
    readings, events = _pipeline(raw_excerpt)
    Loader(conn).write(readings, events)
    refresh_marts(conn)
    assert _count(conn, "SELECT count(*) FROM ml_features_1min WHERE label_sensor_fault") >= 37
    assert _count(conn, "SELECT count(*) FROM ml_features_1min WHERE label_downlink") >= 3
    assert _count(conn, "SELECT count(*) FROM mart_daily_drilling") == 2
    md = data_dictionary(conn)
    for table in ("fact_drilling_reading", "fact_drilling_event", "dim_wellbore", "dim_date"):
        assert f"## `{table}`" in md
