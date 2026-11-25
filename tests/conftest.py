from pathlib import Path

import pytest

from wellstream.volve import iter_raw_readings, load_log

FIXTURE = Path(__file__).parent / "fixtures" / "F-1C_excerpt.parquet"
WELLBORE = "NO 15/9-F-1 C"


@pytest.fixture(scope="session")
def raw_excerpt() -> list[dict]:
    """Two real slices of Volve F-1 C: a downlink (23 Aug) and the frozen SPP transmitter (25 Aug)."""
    return list(iter_raw_readings(load_log(FIXTURE), WELLBORE))


NOISY = ("spp_bar", "torque_knm", "hookload_t")


def reading(ts_s: float, exact: tuple = (), **kw) -> dict:
    """A clean reading during steady rotary drilling, overridable per test.

    Real transmitters never repeat a live value exactly, so small deterministic noise is added to
    SPP, torque and hookload (otherwise the frozen-sensor rule fires). Channels named in
    `exact` are left untouched, to simulate a frozen transmitter.
    """
    base = {"wellbore_id": WELLBORE, "ts": int(ts_s * 1000), "rig_state_code": 1, "hole_depth_m": 3000.0,
            "bit_depth_m": 3000.0, "hookload_t": 130.0, "wob_t": 8.0, "rpm": 140.0, "torque_knm": 16.0,
            "spp_bar": 180.0, "flow_lpm": 2300.0, "rop_mh": 20.0, "mud_weight_kgm3": 1320.0,
            "quality_flags": [], "validator_version": "test"}
    base.update(kw)
    k = int(ts_s) % 7 - 3
    for ch in NOISY:
        if ch not in exact and base[ch] is not None:
            base[ch] += k * 0.01
    return base


def reset_warehouse(conn) -> None:
    """Drop WellStream objects (not the whole schema: that would drop the TimescaleDB extension,
    which cannot be re-created in the same session)."""
    with conn, conn.cursor() as cur:
        cur.execute("""
            DROP MATERIALIZED VIEW IF EXISTS ml_features_1min;
            DROP VIEW IF EXISTS mart_daily_drilling, v_reading_section;
            DROP TABLE IF EXISTS fact_drilling_reading;  -- a hypertable must be dropped on its own
            DROP TABLE IF EXISTS fact_drilling_event, dim_hole_section, dim_wellbore, dim_rig_state,
                                 dim_date CASCADE;
            DROP FUNCTION IF EXISTS upsert_wellbore(TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TIMESTAMPTZ);
        """)
