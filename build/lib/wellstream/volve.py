"""Equinor Volve drilling data: reference data and a reader that turns logs into raw readings.

Source: Equinor's Volve data release ("WITSML Realtime drilling data"), as converted to a
canonical channel table by the open-source DrillDash project (MIT). Values are SI as logged.
Volve data (c) Equinor and the Volve licence partners, Equinor Open Data Licence:
https://www.equinor.com/energy/volve-data-sharing
"""
from __future__ import annotations

import math
from collections.abc import Iterator
from pathlib import Path

import pandas as pd

# Short name -> reference attributes (also loaded into dim_wellbore from reference/wellbores.csv)
WELLBORES = {
    "F-1C": {"wellbore_id": "NO 15/9-F-1 C", "file": "F-1C_drilling.parquet"},
    "F-5": {"wellbore_id": "NO 15/9-F-5", "file": "F-5_drilling.parquet"},
}

# parquet column -> raw schema field
RAW_COLUMNS = {
    "hole_depth": "hole_depth_m",
    "bit_depth": "bit_depth_m",
    "hookload": "hookload_n",
    "wob": "wob_n",
    "rpm": "rpm_rps",
    "torque": "torque_nm",
    "spp": "spp_pa",
    "flow_in": "flow_in_m3s",
    "rop": "rop_ms",
    "mw_in": "mud_weight_kgm3",
}


def load_log(path: str | Path) -> pd.DataFrame:
    """Read one wellbore's log, sorted by time, with a single rig-state column."""
    d = pd.read_parquet(path).sort_values("t").drop_duplicates("t", keep="last")
    # Baker Hughes logs carry the rig's own activity code; older SLB logs do not,
    # so fall back to the rule-based state DrillDash derives (same 0-8 numbering).
    d["rig_state_code"] = d["rig_state_rig"] if (d["rig_state_rig"] > 0).any() else d["rig_state"]
    return d.reset_index(drop=True)


def _num(v) -> float | None:
    if v is None:
        return None
    v = float(v)
    return None if math.isnan(v) else v


def iter_raw_readings(df: pd.DataFrame, wellbore_id: str, source: str = "REPLAY") -> Iterator[dict]:
    """Yield records matching raw_reading.avsc, in time order."""
    cols = list(RAW_COLUMNS)
    for row in df[["t", "rig_state_code", *cols]].itertuples(index=False, name=None):
        t, state, *vals = row
        rec = {"wellbore_id": wellbore_id, "ts": int(t), "source": source,
               "rig_state_code": None if pd.isna(state) else int(state)}
        for col, v in zip(cols, vals, strict=True):
            rec[RAW_COLUMNS[col]] = _num(v)
        yield rec
