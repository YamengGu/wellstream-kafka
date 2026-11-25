"""Batch path: run a whole wellbore log through the same Validator and Detector used on Kafka.

Used for historical backfill (optionally straight into the warehouse) and for the backtest
report that checks detected downlinks against the driller's log.
"""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pandas as pd

from .detector import Detector
from .validator import Validator
from .volve import WELLBORES, iter_raw_readings, load_log


def run_well(data_dir: str | Path, well: str, sink=None, batch_size: int = 5000) -> dict:
    """Process one wellbore. `sink(readings, events)` is called per batch if given."""
    ref = WELLBORES[well]
    df = load_log(Path(data_dir) / ref["file"])
    validator, detector = Validator(), Detector()
    routes, flags, dlq_reasons = Counter(), Counter(), Counter()
    events: dict[str, dict] = {}
    buf_r, buf_e = [], []

    def drain():
        if sink and (buf_r or buf_e):
            sink(list(buf_r), list(buf_e))
        buf_r.clear()
        buf_e.clear()

    for raw in iter_raw_readings(df, ref["wellbore_id"], source="BACKFILL"):
        route, rec = validator.process(raw)
        routes[route] += 1
        if route == "dlq":
            dlq_reasons[rec["reason"]] += 1
            continue
        flags.update(rec["quality_flags"])
        buf_r.append(rec)
        for ev in detector.process(rec):
            events[ev["event_id"]] = ev
            buf_e.append(ev)
        if len(buf_r) >= batch_size:
            drain()
    for ev in detector.flush():
        events[ev["event_id"]] = ev
        buf_e.append(ev)
    drain()
    return {"well": well, "wellbore_id": ref["wellbore_id"], "readings": len(df), "routes": dict(routes),
            "quality_flags": dict(flags.most_common()), "dlq_reasons": dict(dlq_reasons),
            "events": list(events.values())}


def downlink_agreement(events: list[dict], messages_path: str | Path, tolerance: str = "15min") -> dict:
    """Compare DOWNLINK events with 'DLK' entries in the driller's log (WITSML messages)."""
    msgs = pd.DataFrame(json.loads(Path(messages_path).read_text()))
    msgs["time"] = pd.to_datetime(msgs["t"], unit="ms", utc=True)
    dlk = msgs[msgs["text"].str.contains(r"\bDLK\b", regex=True)]
    dl = pd.DataFrame([e for e in events if e["event_type"] == "DOWNLINK"])
    tol = pd.Timedelta(tolerance)
    if dl.empty:
        return {"detected": 0, "confirmed_by_log": 0, "log_entries": len(dlk), "log_entries_found": 0}
    s = pd.to_datetime(dl["start_ts"], unit="ms", utc=True)
    e = pd.to_datetime(dl["end_ts"], unit="ms", utc=True)
    confirmed = sum(((dlk["time"] >= a - tol) & (dlk["time"] <= b + tol)).any() for a, b in zip(s, e, strict=True))
    found = sum(((s - tol <= t) & (e + tol >= t)).any() for t in dlk["time"])
    return {"detected": len(dl), "confirmed_by_log": int(confirmed), "log_entries": len(dlk),
            "log_entries_found": int(found), "tolerance": tolerance}


def summarize(result: dict) -> dict:
    ev = pd.DataFrame(result["events"])
    by_type = {} if ev.empty else ev.groupby("event_type").size().to_dict()
    notable = []
    for e in result["events"]:
        if e["event_type"] != "DOWNLINK":
            notable.append({
                "event_type": e["event_type"], "severity": e["severity"],
                "start_utc": pd.to_datetime(e["start_ts"], unit="ms", utc=True).isoformat(),
                "end_utc": None if e["end_ts"] is None else
                pd.to_datetime(e["end_ts"], unit="ms", utc=True).isoformat(),
                "depth_m": e["depth_m"], "detail": json.loads(e["detail"])})
    notable.sort(key=lambda x: x["start_utc"])
    out = {k: result[k] for k in ("well", "wellbore_id", "readings", "routes", "quality_flags", "dlq_reasons")}
    out["events_by_type"] = {k: int(v) for k, v in by_type.items()}
    out["non_downlink_events"] = notable
    return out
