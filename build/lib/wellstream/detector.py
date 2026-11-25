"""Stage 2: a causal, per-wellbore event detector that runs on the clean stream.

It sees one reading at a time (no look-ahead), so the same code runs live on Kafka
and in the batch backfill. Four event types:

FROZEN_SENSOR  a channel repeats exactly the same live (non-zero) value for >= 2 min while it
               should be moving (SPP while pumping, torque while rotating, hookload always).
DOWNLINK       an abrupt standpipe-pressure drop of > 15 bar below the trailing 20-min
               median that recovers within 10 min. Rotary-steerable tools are steered this
               way; it is routine and logged as severity 1 (info), so it does not page anyone.
PRESSURE_LOSS  the same drop that does NOT recover within 10 min (washout / twist-off
               candidate, severity 3), or a gradual drop held for 5 min (severity 2).
Pressure rules only compare like with like: if flow moves more than 5% from the baseline
window's median (a pump set-point change), the baseline is relearned instead of alarming.

TORQUE_SPIKE   torque more than `torque_z` robust z-scores above its trailing 10-min median
               for >= 30 s, while RPM and WOB are steady (so a set-point change is not an
               anomaly).

Event ids are UUIDv5 of (wellbore, type, channel, start) so a replay re-sends the same ids
and the warehouse upserts instead of duplicating.
"""
from __future__ import annotations

import json
import uuid
from collections import deque
from dataclasses import dataclass, field

import numpy as np

from . import DETECTOR_VERSION

NAMESPACE = uuid.UUID("6f1c2d8e-4b7a-4c1e-9a55-3d2f0b9e7c11")
DRILLING_STATES = {1, 2}  # rotary drilling, slide drilling
ROTARY_DRILLING = 1


@dataclass(frozen=True)
class DetectorConfig:
    pumping_min_flow_lpm: float = 1500.0
    steady_min_rpm: float = 60.0
    frozen_min_s: float = 120.0
    frozen_min_value: tuple = (("spp_bar", 5.0), ("torque_knm", 0.5), ("hookload_t", 5.0))
    base_window_s: float = 1200.0
    base_min_samples: int = 24
    drop_bar: float = 15.0
    recover_bar: float = 5.0
    max_downlink_s: float = 600.0
    merge_gap_s: float = 120.0
    gradual_hold_s: float = 300.0
    flow_tolerance: float = 0.05
    torque_window_s: float = 600.0
    torque_min_samples: int = 60
    torque_z: float = 8.0
    torque_hold_s: float = 30.0
    setpoint_tolerance: float = 0.15
    torque_mad_floor_knm: float = 0.3


@dataclass
class _Frozen:
    value: float | None = None
    start: int = 0
    last: int = 0
    event: dict | None = None


@dataclass
class _Drop:
    start: int
    b0: float
    abrupt: bool
    min_spp: float
    last_low: int
    depth: float | None
    event: dict | None = None


@dataclass
class _WellState:
    frozen: dict[str, _Frozen] = field(default_factory=dict)
    base: deque = field(default_factory=deque)       # (ts, spp, flow) while pumping, outside drops
    recent: deque = field(default_factory=deque)     # (ts, spp) last 90 s while pumping
    drop: _Drop | None = None
    torque: deque = field(default_factory=deque)     # (ts, torque, rpm, wob) steady drilling
    spike_start: int | None = None
    spike_event: dict | None = None


def _median(values) -> float:
    return float(np.median(np.fromiter(values, dtype=float)))


class Detector:
    def __init__(self, config: DetectorConfig | None = None):
        self.cfg = config or DetectorConfig()
        self._wells: dict[str, _WellState] = {}

    # ------------------------------------------------------------------ public
    def process(self, r: dict) -> list[dict]:
        st = self._wells.setdefault(r["wellbore_id"], _WellState())
        events: list[dict] = []
        events += self._frozen_sensors(st, r)
        events += self._pressure(st, r)
        events += self._torque(st, r)
        return events

    def flush(self) -> list[dict]:
        """Close every open event (end of a batch backfill)."""
        out = []
        for st in self._wells.values():
            for fz in st.frozen.values():
                if fz.event:
                    out.append(self._close(fz.event, fz.last))
                    fz.event = None
            if st.drop and st.drop.event:
                out.append(self._close(st.drop.event, st.drop.last_low))
            st.drop = None
            if st.spike_event:
                out.append(self._close(st.spike_event, st.spike_event["start_ts"]))
                st.spike_event = None
        return out

    # ------------------------------------------------------------- helpers
    def _event(self, r: dict, etype: str, severity: int, start: int, depth, detail: dict,
               key: str = "") -> dict:
        eid = uuid.uuid5(NAMESPACE, f"{r['wellbore_id']}|{etype}|{key}|{start}")
        return {"event_id": str(eid), "wellbore_id": r["wellbore_id"], "event_type": etype,
                "status": "OPEN", "severity": severity, "start_ts": int(start), "end_ts": None,
                "depth_m": None if depth is None else round(float(depth), 1),
                "detail": json.dumps(detail, sort_keys=True), "detector_version": DETECTOR_VERSION}

    @staticmethod
    def _close(ev: dict, end: int, **detail) -> dict:
        out = dict(ev, status="CLOSED", end_ts=int(end))
        if detail:
            d = json.loads(ev["detail"])
            d.update(detail)
            out["detail"] = json.dumps(d, sort_keys=True)
        return out

    def _pumping(self, r: dict) -> bool:
        return r.get("rig_state_code") in DRILLING_STATES and (r.get("flow_lpm") or 0) > \
            self.cfg.pumping_min_flow_lpm

    # ------------------------------------------------------- frozen sensor
    def _frozen_sensors(self, st: _WellState, r: dict) -> list[dict]:
        out = []
        active = {
            "spp_bar": (r.get("flow_lpm") or 0) > 100,
            "torque_knm": (r.get("rpm") or 0) > 5,
            "hookload_t": True,
        }
        floor = dict(self.cfg.frozen_min_value)
        for ch, should_move in active.items():
            fz = st.frozen.setdefault(ch, _Frozen())
            v = r.get(ch)
            # a reading stuck at zero is "no signal", not a frozen transmitter; quantized
            # near-zero values (e.g. 0.01 kN.m steps) repeat legitimately
            should_move = should_move and v is not None and abs(v) >= floor[ch]
            if should_move and v == fz.value:
                fz.last = r["ts"]
                if fz.event is None and (fz.last - fz.start) / 1000 >= self.cfg.frozen_min_s:
                    fz.event = self._event(r, "FROZEN_SENSOR", 2, fz.start, r.get("bit_depth_m"),
                                           {"channel": ch, "value": round(v, 4)}, key=ch)
                    out.append(fz.event)
                continue
            if fz.event is not None:
                out.append(self._close(fz.event, fz.last))
                fz.event = None
            fz.value = v if should_move else None
            fz.start = fz.last = r["ts"]
        return out

    # ------------------------------------------------- pressure: downlinks/loss
    def _pressure(self, st: _WellState, r: dict) -> list[dict]:
        c, ts, spp = self.cfg, r["ts"], r.get("spp_bar")
        frozen_spp = st.frozen.get("spp_bar")
        if not self._pumping(r) or spp is None or (frozen_spp and frozen_spp.event):
            # pumps off (connection) or no trustworthy pressure: an open drop is explained, or ends
            out = []
            if st.drop and st.drop.event:
                out.append(self._close(st.drop.event, st.drop.last_low, ended_by="pumps_off_or_no_data"))
            st.drop = None
            st.recent.clear()
            return out

        while st.base and st.base[0][0] < ts - c.base_window_s * 1000:
            st.base.popleft()
        while st.recent and st.recent[0][0] < ts - 90_000:
            st.recent.popleft()
        st.recent.append((ts, spp))
        flow = r["flow_lpm"]
        if len(st.base) < c.base_min_samples:
            st.base.append((ts, spp, flow))
            return []
        base_flow = _median(f for _, _, f in st.base)
        if abs(flow - base_flow) > c.flow_tolerance * base_flow:
            # pump set-point change: SPP follows flow, so relearn the baseline at the new rate
            out = []
            if st.drop and st.drop.event:
                out.append(self._close(st.drop.event, st.drop.last_low, ended_by="flow_setpoint_change"))
            st.drop = None
            st.base.clear()
            st.base.append((ts, spp, flow))
            return out
        base = _median(v for _, v, _ in st.base)

        d = st.drop
        if d is None:
            if spp < base - c.drop_bar:
                before = [v for t, v in st.recent if ts - 90_000 <= t <= ts - 30_000]
                abrupt = bool(before) and _median(before) > base - c.recover_bar
                st.drop = _Drop(ts, base, abrupt, spp, ts, r.get("bit_depth_m"))
            else:
                st.base.append((ts, spp, flow))  # only normal samples teach the baseline
            return []

        out = []
        low = spp < d.b0 - c.drop_bar
        recovered = spp > d.b0 - c.recover_bar
        if low:
            d.last_low, d.min_spp = ts, min(d.min_spp, spp)
        detail = {"baseline_bar": round(d.b0, 1), "max_drop_bar": round(d.b0 - d.min_spp, 1)}

        if d.abrupt:
            if d.event is None and not recovered and (ts - d.start) / 1000 > c.max_downlink_s:
                d.event = self._event(r, "PRESSURE_LOSS", 3, d.start, d.depth,
                                      dict(detail, pattern="abrupt_no_recovery"))
                out.append(d.event)
            if recovered and (ts - d.last_low) / 1000 >= c.merge_gap_s:
                if d.event is not None:
                    out.append(self._close(d.event, d.last_low, **detail))
                elif (d.last_low - d.start) / 1000 <= c.max_downlink_s:
                    ev = self._event(r, "DOWNLINK", 1, d.start, d.depth, detail)
                    out.append(self._close(ev, d.last_low,
                                           duration_min=round((d.last_low - d.start) / 60000, 2)))
                st.drop = None
        else:  # gradual: a slow loss is the washout signature
            if d.event is None and low and (ts - d.start) / 1000 >= c.gradual_hold_s:
                d.event = self._event(r, "PRESSURE_LOSS", 2, d.start, d.depth, dict(detail, pattern="gradual"))
                out.append(d.event)
            if recovered:
                if d.event is not None:
                    out.append(self._close(d.event, d.last_low, **detail))
                st.drop = None
            elif not low and d.event is None:
                st.drop = None  # brief dip that never held: forget it
        return out

    # ----------------------------------------------------------- torque spike
    def _torque(self, st: _WellState, r: dict) -> list[dict]:
        c, ts = self.cfg, r["ts"]
        tq, rpm, wob = r.get("torque_knm"), r.get("rpm"), r.get("wob_t")
        steady = (r.get("rig_state_code") == ROTARY_DRILLING and (r.get("flow_lpm") or 0) > c.pumping_min_flow_lpm
                  and (rpm or 0) > c.steady_min_rpm and tq is not None and wob is not None)
        out = []
        if not steady:
            if st.spike_event:
                out.append(self._close(st.spike_event, ts))
                st.spike_event = None
            st.spike_start = None
            return out
        while st.torque and st.torque[0][0] < ts - c.torque_window_s * 1000:
            st.torque.popleft()
        z = 0.0
        if len(st.torque) >= c.torque_min_samples:
            arr = np.array([(t, w) for _, t, _, w in st.torque])
            med = float(np.median(arr[:, 0]))
            mad = max(float(np.median(np.abs(arr[:, 0] - med))), 0.03 * abs(med), c.torque_mad_floor_knm)
            rpm_med = _median(x[2] for x in st.torque)
            wob_med = float(np.median(arr[:, 1]))
            same_setpoint = (abs(rpm - rpm_med) <= c.setpoint_tolerance * rpm_med
                             and abs(wob - wob_med) <= c.setpoint_tolerance * max(abs(wob_med), 1.0))
            if same_setpoint:
                z = 0.6745 * (tq - med) / mad
        if z > c.torque_z:
            st.spike_start = st.spike_start or ts
            if st.spike_event is None and (ts - st.spike_start) / 1000 >= c.torque_hold_s:
                st.spike_event = self._event(r, "TORQUE_SPIKE", 2, st.spike_start, r.get("bit_depth_m"),
                                             {"robust_z": round(z, 1), "torque_knm": round(tq, 2)})
                out.append(st.spike_event)
        else:
            if st.spike_event and z < c.torque_z / 2:
                out.append(self._close(st.spike_event, ts))
                st.spike_event = None
            if z <= c.torque_z:
                st.spike_start = None
        if z < 4:  # anomalous samples do not teach the baseline
            st.torque.append((ts, tq, rpm, wob))
        return out
