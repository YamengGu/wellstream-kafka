"""Stage 1: validate raw readings, convert to field units, route rejects to the DLQ.

Rules are deliberately simple and explainable:
* a record without a wellbore or timestamp, or that repeats / goes back in time, is rejected;
* a channel outside its physical range is set to null and flagged (the rest of the row is kept);
* a row with no usable channel left is rejected.
"""
from __future__ import annotations

import json
import time

from . import VALIDATOR_VERSION

TONNE_F = 9806.65  # N per tonne-force

# raw field -> (clean field, multiply by)
CONVERSIONS = {
    "hole_depth_m": ("hole_depth_m", 1.0),
    "bit_depth_m": ("bit_depth_m", 1.0),
    "hookload_n": ("hookload_t", 1 / TONNE_F),
    "wob_n": ("wob_t", 1 / TONNE_F),
    "rpm_rps": ("rpm", 60.0),
    "torque_nm": ("torque_knm", 1e-3),
    "spp_pa": ("spp_bar", 1e-5),
    "flow_in_m3s": ("flow_lpm", 60000.0),
    "rop_ms": ("rop_mh", 3600.0),
    "mud_weight_kgm3": ("mud_weight_kgm3", 1.0),
}

# Physical limits in field units. Small negative values are sensor zero offsets and are kept.
BOUNDS = {
    "hole_depth_m": (0.0, 12000.0),
    "bit_depth_m": (-60.0, 12000.0),  # bit above the rotary table during BHA handling
    "hookload_t": (0.0, 600.0),
    "wob_t": (-20.0, 100.0),
    "rpm": (-5.0, 400.0),
    "torque_knm": (-2.0, 80.0),
    "spp_bar": (-2.0, 700.0),
    "flow_lpm": (-150.0, 6000.0),
    "rop_mh": (0.0, 500.0),
    "mud_weight_kgm3": (500.0, 2500.0),
}

# Channels where the logger writes exactly 0 to mean "no value"
NOT_REPORTED_IF_ZERO = {"mud_weight_kgm3": "mud_weight_not_reported", "hole_depth_m": "hole_depth_not_reported"}

MAX_FUTURE_MS = 24 * 3600 * 1000


class Validator:
    def __init__(self) -> None:
        self._last_ts: dict[str, int] = {}

    def process(self, raw: dict, now_ms: int | None = None) -> tuple[str, dict]:
        """Return ("clean", record) or ("dlq", record)."""
        wb, ts = raw.get("wellbore_id"), raw.get("ts")
        if not wb or ts is None:
            return "dlq", self._reject(raw, "missing_key")
        now_ms = int(time.time() * 1000) if now_ms is None else now_ms
        if ts > now_ms + MAX_FUTURE_MS:
            return "dlq", self._reject(raw, "future_timestamp")
        last = self._last_ts.get(wb)
        if last is not None and ts <= last:
            return "dlq", self._reject(raw, "duplicate_or_out_of_order")

        out = {"wellbore_id": wb, "ts": ts, "rig_state_code": raw.get("rig_state_code"),
               "validator_version": VALIDATOR_VERSION}
        flags: list[str] = []
        n_valid = 0
        for raw_name, (name, factor) in CONVERSIONS.items():
            v = raw.get(raw_name)
            if v is not None:
                v = v * factor
                if name in NOT_REPORTED_IF_ZERO and v == 0.0:
                    v, flag = None, NOT_REPORTED_IF_ZERO[name]  # logger writes 0 when unset
                elif not BOUNDS[name][0] <= v <= BOUNDS[name][1]:
                    v, flag = None, f"{name}_out_of_range"
                else:
                    flag = None
                if flag:
                    flags.append(flag)
            if v is not None:
                n_valid += 1
            out[name] = v
        if n_valid == 0:
            return "dlq", self._reject(raw, "no_valid_channels")
        state = out["rig_state_code"]
        if state is not None and not 0 <= state <= 8:
            out["rig_state_code"] = None
            flags.append("rig_state_unknown")
        out["quality_flags"] = flags
        self._last_ts[wb] = ts
        return "clean", out

    @staticmethod
    def _reject(raw: dict, reason: str) -> dict:
        return {"wellbore_id": raw.get("wellbore_id") or "UNKNOWN", "ts": raw.get("ts"), "reason": reason,
                "payload_json": json.dumps(raw, default=str), "validator_version": VALIDATOR_VERSION}
