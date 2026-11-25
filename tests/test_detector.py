import json

import pandas as pd
import pytest
from conftest import reading

from wellstream.detector import Detector
from wellstream.validator import Validator


def run(records, det=None):
    det = det or Detector()
    events = []
    for r in records:
        events += det.process(r)
    return events


def steady(start_s, minutes, step_s=5, **kw):
    """Steady drilling with a little noise on SPP and torque (as real sensors have)."""
    out = []
    for i in range(int(minutes * 60 / step_s)):
        t = start_s + i * step_s
        jitter = ((i * 7919) % 13 - 6) / 10
        r = reading(t, **{"spp_bar": 180.0 + jitter, "torque_knm": 16.0 + jitter / 5, **kw})
        out.append(r)
    return out


def test_no_events_on_steady_drilling():
    assert run(steady(0, 60)) == []


def test_frozen_spp_opens_after_two_minutes_and_closes():
    recs = steady(0, 30)
    t0 = recs[-1]["ts"] / 1000
    frozen = [reading(t0 + 5 * (i + 1), exact=("spp_bar",), spp_bar=198.56) for i in range(60)]
    after = steady(t0 + 305, 5)
    ev = run(recs + frozen + after)
    fz = [e for e in ev if e["event_type"] == "FROZEN_SENSOR"]
    assert [e["status"] for e in fz] == ["OPEN", "CLOSED"]
    assert json.loads(fz[0]["detail"])["channel"] == "spp_bar"
    assert fz[1]["end_ts"] - fz[1]["start_ts"] == 295_000
    # the flat pressure must not also be read as a pressure event
    assert not [e for e in ev if e["event_type"] in ("DOWNLINK", "PRESSURE_LOSS")]


def test_torque_stuck_at_zero_is_not_a_frozen_sensor():
    recs = [reading(5 * i, exact=("torque_knm",), torque_knm=0.0) for i in range(120)]
    assert not [e for e in run(recs) if e["event_type"] == "FROZEN_SENSOR"]


def test_abrupt_drop_that_recovers_is_a_downlink():
    recs = steady(0, 30)
    t0 = recs[-1]["ts"] / 1000
    drop = [reading(t0 + 5 * (i + 1), spp_bar=135.0) for i in range(48)]  # 4 min, -45 bar
    ev = run(recs + drop + steady(t0 + 245, 10))
    assert [(e["event_type"], e["status"], e["severity"]) for e in ev] == [("DOWNLINK", "CLOSED", 1)]
    assert json.loads(ev[0]["detail"])["max_drop_bar"] == pytest.approx(45, abs=1)


def test_abrupt_drop_without_recovery_is_critical_pressure_loss():
    recs = steady(0, 30)
    t0 = recs[-1]["ts"] / 1000
    ev = run(recs + [reading(t0 + 5 * (i + 1), spp_bar=135.0 + (i % 2) / 10) for i in range(150)])
    assert [(e["event_type"], e["status"], e["severity"]) for e in ev] == [("PRESSURE_LOSS", "OPEN", 3)]
    assert json.loads(ev[0]["detail"])["pattern"] == "abrupt_no_recovery"


def test_gradual_loss_is_flagged():
    recs = steady(0, 30)
    t0 = recs[-1]["ts"] / 1000
    ramp = [reading(t0 + 5 * (i + 1), spp_bar=180.0 - min(i, 60) * 0.5) for i in range(150)]
    ev = run(recs + ramp)
    assert [(e["event_type"], e["severity"]) for e in ev] == [("PRESSURE_LOSS", 2)]
    assert json.loads(ev[0]["detail"])["pattern"] == "gradual"


def test_flow_setpoint_change_is_not_a_pressure_event():
    recs = steady(0, 30)
    t0 = recs[-1]["ts"] / 1000
    # driller cuts flow 2300 -> 1900 L/min: SPP falls ~30% with it
    lower = steady(t0 + 5, 30, flow_lpm=1900.0)
    for r in lower:
        r["spp_bar"] -= 55
    assert run(recs + lower) == []


def test_pumps_off_cancels_a_drop():
    recs = steady(0, 30)
    t0 = recs[-1]["ts"] / 1000
    ramp_down = [reading(t0 + 5, spp_bar=150.0), reading(t0 + 10, spp_bar=60.0, flow_lpm=800.0)]
    off = [reading(t0 + 15 + 5 * i, spp_bar=0.5, flow_lpm=0.0, rpm=0.0, rig_state_code=3) for i in range(200)]
    assert run(recs + ramp_down + off) == []


def test_torque_spike_needs_steady_setpoint_and_hold_time():
    recs = steady(0, 20)
    t0 = recs[-1]["ts"] / 1000
    spike = [reading(t0 + 5 * (i + 1), torque_knm=26.0) for i in range(12)]
    ev = run(recs + spike + steady(t0 + 65, 5))
    assert [(e["event_type"], e["status"]) for e in ev] == [("TORQUE_SPIKE", "OPEN"),
                                                            ("TORQUE_SPIKE", "CLOSED")]
    # same torque rise, but the driller doubled RPM: an expected response
    spike_rpm = [reading(t0 + 5 * (i + 1), torque_knm=26.0, rpm=280.0) for i in range(12)]
    assert run(recs + spike_rpm) == []


def test_event_ids_are_deterministic_for_replays():
    recs = steady(0, 30)
    t0 = recs[-1]["ts"] / 1000
    recs += [reading(t0 + 5 * (i + 1), exact=("spp_bar",), spp_bar=198.56) for i in range(40)]
    assert [e["event_id"] for e in run(recs)] == [e["event_id"] for e in run(recs)]


def test_wellbores_have_independent_state():
    a = steady(0, 30)
    b = [dict(r, wellbore_id="OTHER", spp_bar=r["spp_bar"] - 90) for r in steady(0, 30)]  # other regime
    merged = [x for pair in zip(a, b, strict=True) for x in pair]
    assert run(merged) == []


def test_real_volve_excerpt(raw_excerpt):
    """On real F-1 C data: one RSS downlink, and the frozen SPP transmitter from 05:41 to 06:19 UTC."""
    v, d = Validator(), Detector()
    events = []
    for raw in raw_excerpt:
        _, clean = v.process(raw)
        events += d.process(clean)
    events += d.flush()
    final = {e["event_id"]: e for e in events}.values()
    kinds = sorted(e["event_type"] for e in final)
    assert kinds == ["DOWNLINK", "FROZEN_SENSOR"]
    fz = next(e for e in final if e["event_type"] == "FROZEN_SENSOR")
    assert pd.Timestamp(fz["start_ts"], unit="ms", tz="UTC") == pd.Timestamp("2013-08-25 05:41:47", tz="UTC")
    assert pd.Timestamp(fz["end_ts"], unit="ms", tz="UTC") == pd.Timestamp("2013-08-25 06:19:04", tz="UTC")
    assert json.loads(fz["detail"])["channel"] == "spp_bar"
