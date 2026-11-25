import pytest

from wellstream.validator import Validator

NOW = 1_800_000_000_000


def raw(ts=1_377_400_000_000, **kw):
    r = {"wellbore_id": "W", "ts": ts, "rig_state_code": 1, "hole_depth_m": 3000.0, "bit_depth_m": 2999.0,
         "hookload_n": 1.3e6, "wob_n": 7.8e4, "rpm_rps": 2.5, "torque_nm": 16000.0, "spp_pa": 1.8e7,
         "flow_in_m3s": 0.04, "rop_ms": 0.005, "mud_weight_kgm3": 1320.0, "source": "TEST"}
    r.update(kw)
    return r


def test_converts_si_to_field_units():
    route, out = Validator().process(raw(), now_ms=NOW)
    assert route == "clean"
    assert out["spp_bar"] == pytest.approx(180.0)
    assert out["flow_lpm"] == pytest.approx(2400.0)
    assert out["rpm"] == pytest.approx(150.0)
    assert out["torque_knm"] == pytest.approx(16.0)
    assert out["rop_mh"] == pytest.approx(18.0)
    assert out["wob_t"] == pytest.approx(7.954, abs=1e-3)
    assert out["quality_flags"] == []


def test_out_of_range_channel_is_nulled_and_flagged_but_row_kept():
    route, out = Validator().process(raw(wob_n=-1.6e6), now_ms=NOW)
    assert route == "clean"
    assert out["wob_t"] is None
    assert out["quality_flags"] == ["wob_t_out_of_range"]
    assert out["spp_bar"] == pytest.approx(180.0)


@pytest.mark.parametrize("field,flag", [("mud_weight_kgm3", "mud_weight_not_reported"),
                                        ("hole_depth_m", "hole_depth_not_reported")])
def test_logger_zero_means_not_reported(field, flag):
    _, out = Validator().process(raw(**{field: 0.0}), now_ms=NOW)
    assert out[field] is None and flag in out["quality_flags"]


def test_duplicate_or_older_timestamp_goes_to_dlq():
    v = Validator()
    assert v.process(raw(ts=1000), now_ms=NOW)[0] == "clean"
    route, rej = v.process(raw(ts=1000), now_ms=NOW)
    assert route == "dlq" and rej["reason"] == "duplicate_or_out_of_order"
    assert v.process(raw(ts=999), now_ms=NOW)[0] == "dlq"
    assert v.process(raw(ts=1001), now_ms=NOW)[0] == "clean"


def test_ordering_is_tracked_per_wellbore():
    v = Validator()
    assert v.process(raw(ts=5000), now_ms=NOW)[0] == "clean"
    assert v.process(raw(ts=1000, wellbore_id="OTHER"), now_ms=NOW)[0] == "clean"


@pytest.mark.parametrize("kw,reason", [
    ({"wellbore_id": None}, "missing_key"),
    ({"ts": None}, "missing_key"),
    ({"ts": NOW + 3 * 24 * 3600 * 1000}, "future_timestamp"),
])
def test_rejects(kw, reason):
    route, rej = Validator().process(raw(**kw), now_ms=NOW)
    assert route == "dlq" and rej["reason"] == reason
    assert rej["payload_json"].startswith("{")


def test_row_with_no_usable_channel_is_rejected():
    empty = {k: None for k in ("hole_depth_m", "bit_depth_m", "hookload_n", "wob_n", "rpm_rps", "torque_nm",
                               "spp_pa", "flow_in_m3s", "rop_ms", "mud_weight_kgm3")}
    route, rej = Validator().process(raw(**empty), now_ms=NOW)
    assert route == "dlq" and rej["reason"] == "no_valid_channels"


def test_real_excerpt_passes_validation(raw_excerpt):
    v = Validator()
    routes = [v.process(r)[0] for r in raw_excerpt]
    assert routes.count("clean") == len(raw_excerpt)
