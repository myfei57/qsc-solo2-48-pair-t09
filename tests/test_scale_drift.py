"""投料秤漂移监视与标定门控：漂移要提醒并给建议值，超期与对不上要挡住配比。"""

from __future__ import annotations

import pytest

from glassmelt.app import CycleInputs, build_platform
from glassmelt.batch.scale import BASELINE_KEY
from glassmelt.config import default_config
from glassmelt.errors import InterlockError


def _weigh_with_drift(platform, drift_kg: float, batches: int) -> None:
    """模拟物理零点在基线之外又漂了 drift_kg 的秤：读数 = 单批量 + 基线 + 漂移。"""

    raw = platform.config.batch.charge_batch_kg + platform.scale.offset_kg + drift_kg
    for _ in range(batches):
        platform.scale.weigh(raw)


def _drift_events(platform) -> list:
    return [e for e in platform.bus.history("batch") if e.action == "scale_drift_detected"]


def test_drift_beyond_threshold_warns_and_suggests(platform):
    platform.recalibrate_scale(1.0)
    _weigh_with_drift(platform, 3.0, 6)
    report = platform.scale_drift_report()
    assessment = report["assessment"]
    assert assessment["drifting"] is True
    assert assessment["drift_kg"] == pytest.approx(3.0)
    assert assessment["suggested_offset_kg"] == pytest.approx(4.0)
    assert report["warned"] is True
    events = _drift_events(platform)
    assert len(events) == 1
    assert events[0].payload["severity"] == "warning"
    assert events[0].payload["suggested_offset_kg"] == pytest.approx(4.0)
    alarms = platform.audit.alarms()
    assert any(item["action"] == "scale_drift_detected" for item in alarms)


def test_recalibration_with_suggested_value_settles_the_watch(platform):
    platform.recalibrate_scale(1.0)
    _weigh_with_drift(platform, 3.0, 6)
    suggested = platform.scale_drift_report()["assessment"]["suggested_offset_kg"]
    payload = platform.recalibrate_scale(suggested)
    assert payload["fresh"] is True
    settled = platform.scale_drift_report()
    assert settled["assessment"]["drifting"] is False
    assert settled["assessment"]["samples"] == 0
    assert settled["warned"] is False
    _weigh_with_drift(platform, 0.2, 6)
    assert platform.scale_drift_report()["assessment"]["drifting"] is False


def test_drift_within_threshold_stays_quiet(platform):
    platform.recalibrate_scale(1.0)
    _weigh_with_drift(platform, 0.5, 8)
    report = platform.scale_drift_report()
    assert report["assessment"]["drifting"] is False
    assert report["warned"] is False
    assert _drift_events(platform) == []


def test_drift_needs_enough_batches(platform):
    platform.recalibrate_scale(0.0)
    _weigh_with_drift(platform, 5.0, 3)
    report = platform.scale_drift_report()
    assert report["assessment"]["samples"] == 3
    assert report["assessment"]["drifting"] is False
    assert _drift_events(platform) == []


def test_drift_trend_reports_a_slope(platform):
    platform.recalibrate_scale(0.0)
    base = platform.config.batch.charge_batch_kg
    for index in range(5):
        platform.scale.weigh(base + 1.0 + 0.5 * index)
    report = platform.scale_drift_report()
    assert report["assessment"]["slope_kg_per_batch"] == pytest.approx(0.5)
    assert report["recent"][-1]["observed_offset_kg"] == pytest.approx(3.0)


def test_overdue_calibration_blocks_mix_plan(platform):
    platform.recalibrate_scale(2.0)
    platform.clock.advance(platform.config.batch.calibration_max_age_s + 1.0)
    assert platform.scale_drift_report()["overdue"] is True
    with pytest.raises(InterlockError, match="超期"):
        platform.mix.plan(platform.scale.baseline())
    platform.recalibrate_scale(2.0)
    plan = platform.mix.plan(platform.scale.baseline())
    assert plan.baseline_epoch == platform.scale.epoch


def test_overdue_calibration_blocks_the_control_cycle(running):
    running.clock.advance(running.config.batch.calibration_max_age_s + 1.0)
    with pytest.raises(InterlockError, match="超期"):
        running.cycle(CycleInputs(load=0.8, pull_rate=0.9, gob_mass_g=186.0, dt=1.0))
    blocked = [e for e in running.bus.history("batch") if e.action == "mix_plan_blocked"]
    assert blocked


def test_overdue_baseline_blocks_startup(clock, tmp_path):
    root = str(tmp_path / "state")
    first = build_platform(default_config(), clock=clock, storage_root=root)
    first.scale.recalibrate(3.0, clock.now())
    clock.advance(default_config().batch.calibration_max_age_s + 1.0)
    second = build_platform(default_config(), clock=clock, storage_root=root)
    with pytest.raises(InterlockError, match="超期"):
        second.start()


def test_baseline_mismatch_with_disk_blocks_mix_plan(platform):
    platform.recalibrate_scale(2.0)
    platform.store.write(BASELINE_KEY, {"offset_kg": 9.9, "epoch": 99, "calibrated_at": 0.0})
    assert platform.scale.is_durable() is False
    with pytest.raises(InterlockError, match="对不上"):
        platform.mix.plan(platform.scale.baseline())


def test_missing_baseline_file_blocks_mix_plan(platform):
    platform.recalibrate_scale(2.0)
    platform.store.remove(BASELINE_KEY)
    with pytest.raises(InterlockError, match="对不上"):
        platform.mix.plan(platform.scale.baseline())


def test_stale_baseline_is_blocked(platform):
    platform.recalibrate_scale(2.0)
    stale = platform.scale.baseline()
    platform.recalibrate_scale(3.0)
    with pytest.raises(InterlockError, match="老基线"):
        platform.mix.plan(stale)


def test_status_and_readiness_expose_scale_watch(running):
    status = running.status()
    assert status["scale_watch"]["assessment"]["drifting"] is False
    readiness = running.readiness_report()
    assert readiness["scale_watch"]["overdue"] is False
    assert readiness["scale_watch"]["durable"] is True
