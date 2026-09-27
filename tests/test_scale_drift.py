"""称量漂移监测：最近几批残差定漂移，超期或基线不一致挡住配比。"""

from __future__ import annotations

import copy

import pytest

from glassmelt.app import CycleInputs
from glassmelt.batch.scale import BASELINE_KEY
from glassmelt.config import PlatformConfig, config_to_mapping, default_config
from glassmelt.errors import ConfigError, InterlockError


def _feed_drift(platform, drift_kg: float, batches: int = 1) -> None:
    """模拟现场秤漂了 ``drift_kg``：秤读数 = 名义质量 + 标定偏移 + 漂移量。"""

    reference = platform.config.batch.charge_batch_kg
    for _ in range(batches):
        raw_kg = reference + platform.scale.offset_kg + drift_kg
        platform.scale.weigh(raw_kg, reference_kg=reference)


def _cycle_inputs() -> CycleInputs:
    return CycleInputs(load=0.8, pull_rate=1.0, gob_mass_g=186.0, dt=1.0)


def test_clean_weighings_keep_scale_ready(running):
    _feed_drift(running, 0.05, batches=4)
    assessment = running.scale.assess_drift()
    assert assessment.level == "ok"
    assert assessment.samples == 4
    assert assessment.drift_kg == pytest.approx(0.05)
    running.scale.require_ready(running.clock.now())


def test_drift_warns_and_suggests_new_offset(running):
    running.scale.recalibrate(2.0, running.clock.now())
    _feed_drift(running, 2.0, batches=3)
    assessment = running.scale.assess_drift()
    assert assessment.level == "warn"
    assert assessment.drift_kg == pytest.approx(2.0)
    assert assessment.suggested_offset_kg == pytest.approx(4.0)
    # 告警只提醒、不挡配比。
    running.scale.require_ready(running.clock.now())
    warnings = [
        event
        for event in running.bus.history("batch")
        if event.action == "scale_drift_warning"
    ]
    assert warnings
    assert warnings[-1].payload["suggested_offset_kg"] == pytest.approx(4.0)
    assert running.audit.alarms()


def test_drift_warning_fires_once_per_escalation(running):
    _feed_drift(running, 2.0, batches=4)
    warnings = [
        event
        for event in running.bus.history("batch")
        if event.action == "scale_drift_warning"
    ]
    assert len(warnings) == 1


def test_drift_beyond_block_limit_locks_mix(running):
    _feed_drift(running, 5.0, batches=2)
    assessment = running.scale.assess_drift()
    assert assessment.level == "block"
    assert assessment.suggested_offset_kg == pytest.approx(5.0)
    with pytest.raises(InterlockError):
        running.scale.require_ready(running.clock.now())
    with pytest.raises(InterlockError):
        running.cycle(_cycle_inputs())
    blocked = [
        event
        for event in running.bus.history("batch")
        if event.action == "scale_drift_blocked"
    ]
    assert blocked


def test_drift_window_slides_with_recent_batches(running):
    window = running.config.batch.drift_window
    _feed_drift(running, 0.0, batches=window)
    assert running.scale.assess_drift().level == "ok"
    _feed_drift(running, 2.0, batches=window // 2)
    # 窗口里一半 0 一半 2，均值 1.0 还没到告警线。
    assert running.scale.assess_drift().drift_kg == pytest.approx(1.0)
    assert running.scale.assess_drift().level == "ok"
    _feed_drift(running, 2.0, batches=window // 2)
    # 旧批次滑出窗口，均值爬过告警线。
    assert running.scale.assess_drift().level == "warn"


def test_recalibration_clears_drift_and_unlocks_mix(running):
    _feed_drift(running, 5.0, batches=2)
    assert running.scale.assess_drift().level == "block"
    suggested = running.scale.assess_drift().suggested_offset_kg
    payload = running.recalibrate_scale(suggested)
    assert payload["baseline"]["offset_kg"] == pytest.approx(5.0)
    assessment = running.scale.assess_drift()
    assert assessment.level == "ok"
    assert assessment.samples == 0
    running.scale.require_ready(running.clock.now())
    result = running.cycle(_cycle_inputs())
    assert result["mix"]["baseline_epoch"] == payload["baseline"]["epoch"]


def test_overdue_calibration_locks_mix_until_recalibrated(running):
    max_age = running.config.batch.calibration_max_age_s
    running.clock.advance(max_age + 1.0)
    assert running.scale.calibration_overdue(running.clock.now()) is True
    with pytest.raises(InterlockError):
        running.scale.require_ready(running.clock.now())
    with pytest.raises(InterlockError):
        running.cycle(_cycle_inputs())
    running.recalibrate_scale(1.0)
    assert running.scale.calibration_overdue(running.clock.now()) is False
    running.cycle(_cycle_inputs())


def test_missing_baseline_locks_mix(platform):
    assert platform.scale.is_durable() is False
    with pytest.raises(InterlockError):
        platform.scale.require_ready(platform.clock.now())


def test_field_mismatch_locks_mix(platform):
    platform.scale.recalibrate(3.0, platform.clock.now())
    assert platform.scale.is_durable() is True
    # 现场磁盘上的基线被改成了别的值，与内存里的生效基线对不上。
    platform.store.write(
        BASELINE_KEY, {"offset_kg": 9.0, "epoch": 1, "calibrated_at": 1.0}
    )
    assert platform.scale.is_durable() is False
    with pytest.raises(InterlockError):
        platform.scale.require_ready(platform.clock.now())


def test_readiness_report_exposes_scale_gate(running):
    report = running.readiness_report()
    assert report["scale_readiness"]["ok"] is True
    assert report["scale_readiness"]["drift"]["level"] == "ok"
    _feed_drift(running, 5.0, batches=2)
    report = running.readiness_report()
    assert report["scale_readiness"]["ok"] is False
    assert report["scale_readiness"]["drift"]["level"] == "block"
    assert report["scale_readiness"]["drift"]["suggested_offset_kg"] == pytest.approx(5.0)


def test_status_snapshot_carries_drift_and_calibration_age(running):
    snapshot = running.status()["scale"]
    assert snapshot["drift"]["level"] == "ok"
    assert snapshot["overdue"] is False
    assert snapshot["calibration_age_s"] is not None
    assert snapshot["max_age_s"] == pytest.approx(
        running.config.batch.calibration_max_age_s
    )


def test_config_rejects_inverted_drift_thresholds():
    mapping = config_to_mapping(default_config())
    inverted = copy.deepcopy(mapping)
    inverted["limits"]["batch"]["drift_warn_kg"] = 9.0
    with pytest.raises(ConfigError):
        PlatformConfig.from_mapping(inverted)
    missing = copy.deepcopy(mapping)
    del missing["limits"]["batch"]["calibration_max_age_s"]
    with pytest.raises(ConfigError):
        PlatformConfig.from_mapping(missing)
