"""投料秤与标定基线。

投料秤的标定偏移决定生料配比。标定结果必须落盘："基线生效"意味着磁盘上
存着这一版偏移，重启后按它恢复；配比计算只认最新一版基线，标定后还按老
偏移配料就是这份数据里要防的错。

标定不是一劳永逸的：秤会随时间慢慢漂移，等玻璃析晶发白才发现就晚了。
所以基线之上还有一层漂移监测——每批称量都对照名义质量留一个残差，最近
若干批残差的均值就是偏移正在漂的估计量：漂过告警线就提醒重新标定并给出
建议偏移；标定超期、基线与现场不一致或漂移过阻断线时，配比计算直接挡住。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from ..config import BatchLimits
from ..errors import InterlockError
from ..eventbus import Event, EventBus
from ..store.jsonfile import JsonStore

BASELINE_KEY = "batch_scale_baseline"

#: 漂移等级的高低排序，用于判断"等级跃迁"（只在升级时发提醒，避免每批都刷告警）。
_DRIFT_RANK = {"ok": 0, "warn": 1, "block": 2}


@dataclass(frozen=True)
class ScaleBaseline:
    """一版投料秤标定基线。"""

    offset_kg: float
    epoch: int
    calibrated_at: float

    def as_dict(self) -> dict:
        return {
            "offset_kg": round(self.offset_kg, 4),
            "epoch": self.epoch,
            "calibrated_at": round(self.calibrated_at, 3),
        }


@dataclass(frozen=True)
class DriftAssessment:
    """一次漂移评估：最近若干批称量残差给出的结论与重新标定建议值。"""

    samples: int
    drift_kg: float
    suggested_offset_kg: float
    level: str

    def as_dict(self) -> dict:
        return {
            "samples": self.samples,
            "drift_kg": round(self.drift_kg, 4),
            "suggested_offset_kg": round(self.suggested_offset_kg, 4),
            "level": self.level,
        }


class BatchScale:
    """投料秤：标定偏移、落盘、称量折算与漂移监测。"""

    def __init__(
        self,
        bus: EventBus,
        store: JsonStore,
        limits: BatchLimits,
        offset_kg: float = 0.0,
    ) -> None:
        self._bus = bus
        self._store = store
        self._limits = limits
        self._offset_kg = float(offset_kg)
        self._epoch = 0
        self._calibrated_at = 0.0
        self._weighings = 0
        self._residuals: list[float] = []
        self._drift_level = "ok"
        self._lock = threading.RLock()

    @property
    def offset_kg(self) -> float:
        with self._lock:
            return self._offset_kg

    @property
    def epoch(self) -> int:
        with self._lock:
            return self._epoch

    def baseline(self) -> ScaleBaseline:
        """返回当前生效的基线。"""

        with self._lock:
            return ScaleBaseline(
                offset_kg=self._offset_kg,
                epoch=self._epoch,
                calibrated_at=self._calibrated_at,
            )

    def recalibrate(self, offset_kg: float, calibrated_at: float) -> ScaleBaseline:
        """按新的标定偏移重算基线并落盘，返回新基线。

        标定后漂移观察窗口从零重新积累——残差是相对上一版基线量的，
        基线一变，旧残差就不再代表现场。
        """

        offset = float(offset_kg)
        if not self._limits.offset_ok(offset):
            raise InterlockError(
                f"标定偏移 {offset:.3f} kg 超出允许范围 ±{self._limits.scale_offset_max_kg:.3f} kg",
                component="batch",
            )
        with self._lock:
            # 落盘内容就是生效基线，内存按同一精度保存，避免重启前后两个值。
            self._offset_kg = round(offset, 4)
            self._epoch += 1
            self._calibrated_at = float(calibrated_at)
            self._residuals.clear()
            self._drift_level = "ok"
            baseline = self.baseline()
        digest = self._store.write(BASELINE_KEY, baseline.as_dict())
        self._bus.publish(
            Event.of(
                "batch",
                "batch",
                "scale_recalibrated",
                subject="batch_scale",
                severity="notice",
                offset_kg=round(offset, 4),
                epoch=baseline.epoch,
                digest=digest[:12],
            )
        )
        return baseline

    def weigh(self, raw_kg: float, reference_kg: float | None = None) -> float:
        """按当前偏移把秤的读数折算成真实质量。

        给出 ``reference_kg``（本批的名义质量）时，把秤读数相对基线预期的
        残差记入漂移观察窗口；窗口均值就是偏移正在漂的估计量，漂过告警线
        会发提醒并给出重新标定的建议偏移。
        """

        with self._lock:
            self._weighings += 1
            raw = float(raw_kg)
            true_kg = raw - self._offset_kg
            if reference_kg is not None:
                residual = raw - self._offset_kg - float(reference_kg)
                self._residuals.append(residual)
                if len(self._residuals) > self._limits.drift_window:
                    del self._residuals[: len(self._residuals) - self._limits.drift_window]
            assessment = self._assess_locked()
            escalated = _DRIFT_RANK[assessment.level] > _DRIFT_RANK[self._drift_level]
            self._drift_level = assessment.level
        self._bus.publish(
            Event.of(
                "batch",
                "batch",
                "batch_weighed",
                subject="batch_scale",
                raw_kg=round(raw, 3),
                true_kg=round(true_kg, 3),
                epoch=self.epoch,
            )
        )
        if escalated:
            self._bus.publish(
                Event.of(
                    "batch",
                    "batch",
                    "scale_drift_warning"
                    if assessment.level == "warn"
                    else "scale_drift_blocked",
                    subject="batch_scale",
                    severity="warning",
                    samples=assessment.samples,
                    drift_kg=round(assessment.drift_kg, 4),
                    suggested_offset_kg=round(assessment.suggested_offset_kg, 4),
                )
            )
        return true_kg

    def _assess_locked(self) -> DriftAssessment:
        """在锁内评估漂移窗口（调用方必须已持有锁）。"""

        samples = len(self._residuals)
        drift = sum(self._residuals) / samples if samples else 0.0
        return DriftAssessment(
            samples=samples,
            drift_kg=drift,
            suggested_offset_kg=self._offset_kg + drift,
            level=self._limits.drift_level(drift),
        )

    def assess_drift(self) -> DriftAssessment:
        """评估最近若干批称量残差，给出漂移结论与重新标定建议值。"""

        with self._lock:
            return self._assess_locked()

    def calibration_age_s(self, now: float) -> float | None:
        """距上次标定过去了多少秒；从未标定时返回 ``None``。"""

        with self._lock:
            if self._epoch == 0:
                return None
            return max(0.0, float(now) - self._calibrated_at)

    def calibration_overdue(self, now: float) -> bool:
        """标定是否已超期（从未标定按超期处理）。"""

        age = self.calibration_age_s(now)
        return age is None or age > self._limits.calibration_max_age_s

    def readiness(self, now: float) -> dict:
        """配比计算前的就绪评估：落盘一致、标定未超期、漂移未阻断。"""

        assessment = self.assess_drift()
        age = self.calibration_age_s(now)
        durable = self.is_durable()
        overdue = self.calibration_overdue(now)
        ok = durable and not overdue and assessment.level != "block"
        return {
            "ok": ok,
            "durable": durable,
            "overdue": overdue,
            "calibration_age_s": None if age is None else round(age, 3),
            "max_age_s": self._limits.calibration_max_age_s,
            "drift": assessment.as_dict(),
        }

    def require_ready(self, now: float) -> None:
        """配比计算前必须确认基线可用：落盘一致、标定未超期、漂移未阻断。"""

        state = self.readiness(now)
        if not state["durable"]:
            raise InterlockError(
                "投料秤基线尚未落盘或与现场不一致，禁止按配比投料", component="batch"
            )
        if state["overdue"]:
            age = state["calibration_age_s"]
            age_text = "从未标定" if age is None else f"已 {age / 3600.0:.1f} h 未复核"
            raise InterlockError(
                f"标定超期（{age_text}，上限 {state['max_age_s'] / 3600.0:.1f} h），"
                "禁止按配比投料，请先重新标定",
                component="batch",
            )
        drift = state["drift"]
        if drift["level"] == "block":
            raise InterlockError(
                f"称量漂移 {drift['drift_kg']:.3f} kg 达到阻断限 "
                f"{self._limits.drift_block_kg:.3f} kg，禁止按配比投料；"
                f"建议按 {drift['suggested_offset_kg']:.3f} kg 重新标定",
                component="batch",
            )

    def is_durable(self) -> bool:
        """基线是否已落盘且内容与内存一致。"""

        if not self._store.exists(BASELINE_KEY):
            return False
        snapshot = self._store.read(BASELINE_KEY)
        if not snapshot:
            return False
        return snapshot.get("epoch") == self.epoch and float(
            snapshot.get("offset_kg", 0.0)
        ) == self.offset_kg

    def require_durable(self) -> None:
        """配比计算前必须确认基线已落盘。"""

        if not self.is_durable():
            raise InterlockError("投料秤基线尚未落盘，禁止按配比投料", component="batch")

    def restore(self) -> bool:
        """从磁盘恢复标定基线。"""

        snapshot = self._store.read(BASELINE_KEY)
        if not snapshot:
            return False
        with self._lock:
            self._offset_kg = float(snapshot.get("offset_kg", 0.0))
            self._epoch = int(snapshot.get("epoch", 0))
            self._calibrated_at = float(snapshot.get("calibrated_at", 0.0))
            self._residuals.clear()
            self._drift_level = "ok"
            baseline = self.baseline()
        self._bus.publish(
            Event.of(
                "batch",
                "batch",
                "scale_baseline_restored",
                subject="batch_scale",
                offset_kg=round(baseline.offset_kg, 4),
                epoch=baseline.epoch,
            )
        )
        return True

    def snapshot(self, now: float | None = None) -> dict:
        with self._lock:
            payload = {
                "offset_kg": round(self._offset_kg, 4),
                "epoch": self._epoch,
                "calibrated_at": round(self._calibrated_at, 3),
                "durable": self.is_durable(),
                "weighings": self._weighings,
                "max_offset_kg": self._limits.scale_offset_max_kg,
                "drift": self._assess_locked().as_dict(),
            }
        if now is not None:
            age = self.calibration_age_s(now)
            payload["calibration_age_s"] = None if age is None else round(age, 3)
            payload["max_age_s"] = self._limits.calibration_max_age_s
            payload["overdue"] = self.calibration_overdue(now)
        return payload
