"""投料秤漂移监视与标定状态门控。

标定基线落盘之后，秤的物理零点仍会随时间慢慢爬移；等玻璃发白才发现时，
配比已经按老偏移配了很久。本模块在基线之上加一层监视：订阅每一批称量，
按最近几批的观测偏移评估基线是不是在漂——漂过提醒阈值就发告警并给出
建议标定值；标定超期、基线与落盘内容对不上、或配比请求拿的不是当前代次
时，直接门控配比计算，宁可停料也不按一份对不上现场的基线配料。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from ..clock import Clock
from ..config import BatchLimits
from ..errors import InterlockError
from ..eventbus import Event, EventBus
from .scale import BatchScale, ScaleBaseline

#: 评估漂移所需的最少批数（窗口配置得更小时按窗口取）。
_MIN_SAMPLES = 4

#: 快照里保留的最近称量样本数。
_RECENT_SAMPLES = 5


@dataclass(frozen=True)
class WeighingSample:
    """一批称量留下的观测偏移：秤读数相对这批物料约定真值的差。"""

    observed_offset_kg: float
    baseline_epoch: int
    at: float

    def as_dict(self) -> dict:
        return {
            "observed_offset_kg": round(self.observed_offset_kg, 4),
            "baseline_epoch": self.baseline_epoch,
            "at": round(self.at, 3),
        }


@dataclass(frozen=True)
class DriftReport:
    """一次漂移评估的结论。

    ``suggested_offset_kg`` 取窗口内观测偏移的均值：秤的物理零点当前在哪里，
    建议的新标定值就是多少；``slope_kg_per_batch`` 给出爬移速率，便于分辨
    "稳定偏差"与"还在爬"。
    """

    samples: int
    mean_offset_kg: float
    baseline_offset_kg: float
    drift_kg: float
    slope_kg_per_batch: float
    drifting: bool
    suggested_offset_kg: float
    baseline_epoch: int

    def as_dict(self) -> dict:
        return {
            "samples": self.samples,
            "mean_offset_kg": round(self.mean_offset_kg, 4),
            "baseline_offset_kg": round(self.baseline_offset_kg, 4),
            "drift_kg": round(self.drift_kg, 4),
            "slope_kg_per_batch": round(self.slope_kg_per_batch, 4),
            "drifting": self.drifting,
            "suggested_offset_kg": round(self.suggested_offset_kg, 4),
            "baseline_epoch": self.baseline_epoch,
        }


def _slope(values: list[float]) -> float:
    """最小二乘拟合的每批爬移速率（kg/批）。"""

    count = len(values)
    if count < 2:
        return 0.0
    mean_x = (count - 1) / 2.0
    mean_y = sum(values) / count
    denom = sum((index - mean_x) ** 2 for index in range(count))
    if denom == 0.0:
        return 0.0
    numer = sum(
        (index - mean_x) * (value - mean_y) for index, value in enumerate(values)
    )
    return numer / denom


class ScaleDriftMonitor:
    """按最近几批称量数据监视标定漂移，并在配比计算前做标定门控。"""

    def __init__(
        self,
        bus: EventBus,
        clock: Clock,
        limits: BatchLimits,
        scale: BatchScale,
    ) -> None:
        self._bus = bus
        self._clock = clock
        self._limits = limits
        self._scale = scale
        self._samples: list[WeighingSample] = []
        self._warned = False
        self._min_samples = min(_MIN_SAMPLES, limits.drift_window_batches)
        self._lock = threading.RLock()
        bus.subscribe("batch", self._on_event)

    def _on_event(self, event: Event) -> None:
        if event.action == "batch_weighed":
            self._record(event.payload)
        elif event.action in ("scale_recalibrated", "scale_baseline_restored"):
            self._reset()

    def _record(self, payload: dict) -> None:
        reference = float(payload.get("reference_kg", self._limits.charge_batch_kg))
        sample = WeighingSample(
            observed_offset_kg=float(payload.get("raw_kg", 0.0)) - reference,
            baseline_epoch=int(payload.get("epoch", 0)),
            at=self._clock.now(),
        )
        with self._lock:
            self._samples.append(sample)
            overflow = len(self._samples) - self._limits.drift_window_batches
            if overflow > 0:
                del self._samples[:overflow]
            count = len(self._samples)
        if count >= self._min_samples:
            self._evaluate()

    def _reset(self) -> None:
        """重新标定或恢复基线后，旧窗口不再代表新基线，重新积累。"""

        with self._lock:
            self._samples.clear()
            self._warned = False

    def _evaluate(self) -> None:
        report = self.assess()
        if not report.drifting:
            return
        with self._lock:
            if self._warned:
                return
            self._warned = True
        self._bus.publish(
            Event.of(
                "batch",
                "batch",
                "scale_drift_detected",
                subject="batch_scale",
                severity="warning",
                drift_kg=round(report.drift_kg, 4),
                mean_offset_kg=round(report.mean_offset_kg, 4),
                suggested_offset_kg=round(report.suggested_offset_kg, 4),
                slope_kg_per_batch=round(report.slope_kg_per_batch, 4),
                samples=report.samples,
                baseline_epoch=report.baseline_epoch,
            )
        )

    def assess(self) -> DriftReport:
        """按窗口内的观测偏移评估漂移，并给出建议标定值。"""

        with self._lock:
            samples = list(self._samples)
        baseline = self._scale.baseline()
        count = len(samples)
        if count:
            offsets = [item.observed_offset_kg for item in samples]
            mean_offset = sum(offsets) / count
            slope = _slope(offsets)
        else:
            mean_offset = baseline.offset_kg
            slope = 0.0
        drift = mean_offset - baseline.offset_kg
        drifting = count >= self._min_samples and not self._limits.drift_ok(drift)
        return DriftReport(
            samples=count,
            mean_offset_kg=mean_offset,
            baseline_offset_kg=baseline.offset_kg,
            drift_kg=drift,
            slope_kg_per_batch=slope,
            drifting=drifting,
            suggested_offset_kg=mean_offset,
            baseline_epoch=baseline.epoch,
        )

    def is_overdue(self, baseline: ScaleBaseline) -> bool:
        """这版基线的标定是否已超出有效期。"""

        age_s = self._clock.now() - float(baseline.calibrated_at)
        return age_s > self._limits.calibration_max_age_s

    def require_fresh(self, baseline: ScaleBaseline) -> None:
        """配比计算前的门控：基线不可信时拒绝折算。

        三种不可信：拿的不是当前生效的代次、内存基线与落盘内容对不上、
        标定已超期。任何一种都说明这份基线代表现场的能力存疑。
        """

        current = self._scale.baseline()
        if int(baseline.epoch) != current.epoch or float(baseline.offset_kg) != float(
            current.offset_kg
        ):
            self._block(
                f"配比请求拿的是第 {int(baseline.epoch)} 代基线，"
                f"当前生效的是第 {current.epoch} 代，禁止按老基线折算配比"
            )
        if not self._scale.is_durable():
            self._block("投料秤基线与落盘内容对不上，禁止折算配比")
        if self.is_overdue(baseline):
            age_s = self._clock.now() - float(baseline.calibrated_at)
            self._block(
                f"标定已超期（{age_s:.0f} s 未复核，上限 "
                f"{self._limits.calibration_max_age_s:.0f} s），禁止折算配比，请先重新标定"
            )

    def _block(self, reason: str) -> None:
        self._bus.publish(
            Event.of(
                "batch",
                "batch",
                "mix_plan_blocked",
                subject="mix_ratio",
                severity="warning",
                reason=reason,
            )
        )
        raise InterlockError(reason, component="batch")

    def snapshot(self) -> dict:
        baseline = self._scale.baseline()
        report = self.assess()
        with self._lock:
            warned = self._warned
            recent = [item.as_dict() for item in self._samples[-_RECENT_SAMPLES:]]
        return {
            "baseline_epoch": baseline.epoch,
            "durable": self._scale.is_durable(),
            "overdue": self.is_overdue(baseline),
            "calibration_age_s": round(self._clock.now() - baseline.calibrated_at, 1),
            "calibration_max_age_s": self._limits.calibration_max_age_s,
            "drift_warn_kg": self._limits.drift_warn_kg,
            "window_batches": self._limits.drift_window_batches,
            "min_samples": self._min_samples,
            "warned": warned,
            "assessment": report.as_dict(),
            "recent": recent,
        }
