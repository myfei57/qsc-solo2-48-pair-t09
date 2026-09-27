"""投料秤与标定基线。

投料秤的标定偏移决定生料配比。标定结果必须落盘："基线生效"意味着磁盘上
存着这一版偏移，重启后按它恢复；配比计算只认最新一版基线，标定后还按老
偏移配料就是这份数据里要防的错。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from ..config import BatchLimits
from ..errors import InterlockError
from ..eventbus import Event, EventBus
from ..store.jsonfile import JsonStore

BASELINE_KEY = "batch_scale_baseline"


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


class BatchScale:
    """投料秤：标定偏移、落盘与称量折算。"""

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
        """按新的标定偏移重算基线并落盘，返回新基线。"""

        offset = float(offset_kg)
        if not self._limits.offset_ok(offset):
            raise InterlockError(
                f"标定偏移 {offset:.3f} kg 超出允许范围 ±{self._limits.scale_offset_max_kg:.3f} kg",
                component="batch",
            )
        with self._lock:
            self._offset_kg = offset
            self._epoch += 1
            self._calibrated_at = float(calibrated_at)
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

        ``reference_kg`` 是这批物料的约定真值（缺省取单批投料量）；漂移监视
        按"读数 − 约定真值"积累观测偏移，判断秤的物理零点是否还在基线上。
        """

        reference = (
            self._limits.charge_batch_kg if reference_kg is None else float(reference_kg)
        )
        with self._lock:
            self._weighings += 1
            true_kg = float(raw_kg) - self._offset_kg
        self._bus.publish(
            Event.of(
                "batch",
                "batch",
                "batch_weighed",
                subject="batch_scale",
                raw_kg=round(float(raw_kg), 3),
                true_kg=round(true_kg, 3),
                reference_kg=round(reference, 3),
                epoch=self.epoch,
            )
        )
        return true_kg

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

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "offset_kg": round(self._offset_kg, 4),
                "epoch": self._epoch,
                "calibrated_at": round(self._calibrated_at, 3),
                "durable": self.is_durable(),
                "weighings": self._weighings,
                "max_offset_kg": self._limits.scale_offset_max_kg,
            }
