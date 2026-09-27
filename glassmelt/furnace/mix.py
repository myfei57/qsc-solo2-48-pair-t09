"""配比计算。

生料配比按投料秤的当前标定基线折算：秤的标定偏移一变，配比必须跟着变，
否则料方长期按老偏移配料，窑内玻璃成分漂移、析晶发白。所以配比计划里要
带上它用的基线代次，装配与控制台都按代次核对"这一批是不是按最新基线算的"。
基线本身不可信时（标定超期、与落盘内容对不上、或拿的不是当前代次），
折算直接被门控拒绝——宁可停料，也不按一份对不上现场的基线配料。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from ..config import BatchLimits
from ..eventbus import Event, EventBus


@dataclass(frozen=True)
class MixPlan:
    """一份配比计划。"""

    batch_kg: float
    offset_kg: float
    baseline_epoch: int
    mix_total_kg: float
    deviation_pct: float

    def as_dict(self) -> dict:
        return {
            "batch_kg": round(self.batch_kg, 3),
            "offset_kg": round(self.offset_kg, 3),
            "baseline_epoch": self.baseline_epoch,
            "mix_total_kg": round(self.mix_total_kg, 3),
            "deviation_pct": round(self.deviation_pct, 4),
        }


class MixPlanner:
    """按投料秤基线折算配比；``guard`` 在每次折算前校验基线可信度。"""

    def __init__(self, bus: EventBus, limits: BatchLimits, guard) -> None:
        self._bus = bus
        self._limits = limits
        self._guard = guard
        self._plans = 0
        self._lock = threading.RLock()

    def plan(self, baseline) -> MixPlan:
        """按给定基线折算一批配比；基线不可信时门控拒绝。"""

        self._guard.require_fresh(baseline)
        batch_kg = self._limits.charge_batch_kg
        offset_kg = float(baseline.offset_kg)
        epoch = int(baseline.epoch)
        mix_total_kg = batch_kg + offset_kg
        deviation_pct = (offset_kg / batch_kg) * 100.0
        result = MixPlan(
            batch_kg=batch_kg,
            offset_kg=offset_kg,
            baseline_epoch=epoch,
            mix_total_kg=mix_total_kg,
            deviation_pct=deviation_pct,
        )
        with self._lock:
            self._plans += 1
        self._bus.publish(
            Event.of(
                "furnace",
                "furnace",
                "mix_planned",
                subject="mix_ratio",
                severity="notice" if not self._limits.mix_ok(deviation_pct) else "info",
                baseline_epoch=epoch,
                offset_kg=round(offset_kg, 3),
                deviation_pct=round(deviation_pct, 4),
            )
        )
        return result

    def matches(self, plan: MixPlan, baseline) -> bool:
        """核对配比计划是否用的就是当前基线。"""

        return plan.baseline_epoch == int(baseline.epoch) and plan.offset_kg == float(
            baseline.offset_kg
        )

    def tolerance_ok(self, plan: MixPlan) -> bool:
        """配比偏差是否还在公差内。"""

        return self._limits.mix_ok(plan.deviation_pct)

    def snapshot(self, plan: MixPlan | None = None) -> dict:
        with self._lock:
            payload = {
                "plans": self._plans,
                "tolerance_pct": self._limits.mix_tolerance_pct,
            }
        if plan is not None:
            payload["latest"] = plan.as_dict()
            payload["in_tolerance"] = self.tolerance_ok(plan)
        return payload
