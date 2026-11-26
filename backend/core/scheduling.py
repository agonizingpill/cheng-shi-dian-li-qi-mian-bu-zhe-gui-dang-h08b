"""兼容命名：调度/回填规划，实现见 :mod:`core.quota` 与 :mod:`core.queueing`。"""
from core.quota import (  # noqa: F401
    FALLBACK_LOCK,
    MemoryQuota,
    admissible,
    classify_pending,
    memory_cost,
    memory_limit,
    plan_admission,
    select_admissible,
)
from core import quota as _quota

backfill = select_admissible
Scheduler = _quota.Scheduler if hasattr(_quota, 'Scheduler') else MemoryQuota


def schedule(pending, available=None, *, used=0, limit=None):
    """给 ``available`` 走纯准入；给 ``used/limit`` 走三分类。"""
    if available is not None:
        return select_admissible(pending, available)
    return classify_pending(pending, used=used, limit=limit)


__getattr__ = _quota.__getattr__
