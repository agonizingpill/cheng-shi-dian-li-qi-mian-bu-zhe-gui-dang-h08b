"""兼容命名：回填调度纯函数，实现见 :mod:`core.quota` 与 :mod:`core.queueing`。"""
from core.quota import (  # noqa: F401
    admissible,
    classify_pending,
    plan_admission,
    select_admissible,
)
from core import quota as _quota

backfill = select_admissible


def schedule(pending, available=None, *, used=0, limit=None):
    """调度入口：给 (pending, available) 走纯准入；给 used/limit 走分类。"""
    if available is not None:
        return select_admissible(pending, available)
    return classify_pending(pending, used=used, limit=limit)


__getattr__ = _quota.__getattr__
