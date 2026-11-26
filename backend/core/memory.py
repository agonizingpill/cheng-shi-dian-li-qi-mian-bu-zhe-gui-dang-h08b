"""兼容命名：内存额度逻辑的入口之一，实现见 :mod:`core.quota`。"""
from core.quota import *  # noqa: F401,F403
from core import quota as _quota

memory_cost = _quota.memory_cost
estimate_memory = _quota.estimate_memory
memory_limit = _quota.memory_limit
MemoryQuota = _quota.MemoryQuota
MemoryLimitExceeded = _quota.MemoryLimitExceeded
select_admissible = _quota.select_admissible
classify_pending = _quota.classify_pending

__getattr__ = _quota.__getattr__
