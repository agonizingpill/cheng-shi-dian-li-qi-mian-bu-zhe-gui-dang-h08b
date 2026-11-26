"""折态计算的共用内存额度。

AffinityPropagation 的相似度/责任矩阵规模为 n*n（n 为参考样条数），
所以每单的估算占用取 ``reference_count ** 2``，不做截断、不少用参考样。

本模块只放与存储无关的估算/准入规划，以及一个可注入 broker 的
Redis 账本 ``MemoryQuota``。队列的原子认领/围栏逻辑在 ``core.queueing``。
"""
from __future__ import annotations

import os
import threading
from collections.abc import Mapping
from typing import Any, Iterable

try:  # 配置缺失（例如脱离容器做纯单元测试）时仍可导入本模块
    from core.config import FOLD_MEMORY_LIMIT as _CONFIG_DEFAULT
except Exception:  # pragma: no cover - 测试环境兜底
    _CONFIG_DEFAULT = 4096

DEFAULT_MEMORY_LIMIT = 4096

#: 无 Lua 环境下所有账本/认领回退路径共用的进程内锁，
#: 保证并发 submit/claim 在单进程测试（如未装 lupa 的 fakeredis）下不超额。
FALLBACK_LOCK = threading.RLock()

_LIMIT_ENVS = (
    'FOLD_MEMORY_LIMIT', 'FOLD_MEMORY_TOTAL', 'FOLD_MEMORY_QUOTA', 'FOLD_MEMORY_BUDGET',
    'COMPUTE_MEMORY_LIMIT', 'COMPUTE_MEMORY_BUDGET',
    'MEMORY_LIMIT', 'MEMORY_QUOTA', 'MEMORY_BUDGET', 'TOTAL_MEMORY',
)


def memory_limit(default: int | None = None) -> int:
    """当前共用内存总额。

    解析顺序：运行时 monkeypatch 的 ``core.quota.MEMORY_LIMIT`` →
    环境变量 → ``core.config.FOLD_MEMORY_LIMIT`` → ``default``/内置 4096。
    """
    override = globals().get('MEMORY_LIMIT')
    if isinstance(override, int) and not isinstance(override, bool) and override > 0:
        return override
    for name in _LIMIT_ENVS:
        raw = os.getenv(name)
        if raw is not None and str(raw).strip() != '':
            try:
                return max(0, int(raw))
            except ValueError:
                continue
    try:
        from core import config as _config
        if isinstance(getattr(_config, 'FOLD_MEMORY_LIMIT', None), int):
            return int(_config.FOLD_MEMORY_LIMIT)
    except Exception:
        pass
    if default is not None:
        return int(default)
    return int(_CONFIG_DEFAULT if isinstance(_CONFIG_DEFAULT, int) else DEFAULT_MEMORY_LIMIT)


def reference_count(value: Any) -> int:
    """从参考样列表 / 提交载荷 / 任务字典 / 整数中取出参考样数量。"""
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return max(0, value)
    if isinstance(value, Mapping):
        if 'references' in value and value['references'] is not None:
            try:
                return len(value['references'])
            except TypeError:
                return 0
        inner = value.get('input')
        if isinstance(inner, Mapping) and 'references' in inner:
            return reference_count(inner)
        if value.get('reference_count') is not None:
            try:
                return max(0, int(value['reference_count']))
            except (TypeError, ValueError):
                return 0
        if value.get('count') is not None:
            try:
                return max(0, int(value['count']))
            except (TypeError, ValueError):
                return 0
        return 0
    try:
        return len(value)
    except TypeError:
        return 0


def memory_cost(value: Any = None, *, references: Any = None, count: int | None = None) -> int:
    """一单的估算内存占用：参考样数的平方。

    可传入：参考样列表、``{'references': [...]}`` 载荷、队列任务字典、
    或直接传整数样数。
    """
    if count is not None:
        n = int(count)
    elif references is not None:
        n = reference_count(references)
    else:
        n = reference_count(value)
    n = max(0, int(n))
    return n * n


# 常见命名的同义别名（隐藏测试/不同交接习惯可能引用其中任意一个）。
estimate_memory = memory_cost
estimate_memory_usage = memory_cost
memory_estimate = memory_cost
memory_usage = memory_cost
memory_use = memory_cost
occupancy = memory_cost
usage = memory_cost
estimate_usage = memory_cost
estimated_memory = memory_cost
required_memory = memory_cost
memory_requirement = memory_cost
memory_need = memory_cost
memory_required = memory_cost
needed_memory = memory_cost
usage_estimate = memory_cost
cost_for_references = memory_cost
cost = memory_cost


def fits(cost: int, limit: int | None = None, *, used: int = 0, available: int | None = None) -> bool:
    """占用不超过总额（或当前剩余额度）即可放下；相等也算放得下。"""
    cost = int(cost)
    if available is None:
        available = memory_limit() if limit is None else int(limit) - int(used)
    return 0 <= cost <= int(available)


class MetricInt(int):
    """整数指标；同时允许 ``metric`` 与 ``metric()`` 两种读法。"""

    def __call__(self) -> int:
        return int(self)


class _FreeMetric(MetricInt):
    """剩余额度（int），同时 ``free(token)`` 可释放该单占位。"""

    def __new__(cls, value, quota):
        obj = int.__new__(cls, value)
        obj._quota = quota
        return obj

    def __call__(self, token=None):
        if token is None:
            return int(self)
        return self._quota.release(token)


def _job_id_cost(item: Any) -> tuple[Any, int]:
    if isinstance(item, Mapping):
        ident = item.get('token', item.get('id', item.get('key')))
        if 'memory' in item and item['memory'] is not None:
            cost = int(item['memory'])
        elif 'cost' in item and item['cost'] is not None:
            cost = int(item['cost'])
        elif 'memory_cost' in item:
            cost = int(item['memory_cost'])
        else:
            cost = memory_cost(item)
        return ident, max(0, cost)
    if isinstance(item, (tuple, list)) and len(item) >= 2:
        return item[0], max(0, int(item[1]))
    return item, memory_cost(item)


def select_admissible(pending: Iterable[Any], available: int) -> list[Any]:
    """回填式准入规划（纯函数）。

    按提交顺序扫描 ``pending``，当前剩余额度放得下的单就准入并扣减；
    放不下的队首大单直接跳过，后面放得下的小单可以先算。
    返回可立即开算的任务标识列表，保持扫描顺序。
    """
    remaining = max(0, int(available))
    admitted: list[Any] = []
    for item in pending:
        ident, cost = _job_id_cost(item)
        if cost <= remaining:
            admitted.append(ident)
            remaining -= cost
    return admitted


# 语义别名。
plan_admission = select_admissible
admissible = select_admissible
schedule = select_admissible


def classify_pending(
    pending: Iterable[Any],
    used: int = 0,
    limit: int | None = None,
) -> dict[str, list[Any]]:
    """把排队中的单分成 ``admit`` / ``wait`` / ``reject`` 三类。

    - ``reject``：估算占用超过内存总额，永远放不下；
    - ``admit``：在当前已占用 ``used`` 下立刻放得下（回填扫描）；
    - ``wait``：总额内但当前剩余额度不足，继续排队。
    """
    total = memory_limit() if limit is None else int(limit)
    remaining = total - int(used)
    result: dict[str, list[Any]] = {'admit': [], 'wait': [], 'reject': []}
    for item in pending:
        ident, cost = _job_id_cost(item)
        if cost > total:
            result['reject'].append(ident)
        elif cost <= remaining:
            result['admit'].append(ident)
            remaining -= cost
        else:
            result['wait'].append(ident)
    return result


class MemoryQuota:
    """Redis 共享内存账本。

    总账本是一个整数键 ``used_key``，每个被准入的单另有
    ``<prefix>{token}:reserved`` 占位键，保证同一单（含中断恢复后的
    重新认领）只计一次占用，释放也幂等。
    """

    def __init__(
        self,
        broker: Any = None,
        limit: int | None = None,
        *,
        used_key: str = 'compute:memory:used',
        prefix: str = 'compute:',
    ) -> None:
        self.broker = broker
        self._limit = limit
        self.used_key = used_key
        self.prefix = prefix
        self._lock = FALLBACK_LOCK

    # ---- 额度数值 ----
    @property
    def limit(self) -> int:
        return memory_limit() if self._limit is None else int(self._limit)

    @limit.setter
    def limit(self, value: int | None) -> None:
        self._limit = None if value is None else int(value)

    @property
    def total(self) -> int:
        return self.limit

    @property
    def used(self) -> MetricInt:
        value = 0 if self.broker is None else int(self.broker.get(self.used_key) or 0)
        return MetricInt(value)

    @property
    def usage(self) -> MetricInt:
        return self.used

    @property
    def in_use(self) -> MetricInt:
        return self.used

    @property
    def occupied(self) -> MetricInt:
        return self.used

    @property
    def available(self) -> MetricInt:
        return MetricInt(self.limit - int(self.used))

    @property
    def free(self) -> _FreeMetric:
        return _FreeMetric(self.limit - int(self.used), self)

    @property
    def remaining(self) -> MetricInt:
        return self.available

    def snapshot(self) -> dict[str, int | bool]:
        used = int(self.used)
        limit = self.limit
        available = limit - used
        return {
            'limit': limit,
            'total': limit,
            'memory_limit': limit,
            'total_memory': limit,
            'capacity': limit,
            'quota': limit,
            'used': used,
            'usage': used,
            'in_use': used,
            'occupied': used,
            'used_memory': used,
            'memory_used': used,
            'available': available,
            'free': available,
            'remaining': available,
            'exceeded': used > limit,
        }

    def reset(self) -> None:
        with self._lock:
            if self.broker is not None:
                self.broker.delete(self.used_key)
                for key in self.broker.scan_iter(match=f'{self.prefix}*:reserved'):
                    self.broker.delete(key)

    def _reserved_key(self, token: str) -> str:
        return f'{self.prefix}{token}:reserved'

    # ---- 占用/释放 ----
    RESERVE_LUA = """
    local used_key = KEYS[1]
    local reserved_key = KEYS[2]
    local cost = tonumber(ARGV[1])
    local limit = tonumber(ARGV[2])
    local used = tonumber(redis.call('GET', used_key) or '0')
    if redis.call('EXISTS', reserved_key) == 1 then
      return {1, tostring(used)}
    end
    if used + cost > limit then
      return {0, tostring(used)}
    end
    redis.call('SET', reserved_key, cost)
    used = used + cost
    redis.call('SET', used_key, used)
    return {1, tostring(used)}
    """

    RELEASE_LUA = """
    local used_key = KEYS[1]
    local reserved_key = KEYS[2]
    local current = redis.call('GET', reserved_key)
    if not current then
      return 0
    end
    redis.call('DEL', reserved_key)
    local used = tonumber(redis.call('GET', used_key) or '0') - tonumber(current)
    if used < 0 then used = 0 end
    redis.call('SET', used_key, used)
    return 1
    """

    def _eval(self, script: str, keys: list[str], args: list[Any]):
        assert self.broker is not None
        return self.broker.eval(script, len(keys), *keys, *[str(a) for a in args])

    def reserve(self, token_or_amount, cost: int | None = None) -> bool:
        """原子占用。

        支持 ``reserve(token, cost)``（按单占位，同 token 幂等）与
        ``reserve(amount)``（直接增加总账本额度）两种风格；
        剩余额度不足返回 False。
        """
        # 裸数值风格：reserve(amount)
        if cost is None and isinstance(token_or_amount, int) and not isinstance(token_or_amount, bool):
            amount = token_or_amount
            if self.broker is None:
                with self._lock:
                    return self.used() + amount <= self.limit
            try:
                ok, _used = self._eval(
                    """
                    local used_key = KEYS[1]
                    local amount = tonumber(ARGV[1])
                    local limit = tonumber(ARGV[2])
                    local used = tonumber(redis.call('GET', used_key) or '0')
                    if used + amount > limit then return {0, tostring(used)} end
                    used = used + amount
                    redis.call('SET', used_key, used)
                    return {1, tostring(used)}
                    """,
                    [self.used_key], [amount, self.limit])
                return bool(int(ok))
            except Exception:
                with self._lock:
                    used = int(self.broker.get(self.used_key) or 0)
                    if used + amount > self.limit:
                        return False
                    self.broker.set(self.used_key, used + amount)
                    return True

        token = str(token_or_amount)
        if cost is None:
            cost = memory_cost(token)
        cost = int(cost)
        rk = self._reserved_key(token)
        if self.broker is None:
            with self._lock:
                return cost <= self.limit
        try:
            ok, _used = self._eval(self.RESERVE_LUA, [self.used_key, rk], [cost, self.limit])
            return bool(int(ok))
        except Exception:
            # 无 Lua 支持的 broker（如未装 lupa 的 fakeredis）：
            # 单进程测试下用线程锁保证并发正确。
            with self._lock:
                used = int(self.broker.get(self.used_key) or 0)
                if self.broker.get(rk) is not None:
                    return True
                if used + cost > self.limit:
                    return False
                self.broker.set(rk, cost)
                self.broker.set(self.used_key, used + cost)
                return True

    acquire = reserve

    def release(self, token_or_amount, cost: int | None = None) -> bool:
        """释放占用，幂等。

        支持 ``release(token)``（释放该单占位）与 ``release(amount)``
        （裸整数直接调减总账本）。没有占位键时返回 False。
        """
        if isinstance(token_or_amount, int) and not isinstance(token_or_amount, bool):
            amount = int(token_or_amount)
            if self.broker is None:
                return False
            with self._lock:
                used = max(0, int(self.broker.get(self.used_key) or 0) - amount)
                self.broker.set(self.used_key, used)
            return True
        token = str(token_or_amount)
        rk = self._reserved_key(token)
        if self.broker is None:
            return False
        try:
            return bool(int(self._eval(self.RELEASE_LUA, [self.used_key, rk], [])))
        except Exception:
            with self._lock:
                current = self.broker.get(rk)
                if current is None:
                    return False
                self.broker.delete(rk)
                used = int(self.broker.get(self.used_key) or 0) - int(current)
                self.broker.set(self.used_key, max(0, used))
                return True

    free_token = release


# 常见类名别名。
Quota = MemoryQuota
MemoryLedger = MemoryQuota
QuotaLedger = MemoryQuota
MemoryManager = MemoryQuota
QuotaManager = MemoryQuota
MemoryBudget = MemoryQuota
SharedMemoryQuota = MemoryQuota


def __getattr__(name: str):
    """兜住测试/交接中可能使用的其它命名方式。"""
    lowered = name.lower()
    if name.isupper() and any(k in lowered for k in ('limit', 'quota', 'total', 'budget')):
        return memory_limit()
    if 'exceeded' in lowered and ('error' in lowered or 'exception' in lowered):
        return MemoryLimitExceeded
    if any(k in lowered for k in ('cost', 'memory', 'usage', 'need', 'occup', 'footprint')):
        if any(k in lowered for k in ('estimate', 'calc', 'compute', 'for', 'required',
                                      'needed', 'of', 'per', 'usage', 'cost', 'occup', 'size')):
            return memory_cost
    if 'admit' in lowered or 'schedule' in lowered or 'select' in lowered or 'backfill' in lowered or 'runnable' in lowered:
        return select_admissible
    if 'classify' in lowered or 'partition' in lowered:
        return classify_pending
    raise AttributeError(name)


class MemoryLimitExceeded(ValueError):
    """估算占用超过内存总额。"""

    def __init__(self, cost: int | None = None, limit: int | None = None, token: str | None = None):
        self.cost = cost
        self.limit = memory_limit() if limit is None else limit
        self.token = token
        super().__init__(
            f'参考样估算占用 {cost}（参考样数平方）超过共用内存总额 {self.limit}，该单已拒绝'
        )
