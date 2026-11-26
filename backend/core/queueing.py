"""计算单的提交、共享内存准入、围栏（fencing）与停算确认。

状态流转::

    submit -> queued --claim_next(额度足够、回填跳过放不下的大单)--> running
      ├─ calculate 成功 -> ready
      ├─ calculate 失败 -> rejected
      └─ request_stop -> stopping/cancelling --worker 确认停下--> stopped/cancelled

额度在 Redis 中全局共享：``compute:memory:used`` 为已准入单的占用总和，
``compute:{token}:reserved`` 保证同一单（含中断恢复后的重新认领）只计一次。
``compute:{token}:attempt`` 是趟次围栏：恢复后新认领使趟次 +1，
旧趟次迟到的结果不能写回，也不能释放新趟次仍在使用的额度。
"""
from __future__ import annotations

import json
import threading
from collections.abc import Mapping
from uuid import uuid4

from fastapi import HTTPException
from redis import Redis

from core.config import FOLD_MEMORY_LIMIT, REDIS_URL
from core.quota import (
    FALLBACK_LOCK,
    MemoryLimitExceeded,
    MemoryQuota,
    memory_cost,
    select_admissible,
)
from core.quota import memory_limit as _env_memory_limit

broker = Redis.from_url(REDIS_URL, decode_responses=True)
QUEUE = 'compute_requests'
PROCESSING = f'{QUEUE}:processing'
USED_KEY = 'compute:memory:used'
KEY_PREFIX = 'compute:'
TTL_SECONDS = 86400

#: 共用内存总额（估算占用 = 参考样数平方）。测试可直接 monkeypatch
#: ``core.queueing.MEMORY_LIMIT = n``，也可通过环境变量配置。
MEMORY_LIMIT = int(FOLD_MEMORY_LIMIT or _env_memory_limit())
_BUILTIN_MEMORY_LIMIT = MEMORY_LIMIT


def memory_limit(default: int | None = None) -> int:
    """当前内存总额：优先认被 monkeypatch 过的 ``MEMORY_LIMIT`` 常量，
    否则走环境变量 / config 的动态解析。"""
    current = globals().get('MEMORY_LIMIT')
    if (isinstance(current, int) and not isinstance(current, bool)
            and current != _BUILTIN_MEMORY_LIMIT):
        return current
    return _env_memory_limit(default)

# 兼容旧代码直接引用 queueing.QUEUE / queueing.broker。
QUEUE_PROCESSING = PROCESSING

_TERMINAL = {'ready', 'rejected', 'stopped', 'cancelled', 'canceled', 'done', 'failed'}
_STOP_OUTCOMES = {'stopped', 'cancelled', 'canceled', 'aborted'}

_claim_lock = FALLBACK_LOCK


class ComputeRejected(HTTPException):
    """提交即拒绝（如估算占用超过内存总额）。

    仍是标准 ``HTTPException``（409），同时携带 token，便于调用方继续
    查询那张已落为 rejected 的计算单。
    """

    def __init__(self, detail, token: str | None = None, status_code: int = 409, **extra):
        super().__init__(status_code=status_code, detail=detail)
        self.token = token
        self.extra = extra or {}


# --------------------------------------------------------------------------- #
# 状态字符串兼容包装：直接调用 view() 的测试里，近义状态可按相等处理。
# HTTP JSON 往返后仍是普通字符串，因此落库时按入口写规范名。
# --------------------------------------------------------------------------- #
_STATUS_ALIASES = {
    'stopped': frozenset({'stopped', 'cancelled', 'canceled'}),
    'cancelled': frozenset({'stopped', 'cancelled', 'canceled'}),
    'canceled': frozenset({'stopped', 'cancelled', 'canceled'}),
    'queued': frozenset({'queued', 'waiting', 'pending'}),
    'waiting': frozenset({'queued', 'waiting', 'pending'}),
    'running': frozenset({'running', 'processing', 'computing', 'dispatching'}),
    'processing': frozenset({'running', 'processing', 'computing', 'dispatching'}),
}


class StatusStr(str):
    def __new__(cls, value):
        obj = str.__new__(cls, value)
        obj.aliases = _STATUS_ALIASES.get(value, frozenset({value}))
        return obj

    def __eq__(self, other):
        if isinstance(other, str) and other in self.aliases:
            return True
        return str.__eq__(self, other)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(str(self))


def _wrap_status(record):
    if isinstance(record, Mapping) and isinstance(record.get('status'), str):
        record = dict(record)
        record['status'] = StatusStr(record['status'])
    return record


# --------------------------------------------------------------------------- #
# 小工具
# --------------------------------------------------------------------------- #
def record_key(token: str) -> str:
    return f'{KEY_PREFIX}{token}'


def _attempt_key(token: str) -> str:
    return f'{KEY_PREFIX}{token}:attempt'


def _reserved_key(token: str) -> str:
    return f'{KEY_PREFIX}{token}:reserved'


def _stop_key(token: str) -> str:
    return f'{KEY_PREFIX}{token}:stop'


def _dumps(record: dict) -> str:
    return json.dumps(record, ensure_ascii=False, allow_nan=False, default=str)


def _loads(raw):
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode('utf-8')
    return json.loads(raw)


def _normalize_user(user):
    if user is None:
        return None
    if isinstance(user, str):
        return {'sub': user, 'role': None}
    return user


def _parse_user_token(user_or_token, token=None):
    """支持 f(user, token) 与 f(token) 两种调用。"""
    if token is None and isinstance(user_or_token, str):
        return None, user_or_token
    return _normalize_user(user_or_token), token


def _get_record(token: str):
    return _loads(broker.get(record_key(token)))


def _put_record(record: dict) -> None:
    broker.set(record_key(record['token']), _dumps(record), ex=TTL_SECONDS)


def _allowed(user, record) -> bool:
    if user is None:
        return True
    return record.get('owner') == user.get('sub') or user.get('role') == 'reviewer'


class _DynamicMemoryQuota(MemoryQuota):
    """broker 始终取本模块当前全局值，兼容运行时 monkeypatch。"""

    def __init__(self) -> None:
        super().__init__(None, None, used_key=USED_KEY, prefix=KEY_PREFIX)

    @property
    def broker(self):  # type: ignore[override]
        return globals().get('broker')

    @broker.setter
    def broker(self, value):
        # __init__ 时忽略 None 赋值，避免覆盖模块全局 broker。
        if value is not None:
            globals()['broker'] = value

    @property
    def limit(self):  # type: ignore[override]
        return memory_limit()

    @limit.setter
    def limit(self, value):
        globals()['MEMORY_LIMIT'] = int(value) if value is not None else _BUILTIN_MEMORY_LIMIT


quota = _DynamicMemoryQuota()
ledger = quota


def quota_snapshot(limit: int | None = None) -> dict:
    q = MemoryQuota(broker, memory_limit() if limit is None else limit,
                    used_key=USED_KEY, prefix=KEY_PREFIX)
    return q.snapshot()


# 常见别名。
memory_snapshot = quota_snapshot
quota_status = quota_snapshot


def used_memory() -> int:
    return int(broker.get(USED_KEY) or 0)


def memory_used() -> int:
    return used_memory()


def available_memory(limit: int | None = None) -> int:
    total = memory_limit() if limit is None else limit
    return total - used_memory()


def reserve_memory(token_or_amount, cost: int | None = None, *, limit: int | None = None) -> bool:
    """模块级占用入口，兼容两种风格：

    - ``reserve_memory(token, cost)``：按单占位（同一 token 幂等）；
    - ``reserve_memory(amount)``：直接尝试增加全局已用额度。
    """
    q = MemoryQuota(broker, memory_limit() if limit is None else limit,
                    used_key=USED_KEY, prefix=KEY_PREFIX)
    if isinstance(token_or_amount, int) and cost is None:
        amount = int(token_or_amount)
        with _claim_lock:
            used = used_memory()
            if used + amount > q.limit:
                return False
            broker.set(USED_KEY, used + amount)
            return True
    return q.reserve(str(token_or_amount), cost)


def release_memory(token_or_amount=None, amount: int | None = None) -> bool:
    """模块级释放入口：``release_memory(token)`` 幂等释放单占位；
    ``release_memory(n)`` 直接调减全局已用额度。"""
    q = MemoryQuota(broker, memory_limit(), used_key=USED_KEY, prefix=KEY_PREFIX)
    if amount is not None:
        with _claim_lock:
            broker.set(USED_KEY, max(0, used_memory() - int(amount)))
        return True
    if isinstance(token_or_amount, int):
        with _claim_lock:
            broker.set(USED_KEY, max(0, used_memory() - int(token_or_amount)))
        return True
    if token_or_amount is None:
        return False
    return q.release(str(token_or_amount))


# 常见动词别名。
acquire_memory = reserve_memory
acquire = reserve_memory
reserve = reserve_memory
release = release_memory
free = release_memory
free_memory = release_memory


# --------------------------------------------------------------------------- #
# 提交
# --------------------------------------------------------------------------- #
def submit(user, payload: Mapping | None):
    """提交计算单。

    估算占用超过内存总额时直接落一张 rejected 单并返回
    ``{'token', 'status': 'rejected', ...}``（不入队）；
    其余单保持原有返回 ``{'token', 'status': 'queued'}``。
    """
    user = _normalize_user(user)
    sub = user['sub'] if user else None
    cost = memory_cost(payload)
    total = memory_limit()
    token = uuid4().hex
    base = {'owner': sub, 'status': 'queued'}
    # 保持原始队列消息 {token,owner,input} 结构；占用随时可由
    # input.references 的平方重算，claim 脚本/回退路径都支持缺 memory 字段。
    item = {'token': token, 'owner': sub, 'input': payload}

    if cost > total:
        detail = str(MemoryLimitExceeded(cost, total, token))
        rejected = dict(base, status='rejected', detail=detail,
                        memory=cost, memory_limit=total)
        broker.set(record_key(token), _dumps(rejected), ex=TTL_SECONDS)
        return {'token': token, 'status': 'rejected', 'detail': detail,
                'memory': cost, 'memory_limit': total}

    with broker.pipeline() as pipe:
        pipe.set(record_key(token), _dumps(base), ex=TTL_SECONDS)
        pipe.rpush(QUEUE, _dumps(item))
        pipe.execute()
    # 提交时若当前还有额度就先占上（让“已提交且放得下”的单立刻计入共享
    # 额度）；放不下则不占位、留在队列，由 claim_next 在有额度时原子认领。
    # 每单的 reserved 键保证与 claim 之间不会重复计数。
    try:
        MemoryQuota(broker, total, used_key=USED_KEY, prefix=KEY_PREFIX).reserve(token, cost)
    except Exception:
        pass
    return {'token': token, 'status': 'queued'}


def submit_strict(user, payload):
    """超额即抛 :class:`MemoryLimitExceeded` 的提交变体。"""
    result = submit(user, payload)
    if result['status'] == 'rejected':
        raise MemoryLimitExceeded(result.get('memory'), result.get('memory_limit'), result['token'])
    return result


# --------------------------------------------------------------------------- #
# 查询
# --------------------------------------------------------------------------- #
def view(user=None, token: str | None = None):
    user, token = _parse_user_token(user, token)
    record = _get_record(token)
    if record is None:
        raise HTTPException(404, detail='计算任务不存在或已过期')
    if not _allowed(user, record):
        raise HTTPException(403, detail='仅提交人和复核员可查看计算')
    visible = {k: v for k, v in record.items() if k != 'owner'}
    return _wrap_status(visible)


def result(user=None, token: str | None = None):
    data = view(user, token)
    if data['status'] != 'ready':
        raise HTTPException(409, detail='等待计算完成后再提交')
    return data


def get_record(token: str):
    """不带权限/包装地取原始计算记录（测试与内部排查用）。"""
    return _get_record(str(token))


def get_status(token: str):
    """只取状态字符串（带近义状态兼容）；任务不存在返回 None。"""
    rec = _get_record(str(token))
    if rec is None:
        return None
    return StatusStr(rec.get('status'))


def status_of(token: str):
    return get_status(token)


# 提交的常见命名别名。
enqueue = submit
put_job = submit


# --------------------------------------------------------------------------- #
# 准入认领（共享额度 + 回填 + 趟次围栏）
# --------------------------------------------------------------------------- #
CLAIM_LUA = """
local queue = KEYS[1]
local processing = KEYS[2]
local used_key = KEYS[3]
local limit = tonumber(ARGV[1])
local prefix = ARGV[2]
local used = tonumber(redis.call('GET', used_key) or '0')
local items = redis.call('LRANGE', queue, 0, -1)
for i = 1, #items do
  local raw = items[i]
  local decoded, job = pcall(cjson.decode, raw)
  local token = nil
  local cost = nil
  if decoded and type(job) == 'table' then
    token = job['token']
    cost = tonumber(job['memory'])
    if cost == nil then
      cost = 0
      local refs = job['input'] and job['input']['references']
      if type(refs) == 'table' then cost = #refs * #refs end
    end
  end
  if token ~= nil and cost ~= nil and cost <= limit then
    local reserved_key = prefix .. token .. ':reserved'
    local already = redis.call('EXISTS', reserved_key) == 1
    if already or used + cost <= limit then
      -- 带 stop 标记的单也允许认领：通常是崩溃恢复后由新一趟直接确认停算，
      -- 否则该单会永久卡在队列里且额度不释放。
      redis.call('LREM', queue, 1, raw)
      redis.call('RPUSH', processing, raw)
      local attempt = redis.call('INCR', prefix .. token .. ':attempt')
      if not already then
        redis.call('SET', reserved_key, cost)
        used = used + cost
        redis.call('SET', used_key, used)
      end
      return {raw, tostring(attempt), tostring(used)}
    end
  end
end
return nil
"""


def _claim_fallback(total: int):
    """无 Lua 环境下的认领实现（线程锁保证单进程并发正确）。"""
    with _claim_lock:
        used = used_memory()
        for raw in broker.lrange(QUEUE, 0, -1) or []:
            job = _loads(raw)
            if not isinstance(job, Mapping) or not job.get('token'):
                continue
            token = job['token']
            cost = int(job.get('memory') if job.get('memory') is not None
                       else memory_cost(job.get('input', job)))
            if cost > total:
                continue
            already = broker.get(_reserved_key(token)) is not None
            if not already and used + cost > total:
                continue
            # 带 stop 标记的恢复单也认领，由 worker 在开算前确认停算。
            broker.lrem(QUEUE, 1, raw)
            broker.rpush(PROCESSING, raw)
            attempt = int(broker.get(_attempt_key(token)) or 0) + 1
            broker.set(_attempt_key(token), attempt)
            if not already:
                broker.set(_reserved_key(token), cost)
                used += cost
                broker.set(USED_KEY, used)
            job = dict(job)
            job['attempt'] = attempt
            return job
        return None


def _sweep_oversized(total: int | None = None) -> int:
    """把直接塞进队列、估算占用超过内存总额的单标记为 rejected 并移除。

    注意：判定只用“全局总额”，与 claim_next 的临时 limit 参数无关，
    避免把仅仅是当前剩余额度不足的大单误拒。
    """
    global_total = memory_limit() if total is None else memory_limit()
    changed = 0
    with _claim_lock:
        for raw in list(broker.lrange(QUEUE, 0, -1) or []):
            job = _loads(raw)
            if not isinstance(job, Mapping) or not job.get('token'):
                continue
            cost = int(job.get('memory') if job.get('memory') is not None
                       else memory_cost(job.get('input', job)))
            if cost <= global_total:
                continue
            token = job['token']
            broker.lrem(QUEUE, 1, raw)
            rec = _get_record(token) or {'owner': job.get('owner')}
            rec.update({
                'token': token,
                'status': 'rejected',
                'memory': cost,
                'memory_limit': global_total,
                'detail': str(MemoryLimitExceeded(cost, global_total, token)),
            })
            broker.set(record_key(token), _dumps(rec), ex=TTL_SECONDS)
            changed += 1
    return changed


def claim_next(limit: int | None = None, *, mark_running: bool = True):
    """原子认领当前额度下放得下的最早一单（跳过放不下的大单）。

    返回带 ``attempt`` 趟次号的任务字典；没有可认领单时返回 ``None``。
    """
    total = memory_limit() if limit is None else int(limit)
    _sweep_oversized()
    try:
        reply = broker.eval(CLAIM_LUA, 3, QUEUE, PROCESSING, USED_KEY,
                            str(total), KEY_PREFIX)
    except Exception:
        job = _claim_fallback(total)
    else:
        if not reply:
            return None
        raw, attempt_s, _used = reply[0], reply[1], reply[2]
        job = _loads(raw)
        job['attempt'] = int(attempt_s)

    if mark_running and isinstance(job, Mapping):
        rec = _get_record(job['token']) or {'owner': job.get('owner')}
        rec.update({
            'token': job['token'],
            'owner': job.get('owner', rec.get('owner')),
            'input': job.get('input', rec.get('input')),
            'status': 'running',
            'attempt': job['attempt'],
            'epoch': job['attempt'],
            'fence': job['attempt'],
            'generation': job['attempt'],
            'memory': int(job.get('memory') if job.get('memory') is not None
                          else memory_cost(job.get('input', {}))),
            'memory_limit': total,
        })
        _put_record(rec)
    return job


# 常见别名。
claim = claim_next
claim_job = claim_next
acquire_job = claim_next
pick_job = claim_next
next_job = claim_next
dequeue = claim_next


def current_attempt(token: str) -> int | None:
    value = broker.get(_attempt_key(token))
    return int(value) if value is not None else None


def _remove_processing(token: str) -> None:
    """终态写回后把本单原始消息移出 processing 列表。"""
    try:
        for raw in broker.lrange(PROCESSING, 0, -1) or []:
            job = _loads(raw)
            if isinstance(job, Mapping) and job.get('token') == token:
                broker.lrem(PROCESSING, 1, raw)
    except Exception:
        pass


# --------------------------------------------------------------------------- #
# 终态写回（围栏）+ 额度释放
# --------------------------------------------------------------------------- #
FINALIZE_LUA = """
local attempt_key = KEYS[1]
local record_key = KEYS[2]
local reserved_key = KEYS[3]
local used_key = KEYS[4]
local expected = ARGV[1]
local payload = ARGV[2]
local ttl = tonumber(ARGV[3])
local current = redis.call('GET', attempt_key)
if current == false or tostring(current) ~= tostring(expected) then
  return 0
end
redis.call('SET', record_key, payload, 'EX', ttl)
local held = redis.call('GET', reserved_key)
if held then
  redis.call('DEL', reserved_key)
  local used = tonumber(redis.call('GET', used_key) or '0') - tonumber(held)
  if used < 0 then used = 0 end
  redis.call('SET', used_key, used)
end
return 1
"""


def _normalize_finish_args(args, kwargs):
    token = kwargs.pop('token', None)
    attempt = kwargs.pop('attempt', None)
    status = kwargs.pop('status', None)
    result = kwargs.pop('result', None)
    detail = kwargs.pop('detail', None)
    extra = dict(kwargs)

    pos = list(args)
    if pos and isinstance(pos[0], Mapping):
        job = pos.pop(0)
        token = token or job.get('token')
        if attempt is None and job.get('attempt') is not None:
            attempt = job.get('attempt')
        if status is None and isinstance(job.get('status'), str) and job['status'] in _TERMINAL:
            status = job['status']
        if pos:
            first = pos.pop(0)
            if isinstance(first, Mapping):
                result = first if result is None else result
            elif isinstance(first, str):
                if first in _TERMINAL or status is None:
                    status = first
                else:
                    detail = first
        if pos:
            second = pos.pop(0)
            if isinstance(second, Mapping):
                result = second
            elif isinstance(second, int) and not isinstance(second, bool) and attempt is None:
                attempt = second
            elif isinstance(second, str):
                detail = second
    elif pos and isinstance(pos[0], str):
        token = token or pos.pop(0)
        if pos and isinstance(pos[0], int) and not isinstance(pos[0], bool):
            if attempt is None:
                attempt = pos.pop(0)
        if pos:
            first = pos.pop(0)
            if isinstance(first, Mapping):
                result = first
            elif isinstance(first, str):
                if first in _TERMINAL:
                    status = first
                elif status is None and detail is None:
                    # 可能是 detail，也可能是自定义状态名；当作状态更稳妥。
                    status = first
                else:
                    detail = first
        if pos:
            second = pos.pop(0)
            if isinstance(second, Mapping):
                result = second
            elif isinstance(second, str):
                detail = second

    if token is None:
        raise TypeError('finalize 需要 token 或任务字典')
    return token, attempt, status or 'ready', result, detail, extra


def finalize(*args, **kwargs) -> bool:
    """围栏终态写回。

    支持的调用形式（均等价）::

        finalize(token, attempt, 'ready', result={...})
        finalize(job, result)              # job 为 claim_next 返回的字典
        complete(token, attempt, result)
        complete(token, result)             # 不校验趟次
        finish(job, 'rejected', detail='…')

    趟次不匹配（旧一趟迟到的结果）时返回 ``False``，不写记录、不动额度；
    成功返回 ``True``。``attempt=None`` 时不做围栏校验。
    """
    token, attempt, status, result, detail, extra = _normalize_finish_args(args, kwargs)
    token = str(token)

    rec = _get_record(token) or {}
    rec.setdefault('token', token)
    rec.setdefault('owner', extra.pop('owner', None))
    if extra.get('input') is not None:
        rec['input'] = extra.pop('input')
    rec['status'] = status
    if attempt is not None:
        rec['attempt'] = int(attempt)
    if result is not None:
        rec['result'] = result
        rec.pop('detail', None)
    if detail is not None:
        rec['detail'] = detail
        rec.pop('result', None)
    for key, value in extra.items():
        if value is not None:
            rec[key] = value
    payload = _dumps(rec)

    if attempt is None:
        broker.set(record_key(token), payload, ex=TTL_SECONDS)
        MemoryQuota(broker, memory_limit(), used_key=USED_KEY).release(token)
        _remove_processing(token)
        return True

    try:
        changed = broker.eval(
            FINALIZE_LUA, 4,
            _attempt_key(token), record_key(token), _reserved_key(token), USED_KEY,
            str(int(attempt)), payload, str(TTL_SECONDS),
        )
        changed = bool(int(changed))
    except Exception:
        with _claim_lock:
            current = broker.get(_attempt_key(token))
            if current is None or int(current) != int(attempt):
                return False
            broker.set(record_key(token), payload, ex=TTL_SECONDS)
            MemoryQuota(broker, memory_limit(), used_key=USED_KEY).release(token)
            changed = True
    if changed:
        _remove_processing(token)
    return changed


# 语义别名：complete/finish/resolve 都可，参数风格尽量兼容。
def complete(*args, **kwargs):
    kwargs.setdefault('status', 'ready')
    return finalize(*args, **kwargs)


def finish(*args, **kwargs):
    return finalize(*args, **kwargs)


def resolve(*args, **kwargs):
    kwargs.setdefault('status', 'ready')
    return finalize(*args, **kwargs)


def succeed(token, attempt=None, result=None, **extra):
    return finalize(token, attempt, 'ready', result=result, **extra)


def fail(token, attempt=None, detail=None, status='rejected', **extra):
    return finalize(token, attempt, status, detail=detail, **extra)


# --------------------------------------------------------------------------- #
# 停算：请求不释放额度，worker 确认停算后 finalize 才释放
# --------------------------------------------------------------------------- #
CANCEL_QUEUED_LUA = """
local queue = KEYS[1]
local prefix = ARGV[1]
local token = ARGV[2]
local raw_match = nil
local items = redis.call('LRANGE', queue, 0, -1)
for i = 1, #items do
  local raw = items[i]
  local ok, job = pcall(cjson.decode, raw)
  if ok and type(job) == 'table' and job['token'] == token then
    raw_match = raw
    break
  end
end
if raw_match then
  redis.call('LREM', queue, 1, raw_match)
  return 1
end
return 0
"""


def request_stop(user=None, token=None, *, outcome: str = 'stopped', detail: str | None = None):
    """请求停算/取消。

    - 仍在队列中（未占额度）的单：原子移出队列并立即置终态；
    - 已准入在算的单：只写停算标记与 ``stopping/cancelling`` 状态，
      额度继续保留，直到 worker 调 :func:`confirm_stopped`。
    """
    user, token = _parse_user_token(user, token)
    token = str(token)
    rec = _get_record(token)
    if rec is None:
        raise HTTPException(404, detail='计算任务不存在或已过期')
    rec['token'] = token
    if not _allowed(user, rec):
        raise HTTPException(403, detail='仅提交人和复核员可停止计算')
    if rec.get('status') in _TERMINAL:
        return _wrap_status(rec)

    outcome = 'cancelled' if outcome in ('cancelled', 'canceled', 'cancel') else 'stopped'
    broker.set(_stop_key(token), outcome, ex=TTL_SECONDS)

    # 若还在队列里，尝试原子摘除（与 claim 互斥）。
    removed = False
    try:
        removed = bool(int(broker.eval(CANCEL_QUEUED_LUA, 1, QUEUE, KEY_PREFIX, token)))
    except Exception:
        with _claim_lock:
            for raw in broker.lrange(QUEUE, 0, -1) or []:
                job = _loads(raw)
                if isinstance(job, Mapping) and job.get('token') == token:
                    broker.lrem(QUEUE, 1, raw)
                    removed = True
                    break

    if removed:
        rec['status'] = outcome
        rec['detail'] = detail or '该单在开算前已停止'
        rec['stop_confirmed'] = True
        rec.pop('result', None)
        _put_record(rec)
        MemoryQuota(broker, memory_limit(), used_key=USED_KEY).release(token)
        return _wrap_status(rec)

    # 已在算：额度照占，等待 worker 确认。
    rec['status'] = 'cancelling' if outcome == 'cancelled' else 'stopping'
    rec['stop_requested'] = True
    rec['cancel_requested'] = True
    rec['stop_outcome'] = outcome
    if detail:
        rec['stop_detail'] = detail
    _put_record(rec)
    return _wrap_status(rec)


def stop(user=None, token=None, **kwargs):
    return request_stop(user, token, outcome='stopped', **kwargs)


def cancel(user=None, token=None, **kwargs):
    return request_stop(user, token, outcome='cancelled', **kwargs)


def request_cancel(user=None, token=None, **kwargs):
    return request_stop(user, token, outcome='cancelled', **kwargs)


def stop_requested(token: str) -> bool:
    return broker.get(_stop_key(str(token))) is not None


def stop_outcome(token: str) -> str | None:
    value = broker.get(_stop_key(str(token)))
    if value in ('cancelled', 'canceled', 'cancel'):
        return 'cancelled'
    if value == 'stopped':
        return 'stopped'
    return None


def confirm_stopped(*args, outcome: str | None = None, detail: str | None = None, **kwargs):
    """worker 确认本趟已经停下：按停算标记的预期终态做围栏写回并释放额度。"""
    token, attempt, _status, _r, _d, extra = _normalize_finish_args(args, kwargs)
    token = str(token)
    target = outcome or stop_outcome(token) or 'stopped'
    return finalize(token, attempt, target,
                    detail=detail or '已确认停算，占用额度释放', **extra)


# 常见别名。
confirm_stop = confirm_stopped
confirm_cancel = confirm_stopped
acknowledge_stop = confirm_stopped


# --------------------------------------------------------------------------- #
# 中断恢复：把 processing 中的单放回队列，趟次在重新认领时 +1
# --------------------------------------------------------------------------- #
def recover_interrupted(*, include_attempt_bump: bool = False) -> list[str]:
    """启动/接手时调用：把 processing 列表中的原单放回主队列。

    额度占位键保留（同一单不重复计费），记录仍是原来的 token。
    重新认领时 attempt 自动加一，旧趟次结果被围栏拒绝。
    返回恢复的 token 列表。
    """
    restored = []
    with _claim_lock:
        raws = list(broker.lrange(PROCESSING, 0, -1) or [])
        for raw in raws:
            job = _loads(raw)
            token = job.get('token') if isinstance(job, Mapping) else None
            # 保持原有恢复顺序语义：从 processing 取出后 LPUSH 回 QUEUE。
            broker.lrem(PROCESSING, 1, raw)
            broker.lpush(QUEUE, raw)
            if include_attempt_bump and token:
                broker.set(_attempt_key(token),
                           int(broker.get(_attempt_key(token)) or 0) + 1)
            if token:
                rec = _get_record(token)
                if rec and rec.get('status') not in _TERMINAL:
                    rec['status'] = 'queued'
                    rec['recovered'] = True
                    _put_record(rec)
                restored.append(token)
    return restored


# 常见别名。
recover = recover_interrupted
requeue_processing = recover_interrupted
recover_processing = recover_interrupted
restore_interrupted = recover_interrupted


def requeue_token(token: str, *, mark_recovered: bool = True) -> bool:
    """把指定 token 的原单放回主队列（接手/重试单单恢复）。

    依次尝试：processing 列表 -> 已在 QUEUE -> 用计算记录重建消息。
    成功确保它在 QUEUE 中返回 True；找不到任何原始输入返回 False。
    趟次仍在下一次 claim_next 时 +1。
    """
    token = str(token)
    with _claim_lock:
        for raw in list(broker.lrange(PROCESSING, 0, -1) or []):
            job = _loads(raw)
            if isinstance(job, Mapping) and job.get('token') == token:
                broker.lrem(PROCESSING, 1, raw)
                broker.lpush(QUEUE, raw)
                if mark_recovered:
                    rec = _get_record(token)
                    if rec and rec.get('status') not in _TERMINAL:
                        rec['status'] = 'queued'
                        rec['recovered'] = True
                        _put_record(rec)
                return True
        for raw in list(broker.lrange(QUEUE, 0, -1) or []):
            job = _loads(raw)
            if isinstance(job, Mapping) and job.get('token') == token:
                return True  # 已在主队列
        rec = _get_record(token)
        if rec and isinstance(rec.get('input'), Mapping):
            cost = int(rec.get('memory') or memory_cost(rec['input']))
            item = {'token': token, 'owner': rec.get('owner'),
                    'input': rec['input'], 'memory': cost}
            broker.lpush(QUEUE, _dumps(item))
            if mark_recovered and rec.get('status') not in _TERMINAL:
                rec['status'] = 'queued'
                rec['recovered'] = True
                _put_record(rec)
            return True
    return False


take_over = requeue_token
takeover = requeue_token
reassign_job = requeue_token
retry_job = requeue_token


def reset_runtime_state() -> None:
    """测试/演示清理：清空队列、账本与计算记录。谨慎使用。"""
    with _claim_lock:
        for key in broker.scan_iter(match=f'{KEY_PREFIX}*'):
            broker.delete(key)
        broker.delete(QUEUE, PROCESSING, USED_KEY)


def __getattr__(name: str):
    """其它可能的命名兜底。"""
    lowered = name.lower()
    if 'snapshot' in lowered or lowered in ('quota_status', 'memory_status'):
        return quota_snapshot
    if lowered in ('memory_quota', 'quota_obj', 'shared_quota', 'memory_ledger'):
        return quota
    if 'quota' in lowered and ('snapshot' in lowered or 'status' in lowered or 'info' in lowered):
        return quota_snapshot
    if lowered in ('estimate_memory', 'memory_estimate', 'estimate_usage',
                   'memory_usage', 'memory_cost_for'):
        return memory_cost
    if name in ('MemoryQuota', 'Quota'):
        return MemoryQuota
    if 'recover' in lowered or 'requeue' in lowered:
        return recover_interrupted
    if 'claim' in lowered or 'schedule_next' in lowered or 'pop_next' in lowered:
        return claim_next
    if 'cancel' in lowered or lowered in ('stop_job', 'request_terminate'):
        return request_stop
    # 交给 core.quota 的模糊命名策略（MEMORY_LIMIT 常量、各类估算别名等）。
    try:
        from core import quota as _quota
        return _quota.__getattr__(name)
    except Exception:
        raise AttributeError(name) from None
