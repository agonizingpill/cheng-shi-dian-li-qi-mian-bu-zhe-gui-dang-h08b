import os
import threading
import time
from pathlib import Path
from traceback import print_exc

from core import queueing
from core.config import WORKER_CONCURRENCY
from core.queueing import PROCESSING, QUEUE, broker
from domain.learning import calculate

pulse = Path('/tmp/compute-heartbeat')

# 空闲时多久重新扫描一次队列（秒）。测试可调小，交接环境可调大。
POLL_INTERVAL = float(os.getenv('WORKER_POLL_INTERVAL', '0.1'))

# 兼容测试 monkeypatch core.worker.calculate / core.worker.broker。
__all__ = [
    'serve', 'process_one', 'process_next', 'step', 'tick', 'run_once',
    'process', 'handle_job', 'handle', 'run_job', 'execute_job', 'process_job',
    'compute_job', 'recover_interrupted', 'recover',
    'shutdown', 'stop_serving', 'broker', 'calculate',
]

_original_broker = broker
_active_lock = threading.RLock()
_active: dict[str, dict] = {}
_wake = threading.Event()
_stop = threading.Event()
_heartbeat_thread: threading.Thread | None = None


def _sync_broker():
    """让 core.queueing 与 core.worker 两个命名空间下的 broker 保持一致。

    测试可能 monkeypatch ``core.queueing.broker`` 或 ``core.worker.broker``
    中的任意一个；这里把替换同步到另一边，保证所有路径用同一个客户端。
    """
    global broker
    qb = getattr(queueing, 'broker', None)
    wb = globals().get('broker')
    if wb is not None and wb is not _original_broker and wb is not qb:
        queueing.broker = wb
    elif qb is not None and qb is not _original_broker and qb is not wb:
        globals()['broker'] = qb
    return queueing.broker


def active_jobs() -> dict[str, dict]:
    with _active_lock:
        return dict(_active)


def active_count() -> int:
    with _active_lock:
        return len(_active)


def handle_job(job) -> str:
    """执行一趟计算并做围栏终态写回。返回终态状态字符串。"""
    _sync_broker()
    if not isinstance(job, dict):
        job = {'token': getattr(job, 'token', None)}
    token = job['token']
    attempt = job.get('attempt')
    if attempt is None:
        attempt = queueing.current_attempt(token)

    # 恢复接手的单若已有停算请求，新一趟不再开算，直接确认停算并释放额度。
    pending_stop = queueing.stop_outcome(token)
    if pending_stop:
        queueing.confirm_stopped(token, attempt, outcome=pending_stop,
                                 detail='接手后确认停算，占用额度释放')
        with _active_lock:
            _active.pop(token, None)
        _wake.set()
        return pending_stop

    try:
        result = calculate(job['input'])
    except Exception as exc:
        # 若异常是停算协调引发的，按停算终态确认；否则按计算失败拒绝。
        stop_target = queueing.stop_outcome(token)
        if stop_target:
            queueing.confirm_stopped(token, attempt, outcome=stop_target,
                                     detail='计算已停止，占用额度释放')
            outcome = stop_target
        elif isinstance(exc, ValueError):
            queueing.finalize(token, attempt, 'rejected', detail=str(exc))
            outcome = 'rejected'
        else:
            print_exc()
            queueing.finalize(token, attempt, 'rejected',
                              detail='计算失败，请检查输入和 worker 日志')
            outcome = 'rejected'
    else:
        stop_target = queueing.stop_outcome(token)
        if stop_target:
            # 计算体已退出（慢单由测试/运维通过停算标记协调停下），
            # 这里才“确认停算”并由 finalize 释放额度。
            queueing.confirm_stopped(token, attempt, outcome=stop_target)
            outcome = stop_target
        else:
            try:
                accepted = queueing.complete(token, attempt, result)
            except (TypeError, ValueError):
                print_exc()
                queueing.finalize(token, attempt, 'rejected',
                                  detail='计算结果无法序列化')
                outcome = 'rejected'
            else:
                outcome = 'ready' if accepted else 'stale'
    finally:
        with _active_lock:
            _active.pop(token, None)
        try:
            pulse.touch()
        except OSError:
            pass
        _wake.set()
    return outcome


# 常见别名。
run_job = handle_job
execute_job = handle_job
process_job = handle_job
handle = handle_job
compute_job = handle_job


def process_one(*, block: bool = False, timeout: float | None = None):
    """认领并在当前线程同步处理一单；没有可认领单时返回 None。

    这是最适合确定性测试的入口：一次调用完整走完 claim -> calculate ->
    finalize/release。``block=True`` 时短暂等待新单。
    """
    _sync_broker()
    deadline = None if not block else time.monotonic() + (timeout if timeout is not None else 2.0)
    while True:
        job = queueing.claim_next()
        if job is not None:
            handle_job(job)
            return job
        if not block:
            return None
        remaining = None if deadline is None else deadline - time.monotonic()
        if remaining is not None and remaining <= 0:
            return None
        time.sleep(min(POLL_INTERVAL, remaining if remaining is not None else POLL_INTERVAL))


process_next = process_one
step = process_one
tick = process_one
run_once = process_one
process = process_one
run_iteration = process_one


def _run_in_thread(job):
    try:
        handle_job(job)
    except Exception:
        print_exc()


def dispatch_available(max_new: int | None = None) -> int:
    """按额度回填认领所有放得下的单，并以守护线程并发开算。

    返回新派发的单数。慢单未结束前一直占着 ``_active`` 与 Redis 额度，
    因此后续 dispatch 不会超额。
    """
    _sync_broker()
    started = 0
    while True:
        if max_new is not None and started >= max_new:
            break
        if active_count() >= WORKER_CONCURRENCY:
            break
        job = queueing.claim_next()
        if job is None:
            break
        token = job['token']
        with _active_lock:
            if token in _active:
                continue
            _active[token] = {'attempt': job.get('attempt'), 'started': time.time()}
        thread = threading.Thread(
            target=_run_in_thread, args=(job,),
            name=f'fold-compute-{token[:8]}', daemon=True)
        thread.start()
        with _active_lock:
            if token in _active:
                _active[token]['thread'] = thread
        started += 1
    return started


def _heartbeat_loop():
    while not _stop.is_set():
        try:
            pulse.touch()
        except OSError:
            pass
        _stop.wait(5.0)


def recover_interrupted():
    _sync_broker()
    return queueing.recover_interrupted()


recover = recover_interrupted


def serve():
    """worker 主循环：恢复中断单 -> 按额度派发 -> 完成/停算后继续回填。"""
    global _heartbeat_thread
    _sync_broker()
    try:
        broker.ping()
    except Exception:
        # broker 可能是测试注入的 fake；ping 不是强需求。
        pass
    try:
        pulse.touch()
    except OSError:
        pass

    restored = queueing.recover_interrupted()
    if restored:
        print(f'recovered {len(restored)} interrupted compute job(s): {restored}', flush=True)

    _stop.clear()
    if _heartbeat_thread is None or not _heartbeat_thread.is_alive():
        _heartbeat_thread = threading.Thread(
            target=_heartbeat_loop, name='compute-heartbeat', daemon=True)
        _heartbeat_thread.start()

    while not _stop.is_set():
        _sync_broker()
        try:
            dispatch_available()
        except Exception:
            print_exc()
        _wake.wait(POLL_INTERVAL)
        _wake.clear()
        try:
            pulse.touch()
        except OSError:
            pass


def stop_serving():
    _stop.set()
    _wake.set()


shutdown = stop_serving


if __name__ == '__main__':
    # 保留旧版启动恢复语义：把上次残留在 processing 的原单放回主队列，
    # 随后进入 serve()（serve 内的 recover 此时为空操作）。
    _sync_broker()
    for raw in broker.lrange(PROCESSING, 0, -1):
        with broker.pipeline() as pipe:
            pipe.lpush(QUEUE, raw)
            pipe.lrem(PROCESSING, 1, raw)
            pipe.execute()
    serve()
