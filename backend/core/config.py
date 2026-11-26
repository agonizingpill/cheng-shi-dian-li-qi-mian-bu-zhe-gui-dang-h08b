import os

DATABASE_URL = os.environ['DATABASE_URL']
REDIS_URL = os.environ['REDIS_URL']
JWT_SECRET = os.environ['JWT_SECRET']
SEED_DEMO = os.getenv('SEED_DEMO', 'false').lower() == 'true'


def _int_env(names, default):
    for name in names:
        raw = os.getenv(name)
        if raw is not None and str(raw).strip() != '':
            try:
                return max(0, int(raw))
            except ValueError:
                continue
    return default


# 折态计算共用内存额度（占用单位与参考样数平方一致）。
FOLD_MEMORY_LIMIT = _int_env(
    ('FOLD_MEMORY_LIMIT', 'FOLD_MEMORY_TOTAL', 'FOLD_MEMORY_QUOTA', 'FOLD_MEMORY_BUDGET',
     'COMPUTE_MEMORY_LIMIT', 'COMPUTE_MEMORY_BUDGET',
     'MEMORY_LIMIT', 'MEMORY_QUOTA', 'MEMORY_BUDGET', 'TOTAL_MEMORY'),
    4096,
)

# 单个 worker 进程内允许同时开算的单子数（额度本身才是硬边界）。
WORKER_CONCURRENCY = _int_env(('WORKER_CONCURRENCY', 'COMPUTE_CONCURRENCY'), 32)
