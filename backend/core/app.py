from contextlib import asynccontextmanager
import logging

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy import text

from core import auth, queueing
from core.db import metadata, transaction
from core.config import SEED_DEMO
from core.middleware import install
from core.queueing import ComputeRejected, broker, view
from domain.routes import routers
from scripts.seed_demo import seed

@asynccontextmanager
async def lifespan(app):
    from core.db import engine
    metadata.create_all(engine)
    if SEED_DEMO:
        try:
            seed()
        except Exception:
            logging.getLogger('uvicorn.error').exception('demo seed failed')
    yield

app = FastAPI(lifespan=lifespan)
install(app)


@app.exception_handler(ComputeRejected)
async def compute_rejected_handler(request: Request, exc: ComputeRejected):
    # 既保留 HTTP 409 + 字符串 detail（前端可直接展示），
    # 又在顶层带回 token/status，便于按计算单继续查询。
    content = {'detail': exc.detail, 'token': exc.token, 'status': 'rejected'}
    content.update({k: v for k, v in getattr(exc, 'extra', {}).items() if v is not None})
    return JSONResponse(status_code=exc.status_code, content=content)


app.include_router(auth.router)
for router in routers:
    app.include_router(router)

@app.get('/health')
def health():
    with transaction() as conn:
        conn.execute(text('SELECT 1'))
    return {'ok': bool(broker.ping())}


@app.get('/compute/quota')
@app.get('/compute-quota')
@app.get('/compute/memory')
@app.get('/compute/memory-usage')
@app.get('/compute/usage')
def compute_quota(user=Depends(auth.actor)):
    return queueing.quota_snapshot()


class ComputeControl(BaseModel):
    token: str


@app.post('/compute/stop')
def stop_compute_body(body: ComputeControl, user=Depends(auth.actor)):
    return queueing.request_stop(user, body.token, outcome='stopped')


@app.post('/compute/cancel')
def cancel_compute_body(body: ComputeControl, user=Depends(auth.actor)):
    return queueing.request_stop(user, body.token, outcome='cancelled')


@app.post('/compute/{token}/stop')
@app.get('/compute/{token}/stop')
def stop_compute(token: str, user=Depends(auth.actor)):
    return queueing.request_stop(user, token, outcome='stopped')


@app.post('/compute/{token}/cancel')
@app.get('/compute/{token}/cancel')
def cancel_compute(token: str, user=Depends(auth.actor)):
    return queueing.request_stop(user, token, outcome='cancelled')


@app.get('/compute/{token}')
def compute(token: str, user=Depends(auth.actor)):
    return view(user, token)


@app.delete('/compute/{token}')
def delete_compute(token: str, user=Depends(auth.actor)):
    return queueing.request_stop(user, token, outcome='cancelled')
