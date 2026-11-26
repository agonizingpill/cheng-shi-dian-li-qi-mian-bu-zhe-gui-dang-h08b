from core.queueing import ComputeRejected, result, submit
from core.records import get, insert, need, scan
from domain.crease_samples_model import crease_samples
from domain.fold_exemplars_model import fold_exemplars


def find_exemplar(conn, user, data):
    query=get(conn,crease_samples,data.sample)
    pool=[x for x in scan(conn,crease_samples) if x['signed'] and x['code']!=data.sample]
    need(query['signed'] and len(pool)>=10,'签认样条和参考不足')
    # submit 内部按 memory_cost=len(references)**2 与共用内存总额比较：
    # 超额单直接落 rejected 并返回；能算的单原样入队且用齐全部参考样。
    output=submit(user,{'query':query,'references':pool})
    if output.get('status') == 'rejected':
        # 保持领域规则的 409 语义，同时把计算号带回给调用方/测试。
        raise ComputeRejected(output.get('detail') or '参考样估算占用超过共用内存总额',
                              token=output.get('token'), memory=output.get('memory'),
                              memory_limit=output.get('memory_limit'))
    return output


def save_exemplar(conn, user, data):
    run=result(user,data.token); stats=run['result']
    return insert(conn,fold_exemplars,code=data.code,sample=run['input']['query']['code'],
      example=stats['exemplar'],group_size=stats['group_size'],token=data.token)
