import {session, COMPUTE_ACTIVE_STATES} from './store.js';

export async function request(path, data) {
  const headers = {'Content-Type':'application/json'};
  if (session()) headers.Authorization = 'Bearer ' + session().token;
  const response = await fetch('/api' + path, data === undefined ? {headers} :
    {method:'POST', headers, body:JSON.stringify(data)});
  const payload = await response.json();
  if (!response.ok) {
    const detail = Array.isArray(payload.detail) ? payload.detail.map(x => x.loc.at(-1) + ' ' + x.msg).join(' / ') : payload.detail;
    throw new Error(detail || '请求未完成');
  }
  return payload;
}

export async function stopCompute(token, outcome = 'stop') {
  return request(`/compute/${token}/${outcome}`, {});
}

export async function awaitCompute(token, report, {attempts = 240, interval = 250} = {}) {
  for (let i = 0; i < attempts; i++) {
    const data = await request('/compute/' + token);
    report(data);
    // 只有进入终态才结束等待；queued/running/stopping 都继续轮询。
    if (!COMPUTE_ACTIVE_STATES.includes(data.status)) return data;
    await new Promise(resolve => setTimeout(resolve, interval));
  }
  throw new Error('计算仍在排队或进行中，可回到工作页继续查询任务号');
}
