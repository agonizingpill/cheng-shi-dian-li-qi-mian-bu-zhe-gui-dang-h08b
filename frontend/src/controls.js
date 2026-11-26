import {awaitCompute, request, stopCompute} from './api.js';
import {el, display} from './dom.js';
import {COMPUTE_STATES, evidence} from './store.js';
import {status} from './widgets.js';

const fallback = field => field.kind === 'token' ? evidence.get() : field.initial;
function input(field) {
  if (field.kind === 'bool') return el('select', {name:field.name},
    el('option',{value:'true', selected:fallback(field) === true ? '' : undefined},'是'),
    el('option',{value:'false', selected:fallback(field) === false ? '' : undefined},'否'));
  if (field.kind === 'json') return el('textarea', {name:field.name, required:''}, JSON.stringify(fallback(field), null, 2));
  const numeric = ['int','float'].includes(field.kind);
  return el('input',{name:field.name, required:'', type:numeric ? 'number' : 'text',
    step:field.kind === 'float' ? 'any' : undefined, value:fallback(field) ?? ''});
}
function parse(form, fields) {
  const data = new FormData(form);
  return Object.fromEntries(fields.map(field => {
    const raw = data.get(field.name);
    const convert = ['int','float'].includes(field.kind) ? Number :
      ['bool','json'].includes(field.kind) ? JSON.parse : String;
    return [field.name, convert(raw)];
  }));
}
export function actionForm(action, refresh) {
  const notice = status('');
  const stopButton = el('button', {type:'button', class:'stop', hidden:''}, '请求停算并释放额度');
  const button = el('button', {type:'submit'}, action.label);
  const form = el('form', {class:'action', id:action.name},
    el('h3',{},action.label), el('p',{},action.help),
    el('div',{class:'fields'},action.fields.map(field => el('label',{},
      el('span',{},field.label), input(field)))),
    el('div',{class:'buttons'}, button, stopButton), notice);
  let activeToken = null;
  stopButton.addEventListener('click', async () => {
    if (!activeToken) return;
    stopButton.disabled = true;
    try {
      await stopCompute(activeToken, 'stop');
      notice.textContent = '已请求停算，等待 worker 确认后释放额度…';
    } catch (error) {
      notice.textContent = error.message;
      notice.classList.add('error');
      stopButton.disabled = false;
    }
  });
  form.addEventListener('submit',async event => {
    event.preventDefault();
    button.disabled = true;
    stopButton.hidden = true;
    stopButton.disabled = false;
    notice.className = 'status';
    try {
      const data = await request(action.path, parse(form, action.fields));
      if (data.token && (data.status === 'queued' || data.status === 'running')) {
        activeToken = data.token;
        evidence.save(data.token);
        stopButton.hidden = false;
        const result = await awaitCompute(data.token, current => {
          const memory = current.memory ? `（估算占用 ${current.memory}）` : '';
          notice.textContent = (COMPUTE_STATES[current.status] || current.status) + memory;
        });
        stopButton.hidden = true;
        if (result.status === 'rejected') throw new Error(result.detail);
        if (result.status === 'stopped' || result.status === 'cancelled' || result.status === 'canceled') {
          notice.textContent = (COMPUTE_STATES[result.status] || result.status) + ' · 额度已释放';
          notice.classList.add('error');
          await refresh();
          return;
        }
        notice.textContent = '任务 ' + data.token + ' · ' + display(result.result);
      } else if (data.token && data.status === 'rejected') {
        throw new Error(data.detail || '计算单因内存额度被拒绝');
      } else {
        notice.textContent = action.success + ' · ' + display(data.code ?? data.status);
      }
      notice.classList.add('ok');
      await refresh();
    } catch (error) {
      stopButton.hidden = true;
      notice.textContent = error.message;
      notice.classList.add('error');
    } finally { button.disabled = false; activeToken = null; }
  });
  return form;
}
