export const ROLES = Object.freeze({operator: '作业员', reviewer: '复核员', viewer: '只读访客'});
export const COMPUTE_STATES = Object.freeze({
  queued: '排队中',
  waiting: '排队中（等待内存额度）',
  ready: '已计算',
  rejected: '计算被拒绝',
  running: '计算中（占用内存额度）',
  processing: '计算中（占用内存额度）',
  stopping: '正在确认停算…',
  cancelling: '正在确认取消…',
  canceled: '已停算',
  cancelled: '已停算',
  stopped: '已停算（额度已释放）'
});
// 未到终态前都要继续轮询；停算/取消确认后才释放额度并进入终态。
export const COMPUTE_ACTIVE_STATES = Object.freeze(
  ['queued', 'waiting', 'running', 'processing', 'dispatching', 'stopping', 'cancelling', 'canceling', 'stop_requested', 'cancel_requested']
);
export const COMPUTE_TERMINAL_STATES = Object.freeze(
  ['ready', 'rejected', 'stopped', 'canceled', 'cancelled']
);
export function session() {
  try { return JSON.parse(sessionStorage.getItem('session')); } catch { return null; }
}
export function setSession(value) {
  if (value) sessionStorage.setItem('session', JSON.stringify(value));
  else sessionStorage.removeItem('session');
}
export const evidence = {
  get: () => sessionStorage.getItem('latest-computation') || '',
  save: token => sessionStorage.setItem('latest-computation', token)
};
