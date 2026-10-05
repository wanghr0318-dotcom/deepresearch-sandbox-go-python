// 把 API 错误（扁平 {code, message}）转成面向演示的明确提示。
import { ApiError, NetworkError } from "../api/client";
import { ProtocolError } from "../api/sse";

export interface ErrorView {
  /** 错误码（ApiError.code）或 "network" / "protocol" / "client"。 */
  code: string;
  title: string;
  detail: string;
  status?: number;
}

const TITLES: Record<string, string> = {
  diagnostic_mode: "服务处于诊断模式：只读，写操作被拒绝",
  ownership_lost: "服务已失去数据目录所有权，暂停处理写请求",
  budget_exhausted: "预算已耗尽",
  budget_insufficient_for_request: "剩余预算不足以发起该请求",
  unauthorized: "令牌缺失或错误",
  forbidden_host: "Host 不在允许列表中",
  forbidden_origin: "Origin 不在允许列表中",
  request_conflict: "request_id 已用于不同内容的请求",
  task_ended: "任务已结束，不能再控制",
  cancel_pending: "已接受取消，不能再改为其他控制",
  not_paused: "任务未处于暂停状态，不能恢复",
  task_not_found: "任务不存在",
  artifact_not_found: "产物不存在或不可下载（internal 产物与缺失等同）",
  blob_unavailable: "产物内容暂时无法从 BlobStore 读取",
  not_ready: "任务已结束但没有记录结果",
  task_not_terminal: "任务尚未结束，还没有结果",
  invalid_request: "请求格式不正确",
  invalid_limits: "limits 不合法或超出服务容量",
  invalid_cursor: "事件游标无效",
  store_unavailable: "存储暂不可用",
  contention: "存储繁忙",
  commit_unknown: "提交结果未知",
  internal: "服务内部错误",
};

const HINTS: Record<string, string> = {
  diagnostic_mode: "服务以诊断模式运行（例如启动恢复检查未通过）。读取仍可用；请在服务器上检查 agentbox status。",
  ownership_lost: "另一个实例可能接管了数据目录。本实例不再提交任何写入，请检查部署并重启服务。",
  budget_exhausted: "任务的 budget_micro 已用完，后续模型调用会被 Gateway 拒绝。",
  budget_insufficient_for_request: "单次调用的预估费用超过剩余预算；可提高 limits.budget_micro 后重新提交。",
  unauthorized: "请重新输入访问令牌。",
  commit_unknown: "再次提交会复用同一 request_id，服务端按幂等语义返回首次结果。",
  store_unavailable: "稍后再次提交会复用同一 request_id。",
  contention: "稍后再次提交会复用同一 request_id。",
};

export function describeError(e: unknown): ErrorView {
  if (e instanceof ApiError) {
    const byCode = TITLES[e.code];
    const title = byCode ?? (e.code.startsWith("budget") ? "预算限制" : `请求失败（HTTP ${e.status}）`);
    const msg = e.message && e.message !== e.code ? e.message : "";
    const detail = [msg, HINTS[e.code] ?? ""].filter(Boolean).join(" ");
    return { code: e.code, status: e.status, title, detail };
  }
  if (e instanceof NetworkError) {
    return { code: "network", title: "无法连接 agentbox 服务", detail: e.message };
  }
  if (e instanceof ProtocolError) {
    return { code: "protocol", title: "事件流协议错误", detail: e.message };
  }
  return { code: "client", title: "操作失败", detail: e instanceof Error ? e.message : String(e) };
}
