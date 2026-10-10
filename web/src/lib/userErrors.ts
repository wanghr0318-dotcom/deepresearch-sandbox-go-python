// 用户可见的错误文案（唯一的文案表；不暴露错误码、预算或内部细节）。
// - USER_ERRORS：账号、研究列表与详情等页面的通用文案。
// - CHAT_ERRORS：对话页的文案，同名错误码在对话页优先（例如 user_task_running 在对话页提示"先停止它"）；
//   也用于把轮次失败原因（turn_status 的 reason）翻译为中文。
// 运维工作台的错误说明另见 errors.ts（describeError，面向运维，带错误码与提示）。

import { ApiError, NetworkError } from "../api/client";

export const USER_ERRORS: Record<string, string> = {
  user_task_running: "已有研究在进行中，请等它完成后再开始新的研究",
  rate_limited: "尝试过于频繁，请稍后再试",
  invalid_credentials: "用户名或密码错误",
  username_taken: "用户名已被使用",
  invalid_username: "用户名需为 3–32 位字母、数字、下划线、点或连字符",
  invalid_password: "密码须为 8–16 位，且至少包含数字、大写字母、小写字母中的两种",
  invalid_topic: "主题不能为空，且不超过 500 个字符",
  task_not_found: "找不到这项研究",
  unauthorized: "登录已过期，请重新登录",
  task_ended: "这项研究已经结束了",
  cancel_pending: "已在取消中",
  diagnostic_mode: "服务正在维护，请稍后再试",
  ownership_lost: "服务正在维护，请稍后再试",
};

export const CHAT_ERRORS: Record<string, string> = {
  model_unavailable: "模型服务暂时不可用，请重试",
  model_degraded: "模型服务暂时不可用（所有供应商均不可用），请稍后再试",
  session_unavailable: "会话暂时无法恢复",
  sessions_unavailable: "对话服务暂未开放，请稍后再试",
  session_not_found: "找不到这个对话，可能已被删除",
  session_closed: "这个对话正在删除",
  turn_not_found: "找不到这一轮对话",
  turn_in_progress: "当前研究还在进行，请先停止当前研究",
  user_task_running: "你有另一项研究正在进行，请先停止它",
  invalid_turn_state: "这一轮的状态已经变化，请刷新后再试",
  not_restorable: "这项研究无法恢复",
  invalid_text: "消息需为 1–4000 个字符",
  invalid_title: "标题需为 1–80 个字符",
  invalid_answers: "请回答每一个问题",
  request_conflict: "请求冲突，请刷新后再试",
};

/** 任意错误 → 面向用户的文案（通用页面）。 */
export function userErrorMessage(e: unknown): string {
  if (e instanceof ApiError) {
    const msg = USER_ERRORS[e.code];
    if (msg) return msg;
    if (e.status === 401) return USER_ERRORS.unauthorized!;
    if (e.status >= 500) return "服务暂时不可用，请稍后再试";
    return "请求未能完成，请稍后再试";
  }
  if (e instanceof NetworkError) return "无法连接服务，请检查网络后重试";
  return "出了点问题，请稍后再试";
}

/** 任意错误 → 面向用户的文案（对话页：先查 CHAT_ERRORS，再按通用规则）。 */
export function chatErrorMessage(e: unknown): string {
  if (e instanceof ApiError) {
    const msg = CHAT_ERRORS[e.code];
    if (msg) return msg;
  }
  return userErrorMessage(e);
}
