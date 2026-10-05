// 极简 hash 路由：页面路由全部放在 # 之后，永远不会与 API 路径（/tasks、/status、/auth、/research…）冲突。
// - 用户页：#/login、#/register、#/（DeepResearch 助手）、#/research/<id>（进度与报告）
// - 运维工作台：#/admin、#/admin/tasks、#/admin/tasks/<id>（TokenGate 不变）
import { onBeforeUnmount, ref } from "vue";
import type { Ref } from "vue";

export type AdminRoute = { name: "tasks" } | { name: "task"; id: string };

export type Route = { name: "login" } | { name: "register" } | { name: "home" } | { name: "research"; id: string } | AdminRoute;

/** 运维工作台的路由（#/admin/...）。 */
export function isAdminRoute(r: Route): r is AdminRoute {
  return r.name === "tasks" || r.name === "task";
}

/** 需要登录的用户页。 */
export function needsSession(r: Route): boolean {
  return r.name === "home" || r.name === "research";
}

function decodeId(raw: string): string | null {
  try {
    return decodeURIComponent(raw);
  } catch {
    return null;
  }
}

export function parseHash(hash: string): Route {
  const path = hash.replace(/^#/, "");
  if (/^\/login\/?$/.test(path)) return { name: "login" };
  if (/^\/register\/?$/.test(path)) return { name: "register" };

  const research = /^\/research\/([^/?#]+)\/?$/.exec(path);
  if (research?.[1]) {
    const id = decodeId(research[1]);
    return id ? { name: "research", id } : { name: "home" };
  }

  // 工作台：#/admin/tasks/<id>；旧链接 #/tasks/<id> 同样指向工作台。
  const task = /^(?:\/admin)?\/tasks\/([^/?#]+)\/?$/.exec(path);
  if (task?.[1]) {
    const id = decodeId(task[1]);
    return id ? { name: "task", id } : { name: "tasks" };
  }
  if (/^\/admin(?:\/.*)?$/.test(path) || /^\/tasks(?:\/.*)?$/.test(path)) return { name: "tasks" };
  return { name: "home" };
}

export function loginHref(): string {
  return "#/login";
}

export function registerHref(): string {
  return "#/register";
}

export function homeHref(): string {
  return "#/";
}

export function researchHref(id: string): string {
  return `#/research/${encodeURIComponent(id)}`;
}

export function tasksHref(): string {
  return "#/admin/tasks";
}

export function taskHref(id: string): string {
  return `#/admin/tasks/${encodeURIComponent(id)}`;
}

export function navigate(href: string): void {
  window.location.hash = href.replace(/^#/, "");
}

/** 当前路由（随 hashchange 更新）。 */
export function useHashRoute(): Ref<Route> {
  const route = ref<Route>(parseHash(window.location.hash));
  const onChange = () => {
    route.value = parseHash(window.location.hash);
  };
  window.addEventListener("hashchange", onChange);
  onBeforeUnmount(() => window.removeEventListener("hashchange", onChange));
  return route;
}
