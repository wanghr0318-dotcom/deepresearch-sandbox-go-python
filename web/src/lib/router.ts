// 极简 hash 路由：页面路由放在 # 之后（#/tasks、#/tasks/<id>），
// 永远不会与 API 路径（/tasks、/status、/sessions、/events）冲突。
import { onBeforeUnmount, ref } from "vue";
import type { Ref } from "vue";

export type Route = { name: "tasks" } | { name: "task"; id: string };

export function parseHash(hash: string): Route {
  const path = hash.replace(/^#/, "");
  const m = /^\/tasks\/([^/?#]+)\/?$/.exec(path);
  if (m?.[1]) {
    try {
      return { name: "task", id: decodeURIComponent(m[1]) };
    } catch {
      return { name: "tasks" };
    }
  }
  return { name: "tasks" };
}

export function tasksHref(): string {
  return "#/tasks";
}

export function taskHref(id: string): string {
  return `#/tasks/${encodeURIComponent(id)}`;
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
