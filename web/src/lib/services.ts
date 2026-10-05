// 视图依赖的服务（API 客户端、事件流、保存文件），通过 provide/inject 注入，测试中替换为 mock。
import { inject } from "vue";
import type { InjectionKey } from "vue";
import { api, tokenStore } from "../api";
import type { ApiClient } from "../api/client";
import { watchTaskEvents } from "../api/sse";
import type { EventStream, WatchOptions } from "../api/sse";

export type ApiLike = Pick<
  ApiClient,
  | "getStatus"
  | "listTasks"
  | "getTask"
  | "createTask"
  | "cancelTask"
  | "pauseTask"
  | "resumeTask"
  | "getResult"
  | "inspectTask"
  | "downloadArtifact"
>;

export type WatchFn = (opts: Pick<WatchOptions, "taskId" | "cursor" | "onEvent" | "onState">) => EventStream;

export interface Services {
  api: ApiLike;
  watch: WatchFn;
  /** 把 Blob 存为本地文件（object URL + <a download>；token 只在请求头里，从不进入 URL）。 */
  saveBlob: (blob: Blob, filename: string) => void;
}

export function saveBlob(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.rel = "noopener";
  a.style.display = "none";
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 30_000);
}

export function defaultServices(): Services {
  return {
    api,
    watch: (opts) => watchTaskEvents({ ...opts, getToken: () => tokenStore.get() }),
    saveBlob,
  };
}

export const servicesKey: InjectionKey<Services> = Symbol("agentbox.services");

export function useServices(): Services {
  return inject(servicesKey, defaultServices, true);
}
