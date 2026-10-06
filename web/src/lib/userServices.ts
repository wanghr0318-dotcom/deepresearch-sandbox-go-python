// 用户页依赖的服务（账号与早期研究 API、对话会话 API、事件流、保存文件），通过 provide/inject 注入，测试中替换为 mock。
// 与工作台的 services.ts 分开：用户页从不接触运维 token。
import { inject } from "vue";
import type { InjectionKey } from "vue";
import { chatApi } from "../api/chat";
import type { ChatApi, ChatApiLike } from "../api/chat";
import { sessionApi } from "../api/session";
import type { SessionApiLike } from "../api/session";
import { saveBlob } from "./services";
import type { WatchFn } from "./services";

export interface UserServices {
  api: SessionApiLike;
  chat: ChatApiLike;
  watch: WatchFn;
  watchSession: ChatApi["watchSession"];
  saveBlob: (blob: Blob, filename: string) => void;
}

let defaults: UserServices | null = null;

export function defaultUserServices(): UserServices {
  defaults ??= {
    api: sessionApi,
    chat: chatApi,
    watch: (opts) => sessionApi.watch(opts),
    watchSession: (opts) => chatApi.watchSession(opts),
    saveBlob,
  };
  return defaults;
}

export const userServicesKey: InjectionKey<UserServices> = Symbol("agentbox.userServices");

export function useUserServices(): UserServices {
  return inject(userServicesKey, defaultUserServices, true);
}
