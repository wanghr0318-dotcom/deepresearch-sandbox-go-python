// 用户页依赖的服务（会话 API、事件流、保存文件），通过 provide/inject 注入，测试中替换为 mock。
// 与工作台的 services.ts 分开：用户页从不接触运维 token。
import { inject } from "vue";
import type { InjectionKey } from "vue";
import { sessionApi } from "../api/session";
import type { SessionApiLike } from "../api/session";
import { saveBlob } from "./services";
import type { WatchFn } from "./services";

export interface UserServices {
  api: SessionApiLike;
  watch: WatchFn;
  saveBlob: (blob: Blob, filename: string) => void;
}

let defaults: UserServices | null = null;

export function defaultUserServices(): UserServices {
  defaults ??= {
    api: sessionApi,
    watch: (opts) => sessionApi.watch(opts),
    saveBlob,
  };
  return defaults;
}

export const userServicesKey: InjectionKey<UserServices> = Symbol("agentbox.userServices");

export function useUserServices(): UserServices {
  return inject(userServicesKey, defaultUserServices, true);
}
