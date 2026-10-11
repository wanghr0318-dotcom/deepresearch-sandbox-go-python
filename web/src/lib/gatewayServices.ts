// 模型网关页面（API 密钥、用量、成员）依赖的服务，通过 provide/inject 注入，测试中替换为 mock。
// 与 userServices 分开，原有页面与测试不受影响。
import { inject } from "vue";
import type { InjectionKey } from "vue";
import { gatewayApi } from "../api/gateway";
import type { GatewayApiLike } from "../api/gateway";

export interface GatewayServices {
  api: GatewayApiLike;
  /** 复制文本到剪贴板（测试中替换）。 */
  copy: (text: string) => Promise<void>;
  /** 当前时间（测试中固定）。 */
  now: () => Date;
}

let defaults: GatewayServices | null = null;

export function defaultGatewayServices(): GatewayServices {
  defaults ??= {
    api: gatewayApi,
    copy: (text) => navigator.clipboard.writeText(text),
    now: () => new Date(),
  };
  return defaults;
}

export const gatewayServicesKey: InjectionKey<GatewayServices> = Symbol("agentbox.gatewayServices");

export function useGatewayServices(): GatewayServices {
  return inject(gatewayServicesKey, defaultGatewayServices, true);
}
