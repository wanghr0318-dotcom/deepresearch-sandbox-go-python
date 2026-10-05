// 令牌状态的响应式视图。令牌本身仍只由 TokenStore 保管（默认仅内存，可选 sessionStorage），
// 这里只暴露"是否已设置"等展示信息，令牌值不进入任何响应式状态。
import { reactive } from "vue";
import { tokenStore } from "../api";
import type { TokenPersistence } from "../api/auth";

export const authState = reactive({
  hasToken: tokenStore.get() !== "",
  persistence: tokenStore.persistence() as TokenPersistence,
  /** 用户选择不使用令牌继续（本机 loopback 访问时服务可不要求令牌）。 */
  skipped: false,
});

export function setToken(token: string, persistence: TokenPersistence): void {
  tokenStore.set(token, persistence);
  authState.hasToken = tokenStore.get() !== "";
  authState.persistence = tokenStore.persistence();
  authState.skipped = !authState.hasToken;
}

export function clearToken(): void {
  tokenStore.clear();
  authState.hasToken = false;
  authState.persistence = "memory";
  authState.skipped = false;
}

export function skipToken(): void {
  authState.skipped = true;
}
