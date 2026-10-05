// 工作台共享的 API 单例：同源访问（baseUrl 为空），token 由 TokenStore 在运行时提供。
import { TokenStore } from "./auth";
import { ApiClient } from "./client";

export const tokenStore = new TokenStore();
export const api = new ApiClient({ getToken: () => tokenStore.get() });
