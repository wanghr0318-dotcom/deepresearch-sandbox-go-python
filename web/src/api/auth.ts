// API token 的保管（规格 §15.3、Plan 10 全局约束）：
// - 默认只在内存里；用户可在设置中选择存入 sessionStorage（关闭标签页即失效）。
// - 永不写 localStorage，永不进入 URL，只经 client/sse 放进 Authorization 头。
// - 构建产物里没有 token：它只在运行时由用户输入。

export type TokenPersistence = "memory" | "session";

const SESSION_KEY = "agentbox.api_token";

function sessionStore(): Storage | null {
  try {
    return typeof sessionStorage === "undefined" ? null : sessionStorage;
  } catch {
    return null;
  }
}

export class TokenStore {
  private token = "";
  private mode: TokenPersistence = "memory";

  constructor() {
    // 页面刷新后，若用户此前选择了 session 持久化，则恢复。
    try {
      const saved = sessionStore()?.getItem(SESSION_KEY);
      if (saved) {
        this.token = saved;
        this.mode = "session";
      }
    } catch {
      // 存储不可用时只用内存。
    }
  }

  get(): string {
    return this.token;
  }

  persistence(): TokenPersistence {
    return this.mode;
  }

  set(token: string, mode: TokenPersistence = this.mode): void {
    this.token = token.trim();
    this.mode = mode;
    const store = sessionStore();
    try {
      if (mode === "session" && this.token) {
        store?.setItem(SESSION_KEY, this.token);
      } else {
        store?.removeItem(SESSION_KEY);
      }
    } catch {
      // 忽略：token 仍在内存中。
    }
  }

  clear(): void {
    this.set("", "memory");
  }
}

/** 为请求生成头部；token 只出现在 Authorization 里。 */
export function authHeaders(token: string): Record<string, string> {
  return token ? { Authorization: `Bearer ${token}` } : {};
}
