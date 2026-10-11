// 模型网关的用户侧 API（cookie 会话模式，与 session.ts 相同：不发送 Authorization 头，会话 cookie 由浏览器携带）：
// - /v1/me、/v1/keys（API 密钥）、/v1/budgets（月度预算）、/v1/usage 与 /v1/bills（用量与账单）、
//   /v1/orgs/members 与 /v1/orgs/invites（成员与邀请）。
// - 写请求（创建密钥、吊销、邀请、接受、拒绝、离开）只尝试一次：重试可能重复创建密钥或邀请；读请求沿用有界重试。
// - 完整密钥只在 createKey 的响应中出现一次，本模块不保存它。

import { ApiClient, pathSeg } from "./client";
import type { ClientOptions, Schemas } from "./client";

export type Me = Schemas["Me"];
export type ApiKey = Schemas["ApiKey"];
export type CreatedKey = Schemas["CreatedKey"];
export type CreateKeyRequest = Schemas["CreateKeyRequest"];
export type Budget = Schemas["Budget"];
export type Usage = Schemas["Usage"];
export type UsageRow = Schemas["UsageRow"];
export type UsageTotals = Schemas["UsageTotals"];
export type Bill = Schemas["Bill"];
export type BillLine = Schemas["BillLine"];
export type Membership = Schemas["Membership"];
export type MemberList = Schemas["MemberList"];
export type Invite = Schemas["Invite"];
export type Scope = ApiKey["scopes"][number];
export type Role = Membership["role"];
export type BudgetKind = Budget["kind"];

export interface UsageParams {
  from?: string;
  to?: string;
  group_by?: string;
  key?: string;
  user?: string;
}

export type GatewayOptions = Pick<ClientOptions, "baseUrl" | "fetch" | "maxAttempts" | "backoffBaseMs" | "backoffCapMs">;

export class GatewayApi {
  private readonly once: ApiClient;
  private readonly client: ApiClient;

  constructor(opts: GatewayOptions = {}) {
    // 刻意不传 getToken：这些页面只用会话 cookie。
    this.once = new ApiClient({ ...opts, maxAttempts: 1 });
    this.client = new ApiClient(opts);
  }

  private async json<T>(c: ApiClient, method: string, path: string, body?: unknown): Promise<T> {
    const resp = await c.send(method, path, body);
    return (await resp.json()) as T;
  }

  me(): Promise<Me> {
    return this.json(this.client, "GET", "/v1/me");
  }

  async listKeys(): Promise<ApiKey[]> {
    return (await this.json<Schemas["KeyList"]>(this.client, "GET", "/v1/keys")).keys;
  }

  createKey(req: CreateKeyRequest): Promise<CreatedKey> {
    return this.json(this.once, "POST", "/v1/keys", req);
  }

  revokeKey(id: string): Promise<ApiKey> {
    return this.json(this.once, "DELETE", `/v1/keys/${pathSeg(id)}`);
  }

  budget(kind: BudgetKind, id: string): Promise<Budget> {
    return this.json(this.client, "GET", `/v1/budgets/${pathSeg(kind)}/${pathSeg(id)}`);
  }

  usage(params: UsageParams = {}): Promise<Usage> {
    const q = new URLSearchParams();
    for (const [k, v] of Object.entries(params)) if (v) q.set(k, v);
    const qs = q.toString();
    return this.json(this.client, "GET", `/v1/usage${qs ? `?${qs}` : ""}`);
  }

  bill(month: string): Promise<Bill> {
    return this.json(this.client, "GET", `/v1/bills/${pathSeg(month)}`);
  }

  listMembers(): Promise<MemberList> {
    return this.json(this.client, "GET", "/v1/orgs/members");
  }

  /** 移出成员（owner），或传自己的用户名离开 org。 */
  async removeMember(username: string): Promise<void> {
    await this.once.send("DELETE", `/v1/orgs/members/${pathSeg(username)}`);
  }

  async listInvites(): Promise<Invite[]> {
    return (await this.json<Schemas["InviteList"]>(this.client, "GET", "/v1/orgs/invites")).invites;
  }

  createInvite(username: string, role: Role): Promise<Invite> {
    return this.json(this.once, "POST", "/v1/orgs/invites", { username: username.trim(), role });
  }

  acceptInvite(id: string): Promise<Membership> {
    return this.json(this.once, "POST", `/v1/orgs/invites/${pathSeg(id)}/accept`);
  }

  /** 拒绝收到的邀请（被邀请者），或撤销发出的邀请（owner）。 */
  async deleteInvite(id: string): Promise<void> {
    await this.once.send("DELETE", `/v1/orgs/invites/${pathSeg(id)}`);
  }
}

export type GatewayApiLike = Pick<
  GatewayApi,
  | "me"
  | "listKeys"
  | "createKey"
  | "revokeKey"
  | "budget"
  | "usage"
  | "bill"
  | "listMembers"
  | "removeMember"
  | "listInvites"
  | "createInvite"
  | "acceptInvite"
  | "deleteInvite"
>;

/** 用户页共享的单例（同源）。 */
export const gatewayApi = new GatewayApi();
