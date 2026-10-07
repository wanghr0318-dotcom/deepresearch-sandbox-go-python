# 用户账号体系：面向用户的 DeepResearch 助手

> 日期：2026-10-06。状态：设计已批准（项目负责人，2026-10-06）。顺序：M3 门槛 → 本设计的实施（M3 Plan 11）→ 仓库整合（[整合设计](https://github.com/wanghr0318-dotcom/go-agentbox/blob/m4-gate/docs/superpowers/specs/2026-10-06-repo-consolidation-design.md)）。

## 目标

对用户而言，go-agentbox 是一个 **DeepResearch 助手**：注册、登录、输入研究主题、观看进度、阅读与下载报告。模型与搜索的用量由项目负责人的 Key 承担；用户看不到 API Key、token、预算或调用内部细节。现有工作台保留为**运维后台**。

不做：按用户的用量配额（项目负责人明确不做）、密码找回、邮件/短信验证、第三方登录。

## 方案：服务端会话

用户与会话存于 PostgreSQL，浏览器持有 `HttpOnly; Secure; SameSite=Strict` 的会话 cookie。登出与停用立即生效。未采用：JWT（无法提前吊销）、独立认证服务（过度设计）。

## 数据（migration 0005）

- `users`：`id`、`username`（唯一，大小写不敏感，3–32 位 `[A-Za-z0-9_.-]`）、`password_hash`、`role`（`user` | `admin`）、`disabled`、`created_at`。
- `sessions`：`id_hash`（会话 ID 的 SHA-256；明文 ID 只在 cookie 中）、`user_id`、`created_at`、`expires_at`（7 天）、`last_seen_at`。
- `tasks.owner_user_id`（可空）：经运维 token 或 CLI 创建的任务为空。
- 密码：PBKDF2-SHA256（Go 标准库 `crypto/pbkdf2`），随机盐，迭代次数按 OWASP 当前建议（600,000），哈希串自带算法与参数以便将来升级；比较用常量时间。不引入新的 Go 模块。

## API

- `POST /auth/register`（用户名 + 密码，成功即登录）、`POST /auth/login`、`POST /auth/logout`、`GET /auth/me`。
- **用户侧**（会话 cookie）：
  - 提交只含**主题**：server 用固定配置补全模型（编排 kimi-k3、worker kimi-k2.6）、搜索供应商与预算；用户不能指定模型、预算或 limits。
  - 只能访问自己的任务：列表、事件流、结果、产物下载、取消。他人任务一律 `404`（不泄露存在性）。
  - 不可访问：inspect、调用明细、费用、预算与其他内部端点（`403`）。
- **运维侧**：现有 `api.token`（Bearer）即管理员凭据，权限不变（全部任务与内部细节）。CLI：`agentbox user list`、`agentbox user disable <name>`、`agentbox user enable <name>`；停用即吊销其全部会话。
- OpenAPI 同步更新；cookie 鉴权与 Bearer 鉴权并存，Bearer 优先。

## 防护（不做用量配额）

- 注册与登录按 IP 限速（内存令牌桶；例如每 IP 每分钟 5 次注册、10 次登录）。
- 登录失败统一返回同一错误，不区分"用户不存在"与"密码错误"；用户不存在时也做一次哈希计算，避免时序差异。
- 密码最少 8 位；用户名规则见上。
- 改变状态的请求校验 `Origin`（沿用 M3 的允许列表）；cookie `SameSite=Strict`。
- **每个用户同一时间最多 1 个运行中的任务**（公平使用，不是配额；共有 3 个 run slot）。超出返回 `409 user_task_running`。
- 会话 cookie 与密码不出现在日志、事件或错误中。

## Web

- **用户侧（`#/`）**：注册/登录页 → 助手页："新研究"输入框、"我的研究"列表、实时进度（阶段与证据数，不显示调用费用）、完成后安全渲染报告并提供下载。
- **运维侧（`#/admin`）**：现有工作台，使用运维 token。
- 用户侧的错误提示面向用户（例如"已有研究在进行中，请稍候"），不暴露内部错误码细节。

## 测试与验收

- Go：注册/登录/登出、会话过期与吊销、停用用户、PBKDF2 参数与常量时间比较、限速、跨用户访问一律 404、用户访问内部端点 403、每用户 1 个运行中任务、Bearer 与 cookie 并存。
- Vue：认证页与助手流程、错误提示、cookie 模式下不在任何 URL/存储中出现会话 ID。
- 服务器真实验收：在 `https://49.235.41.14` 注册两个用户，各自完成一次研究；互相看不到对方的任务；运维后台能看到全部任务与费用。

## 不在本设计内

- 用量配额、计费、密码找回、邮箱/短信验证、OAuth、多租户隔离（沙箱隔离不变：每个任务仍在独立沙箱中运行）。
