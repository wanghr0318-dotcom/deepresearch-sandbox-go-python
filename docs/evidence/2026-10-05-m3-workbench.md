# M3 工作台（Plan 10）——验证记录（2026-10-05）

本文件记录 Vue 工作台与浏览器安全的验收证据：E26、E27 来自自动化测试（下文"已执行"），浏览器联调由协调方在演示服务器上执行后补入"在线浏览器联调"一节。

## 环境

- Go 测试：WSL2 6.6.87.2-microsoft-standard-WSL2 x86_64，非 root；PostgreSQL 16（127.0.0.1:5432）与 Redis（127.0.0.1:6379），`CI=true`（数据库与缓存用例不得跳过）。
- 前端：Windows 11，Node v24.11.1，npm 11.6.2。
- 分支 `m3-p10-t5`（基于 m3-batch，含 Plan 10 Task 1–4 与 Plan 9 Task 1–4）。

## 已执行：构建与检查

```bash
cd web
npm ci                 # 0 vulnerabilities
npm run lint           # 无输出（通过）
npm run typecheck      # 通过
npm test               # 7 个文件，87 个用例全部通过（连续 3 次）
npm run gen:api && git diff --exit-code src/api/schema.d.ts   # 提交后无差异
npm run build          # web/dist：index.html 0.40 kB、CSS 13.75 kB、JS 182.24 kB（gzip 65.96 kB）
```

Go：`gofmt`（去 CR 后）、`go vet ./...`、`GOOS=windows go build ./...` 通过；`go test -count=1 ./internal/api/ ./internal/app/ ./cmd/... ./internal/gateway/call/` 连续 3 次通过；完整非 root 套件 `go test -count=1 ./...` 一次通过（退出码 0）。

## E26：外部 Origin/Host 请求；token 泄漏扫描 → 拒绝；URL、日志、构建产物中无 token

| 检查 | 证据 | 结果 |
|---|---|---|
| 外部 `Origin` 的 POST 返回 403 `forbidden_origin` 且不触达存储；`Origin: null` 拒绝；错误 `Host` 返回 403 `forbidden_host`；同源 POST 与无 `Origin`（CLI）POST 成功并带安全头；TLS 下默认 Origin 为 https，http Origin 拒绝 | `internal/api` `TestBrowserOriginAndHost` | PASS |
| 非 loopback 且无 TLS 时警告（JSON 日志与 stderr 纯文本各一条） | `TestNonLoopbackWithoutTLSWarns`、`cmd/agentbox` `TestPlaintextListenWarning` | PASS |
| `--web-dir` 静态文件、SPA 回退、带扩展名的缺失路径 404；CSP、`nosniff`、`Referrer-Policy` 精确值；API 路径不被静态处理器接管；访问日志中无 token、`Bearer`、`Authorization`（含 token 放在查询串时） | `TestWebDirStaticAndSPAFallback` | PASS |
| 真实 PostgreSQL 上的 HTTPS 监听 + 工作台：同源 https Origin 可用、http Origin 拒绝、SPA 回退经 HTTPS 工作 | `internal/app` `TestTLSListenerAndWebDir` | PASS |
| 前端：token 从不进入任何 URL，只在 `Authorization: Bearer` 头；默认只存内存，可选 `sessionStorage`，从不写 `localStorage` | `web/src/api/client.test.ts`、`sse.test.ts`、`auth.test.ts` | PASS |
| 构建产物扫描：`web/dist/assets/*.js` 中 `localStorage` 出现 0 次；`Bearer` 只出现在请求头模板 `Bearer ${e}` 与界面说明文字中；无 `VITE_*`、无 sourcemap、无测试 token 或 vitest 代码 | `grep` 于 `npm run build` 之后 | 通过 |

## E27：报告含恶意 Markdown/HTML；HTML/SVG 产物 → 清理；仅下载

| 检查 | 证据 | 结果 |
|---|---|---|
| `marked` → DOMPurify：去除 `script`、`iframe`、`object`、`embed`、`style`、`svg`、表单元素；去除所有事件属性与 `style` 属性；去除 `javascript:`（含大小写、实体、前导空格变体）与 `data:` 链接；链接加 `rel="noopener noreferrer"`；插入实际文档后触发事件不执行任何代码 | `web/src/lib/markdown.test.ts`（`renderMarkdown (E27)`） | PASS |
| HTML 与 SVG 产物只提供下载按钮；未声明类型但响应为主动内容或 `Content-Disposition: attachment` 时降级为只下载；挂载后的报告预览已清理；下载经 ApiClient 取 Blob，页面中没有指向 API 路径的链接 | `web/src/components/components.test.ts`（`ArtifactsPanel (E27)`） | PASS |
| 服务端：`text/html`、`image/svg+xml` 及无法解析的类型以 `Content-Disposition: attachment` 下载并带 `nosniff`；只有 `visibility = output` 的产物可下载 | `internal/api` `TestArtifactDownload` | PASS |

## 本任务的两项修正

- 调用表的模型：`calls.model`（迁移 0004）在 BeginCall 时写入解析后的模型；`GET /tasks/{id}/inspect` 的 `Call.model` 对 chat 调用给出模型，其他端点省略；工作台调用表显示它。测试：`internal/gateway/call` `TestCostUsesPerModelPricing`、`internal/persistence/postgres` `TestGatewayTaskFactsAndInspect`、`internal/api` `TestInspect`、`web` `CallsPanel`。
- `503 diagnostic_mode` / `ownership_lost` 不再重试：ApiClient 立即抛出带服务端消息的错误，SSE 立即停止并报告；其他暂时性 5xx 仍按有界退避重试。测试：`web/src/api/client.test.ts`、`sse.test.ts`。

## 在线浏览器联调

pending: on the demo server（由协调方在上海 CVM 演示服务器上执行：`--web-dir web/dist` 启动 server，经 `ssh -L 8080:127.0.0.1:8080 ubuntu@<server>` 访问；输入 token → 提交任务 → 观察事件时间线 → 控制按钮 → 下载报告与产物 → 检查调用表的模型列；记录页面快照或截图描述）。
