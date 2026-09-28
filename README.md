# go-agentbox

给 Agent 用的工具沙箱运行时 —— 隔离执行、资源限额、自动回收、产物持久化；外挂一层可按场景装配的 ReAct 执行框架。

> 状态：设计阶段。代码尚未开始，设计稿见 [`docs/design/`](docs/design/)。

---

## 这是什么

Agent 要真正干活，就得跑 shell、改文件、装依赖。这些操作必须在隔离环境里执行，否则一个失控的 `rm -rf` 就能毁掉宿主机。

本项目做两件事：

1. **沙箱运行时**（主体）—— 会话级沙箱的创建、复用、冻结、回收、快照，以及底层的 Linux 隔离实现
2. **框架外壳** —— 一个薄的 ReAct 执行循环，让沙箱有真实的使用场景，并支撑"同一个二进制、多个场景、不同安全姿态"

## 架构

```
入口层（按 scenario 装配）
   └─ Template ──► ReAct Agent ──► 流式输出
                       └─ ToolNode ──► 中间件链 ──► Sandbox Manager
                                                        │
                                                 Provider 接口
                                    ┌───────────────────┴──────────────┐
                              LocalProvider                    TencentProvider
                        namespace + cgroup + overlayfs           AGS 控制面
                            （零外部依赖可跑）
```

## 设计要点

**管控面**
- 会话级命名复用，沙箱名同时作为分布式锁键与绑定标识
- Provider 是真相源，binding 是缓存，执行前必探活
- 三层 TTL（锁 / 业务 / provider），provider TTL 严格大于业务 TTL
- 回收任务幂等优先于选主；延迟队列削峰
- 会话信号量用带自动过期的 ZSET，持有者崩溃自愈

**LocalProvider**
- overlayfs 分层，建箱 = mkdir + 一次 mount，不拷贝 rootfs
- re-exec `/proc/self/exe` 规避 Go runtime 与线程级 syscall 的冲突
- 沙箱内自写 mini-envd，使两个 provider 的 Exec 语义一致
- cgroup v2 freezer 实现 hibernate
- Close 八步有序清理，先冻结、必轮询

**框架外壳**
- 沙箱在中间件中 ensure，工具只声明 `NeedsSandbox`
- 准入控制为强制层，场景无权关闭
- 中间件同名覆盖、异名追加 —— 配置自由，不给编排自由

## 能力边界

明确声明，不假装覆盖：

| 项 | 本项目 | 生产级 |
|---|---|---|
| 隔离强度 | 容器级，共享宿主内核 | microVM，独立内核 |
| 适用租户模型 | 单租户可信场景 | 多租户不可信 |
| Hibernate | 冻结进程，不释放内存 | 内存落盘并释放 |
| Snapshot | 文件系统层 | 含内存与进程状态 |
| 浏览器沙箱 | 第一版不支持 | 支持 |

## 参考与致谢

设计过程中对标了以下项目。**参考其设计，代码自行实现**：

- [CloudWeGo Eino](https://github.com/cloudwego/eino) —— Go LLM/Agent 框架，编排层抽象参考
- [Tencent Cloud Cube Sandbox](https://github.com/tencentcloud/CubeSandbox) —— Agent 沙箱，Provider 能力面参考
- [runc](https://github.com/opencontainers/runc) —— 容器运行时，re-exec init 模式参考
- E2B —— 沙箱内 daemon + Connect 风格 API 的接口语义参考

## 开发环境

需要 Linux（namespace / cgroup v2）。Windows 下用 WSL2。

```bash
stat -fc %T /sys/fs/cgroup    # 应输出 cgroup2fs
```

## License

待定。
