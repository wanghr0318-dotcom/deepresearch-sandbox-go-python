package app

// 本文件装配工作区工具与 MCP（设计 docs/design/2026-10-10-shell-file-mcp-design.md）：--workspace-tools 打开
// /v1/workspace/*（工作区由 internal/gateway/workspace 管理，命令经 call.Coordinator.ExecShell 按 exec 记账），
// --mcp-config 打开 /v1/mcp/*（internal/gateway/mcp 的 Hub 与 kind mcp 的上游 adapter）。两者默认关闭，关闭时
// Gateway、turn spec 与 Worker 的行为与之前完全相同。

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/blob"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/call"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/mcp"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/gateway/workspace"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/task"
)

// 工作区的默认值。
const (
	DefaultWorkspaceIdleTimeout = workspace.DefaultIdleTimeout
	workspaceSweepInterval      = 30 * time.Second
)

// workspacesDir 是工作区状态文件的目录 <data>/tool-workspaces（0700，只有 server 访问；文件内容在 BlobStore 中）。
// 不用 <data>/workspaces：那里是任务环境 /workspace 挂载的宿主目录。
func workspacesDir(dataDir string) string { return filepath.Join(dataDir, "tool-workspaces") }

// validateTools 校验工作区与 MCP 配置（withDefaults 之后）。
func (c Config) validateTools() error {
	if c.WorkspaceTools && !c.Exec.Enabled() {
		return errors.New("app: --workspace-tools 需要 exec（--exec-slots > 0）：工作区命令在 exec 环境中运行")
	}
	if c.WorkspaceIdleTimeout < 0 {
		return fmt.Errorf("app: --workspace-idle-timeout 须 > 0，得到 %s", c.WorkspaceIdleTimeout)
	}
	if c.MCP != nil {
		if err := c.MCP.Validate(); err != nil {
			return fmt.Errorf("app: --mcp-config: %w", err)
		}
	}
	return nil
}

// turnTools 是 turn spec 的 tools 成员：两者都关闭时为 nil（spec 不含 tools，与之前的字节相同）。
func (c Config) turnTools() *turnToolsSpec {
	if !c.WorkspaceTools && c.MCP == nil {
		return nil
	}
	return &turnToolsSpec{Workspace: c.WorkspaceTools, MCP: c.MCP != nil}
}

type turnToolsSpec struct {
	Workspace bool `json:"workspace"`
	MCP       bool `json:"mcp"`
}

// mcpAdapter 在配置了 MCP 时建立 Hub（stdio 服务器在第一次使用时启动）并返回 kind mcp 的 adapter。
func (s *server) mcpAdapter() (*mcp.Adapter, error) {
	if s.cfg.MCP == nil {
		return nil, nil
	}
	hub, err := mcp.NewHub(*s.cfg.MCP, s.log)
	if err != nil {
		return nil, fmt.Errorf("app: MCP: %w", err)
	}
	s.mcpHub = hub
	names := make([]string, 0, len(s.cfg.MCP.Servers))
	for _, sv := range s.cfg.MCP.Servers {
		names = append(names, sv.Name+"("+sv.Transport+")")
	}
	s.log.Info("MCP 已启用", "servers", names)
	return mcp.NewAdapter(hub), nil
}

// assembleWorkspaces 在 --workspace-tools 时建立工作区管理器（需要 call 协调器已建立）。
func (s *server) assembleWorkspaces(blobs blob.Store) error {
	if !s.cfg.WorkspaceTools {
		return nil
	}
	m, err := workspace.New(workspace.Config{Dir: workspacesDir(s.d.DataDir), Exec: s.calls, Access: s.calls, Blobs: blobs,
		Tasks: taskTerminal{s: s.store}, IdleTimeout: s.cfg.WorkspaceIdleTimeout, MaxFiles: workspace.DefaultMaxFiles,
		MaxBytes: s.cfg.Exec.OutBytes, Logger: s.log})
	if err != nil {
		return fmt.Errorf("app: 工作区: %w", err)
	}
	s.workspaces = m
	s.log.Info("工作区工具已启用", "idle_timeout", s.cfg.WorkspaceIdleTimeout.String(), "max_bytes", s.cfg.Exec.OutBytes)
	return nil
}

// runWorkspaceSweep 在执行开始后运行工作区清扫（终态任务销毁、空闲过期；启动时先扫一次，覆盖上一进程留下的工作区）。
func (s *server) runWorkspaceSweep() {
	if s.workspaces == nil {
		return
	}
	s.bg.Add(1)
	go func() {
		defer s.bg.Done()
		every := workspaceSweepInterval
		if s.cfg.workspaceSweep > 0 {
			every = s.cfg.workspaceSweep
		}
		s.workspaces.Run(s.workCtx, every)
	}()
}

// closeTools 在 Gateway 关闭之后停止工作区等待与 MCP 服务器。
func (s *server) closeTools() {
	if s.workspaces != nil {
		s.workspaces.Close()
	}
	if s.mcpHub != nil {
		s.mcpHub.Close()
	}
}

// taskTerminal 实现 workspace.Tasks：任务已是终态或不存在时为真。
type taskTerminal struct{ s Store }

func (t taskTerminal) TaskTerminal(ctx context.Context, taskID string) (bool, error) {
	st, err := t.s.LoadTask(ctx, taskID)
	if errors.Is(err, persistence.ErrNotFound) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return task.IsTerminal(st.Status), nil
}

var _ workspace.Execer = (*call.Coordinator)(nil)
