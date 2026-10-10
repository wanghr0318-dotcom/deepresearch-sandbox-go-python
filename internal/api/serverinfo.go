package api

// GET /server-info（运维专用；S2 评测平台的 run manifest 据此固定被评测的服务端）：构建信息（模块版本、VCS 修订、
// Go 版本）与 Config.ServerInfo 给出的非机密配置（默认模型与白名单、搜索供应商、Worker 命令、exec 模板摘要等）。
// 不含任何 Key、token、价格或带凭据的地址。

import (
	"net/http"
	"runtime/debug"
)

// BuildInfo 返回本二进制的构建信息（模块版本、vcs.revision、vcs.time、vcs.modified、Go 版本）；不可用时为 nil。
func BuildInfo() map[string]string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return nil
	}
	out := map[string]string{"go": bi.GoVersion, "module_version": bi.Main.Version}
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision", "vcs.time", "vcs.modified":
			out[s.Key] = s.Value
		}
	}
	return out
}

func (h *Handler) serverInfo(w http.ResponseWriter, _ *http.Request) {
	out := map[string]any{}
	if h.cfg.ServerInfo != nil {
		for k, v := range h.cfg.ServerInfo() {
			out[k] = v
		}
	}
	out["build"] = BuildInfo()
	out["config_version"] = h.cfg.ConfigVersion
	writeJSON(w, http.StatusOK, out)
}
