package app

// serverInfo 给出 GET /server-info（运维专用）的非机密配置：评测（agentbox eval）的 run manifest 据此固定被评测的
// 服务端。只含模型名、供应商名、命令与摘要；不含 Key、token、价格与上游地址。

func (s *server) serverInfo() map[string]any {
	c := s.cfg
	models := map[string]any{"default": c.Model.Name, "declared": declaredModels(c.Model)}
	if c.Accounts {
		models["user_orchestrator"] = c.UserOrchestratorModel
		models["user_worker"] = c.UserWorkerModel
	}
	if c.Model.BaseURL == "" {
		models = map[string]any{}
	}
	search := c.SearchProvider
	if search == "" {
		search = "ddg_lite"
	}
	info := map[string]any{
		"template":        c.Template,
		"worker_argv":     append([]string{}, c.WorkerArgv...),
		"models":          models,
		"search_provider": search,
		"accounts":        c.Accounts,
		"sessions":        c.sessionsEnabled(),
		"worker_subruns":  c.WorkerSubruns,
		"exec":            map[string]any{"enabled": c.Exec.Enabled(), "slots": c.Exec.Slots, "image_digest": s.execDigest},
	}
	if c.sessionsEnabled() {
		info["session_worker_argv"] = append([]string{}, c.SessionWorkerArgv...)
	}
	return info
}

func declaredModels(m ModelConfig) []string {
	out := []string{}
	if len(m.Models) == 0 {
		if m.Name != "" {
			out = append(out, m.Name)
		}
		return out
	}
	return append(out, m.Models...)
}
