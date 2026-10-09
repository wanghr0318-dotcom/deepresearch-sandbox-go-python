package app

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestServerInfoHasNoSecrets：/server-info 的配置部分给出模型名、搜索供应商、Worker 命令与 exec 摘要，
// 不含 Key、上游地址或价格。
func TestServerInfoHasNoSecrets(t *testing.T) {
	s := &server{execDigest: "sha256:feed", cfg: Config{
		Template: "default", WorkerArgv: []string{"python3", "-m", "evalworker"},
		Model:          ModelConfig{BaseURL: "https://models.example/v1", Name: "m1", Models: []string{"m1", "m2"}, APIKey: "sk-secret"},
		SearchProvider: "serper", SearchAPIKey: "search-secret", Exec: ExecConfig{Slots: 2},
		Accounts: true, UserOrchestratorModel: "m2", UserWorkerModel: "m1",
	}}
	b, err := json.Marshal(s.serverInfo())
	if err != nil {
		t.Fatal(err)
	}
	out := string(b)
	for _, bad := range []string{"sk-secret", "search-secret", "models.example"} {
		if strings.Contains(out, bad) {
			t.Fatalf("server-info 含 %q: %s", bad, out)
		}
	}
	for _, want := range []string{`"default":"m1"`, `"declared":["m1","m2"]`, `"search_provider":"serper"`, `"image_digest":"sha256:feed"`, `"evalworker"`, `"user_orchestrator":"m2"`} {
		if !strings.Contains(out, want) {
			t.Fatalf("server-info 缺少 %s: %s", want, out)
		}
	}
	fp := s.serverInfo()["upstream_fingerprint"].(string)
	other := &server{execDigest: s.execDigest, cfg: s.cfg}
	other.cfg.Model.Pricing.InputMicroPerMTok++
	if len(fp) != 16 || other.serverInfo()["upstream_fingerprint"] == fp || s.serverInfo()["upstream_fingerprint"] != fp {
		t.Fatalf("upstream_fingerprint 须稳定且随单价变化: %q", fp)
	}
	none := (&server{cfg: Config{}}).serverInfo()
	if m := none["models"].(map[string]any); len(m) != 0 {
		t.Fatalf("未配置模型上游时 models 应为空: %v", m)
	}
}
