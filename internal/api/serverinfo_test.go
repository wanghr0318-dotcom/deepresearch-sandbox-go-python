package api

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestServerInfo：GET /server-info 只给运维（Bearer），任何模式下可用；含构建信息与 Config.ServerInfo 的字段，
// 用户（会话 cookie）为 403，匿名为 401。
func TestServerInfo(t *testing.T) {
	ts := newTestServer(t, func(c *Config) {
		c.ConfigVersion = "cv-1"
		c.ServerInfo = func() map[string]any {
			return map[string]any{"models": map[string]any{"default": "fake-model"}, "exec": map[string]any{"image_digest": "sha256:ab"}}
		}
	})
	for _, m := range []Mode{ModeNormal, ModeDiagnostic, ModeOwnershipLost} {
		ts.setMode(m)
		st, b, _ := ts.do("GET", "/server-info", "", nil)
		expect(t, st, b, 200, "")
		var v map[string]any
		if err := json.Unmarshal(b, &v); err != nil {
			t.Fatal(err)
		}
		if v["config_version"] != "cv-1" || v["models"].(map[string]any)["default"] != "fake-model" {
			t.Fatalf("server-info = %s", b)
		}
		if _, ok := v["build"]; !ok {
			t.Fatalf("server-info 缺少 build: %s", b)
		}
	}

	as, _ := newAccountServer(t, nil)
	st, b, _ := as.do("GET", "/server-info", "", nil)
	expect(t, st, b, 401, "unauthorized")
	st, b, _ = as.do("GET", "/server-info", "", map[string]string{"Authorization": "Bearer " + adminToken})
	expect(t, st, b, 200, "")
	if strings.Contains(string(b), adminToken) {
		t.Fatal("server-info 含 token")
	}
	st, b, h := as.do("POST", "/auth/register", `{"username":"alice","password":"Passw0rdX"}`, nil)
	expect(t, st, b, 201, "")
	c, _ := sessionCookie(t, h)
	st, b, _ = as.do("GET", "/server-info", "", session(c.Value))
	expect(t, st, b, 403, "forbidden")
}
