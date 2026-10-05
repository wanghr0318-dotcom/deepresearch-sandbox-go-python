package cache

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/wanghr0318-dotcom/go-agentbox/internal/gateway/upstream"
)

// ---- 键与 URL 规范化（§11.3） ----

func TestKeyCanonicalization(t *testing.T) {
	mustKey := func(kind upstream.Kind, provider, ver, params string) string {
		t.Helper()
		k, err := Key(kind, provider, ver, []byte(params))
		if err != nil {
			t.Fatalf("Key(%s, %s, %s, %s): %v", kind, provider, ver, params, err)
		}
		return k
	}
	a := mustKey(upstream.KindSearch, "brave", "search/1", `{"q":"go","count":5,"opts":{"b":1,"a":2}}`)
	b := mustKey(upstream.KindSearch, "brave", "search/1", ` { "opts":{"a":2,"b":1}, "count":5.0, "q":"go" }`)
	if a != b {
		t.Errorf("键对参数顺序、空白与数字写法敏感：%s ≠ %s", a, b)
	}
	if !strings.HasPrefix(a, "v1:search:brave:search/1:") || len(a) != len("v1:search:brave:search/1:")+64 {
		t.Errorf("键格式 %q 不是 v1:{kind}:{provider}:{ver}:sha256", a)
	}
	for name, other := range map[string]string{
		"provider": mustKey(upstream.KindSearch, "tavily", "search/1", `{"q":"go","count":5,"opts":{"b":1,"a":2}}`),
		"version":  mustKey(upstream.KindSearch, "brave", "search/2", `{"q":"go","count":5,"opts":{"b":1,"a":2}}`),
		"params":   mustKey(upstream.KindSearch, "brave", "search/1", `{"q":"go","count":6,"opts":{"b":1,"a":2}}`),
	} {
		if other == a {
			t.Errorf("键对 %s 不敏感", name)
		}
	}

	f1 := mustKey(upstream.KindFetch, "http_get", "fetch/1", `{"url":"HTTP://Example.COM:80/a/B?x=1&y=2#frag"}`)
	f2 := mustKey(upstream.KindFetch, "http_get", "fetch/1", `{"url":"http://example.com/a/B?x=1&y=2"}`)
	if f1 != f2 {
		t.Errorf("抓取键未按 URL 规范化：%s ≠ %s", f1, f2)
	}
	if f3 := mustKey(upstream.KindFetch, "http_get", "fetch/1", `{"url":"http://example.com/a/B?y=2&x=1"}`); f3 == f1 {
		t.Error("查询串应原样保留（参数顺序不同的 URL 不应得到相同的键）")
	}
	if f4 := mustKey(upstream.KindFetch, "http_get", "fetch/1", `{"url":"http://example.com/a/b?x=1&y=2"}`); f4 == f1 {
		t.Error("路径大小写应保留")
	}

	for name, c := range map[string]struct {
		kind              upstream.Kind
		provider, ver, ps string
	}{
		"chat 不可缓存":        {upstream.KindChat, "p", "v", `{}`},
		"provider 含冒号":     {upstream.KindSearch, "a:b", "v", `{}`},
		"version 为空":       {upstream.KindSearch, "p", "", `{}`},
		"provider 含空白":     {upstream.KindSearch, "a b", "v", `{}`},
		"重复属性名":            {upstream.KindSearch, "p", "v", `{"q":1,"q":2}`},
		"参数不是对象":           {upstream.KindSearch, "p", "v", `[1]`},
		"参数不是 JSON":        {upstream.KindSearch, "p", "v", `{`},
		"抓取缺 url":          {upstream.KindFetch, "p", "v", `{"u":"http://a/"}`},
		"抓取 url 不是字符串":     {upstream.KindFetch, "p", "v", `{"url":1}`},
		"抓取 url 非 http":    {upstream.KindFetch, "p", "v", `{"url":"ftp://a/"}`},
		"抓取占用保留字段":         {upstream.KindFetch, "p", "v", `{"url":"http://a/","accept_encoding":"br"}`},
		"参数为 null":         {upstream.KindSearch, "p", "v", `null`},
		"version 含控制字符":    {upstream.KindSearch, "p", "v\x01", `{}`},
		"provider 含 DEL":   {upstream.KindSearch, "p\x7f", "v", `{}`},
		"抓取 url 为相对路径":     {upstream.KindFetch, "p", "v", `{"url":"/a"}`},
		"抓取 url 无 host":    {upstream.KindFetch, "p", "v", `{"url":"http:///a"}`},
		"抓取 url opaque 形式": {upstream.KindFetch, "p", "v", `{"url":"http:a"}`},
	} {
		if k, err := Key(c.kind, c.provider, c.ver, []byte(c.ps)); err == nil {
			t.Errorf("%s：应失败，得到键 %s", name, k)
		}
	}
}

func TestNormalizeURL(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"HTTP://WWW.Example.COM/Path?Q=A#Frag", "http://www.example.com/Path?Q=A"},
		{"http://example.com:80/", "http://example.com/"},
		{"https://example.com:443/x", "https://example.com/x"},
		{"http://example.com:443/x", "http://example.com:443/x"},
		{"https://example.com:80/x", "https://example.com:80/x"},
		{"https://example.com:8443/x", "https://example.com:8443/x"},
		{"http://example.com:/x", "http://example.com/x"},
		{"http://[2001:DB8::1]:80/x", "http://[2001:db8::1]/x"},
		{"http://[2001:db8::1]:8080/x", "http://[2001:db8::1]:8080/x"},
		{"http://example.com", "http://example.com"},
		{"http://example.com/a%2Fb?x=%41&y=1+2&&z", "http://example.com/a%2Fb?x=%41&y=1+2&&z"},
		{"http://example.com/?", "http://example.com/?"},
		{"http://example.com/#", "http://example.com/"},
	} {
		got, err := NormalizeURL(c.in)
		if err != nil {
			t.Errorf("NormalizeURL(%q): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("NormalizeURL(%q) = %q，期望 %q", c.in, got, c.want)
		}
	}
	for _, bad := range []string{"", "example.com/x", "mailto:a@b", "file:///etc/passwd", "http://%zz/", "http:a"} {
		if got, err := NormalizeURL(bad); err == nil {
			t.Errorf("NormalizeURL(%q) 应失败，得到 %q", bad, got)
		}
	}
}

// ---- 准入（§11.3） ----

func TestAdmit(t *testing.T) {
	ok200 := func(h map[string][]string) Response { return Response{Status: 200, Header: h, Size: 1024} }
	const page = "https://example.com/doc?q=go"
	for _, c := range []struct {
		name   string
		kind   upstream.Kind
		url    string
		r      Response
		reason string // "" 表示可缓存
	}{
		{"可缓存抓取", upstream.KindFetch, page, ok200(map[string][]string{"Cache-Control": {"public, max-age=60"}}), ""},
		{"Vary 仅 Accept-Encoding", upstream.KindFetch, page, ok200(map[string][]string{"Vary": {"accept-encoding"}}), ""},
		{"恰好 2 MiB", upstream.KindFetch, page, Response{Status: 200, Size: 2 << 20}, ""},
		{"搜索成功结果", upstream.KindSearch, "", Response{Status: 200, Size: 10}, ""},
		{"非凭据参数 keyword/author", upstream.KindFetch, "https://example.com/?keyword=go&author=x&monkey=1", ok200(nil), ""},

		{"模型调用", upstream.KindChat, page, ok200(nil), ReasonKind},
		{"未知类别", upstream.Kind("exec"), page, ok200(nil), ReasonKind},
		{"错误响应 404", upstream.KindFetch, page, Response{Status: 404, Size: 10}, ReasonStatus},
		{"非 200 的 2xx", upstream.KindFetch, page, Response{Status: 203, Size: 10}, ReasonStatus},
		{"搜索失败结果", upstream.KindSearch, "", Response{Status: 500, Size: 10}, ReasonStatus},
		{"超过 2 MiB", upstream.KindFetch, page, Response{Status: 200, Size: 2<<20 + 1}, ReasonSize},
		{"大小未知", upstream.KindFetch, page, Response{Status: 200, Size: -1}, ReasonSize},
		{"Set-Cookie", upstream.KindFetch, page, ok200(map[string][]string{"Set-Cookie": {"a=b"}}), ReasonSetCookie},
		{"set-cookie 非规范大小写", upstream.KindFetch, page, ok200(map[string][]string{"set-cookie": {"a=b"}}), ReasonSetCookie},
		{"no-store", upstream.KindFetch, page, ok200(map[string][]string{"Cache-Control": {"public", "max-age=60, No-Store"}}), ReasonNoStore},
		{"private（带字段名）", upstream.KindFetch, page, ok200(map[string][]string{"Cache-Control": {`private="X-Foo", max-age=60`}}), ReasonPrivate},
		{"no-cache", upstream.KindFetch, page, ok200(map[string][]string{"Cache-Control": {"no-cache"}}), ReasonNoCache},
		{"Vary: *", upstream.KindFetch, page, ok200(map[string][]string{"Vary": {"Accept-Encoding, *"}}), ReasonVaryStar},
		{"Vary 其他维度", upstream.KindFetch, page, ok200(map[string][]string{"Vary": {"Accept-Encoding", "User-Agent"}}), ReasonVaryDimension},
		{"Vary Cookie", upstream.KindFetch, page, ok200(map[string][]string{"Vary": {"Cookie"}}), ReasonVaryDimension},
		{"抓取 URL 为空", upstream.KindFetch, "", ok200(nil), ReasonURLInvalid},
		{"查询参数名无法解码", upstream.KindFetch, "https://example.com/?%zz=1", ok200(nil), ReasonURLInvalid},
		{"userinfo", upstream.KindFetch, "https://user:pw@example.com/", ok200(nil), ReasonURLCredentials},
		{"token", upstream.KindFetch, "https://example.com/?q=1&token=abc", ok200(nil), ReasonURLCredentials},
		{"sig（大写）", upstream.KindFetch, "https://example.com/?SIG=abc", ok200(nil), ReasonURLCredentials},
		{"signature", upstream.KindFetch, "https://example.com/?signature=abc", ok200(nil), ReasonURLCredentials},
		{"key", upstream.KindFetch, "https://example.com/?key=abc", ok200(nil), ReasonURLCredentials},
		{"auth", upstream.KindFetch, "https://example.com/?auth=abc", ok200(nil), ReasonURLCredentials},
		{"X-Amz-*", upstream.KindFetch, "https://b.s3.amazonaws.com/o?X-Amz-Signature=abc&X-Amz-Date=1", ok200(nil), ReasonURLCredentials},
		{"access_token", upstream.KindFetch, "https://example.com/?access_token=abc", ok200(nil), ReasonURLCredentials},
		{"api-key", upstream.KindFetch, "https://example.com/?api-key=abc", ok200(nil), ReasonURLCredentials},
		{"百分号编码的参数名", upstream.KindFetch, "https://example.com/?%74oken=abc", ok200(nil), ReasonURLCredentials},
		{"分号分隔", upstream.KindFetch, "https://example.com/?a=1;token=abc", ok200(nil), ReasonURLCredentials},
	} {
		ok, reason := Admit(c.kind, c.url, c.r)
		if ok != (c.reason == "") || reason != c.reason {
			t.Errorf("%s：Admit = (%v, %q)，期望原因 %q", c.name, ok, reason, c.reason)
		}
	}
}

// ---- 新鲜度（§11.3、E25） ----

func TestLifetimeE25(t *testing.T) {
	base := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	date := func(d time.Duration) string { return base.Add(d).Format(time.RFC1123) }
	// resp 构造一个 request_time = base−1s、response_time = base 的 200 响应。
	resp := func(h map[string][]string) Response {
		return Response{Status: 200, Header: h, Size: 100, RequestTime: base.Add(-time.Second), ResponseTime: base}
	}
	const never = time.Duration(-1)
	for _, c := range []struct {
		name string
		r    Response
		now  time.Time
		want time.Duration // 相对 base 的 expires_at；never 表示不缓存
	}{
		{"max-age 减去响应延迟", resp(map[string][]string{"Cache-Control": {"max-age=60"}, "Date": {date(0)}}), base, 59 * time.Second},
		{"短 max-age：到手即过期", resp(map[string][]string{"Cache-Control": {"max-age=1"}, "Date": {date(0)}}), base, never},
		{"max-age=0", resp(map[string][]string{"Cache-Control": {"max-age=0"}}), base, never},
		{"Age 计入年龄", resp(map[string][]string{"Cache-Control": {"max-age=60"}, "Age": {"50"}, "Date": {date(0)}}), base, 9 * time.Second},
		{"Age 超过寿命", resp(map[string][]string{"Cache-Control": {"max-age=60"}, "Age": {"120"}}), base, never},
		{"Date 落后：apparent_age", resp(map[string][]string{"Cache-Control": {"max-age=60"}, "Date": {date(-30 * time.Second)}}), base, 30 * time.Second},
		{"Date 超前：apparent_age 取 0", resp(map[string][]string{"Cache-Control": {"max-age=60"}, "Date": {date(time.Hour)}}), base, 59 * time.Second},
		{"无 Date 的 max-age", resp(map[string][]string{"Cache-Control": {"max-age=60"}}), base, 59 * time.Second},
		{"s-maxage 优先于 max-age", resp(map[string][]string{"Cache-Control": {"max-age=1000, s-maxage=10"}}), base, 9 * time.Second},
		{"max-age 优先于 Expires", resp(map[string][]string{"Cache-Control": {"max-age=60"}, "Date": {date(0)}, "Expires": {date(time.Hour)}}), base, 59 * time.Second},
		{"Expires − Date", resp(map[string][]string{"Date": {date(0)}, "Expires": {date(2 * time.Minute)}}), base, 119 * time.Second},
		{"Expires − Date（RFC 850 与 asctime）", resp(map[string][]string{
			"Date": {base.Format(time.RFC850)}, "Expires": {base.Add(2 * time.Minute).Format(time.ANSIC)}}), base, 119 * time.Second},
		{"Date 由调用方给出", Response{Status: 200, Header: map[string][]string{"Expires": {date(2 * time.Minute)}}, Date: base,
			RequestTime: base.Add(-time.Second), ResponseTime: base}, base, 119 * time.Second},
		{"Expires 无 Date", resp(map[string][]string{"Expires": {date(time.Hour)}}), base, never},
		{"Expires 已过", resp(map[string][]string{"Date": {date(0)}, "Expires": {date(-time.Minute)}}), base, never},
		{"Expires 无效（0）", resp(map[string][]string{"Date": {date(0)}, "Expires": {"0"}}), base, never},
		{"Expires 无效且有 max-age", resp(map[string][]string{"Cache-Control": {"max-age=60"}, "Expires": {"-1"}}), base, never},
		{"引号形式的 max-age", resp(map[string][]string{"Cache-Control": {`max-age="60"`}}), base, 59 * time.Second},
		{"大小写与多行指令", resp(map[string][]string{"Cache-Control": {"Public", "Max-Age=60, must-revalidate"}}), base, 59 * time.Second},
		{"无显式新鲜度", resp(map[string][]string{"Cache-Control": {"public"}, "Date": {date(0)}, "Last-Modified": {date(-24 * time.Hour)}}), base, never},
		{"没有任何头", resp(nil), base, never},
		{"冲突：两个 max-age", resp(map[string][]string{"Cache-Control": {"max-age=60", "max-age=120"}}), base, never},
		{"冲突：两个相同的 max-age", resp(map[string][]string{"Cache-Control": {"max-age=60, max-age=60"}}), base, never},
		{"冲突：两个 Expires", resp(map[string][]string{"Date": {date(0)}, "Expires": {date(time.Hour), date(2 * time.Hour)}}), base, never},
		{"冲突：两个 Date", resp(map[string][]string{"Cache-Control": {"max-age=60"}, "Date": {date(0), date(-time.Minute)}}), base, never},
		{"冲突：两种大小写的 Date", resp(map[string][]string{"Cache-Control": {"max-age=60"}, "Date": {date(0)}, "date": {date(0)}}), base, never},
		{"冲突：Date 与调用方给出的不一致", Response{Status: 200, Header: map[string][]string{"Cache-Control": {"max-age=60"}, "Date": {date(0)}},
			Date: base.Add(-time.Minute), RequestTime: base.Add(-time.Second), ResponseTime: base}, base, never},
		{"冲突：两个 Age", resp(map[string][]string{"Cache-Control": {"max-age=60"}, "Age": {"1", "2"}}), base, never},
		{"无效 Age", resp(map[string][]string{"Cache-Control": {"max-age=60"}, "Age": {"-1"}}), base, never},
		{"无效 max-age", resp(map[string][]string{"Cache-Control": {"max-age=abc"}}), base, never},
		{"max-age 缺参数", resp(map[string][]string{"Cache-Control": {"max-age"}}), base, never},
		{"max-age 空参数", resp(map[string][]string{"Cache-Control": {"max-age="}}), base, never},
		{"Cache-Control 语法错误", resp(map[string][]string{"Cache-Control": {`max-age=60, "x"`}}), base, never},
		{"引号未闭合", resp(map[string][]string{"Cache-Control": {`max-age="60`}}), base, never},
		{"不支持的指令", resp(map[string][]string{"Cache-Control": {"max-age=60, x-custom"}}), base, never},
		{"no-store", resp(map[string][]string{"Cache-Control": {"max-age=60, no-store"}}), base, never},
		{"no-cache", resp(map[string][]string{"Cache-Control": {"max-age=60, no-cache"}}), base, never},
		{"private", resp(map[string][]string{"Cache-Control": {"max-age=60, private"}}), base, never},
		{"24 h 上限", resp(map[string][]string{"Cache-Control": {"max-age=172800"}}), base, 24 * time.Hour},
		{"24 h 上限（Expires）", resp(map[string][]string{"Date": {date(0)}, "Expires": {date(72 * time.Hour)}}), base, 24 * time.Hour},
		{"溢出的 delta-seconds 封顶后仍受 24 h 上限", resp(map[string][]string{"Cache-Control": {"max-age=99999999999999999999"}}), base, 24 * time.Hour},
		{"不延长：稍后重算截止点不变", resp(map[string][]string{"Cache-Control": {"max-age=60"}}), base.Add(30 * time.Second), 59 * time.Second},
		{"不延长：上限锚定在响应时刻", resp(map[string][]string{"Cache-Control": {"max-age=172800"}}), base.Add(time.Hour), 24 * time.Hour},
		{"不延长：截止点之后不缓存", resp(map[string][]string{"Cache-Control": {"max-age=60"}}), base.Add(59 * time.Second), never},
		{"缺 RequestTime", Response{Status: 200, Header: map[string][]string{"Cache-Control": {"max-age=60"}}, ResponseTime: base}, base, never},
		{"响应早于请求", Response{Status: 200, Header: map[string][]string{"Cache-Control": {"max-age=60"}},
			RequestTime: base, ResponseTime: base.Add(-time.Second)}, base, never},
	} {
		got, ok := Lifetime(c.r, c.now)
		switch {
		case c.want == never && ok:
			t.Errorf("%s：应不缓存，得到 expires_at = base%+v", c.name, got.Sub(base))
		case c.want != never && !ok:
			t.Errorf("%s：应可缓存至 base%+v，得到不缓存", c.name, c.want)
		case c.want != never && !got.Equal(base.Add(c.want)):
			t.Errorf("%s：expires_at = base%+v，期望 base%+v", c.name, got.Sub(base), c.want)
		}
	}

	// E25 的 Vary 行：新鲜度本身成立，准入决定是否缓存（只允许 Accept-Encoding 维度）。
	for _, c := range []struct {
		vary      string
		cacheable bool
	}{{"Accept-Encoding", true}, {"Accept-Language", false}, {"*", false}} {
		r := resp(map[string][]string{"Cache-Control": {"max-age=60"}, "Vary": {c.vary}})
		admitted, _ := Admit(upstream.KindFetch, "https://example.com/", r)
		_, fresh := Lifetime(r, base)
		if got := admitted && fresh; got != c.cacheable {
			t.Errorf("Vary: %s：可缓存 = %v，期望 %v", c.vary, got, c.cacheable)
		}
	}
}

// ---- 签名值与密钥（§11.3、§11.5） ----

func policySampleValue(base time.Time) Value {
	return Value{
		BlobSHA256:  strings.Repeat("ab", 32),
		Status:      200,
		ContentType: "text/html; charset=utf-8",
		FinalURL:    "https://example.com/doc",
		Size:        1234,
		FetchedAt:   base,
		ExpiresAt:   base.Add(time.Hour),
	}
}

// policyMutate 解码缓存值、修改后重新编码（保持合法 JSON，用于篡改用例）。
func policyMutate(t *testing.T, raw []byte, f func(m map[string]any)) []byte {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	f(m)
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestSealOpen(t *testing.T) {
	dir := t.TempDir()
	s, err := LoadKeys(dir)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 10, 5, 12, 0, 0, 123456789, time.FixedZone("CST", 8*3600))
	const key = "v1:fetch:http_get:fetch/1:" + "00"
	v := policySampleValue(base)
	raw, err := s.Seal(key, v)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Open(key, raw, base.Add(time.Minute))
	if err != nil {
		t.Fatalf("往返: %v", err)
	}
	if got.BlobSHA256 != v.BlobSHA256 || got.Status != v.Status || got.ContentType != v.ContentType ||
		got.FinalURL != v.FinalURL || got.Size != v.Size || !got.FetchedAt.Equal(v.FetchedAt) ||
		!got.ExpiresAt.Equal(v.ExpiresAt) || got.KID != s.cur.kid || len(got.MAC) != 32 {
		t.Errorf("往返不一致：%+v", got)
	}

	var other string
	{
		s2, err := LoadKeys(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		other = s2.cur.kid
	}
	now := base.Add(time.Minute)
	for _, c := range []struct {
		name string
		key  string
		raw  []byte
		now  time.Time
		want error
	}{
		{"篡改 MAC", key, policyMutate(t, raw, func(m map[string]any) {
			mac := []byte(m["mac"].(string))
			if mac[0] == 'A' {
				mac[0] = 'B'
			} else {
				mac[0] = 'A'
			}
			m["mac"] = string(mac)
		}), now, ErrEntryBadMAC},
		{"换键", key + "1", raw, now, ErrEntryBadMAC},
		{"篡改 expires_at（延长）", key, policyMutate(t, raw, func(m map[string]any) {
			m["expires_at"] = base.Add(48 * time.Hour).UTC().Format(time.RFC3339Nano)
		}), now, ErrEntryBadMAC},
		{"篡改 blob_sha256", key, policyMutate(t, raw, func(m map[string]any) { m["blob_sha256"] = strings.Repeat("cd", 32) }), now, ErrEntryBadMAC},
		{"篡改 size", key, policyMutate(t, raw, func(m map[string]any) { m["size"] = 1 }), now, ErrEntryBadMAC},
		{"篡改 status", key, policyMutate(t, raw, func(m map[string]any) { m["status"] = 203 }), now, ErrEntryBadMAC},
		{"篡改 final_url", key, policyMutate(t, raw, func(m map[string]any) { m["final_url"] = "https://evil.example/" }), now, ErrEntryBadMAC},
		{"篡改 kid 为未知", key, policyMutate(t, raw, func(m map[string]any) { m["kid"] = other }), now, ErrEntryUnknownKID},
		{"kid 为空", key, policyMutate(t, raw, func(m map[string]any) { m["kid"] = "" }), now, ErrEntryUnknownKID},
		{"已过期（恰在 expires_at）", key, raw, v.ExpiresAt, ErrEntryExpired},
		{"已过期（之后）", key, raw, v.ExpiresAt.Add(time.Second), ErrEntryExpired},
		{"未知字段", key, policyMutate(t, raw, func(m map[string]any) { m["extra"] = 1 }), now, ErrEntryMalformed},
		{"缺 MAC", key, policyMutate(t, raw, func(m map[string]any) { delete(m, "mac") }), now, ErrEntryMalformed},
		{"MAC 长度错误", key, policyMutate(t, raw, func(m map[string]any) { m["mac"] = "AAAA" }), now, ErrEntryMalformed},
		{"尾随内容", key, append(append([]byte{}, raw...), []byte(` {}`)...), now, ErrEntryMalformed},
		{"重复属性名", key, append([]byte(`{"kid":"x",`), raw[1:]...), now, ErrEntryMalformed},
		{"不是 JSON", key, []byte("not json"), now, ErrEntryMalformed},
		{"超过 4 KiB 不解析", key, []byte(`{"pad":"` + strings.Repeat("x", MaxSealedBytes) + `"}`), now, ErrEntryTooLarge},
	} {
		if _, err := s.Open(c.key, c.raw, c.now); !errors.Is(err, c.want) {
			t.Errorf("%s：Open 错误 = %v，期望 %v", c.name, err, c.want)
		}
	}

	big := policySampleValue(base)
	big.FinalURL = "https://example.com/" + strings.Repeat("a", MaxSealedBytes)
	if _, err := s.Seal(key, big); !errors.Is(err, ErrEntryTooLarge) {
		t.Errorf("超过 4 KiB 的值：Seal 错误 = %v，期望 ErrEntryTooLarge", err)
	}
	for name, f := range map[string]func(v *Value){
		"blob_sha256 大写": func(v *Value) { v.BlobSHA256 = strings.Repeat("AB", 32) },
		"blob_sha256 过短": func(v *Value) { v.BlobSHA256 = "ab" },
		"状态码无效":          func(v *Value) { v.Status = 0 },
		"大小为负":           func(v *Value) { v.Size = -1 },
		"expires_at 为零":  func(v *Value) { v.ExpiresAt = time.Time{} },
	} {
		bad := policySampleValue(base)
		f(&bad)
		if _, err := s.Seal(key, bad); err == nil {
			t.Errorf("%s：Seal 应失败", name)
		}
	}
	if _, err := s.Seal("", v); err == nil {
		t.Error("空缓存键：Seal 应失败")
	}
}

func TestKeyFileAndRotation(t *testing.T) {
	dir := t.TempDir()
	if err := RotateKey(dir); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("密钥文件不存在时 RotateKey 错误 = %v，期望 ErrNotExist", err)
	}
	s0, err := LoadKeys(dir)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, KeyFileName))
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Errorf("cache.key 权限 %v，期望 0600", info.Mode().Perm())
	}
	again, err := LoadKeys(dir)
	if err != nil {
		t.Fatal(err)
	}
	if again.cur.kid != s0.cur.kid || again.prev != nil {
		t.Fatalf("重复加载生成了新密钥：%s → %s", s0.cur.kid, again.cur.kid)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("数据目录残留临时文件：%v", entries)
	}

	base := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	const key = "v1:search:brave:search/1:00"
	raw0, err := s0.Seal(key, policySampleValue(base))
	if err != nil {
		t.Fatal(err)
	}

	if err := RotateKey(dir); err != nil {
		t.Fatal(err)
	}
	s1, err := LoadKeys(dir)
	if err != nil {
		t.Fatal(err)
	}
	if s1.cur.kid == s0.cur.kid || s1.prev == nil || s1.prev.kid != s0.cur.kid {
		t.Fatalf("轮换后当前/上一 kid 不正确")
	}
	if _, err := s1.Open(key, raw0, base); err != nil {
		t.Errorf("一次轮换后上一 kid 应仍可验证：%v", err)
	}
	raw1, err := s1.Seal(key, policySampleValue(base))
	if err != nil {
		t.Fatal(err)
	}
	if v, err := s1.Open(key, raw1, base); err != nil || v.KID != s1.cur.kid {
		t.Errorf("新值应以当前 kid 签名：kid=%s err=%v", v.KID, err)
	}
	if _, err := s0.Open(key, raw1, base); !errors.Is(err, ErrEntryUnknownKID) {
		t.Errorf("旧 Signer 不认识新 kid：错误 = %v", err)
	}

	if err := RotateKey(dir); err != nil {
		t.Fatal(err)
	}
	s2, err := LoadKeys(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s2.Open(key, raw0, base); !errors.Is(err, ErrEntryUnknownKID) {
		t.Errorf("两次轮换后最初的 kid 应失效：错误 = %v", err)
	}
	if _, err := s2.Open(key, raw1, base); err != nil {
		t.Errorf("两次轮换后上一 kid 应可验证：%v", err)
	}

	// 损坏的密钥文件：报错，不重新生成（否则掩盖问题并使全部条目失效）。
	path := filepath.Join(dir, KeyFileName)
	good, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{
		"截断":        string(good[:len(good)/2]),
		"kid 与密钥不符": strings.Replace(string(good), s2.cur.kid, strings.Repeat("0", 16), 1),
		"版本未知":      strings.Replace(string(good), `"version": 1`, `"version": 2`, 1),
		"未知字段":      strings.Replace(string(good), `"version": 1`, `"version": 1, "x": 1`, 1),
		"空文件":       "",
	} {
		if content == string(good) {
			t.Fatalf("%s：替换没有生效", name)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadKeys(dir); err == nil {
			t.Errorf("%s：LoadKeys 应失败", name)
		}
		after, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if string(after) != content {
			t.Errorf("%s：损坏的密钥文件被覆盖", name)
		}
	}
	if _, err := LoadKeys(filepath.Join(dir, "missing")); err == nil {
		t.Error("数据目录不存在时 LoadKeys 应失败")
	}
}
