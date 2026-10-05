package cache

import (
	"math"
	"strconv"
	"strings"
	"time"
)

// MaxLifetime 是 expires_at 相对响应时刻的上限（§11.3、§19：24 h）。
const MaxLifetime = 24 * time.Hour

// knownResponseDirectives 是本子集理解且不阻止缓存的 Cache-Control 响应指令。只在条目新鲜期内返回、
// 从不返回过期条目，因此与过期后行为有关的指令（must-revalidate、stale-* 等）不影响结果。
// 其他指令一律视为不支持 → 不缓存。no-store、no-cache、private 使响应不可缓存。
var knownResponseDirectives = map[string]bool{
	"max-age": true, "s-maxage": true, "public": true, "must-revalidate": true, "proxy-revalidate": true,
	"no-transform": true, "immutable": true, "stale-while-revalidate": true, "stale-if-error": true,
	"must-understand": true,
}

// Lifetime 按 RFC 9111 子集（§11.3）计算响应的新鲜度截止点 expires_at。
//
//   - 寿命取 s-maxage，否则 max-age，否则 Expires − Date；没有显式新鲜度不缓存（不使用启发式）。
//   - 年龄按 RFC 9111 §4.2.3 完整计算：corrected_initial_age = max(response_time − date, Age +
//     (response_time − request_time))；截止点 = response_time − corrected_initial_age + 寿命，
//     与 now 无关，因此重算或命中都不会延长它。
//   - 截止点不晚于 min(response_time, now) + 24 h；不设下限；已不新鲜（截止点 ≤ now）不缓存。
//   - 无效、重复（冲突）或不支持的 Cache-Control / Expires / Date / Age，以及 no-store、no-cache、
//     private → 不缓存。缺少 RequestTime/ResponseTime 或二者颠倒同样不缓存。
func Lifetime(r Response, now time.Time) (expiresAt time.Time, ok bool) {
	if r.RequestTime.IsZero() || r.ResponseTime.IsZero() || r.ResponseTime.Before(r.RequestTime) {
		return time.Time{}, false
	}
	cc, valid := parseCacheControl(r.Header)
	if !valid {
		return time.Time{}, false
	}
	for name := range cc {
		if !knownResponseDirectives[name] {
			return time.Time{}, false
		}
	}
	date, valid := responseDate(r)
	if !valid {
		return time.Time{}, false
	}
	expires, hasExpires, valid := singleHTTPDate(r.Header, "Expires")
	if !valid {
		return time.Time{}, false
	}
	var age time.Duration
	switch vs := headerValues(r.Header, "Age"); len(vs) {
	case 0:
	case 1:
		d, valid := deltaSeconds(strings.TrimSpace(vs[0]))
		if !valid {
			return time.Time{}, false
		}
		age = d
	default:
		return time.Time{}, false
	}

	var lifetime time.Duration
	if d, present, valid := ccDelta(cc, "s-maxage"); !valid {
		return time.Time{}, false
	} else if present {
		lifetime = d
	} else if d, present, valid := ccDelta(cc, "max-age"); !valid {
		return time.Time{}, false
	} else if present {
		lifetime = d
	} else if hasExpires && !date.IsZero() {
		lifetime = expires.Sub(date)
	} else {
		return time.Time{}, false // 无显式新鲜度（Expires 无 Date 时也无法计算 Expires − Date）
	}
	if lifetime <= 0 {
		return time.Time{}, false
	}

	var apparentAge time.Duration
	if !date.IsZero() {
		apparentAge = max(0, r.ResponseTime.Sub(date))
	}
	correctedAgeValue := age + r.ResponseTime.Sub(r.RequestTime)
	correctedInitialAge := max(apparentAge, correctedAgeValue)
	expiresAt = r.ResponseTime.Add(-correctedInitialAge).Add(lifetime)

	anchor := r.ResponseTime
	if now.Before(anchor) {
		anchor = now
	}
	if limit := anchor.Add(MaxLifetime); expiresAt.After(limit) {
		expiresAt = limit
	}
	if !now.Before(expiresAt) {
		return time.Time{}, false
	}
	return expiresAt, true
}

// responseDate 返回 date_value：Date 头（至多一行，须为合法 HTTP-date）或调用方给出的 r.Date；
// 二者都有而不一致视为冲突。都没有时返回零值（不参与 apparent_age，Expires 无法使用）。
func responseDate(r Response) (time.Time, bool) {
	d, present, valid := singleHTTPDate(r.Header, "Date")
	if !valid {
		return time.Time{}, false
	}
	switch {
	case present && !r.Date.IsZero() && !d.Equal(r.Date):
		return time.Time{}, false
	case present:
		return d, true
	default:
		return r.Date, true
	}
}

// singleHTTPDate 解析至多出现一次的 HTTP-date 头。
func singleHTTPDate(h map[string][]string, name string) (t time.Time, present, valid bool) {
	vs := headerValues(h, name)
	switch len(vs) {
	case 0:
		return time.Time{}, false, true
	case 1:
		t, err := parseHTTPDate(strings.TrimSpace(vs[0]))
		if err != nil {
			return time.Time{}, true, false
		}
		return t, true, true
	default:
		return time.Time{}, true, false
	}
}

// httpDateLayouts 是 RFC 9110 §5.6.7 的三种 HTTP-date 格式（IMF-fixdate、RFC 850、asctime）。
var httpDateLayouts = []string{time.RFC1123, time.RFC850, time.ANSIC}

func parseHTTPDate(s string) (time.Time, error) {
	var err error
	for _, layout := range httpDateLayouts {
		var t time.Time
		if t, err = time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, err
}

// deltaSeconds 解析 RFC 9111 §1.2.2 的 delta-seconds（1*DIGIT）；超过 2^31 按 2^31 处理。
func deltaSeconds(s string) (time.Duration, bool) {
	if s == "" {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n > math.MaxInt32+1 {
		n = math.MaxInt32 + 1 // 只可能是溢出（已确认全为数字）
	}
	return time.Duration(n) * time.Second, true
}

// ccDelta 读取 delta-seconds 型指令：出现多次（冲突）、缺少参数或参数不合法 → valid = false。
func ccDelta(cc map[string][]string, name string) (d time.Duration, present, valid bool) {
	vs, ok := cc[name]
	if !ok {
		return 0, false, true
	}
	if len(vs) != 1 {
		return 0, true, false
	}
	d, valid = deltaSeconds(vs[0])
	return d, true, valid
}

// parseCacheControl 解析全部 Cache-Control 字段行：指令名（token，小写）→ 各次出现的参数（token 或
// 去引号后的 quoted-string；无参数时为 ""）。语法错误返回 valid = false。
func parseCacheControl(h map[string][]string) (map[string][]string, bool) {
	cc := map[string][]string{}
	for _, line := range headerValues(h, "Cache-Control") {
		for _, item := range splitHeaderList(line) {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			name, arg, hasArg := strings.Cut(item, "=")
			name = strings.ToLower(strings.TrimSpace(name))
			if !isHTTPToken(name) {
				return nil, false
			}
			if hasArg {
				arg = strings.TrimSpace(arg)
				if strings.HasPrefix(arg, `"`) {
					v, ok := unquoteHTTP(arg)
					if !ok {
						return nil, false
					}
					arg = v
				} else if !isHTTPToken(arg) {
					return nil, false
				}
			}
			cc[name] = append(cc[name], arg)
		}
	}
	return cc, true
}

// isHTTPToken 判断 s 是否为 RFC 9110 §5.6.2 的 token。
func isHTTPToken(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		case strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0:
		default:
			return false
		}
	}
	return true
}

// unquoteHTTP 解析完整的 quoted-string（RFC 9110 §5.6.4）。
func unquoteHTTP(s string) (string, bool) {
	if len(s) < 2 || s[0] != '"' || s[len(s)-1] != '"' {
		return "", false
	}
	var b strings.Builder
	for i := 1; i < len(s)-1; i++ {
		c := s[i]
		switch {
		case c == '\\':
			if i+1 >= len(s)-1 {
				return "", false
			}
			i++
			b.WriteByte(s[i])
		case c == '"':
			return "", false
		default:
			b.WriteByte(c)
		}
	}
	return b.String(), true
}
