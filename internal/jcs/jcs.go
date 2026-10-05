// Package jcs 实现 RFC 8785（JSON Canonicalization Scheme）规范化。
//
// 输入先按 encoding/json 编码（结构体、json.RawMessage 等均可），再逐字解析并按 RFC 8785 输出：
// 对象成员按键的 UTF-16 码元排序、无空白；字符串只转义 RFC 8785 要求的字符；数字按 IEEE 754 双精度
// 解析后以 ECMAScript Number.prototype.toString 的形式输出。与 I-JSON（RFC 7493）一致，
// 拒绝同一对象中的重复属性名、非法 UTF-8、孤立代理项，以及无法表示为有限双精度数的数字。
package jcs

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// ErrDuplicateKey 表示同一对象中出现重复的属性名（按解码后的字符串比较）。
var ErrDuplicateKey = errors.New("jcs: 重复的 JSON 属性名")

// ErrInvalid 表示输入不是合法的 I-JSON（语法、UTF-8、代理项或数字范围）。
var ErrInvalid = errors.New("jcs: 不是合法的 I-JSON")

// Canonical 返回 v 的 RFC 8785 规范化编码。v 为 json.RawMessage 时按其原文规范化（[]byte 会被 encoding/json
// 编码为 base64 字符串，须先转为 json.RawMessage）。重复属性名返回 ErrDuplicateKey。
func Canonical(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	p := parser{in: b}
	p.skipWS()
	var out bytes.Buffer
	out.Grow(len(b))
	if err := p.value(&out, 0); err != nil {
		return nil, err
	}
	p.skipWS()
	if p.pos != len(p.in) {
		return nil, p.errf("值之后有多余内容")
	}
	return out.Bytes(), nil
}

// maxDepth 限制嵌套深度，防止恶意输入耗尽栈。
const maxDepth = 512

type parser struct {
	in  []byte
	pos int
}

func (p *parser) errf(format string, args ...any) error {
	return fmt.Errorf("%w: 偏移 %d: %s", ErrInvalid, p.pos, fmt.Sprintf(format, args...))
}

func (p *parser) skipWS() {
	for p.pos < len(p.in) {
		switch p.in[p.pos] {
		case ' ', '\t', '\n', '\r':
			p.pos++
		default:
			return
		}
	}
}

func (p *parser) value(out *bytes.Buffer, depth int) error {
	if depth > maxDepth {
		return p.errf("嵌套过深")
	}
	if p.pos >= len(p.in) {
		return p.errf("意外结束")
	}
	switch c := p.in[p.pos]; {
	case c == '{':
		return p.object(out, depth)
	case c == '[':
		return p.array(out, depth)
	case c == '"':
		s, err := p.str()
		if err != nil {
			return err
		}
		writeString(out, s)
		return nil
	case c == '-' || (c >= '0' && c <= '9'):
		return p.number(out)
	default:
		for _, lit := range []string{"true", "false", "null"} {
			if bytes.HasPrefix(p.in[p.pos:], []byte(lit)) {
				p.pos += len(lit)
				out.WriteString(lit)
				return nil
			}
		}
		return p.errf("非法字符 %q", c)
	}
}

type member struct {
	key string
	val []byte
}

func (p *parser) object(out *bytes.Buffer, depth int) error {
	p.pos++ // {
	var members []member
	seen := map[string]bool{}
	p.skipWS()
	if p.pos < len(p.in) && p.in[p.pos] == '}' {
		p.pos++
		out.WriteString("{}")
		return nil
	}
	for {
		p.skipWS()
		if p.pos >= len(p.in) || p.in[p.pos] != '"' {
			return p.errf("期望属性名")
		}
		k, err := p.str()
		if err != nil {
			return err
		}
		if seen[k] {
			return fmt.Errorf("%w: %q", ErrDuplicateKey, k)
		}
		seen[k] = true
		p.skipWS()
		if p.pos >= len(p.in) || p.in[p.pos] != ':' {
			return p.errf("期望 ':'")
		}
		p.pos++
		p.skipWS()
		var v bytes.Buffer
		if err := p.value(&v, depth+1); err != nil {
			return err
		}
		members = append(members, member{key: k, val: v.Bytes()})
		p.skipWS()
		if p.pos >= len(p.in) {
			return p.errf("对象未结束")
		}
		if p.in[p.pos] == ',' {
			p.pos++
			continue
		}
		if p.in[p.pos] == '}' {
			p.pos++
			break
		}
		return p.errf("期望 ',' 或 '}'")
	}
	sort.Slice(members, func(i, j int) bool { return lessUTF16(members[i].key, members[j].key) })
	out.WriteByte('{')
	for i, m := range members {
		if i > 0 {
			out.WriteByte(',')
		}
		writeString(out, m.key)
		out.WriteByte(':')
		out.Write(m.val)
	}
	out.WriteByte('}')
	return nil
}

func (p *parser) array(out *bytes.Buffer, depth int) error {
	p.pos++ // [
	out.WriteByte('[')
	p.skipWS()
	if p.pos < len(p.in) && p.in[p.pos] == ']' {
		p.pos++
		out.WriteByte(']')
		return nil
	}
	for i := 0; ; i++ {
		if i > 0 {
			out.WriteByte(',')
		}
		p.skipWS()
		if err := p.value(out, depth+1); err != nil {
			return err
		}
		p.skipWS()
		if p.pos >= len(p.in) {
			return p.errf("数组未结束")
		}
		if p.in[p.pos] == ',' {
			p.pos++
			continue
		}
		if p.in[p.pos] == ']' {
			p.pos++
			out.WriteByte(']')
			return nil
		}
		return p.errf("期望 ',' 或 ']'")
	}
}

// str 解析一个字符串字面量并返回其解码后的值（合法 UTF-8，无孤立代理项）。
func (p *parser) str() (string, error) {
	p.pos++ // "
	var sb strings.Builder
	for {
		if p.pos >= len(p.in) {
			return "", p.errf("字符串未结束")
		}
		c := p.in[p.pos]
		switch {
		case c == '"':
			p.pos++
			return sb.String(), nil
		case c == '\\':
			r, err := p.escape()
			if err != nil {
				return "", err
			}
			sb.WriteRune(r)
		case c < 0x20:
			return "", p.errf("字符串含未转义的控制字符")
		case c < utf8.RuneSelf:
			sb.WriteByte(c)
			p.pos++
		default:
			r, n := utf8.DecodeRune(p.in[p.pos:])
			if r == utf8.RuneError && n <= 1 {
				return "", p.errf("非法 UTF-8")
			}
			sb.WriteRune(r)
			p.pos += n
		}
	}
}

func (p *parser) escape() (rune, error) {
	if p.pos+1 >= len(p.in) {
		return 0, p.errf("转义未结束")
	}
	c := p.in[p.pos+1]
	p.pos += 2
	switch c {
	case '"', '\\', '/':
		return rune(c), nil
	case 'b':
		return '\b', nil
	case 'f':
		return '\f', nil
	case 'n':
		return '\n', nil
	case 'r':
		return '\r', nil
	case 't':
		return '\t', nil
	case 'u':
		r1, err := p.hex4()
		if err != nil {
			return 0, err
		}
		if !utf16.IsSurrogate(r1) {
			return r1, nil
		}
		if r1 >= 0xDC00 || p.pos+1 >= len(p.in) || p.in[p.pos] != '\\' || p.in[p.pos+1] != 'u' {
			return 0, p.errf("孤立的 UTF-16 代理项")
		}
		p.pos += 2
		r2, err := p.hex4()
		if err != nil {
			return 0, err
		}
		r := utf16.DecodeRune(r1, r2)
		if r == utf8.RuneError {
			return 0, p.errf("孤立的 UTF-16 代理项")
		}
		return r, nil
	default:
		return 0, p.errf("非法转义 \\%c", c)
	}
}

func (p *parser) hex4() (rune, error) {
	if p.pos+4 > len(p.in) {
		return 0, p.errf("\\u 转义不完整")
	}
	n, err := strconv.ParseUint(string(p.in[p.pos:p.pos+4]), 16, 32)
	if err != nil {
		return 0, p.errf("\\u 转义不是十六进制")
	}
	p.pos += 4
	return rune(n), nil
}

func (p *parser) number(out *bytes.Buffer) error {
	start := p.pos
	if p.in[p.pos] == '-' {
		p.pos++
	}
	digits := func() int {
		n := 0
		for p.pos < len(p.in) && p.in[p.pos] >= '0' && p.in[p.pos] <= '9' {
			p.pos++
			n++
		}
		return n
	}
	intStart := p.pos
	if digits() == 0 {
		return p.errf("数字缺少整数部分")
	}
	if p.in[intStart] == '0' && p.pos-intStart > 1 {
		return p.errf("数字有前导零")
	}
	if p.pos < len(p.in) && p.in[p.pos] == '.' {
		p.pos++
		if digits() == 0 {
			return p.errf("小数点后缺少数字")
		}
	}
	if p.pos < len(p.in) && (p.in[p.pos] == 'e' || p.in[p.pos] == 'E') {
		p.pos++
		if p.pos < len(p.in) && (p.in[p.pos] == '+' || p.in[p.pos] == '-') {
			p.pos++
		}
		if digits() == 0 {
			return p.errf("指数缺少数字")
		}
	}
	f, err := strconv.ParseFloat(string(p.in[start:p.pos]), 64)
	if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
		return p.errf("数字超出双精度范围")
	}
	out.WriteString(FormatNumber(f))
	return nil
}

// FormatNumber 按 ECMAScript Number.prototype.toString（RFC 8785 §3.2.2.3）格式化有限双精度数。
func FormatNumber(f float64) string {
	if f == 0 {
		return "0" // 含 -0
	}
	neg := f < 0
	if neg {
		f = -f
	}
	// 最短往返表示：d.ddddde±x
	e := strconv.FormatFloat(f, 'e', -1, 64)
	mant, expStr, _ := strings.Cut(e, "e")
	digits := strings.Replace(mant, ".", "", 1)
	exp, _ := strconv.Atoi(expStr)
	k := len(digits)
	n := exp + 1 // 小数点位置
	var s string
	switch {
	case k <= n && n <= 21:
		s = digits + strings.Repeat("0", n-k)
	case 0 < n && n <= 21:
		s = digits[:n] + "." + digits[n:]
	case -6 < n && n <= 0:
		s = "0." + strings.Repeat("0", -n) + digits
	default:
		s = digits[:1]
		if k > 1 {
			s += "." + digits[1:]
		}
		x := n - 1
		if x >= 0 {
			s += "e+" + strconv.Itoa(x)
		} else {
			s += "e-" + strconv.Itoa(-x)
		}
	}
	if neg {
		return "-" + s
	}
	return s
}

// writeString 按 RFC 8785 §3.2.2.2 输出字符串：只转义 '"'、'\\' 与 U+0000–U+001F（\b \t \n \f \r 用短形式，
// 其余为小写十六进制 \u00xx）；其他字符原样输出。
func writeString(out *bytes.Buffer, s string) {
	out.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"':
			out.WriteString(`\"`)
		case '\\':
			out.WriteString(`\\`)
		case '\b':
			out.WriteString(`\b`)
		case '\t':
			out.WriteString(`\t`)
		case '\n':
			out.WriteString(`\n`)
		case '\f':
			out.WriteString(`\f`)
		case '\r':
			out.WriteString(`\r`)
		default:
			if c < 0x20 {
				fmt.Fprintf(out, `\u%04x`, c)
			} else {
				out.WriteByte(c)
			}
		}
	}
	out.WriteByte('"')
}

// lessUTF16 按 UTF-16 码元序比较（RFC 8785 §3.2.3）。
func lessUTF16(a, b string) bool {
	ua, ub := utf16.Encode([]rune(a)), utf16.Encode([]rune(b))
	for i := 0; i < len(ua) && i < len(ub); i++ {
		if ua[i] != ub[i] {
			return ua[i] < ub[i]
		}
	}
	return len(ua) < len(ub)
}
