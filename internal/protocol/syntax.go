package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// checkSyntax 在构建任何对象之前逐个读取记号，检查 JSON 结构限制（规格 §5.10）：
// 嵌套深度、重复键（按解码后的键名比较）与数字字面量。违反时为 malformed_json。
// json.Decoder.Token 不递归，读到第 MaxNestingDepth+1 层即停止，不会先解析整个输入。
func checkSyntax(line []byte) error {
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.UseNumber()
	var s syntaxScan
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return newError(CodeMalformedJSON, "%v", err)
		}
		if err := s.token(tok); err != nil {
			return err
		}
	}
	if !s.done {
		return newError(CodeMalformedJSON, "JSON 不完整")
	}
	return nil
}

type syntaxFrame struct {
	object    bool
	keys      map[string]struct{}
	expectKey bool
}

type syntaxScan struct {
	stack []syntaxFrame
	done  bool // 顶层值已结束
}

func (s *syntaxScan) token(tok json.Token) error {
	if s.done {
		return newError(CodeMalformedJSON, "顶层值之后还有内容")
	}
	if n := len(s.stack); n > 0 && s.stack[n-1].object && s.stack[n-1].expectKey {
		if tok == json.Delim('}') {
			s.close()
			return nil
		}
		return s.key(tok.(string)) // Decoder 保证对象中这一位置是字符串键
	}
	switch v := tok.(type) {
	case json.Delim:
		if v == '{' || v == '[' {
			return s.open(v == '{')
		}
		s.close()
		return nil
	case json.Number:
		if err := checkNumberLiteral(string(v)); err != nil {
			return err
		}
	}
	s.valueDone()
	return nil
}

func (s *syntaxScan) open(object bool) error {
	if len(s.stack) == MaxNestingDepth {
		return newError(CodeMalformedJSON, "嵌套超过 %d 层", MaxNestingDepth)
	}
	f := syntaxFrame{object: object, expectKey: object}
	if object {
		f.keys = map[string]struct{}{}
	}
	s.stack = append(s.stack, f)
	return nil
}

func (s *syntaxScan) close() {
	s.stack = s.stack[:len(s.stack)-1]
	s.valueDone()
}

func (s *syntaxScan) key(k string) error {
	f := &s.stack[len(s.stack)-1]
	if _, dup := f.keys[k]; dup {
		return newError(CodeMalformedJSON, "重复的键 %q", k)
	}
	f.keys[k] = struct{}{}
	f.expectKey = false
	return nil
}

func (s *syntaxScan) valueDone() {
	n := len(s.stack)
	if n == 0 {
		s.done = true
		return
	}
	if s.stack[n-1].object {
		s.stack[n-1].expectKey = true
	}
}

// checkNumberLiteral 检查数字字面量的长度（含负号、小数点、指数符号与指数正负号），
// 以及浮点字面量不上溢为无穷、非零值不下溢为零。整数字面量在自由格式字段中可以超出 int64。
func checkNumberLiteral(lit string) error {
	if len(lit) > MaxNumberLiteralBytes {
		return newError(CodeMalformedJSON, "数字字面量 %d 个字符，上限 %d", len(lit), MaxNumberLiteralBytes)
	}
	if !strings.ContainsAny(lit, ".eE") {
		return nil
	}
	f, err := strconv.ParseFloat(lit, 64)
	if err != nil || math.IsInf(f, 0) {
		return newError(CodeMalformedJSON, "数字 %s 超出 float64 范围", lit)
	}
	mantissa, _, _ := strings.Cut(strings.ToLower(lit), "e")
	if f == 0 && strings.ContainsAny(mantissa, "123456789") {
		return newError(CodeMalformedJSON, "数字 %s 下溢为零", lit)
	}
	return nil
}

// checkKeyCase 拒绝与已定义字段只差大小写的键（键名区分大小写）。按消息类型逐层检查：
// 顶层消息与其中的结构体字段；自由格式字段（json.RawMessage）不检查。键按字典序检查，
// 错误与字段声明顺序无关。比较采用 Unicode 简单大小写折叠（含 U+212A、U+017F）。
func checkKeyCase(raw json.RawMessage, t reflect.Type) error {
	var obj map[string]json.RawMessage
	if json.Unmarshal(raw, &obj) != nil {
		return nil // 不是对象：交给字段类型检查
	}
	fields := structFields(t)
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		nested, exact := fields[k]
		if exact {
			if nested != nil {
				if err := checkKeyCase(obj[k], nested); err != nil {
					return err
				}
			}
			continue
		}
		for name := range fields {
			if strings.EqualFold(k, name) {
				return newError(CodeInvalidField, "键 %q 与字段 %q 只差大小写", k, name)
			}
		}
	}
	return nil
}

var fieldCache sync.Map // reflect.Type -> map[string]reflect.Type

// structFields 返回结构体（含嵌入结构体）的 JSON 字段名；值为该字段的结构体类型，
// 非结构体字段为 nil。
func structFields(t reflect.Type) map[string]reflect.Type {
	if cached, ok := fieldCache.Load(t); ok {
		return cached.(map[string]reflect.Type)
	}
	fields := map[string]reflect.Type{}
	collectFields(t, fields)
	fieldCache.Store(t, fields)
	return fields
}

func collectFields(t reflect.Type, fields map[string]reflect.Type) {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Anonymous {
			collectFields(f.Type, fields)
			continue
		}
		name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if name == "" || name == "-" {
			continue
		}
		ft := f.Type
		if ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		if ft.Kind() == reflect.Struct {
			fields[name] = ft
		} else {
			fields[name] = nil
		}
	}
}
