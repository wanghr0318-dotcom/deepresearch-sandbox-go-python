package llmproto

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// object decodes a JSON object into its raw fields; field names the object in errors.
func object(raw json.RawMessage, field string) (map[string]json.RawMessage, error) {
	var obj map[string]json.RawMessage
	if len(bytes.TrimSpace(raw)) == 0 || json.Unmarshal(raw, &obj) != nil || obj == nil {
		if field == "" {
			return nil, invalidf("", "body must be a JSON object")
		}
		return nil, invalidf(field, "must be an object")
	}
	return obj, nil
}

// array decodes a JSON array into its raw elements.
func array(raw json.RawMessage, field string) ([]json.RawMessage, error) {
	var xs []json.RawMessage
	if isNull(raw) || json.Unmarshal(raw, &xs) != nil {
		return nil, invalidf(field, "must be an array")
	}
	return xs, nil
}

// checkAllowed rejects any field of obj outside allowed with an *UnsupportedFieldError.
func checkAllowed(obj map[string]json.RawMessage, prefix string, allowed ...string) error {
	var bad []string
	for k := range obj {
		ok := false
		for _, a := range allowed {
			if k == a {
				ok = true
				break
			}
		}
		if !ok {
			bad = append(bad, k)
		}
	}
	if len(bad) == 0 {
		return nil
	}
	// Report the smallest name so the error is deterministic.
	minName := bad[0]
	for _, b := range bad[1:] {
		if b < minName {
			minName = b
		}
	}
	return unsupported(join(prefix, minName), "unknown field")
}

func join(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + "." + name
}

func idx(prefix string, i int) string { return fmt.Sprintf("%s[%d]", prefix, i) }

func isNull(raw json.RawMessage) bool {
	t := bytes.TrimSpace(raw)
	return len(t) == 0 || string(t) == "null"
}

func isString(raw json.RawMessage) bool { return bytes.HasPrefix(bytes.TrimSpace(raw), []byte(`"`)) }

func isArray(raw json.RawMessage) bool { return bytes.HasPrefix(bytes.TrimSpace(raw), []byte(`[`)) }

func str(raw json.RawMessage, field string) (string, error) {
	var s string
	if !isString(raw) || json.Unmarshal(raw, &s) != nil {
		return "", invalidf(field, "must be a string")
	}
	return s, nil
}

// optStr is a string that may be absent or null.
func optStr(raw json.RawMessage, field string) (string, error) {
	if isNull(raw) {
		return "", nil
	}
	return str(raw, field)
}

func boolean(raw json.RawMessage, field string) (bool, error) {
	switch strings.TrimSpace(string(raw)) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	}
	return false, invalidf(field, "must be a boolean")
}

func positiveInt(raw json.RawMessage, field string) (int64, error) {
	n, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil || n <= 0 {
		return 0, invalidf(field, "must be a positive integer")
	}
	return n, nil
}

func number(raw json.RawMessage, field string) (*float64, error) {
	if isNull(raw) {
		return nil, nil
	}
	var f float64
	if json.Unmarshal(raw, &f) != nil {
		return nil, invalidf(field, "must be a number")
	}
	return &f, nil
}

// compactJSON returns raw compacted, or an error when it is not valid JSON.
func compactJSON(raw []byte) (string, error) {
	var b bytes.Buffer
	if err := json.Compact(&b, raw); err != nil {
		return "", err
	}
	return b.String(), nil
}

// clone copies a raw JSON value (decoded values must not alias the caller's buffer).
func clone(raw json.RawMessage) json.RawMessage {
	if raw == nil {
		return nil
	}
	return append(json.RawMessage(nil), raw...)
}

// marshalNoEscape marshals v without HTML escaping (model text often contains <, > and &).
func marshalNoEscape(v any) ([]byte, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(b.Bytes(), []byte("\n")), nil
}

func mustMarshalNoEscape(v any) []byte {
	b, err := marshalNoEscape(v)
	if err != nil {
		panic("llmproto: " + err.Error()) // only values of this package's own types are marshalled
	}
	return b
}
