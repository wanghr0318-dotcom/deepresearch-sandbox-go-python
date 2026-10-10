package doctor

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// A server flags file holds `agentbox server` flags, one per line: `--name=value`, `--name value` or `--name`
// (boolean); blank lines and lines starting with # are kept as they are. Scripts expand it into argv
// (scripts/demo-doctor.sh). The doctor reads it to learn the current configuration and writes a modified copy for
// an experiment run; it never rewrites the input.

// Flags is a parsed flags file that preserves order and comments.
type Flags struct {
	lines []flagLine
}

type flagLine struct {
	raw   string // comments and blank lines
	name  string // without leading dashes; empty for raw lines
	value string
	bare  bool // --name without a value
}

// ParseFlags parses a flags file.
func ParseFlags(data []byte) (*Flags, error) {
	f := &Flags{}
	text := strings.ReplaceAll(string(data), "\r\n", "\n")
	text = strings.TrimSuffix(text, "\n")
	if text == "" {
		return f, nil
	}
	for i, l := range strings.Split(text, "\n") {
		t := strings.TrimSpace(l)
		if t == "" || strings.HasPrefix(t, "#") {
			f.lines = append(f.lines, flagLine{raw: l})
			continue
		}
		if !strings.HasPrefix(t, "-") {
			return nil, fmt.Errorf("line %d: %q is not a flag (want --name=value)", i+1, t)
		}
		name := strings.TrimLeft(t, "-")
		var value string
		bare := true
		if n, v, ok := strings.Cut(name, "="); ok {
			name, value, bare = n, v, false
		} else if n, v, ok := strings.Cut(name, " "); ok {
			name, value, bare = n, strings.TrimSpace(v), false
		}
		if name == "" || strings.ContainsAny(name, " \t") {
			return nil, fmt.Errorf("line %d: bad flag %q", i+1, t)
		}
		f.lines = append(f.lines, flagLine{name: name, value: value, bare: bare})
	}
	return f, nil
}

// Get returns the value of the last occurrence of a flag (name with or without leading dashes).
func (f *Flags) Get(name string) (string, bool) {
	name = strings.TrimLeft(name, "-")
	for i := len(f.lines) - 1; i >= 0; i-- {
		if f.lines[i].name == name {
			return f.lines[i].value, true
		}
	}
	return "", false
}

// Set replaces the value of every occurrence of the flag, or appends it after a comment line.
func (f *Flags) Set(name, value, comment string) {
	name, value = SafeText(strings.TrimLeft(name, "-")), SafeText(value)
	found := false
	for i := range f.lines {
		if f.lines[i].name == name {
			f.lines[i].value, f.lines[i].bare = value, false
			found = true
		}
	}
	if found {
		return
	}
	if comment = SafeText(comment); comment != "" {
		f.lines = append(f.lines, flagLine{raw: "# " + comment})
	}
	f.lines = append(f.lines, flagLine{name: name, value: value})
}

// SafeText flattens text that ends up in a generated file or report onto one line: control characters (newlines
// included), Unicode line/paragraph separators and format characters (e.g. bidi overrides) become spaces and runs of
// whitespace collapse. Model replies and journal fields pass through it before they reach experiment.flags,
// proposal.patch or findings.md, so they can never start a new flag line or a new Markdown block.
func SafeText(s string) string {
	b := strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || unicode.In(r, unicode.Zl, unicode.Zp) || r == utf8.RuneError {
			return ' '
		}
		return r
	}, s)
	return strings.Join(strings.Fields(b), " ")
}

// Clone returns a deep copy.
func (f *Flags) Clone() *Flags {
	return &Flags{lines: append([]flagLine(nil), f.lines...)}
}

// Bytes formats the file (LF line endings, `--name=value`).
func (f *Flags) Bytes() []byte {
	var b strings.Builder
	for _, l := range f.lines {
		switch {
		case l.name == "":
			b.WriteString(l.raw)
		case l.bare:
			b.WriteString("--" + l.name)
		default:
			b.WriteString("--" + l.name + "=" + l.value)
		}
		b.WriteByte('\n')
	}
	return []byte(b.String())
}

// Args returns the flags as an argv slice (`--name=value`).
func (f *Flags) Args() []string {
	var out []string
	for _, l := range f.lines {
		switch {
		case l.name == "":
		case l.bare:
			out = append(out, "--"+l.name)
		default:
			out = append(out, "--"+l.name+"="+l.value)
		}
	}
	return out
}

// ---- allowlist ----

type valueKind int

const (
	durationValue valueKind = iota
	intValue
)

// flagSpec is one flag the doctor may change. Durations are bounded in milliseconds.
type flagSpec struct {
	name      string
	kind      valueKind
	min, max  int64
	allowZero bool   // 0 (off) is allowed besides [min, max]
	def       string // the server default (for display and as the current value when absent)
	meaning   string
}

// allowlist is the complete set of flags a proposal may change (design §6.3).
var allowlist = []flagSpec{
	{name: "model-try-timeout", kind: durationValue, min: 1000, max: 120_000, allowZero: true, def: "0s",
		meaning: "per-try timeout of model calls in a fallback chain (0 = off)"},
	{name: "model-hedge-delay", kind: durationValue, min: 200, max: 60_000, allowZero: true, def: "0s",
		meaning: "delay before a hedged second model request (0 = off)"},
	{name: "model-breaker-failures", kind: intValue, min: 1, max: 10, def: "3",
		meaning: "consecutive failures that open a provider's circuit breaker"},
	{name: "model-breaker-open", kind: durationValue, min: 5000, max: 600_000, def: "30s",
		meaning: "how long an open breaker skips its provider"},
	{name: "call-deadline", kind: durationValue, min: 5000, max: 300_000, def: "2m0s",
		meaning: "deadline of search and fetch calls (queue, backoff and all tries)"},
	{name: "model-call-deadline", kind: durationValue, min: 30_000, max: 900_000, def: "5m0s",
		meaning: "deadline of model calls"},
	{name: "turn-tool-budget", kind: intValue, min: 1, max: 1000, def: "30",
		meaning: "web_search + web_fetch calls per session turn"},
}

func specFor(name string) (flagSpec, bool) {
	name = strings.TrimLeft(name, "-")
	for _, s := range allowlist {
		if s.name == name {
			return s, true
		}
	}
	return flagSpec{}, false
}

// parseValue returns the numeric value (ms for durations).
func (s flagSpec) parseValue(v string) (int64, error) {
	v = strings.TrimSpace(v)
	if s.kind == intValue {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("--%s: %q is not an integer", s.name, v)
		}
		return n, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("--%s: %q is not a duration", s.name, v)
	}
	return d.Milliseconds(), nil
}

// format renders a numeric value in the flag's syntax.
func (s flagSpec) format(n int64) string {
	if s.kind == intValue {
		return strconv.FormatInt(n, 10)
	}
	return formatMs(n)
}

// formatMs renders milliseconds as a Go duration ("10s", "2m0s", "250ms"), which the server's flag parser reads.
func formatMs(ms int64) string {
	return (time.Duration(ms) * time.Millisecond).String()
}

// errNotAllowed is returned for flags outside the allowlist.
var errNotAllowed = errors.New("flag is not on the doctor's allowlist")

// Validate checks a proposed value against the allowlist and returns it normalised.
func Validate(flag, value string) (string, error) {
	s, ok := specFor(flag)
	if !ok {
		return "", fmt.Errorf("%s: %w", flag, errNotAllowed)
	}
	n, err := s.parseValue(value)
	if err != nil {
		return "", err
	}
	if n == 0 && s.allowZero {
		return s.format(0), nil
	}
	if n < s.min || n > s.max {
		return "", fmt.Errorf("--%s=%s is outside [%s, %s]", s.name, value, s.format(s.min), s.format(s.max))
	}
	return s.format(n), nil
}

// current returns the flag's current numeric value from the config (or the server default) and its display text.
func current(cfg *Flags, name string) (int64, string) {
	s, _ := specFor(name)
	if cfg != nil {
		if v, ok := cfg.Get(name); ok {
			if n, err := s.parseValue(v); err == nil {
				return n, v
			}
		}
	}
	n, _ := s.parseValue(s.def)
	return n, "default " + s.def
}

// clampMs bounds ms to [lo, hi] and rounds up to whole seconds when ≥ 1 s.
func clampMs(ms, lo, hi int64) int64 {
	if ms >= 1000 {
		ms = int64(math.Ceil(float64(ms)/1000)) * 1000
	}
	return max(lo, min(hi, ms))
}
