package workspace

import (
	"path"
	"strings"
	"unicode/utf8"
)

// Path limits (design §4.3).
const (
	MaxPathBytes    = 512
	MaxPathSegments = 32
)

// ValidPath reports whether p is an acceptable workspace file path: relative, canonical (path.Clean leaves it
// unchanged, so no "a//b", "./a", trailing "/"), no ".." segment, no NUL, no backslash, valid UTF-8, at most
// MaxPathBytes bytes and MaxPathSegments segments. The root itself ("" or ".") is not a file path.
//
// Paths are only ever used as keys of the manifest and joined under /in/ws inside an exec environment; they are never
// resolved against a host directory, so this check is about canonical names, not about filesystem safety.
func ValidPath(p string) bool {
	if p == "" || p == "." || len(p) > MaxPathBytes || !utf8.ValidString(p) || strings.ContainsAny(p, "\x00\\") ||
		strings.HasPrefix(p, "/") || path.Clean(p) != p {
		return false
	}
	segs := strings.Split(p, "/")
	if len(segs) > MaxPathSegments {
		return false
	}
	for _, s := range segs {
		if s == ".." || s == "." || s == "" {
			return false
		}
	}
	return true
}

// validDir accepts "" (the root) or a ValidPath.
func validDir(p string) bool { return p == "" || ValidPath(p) }

// under reports whether file p is inside directory dir ("" = root).
func under(p, dir string) bool { return dir == "" || strings.HasPrefix(p, dir+"/") }
