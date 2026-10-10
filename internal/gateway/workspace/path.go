package workspace

import (
	"path"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Path limits (design §4.3).
const (
	MaxPathBytes    = 512
	MaxPathSegments = 32
	MaxSegmentBytes = 255 // NAME_MAX: a longer segment cannot be staged into the exec environment
)

// reservedDir is reserved by the exec staging layout (/in/.agentbox holds the command); workspace files never use it.
const reservedDir = ".agentbox"

// ValidPath reports whether p is an acceptable workspace file path. It matches what exec staging can stage (so a
// workspace can always be run): relative, canonical (path.Clean leaves it unchanged, so no "a//b", "./a", trailing
// "/"), no "." or ".." segment, no NUL, no backslash, no Unicode control (Cc, including C1) or format (Cf)
// characters (bidi overrides and isolates, zero-width characters, BOM, soft hyphen), valid UTF-8, at most MaxPathBytes bytes,
// MaxPathSegments segments and MaxSegmentBytes bytes per segment, and not under the reserved ".agentbox" directory.
// The root itself ("" or ".") is not a file path.
//
// Paths are only ever used as keys of the manifest and joined under /in/ws inside an exec environment; they are never
// resolved against a host directory, so this check is about stageable canonical names, not about filesystem safety.
func ValidPath(p string) bool {
	if p == "" || p == "." || len(p) > MaxPathBytes || !utf8.ValidString(p) || strings.ContainsRune(p, '\\') ||
		strings.HasPrefix(p, "/") || path.Clean(p) != p {
		return false
	}
	for _, r := range p {
		if unicode.In(r, unicode.Cc, unicode.Cf) {
			return false
		}
	}
	segs := strings.Split(p, "/")
	if len(segs) > MaxPathSegments || segs[0] == reservedDir {
		return false
	}
	for _, s := range segs {
		if s == ".." || s == "." || s == "" || len(s) > MaxSegmentBytes {
			return false
		}
	}
	return true
}

// validDir accepts "" (the root) or a ValidPath.
func validDir(p string) bool { return p == "" || ValidPath(p) }

// under reports whether file p is inside directory dir ("" = root).
func under(p, dir string) bool { return dir == "" || strings.HasPrefix(p, dir+"/") }
