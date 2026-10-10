package workspace

import (
	"path"
	"strings"
	"unicode"
	"unicode/utf8"
)

// invisibleFormat lists the format characters (part of Cf) rejected in paths: bidi controls (ALM, LRM, RLM,
// embeddings and overrides, isolates), BOM, soft hyphen and zero-width space — they make the displayed name differ
// from the real one. Zero-width (non-)joiners (U+200C, U+200D) and tag characters (U+E0020–U+E007F) occur in emoji
// sequences and some scripts and are allowed.
var invisibleFormat = map[rune]bool{
	0x061C: true, 0x200E: true, 0x200F: true, // ALM, LRM, RLM
	0x202A: true, 0x202B: true, 0x202C: true, 0x202D: true, 0x202E: true, // embeddings and overrides
	0x2066: true, 0x2067: true, 0x2068: true, 0x2069: true, // isolates
	0xFEFF: true, 0x00AD: true, 0x200B: true, // BOM, soft hyphen, zero-width space
}

// forbiddenRune reports characters rejected in new paths: controls (Cc, including C1) and invisibleFormat.
func forbiddenRune(r rune) bool { return unicode.Is(unicode.Cc, r) || invisibleFormat[r] }

// legacyForbiddenRune is what earlier versions rejected (C0 controls and DEL). State files are read with it only, so a
// path an earlier version accepted does not make an existing workspace lost after an upgrade; new writes and exec
// outputs are checked with forbiddenRune.
func legacyForbiddenRune(r rune) bool { return r < 0x20 || r == 0x7f }

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
// "/"), no "." or ".." segment, no NUL, no backslash, no Unicode control characters (Cc, including C1) and none of
// the invisible format characters in invisibleFormat (bidi controls, BOM, soft hyphen, zero-width space), valid UTF-8,
// at most MaxPathBytes bytes, MaxPathSegments segments and MaxSegmentBytes bytes per segment, and not under the
// reserved ".agentbox" directory.
// The root itself ("" or ".") is not a file path.
//
// Paths are only ever used as keys of the manifest and joined under /in/ws inside an exec environment; they are never
// resolved against a host directory, so this check is about stageable canonical names, not about filesystem safety.
func ValidPath(p string) bool { return validPath(p, forbiddenRune) }

// storedPath is the path check for state files: ValidPath, but characters are checked with legacyForbiddenRune.
func storedPath(p string) bool { return validPath(p, legacyForbiddenRune) }

func validPath(p string, forbidden func(rune) bool) bool {
	if p == "" || p == "." || len(p) > MaxPathBytes || !utf8.ValidString(p) || strings.ContainsRune(p, '\\') ||
		strings.HasPrefix(p, "/") || path.Clean(p) != p {
		return false
	}
	for _, r := range p {
		if forbidden(r) {
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
