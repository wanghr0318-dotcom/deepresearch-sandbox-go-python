package obs

// W3C Trace Context `traceparent` (https://www.w3.org/TR/trace-context/#traceparent-header), version 00 only:
//
//	00-<32 lowercase hex trace-id>-<16 lowercase hex parent-id>-<2 lowercase hex flags>
//
// The all-zero trace-id and parent-id are invalid; version ff is invalid. Higher versions are not accepted
// here (the host only ever produces 00, and the sandbox is untrusted: anything unexpected is dropped).
// worker/agentbox_worker/tracecontext.py implements the same rules; the two share test vectors.

// TraceContext is a parsed traceparent.
type TraceContext struct {
	TraceID, ParentID string
	Flags             byte
}

// Sampled reports the sampled flag.
func (t TraceContext) Sampled() bool { return t.Flags&1 == 1 }

// ParseTraceparent parses a version-00 traceparent. ok is false for any malformed value.
func ParseTraceparent(s string) (tc TraceContext, ok bool) {
	if len(s) != 55 || s[2] != '-' || s[35] != '-' || s[52] != '-' || s[:2] != "00" {
		return TraceContext{}, false
	}
	trace, parent, flags := s[3:35], s[36:52], s[53:55]
	if !lowerHex(trace) || !lowerHex(parent) || !lowerHex(flags) || allZero(trace) || allZero(parent) {
		return TraceContext{}, false
	}
	return TraceContext{TraceID: trace, ParentID: parent, Flags: hexByte(flags)}, true
}

// FormatTraceparent formats a version-00 traceparent (inputs are assumed valid).
func FormatTraceparent(tc TraceContext) string {
	const digits = "0123456789abcdef"
	return "00-" + tc.TraceID + "-" + tc.ParentID + "-" + string([]byte{digits[tc.Flags>>4], digits[tc.Flags&0xf]})
}

func lowerHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func allZero(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] != '0' {
			return false
		}
	}
	return true
}

func hexByte(s string) byte {
	v := func(c byte) byte {
		if c <= '9' {
			return c - '0'
		}
		return c - 'a' + 10
	}
	return v(s[0])<<4 | v(s[1])
}
