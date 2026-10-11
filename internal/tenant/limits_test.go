package tenant

import "testing"

// TestLimitsAuthorization: owners read and write their org's users and keys and read (not write) the org itself;
// members and API keys never; the operator everything; nobody sees another org.
func TestLimitsAuthorization(t *testing.T) {
	op := Principal{Operator: true, Scopes: AllScopes}
	owner := Principal{OrgID: 1, UserID: 10, Role: Owner, Scopes: AllScopes}
	member := Principal{OrgID: 1, UserID: 11, Role: Member, Scopes: AllScopes}
	key := Principal{OrgID: 1, UserID: 10, Role: Owner, KeyID: "k_x", Scopes: AllScopes}
	for _, c := range []struct {
		name                   string
		p                      Principal
		org                    int64
		read, writeOrg, writeU bool
	}{
		{"operator", op, 1, true, true, true},
		{"owner own org", owner, 1, true, false, true},
		{"owner other org", owner, 2, false, false, false},
		{"member", member, 1, false, false, false},
		{"api key of an owner", key, 1, false, false, false},
	} {
		if got := CanReadLimits(c.p, c.org); got != c.read {
			t.Errorf("%s read = %v", c.name, got)
		}
		if got := CanWriteLimits(c.p, true, c.org); got != c.writeOrg {
			t.Errorf("%s write org = %v", c.name, got)
		}
		if got := CanWriteLimits(c.p, false, c.org); got != c.writeU {
			t.Errorf("%s write user/key = %v", c.name, got)
		}
	}
}
