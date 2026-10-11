package tenant

import (
	"context"
	"slices"
	"testing"
)

func TestParseScopes(t *testing.T) {
	for _, tc := range []struct {
		in   []string
		want Scopes
		ok   bool
	}{
		{[]string{"runtime"}, Runtime, true},
		{[]string{"models"}, Models, true},
		{[]string{"models", "runtime"}, AllScopes, true},
		{nil, 0, false},
		{[]string{}, 0, false},
		{[]string{"admin"}, 0, false},
		{[]string{"runtime", "runtime"}, 0, false},
		{[]string{"Runtime"}, 0, false},
	} {
		got, err := ParseScopes(tc.in)
		if (err == nil) != tc.ok || got != tc.want {
			t.Errorf("ParseScopes(%v) = %v, %v; want %v ok=%v", tc.in, got, err, tc.want, tc.ok)
		}
	}
	if n := AllScopes.Names(); !slices.Equal(n, []string{"runtime", "models"}) {
		t.Errorf("Names = %v", n)
	}
	if !AllScopes.Has(Runtime) || Models.Has(Runtime) || !Runtime.Has(0) {
		t.Error("Has")
	}
}

func TestPrincipalContext(t *testing.T) {
	if _, ok := FromContext(context.Background()); ok {
		t.Fatal("empty context has no principal")
	}
	p := Principal{OrgID: 3, UserID: 7, KeyID: "k_x", Scopes: Models, Role: Member}
	got, ok := FromContext(WithPrincipal(context.Background(), p))
	if !ok || got != p || !got.IsKey() {
		t.Fatalf("FromContext = %+v, %v", got, ok)
	}
}

func TestValidateOrgName(t *testing.T) {
	for _, ok := range []string{"acme", "a1", "acme-labs.cn", "x_y"} {
		if err := ValidateOrgName(ok); err != nil {
			t.Errorf("%q: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "a", "Acme", "-x", "operator", "user-12", "has space", "中文"} {
		if ValidateOrgName(bad) == nil {
			t.Errorf("%q should be rejected", bad)
		}
	}
	if PersonalOrgName(42) != "user-42" || !IsPersonalOrg("user-42", 42) || IsPersonalOrg("user-42", 4) {
		t.Error("personal org name")
	}
}

// TestAuthorizationMatrix: member / owner / operator / API key against keys, members and orgs.
func TestAuthorizationMatrix(t *testing.T) {
	const org, other = 10, 20
	member := Principal{OrgID: org, UserID: 1, Role: Member, Scopes: AllScopes}
	owner := Principal{OrgID: org, UserID: 2, Role: Owner, Scopes: AllScopes}
	op := Principal{Operator: true, Scopes: AllScopes}
	key := Principal{OrgID: org, UserID: 2, Role: Owner, KeyID: "k_a", Scopes: AllScopes}
	ownKey := Key{ID: "k_1", OrgID: org, UserID: 1}
	ownerKey := Key{ID: "k_2", OrgID: org, UserID: 2}
	foreign := Key{ID: "k_3", OrgID: other, UserID: 3}
	for _, tc := range []struct {
		name string
		p    Principal
		k    Key
		want bool
	}{
		{"member own", member, ownKey, true},
		{"member other's", member, ownerKey, false},
		{"member foreign", member, foreign, false},
		{"owner org's", owner, ownKey, true},
		{"owner foreign", owner, foreign, false},
		{"operator foreign", op, foreign, true},
		{"key own", key, ownerKey, false},
		{"key org's", key, ownKey, false},
	} {
		if got := CanSeeKey(tc.p, tc.k); got != tc.want {
			t.Errorf("CanSeeKey %s = %v", tc.name, got)
		}
	}
	if f, ok := KeyListFilter(member); !ok || f != (KeyFilter{OrgID: org, UserID: 1}) {
		t.Errorf("member filter %+v", f)
	}
	if f, ok := KeyListFilter(owner); !ok || f != (KeyFilter{OrgID: org}) {
		t.Errorf("owner filter %+v", f)
	}
	if f, ok := KeyListFilter(op); !ok || f != (KeyFilter{}) {
		t.Errorf("operator filter %+v", f)
	}
	if _, ok := KeyListFilter(key); ok {
		t.Error("a key cannot list keys")
	}
	self := Membership{OrgID: org, UserID: 1}
	if !CanCreateKeyFor(member, self) || CanCreateKeyFor(owner, self) || !CanCreateKeyFor(op, self) ||
		CanCreateKeyFor(Principal{OrgID: org, UserID: 1, KeyID: "k_b", Scopes: AllScopes}, self) {
		t.Error("CanCreateKeyFor: users mint for themselves, the operator for anyone, a key never")
	}
	if CanManageMembers(member, org) || !CanManageMembers(owner, org) || CanManageMembers(owner, other) ||
		!CanManageMembers(op, other) || CanManageMembers(key, org) {
		t.Error("CanManageMembers")
	}
	if CanCreateOrg(owner) || !CanCreateOrg(op) || CanCreateOrg(Principal{Operator: true, KeyID: "k_c"}) {
		t.Error("CanCreateOrg")
	}
}
