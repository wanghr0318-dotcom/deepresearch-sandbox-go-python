// Package tenant holds the tenancy model of the model gateway (design 2026-10-11 §4–§5, §8): orgs, members and
// roles, the request Principal, and the pure authorization rules for managing keys and members. It does no I/O;
// storage is behind the Store interface (implemented in internal/persistence/postgres).
package tenant

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Role is a user's role in their org.
type Role string

const (
	Owner  Role = "owner"
	Member Role = "member"
)

// ParseRole accepts "owner" or "member".
func ParseRole(s string) (Role, error) {
	switch r := Role(s); r {
	case Owner, Member:
		return r, nil
	}
	return "", fmt.Errorf("tenant: role must be owner or member, got %q", s)
}

// Scopes is the set of API surfaces a credential may use.
type Scopes uint8

const (
	// Runtime covers sessions, tasks, research and their events.
	Runtime Scopes = 1 << iota
	// Models covers the model API (/v1/chat/completions, /v1/messages, /v1/models).
	Models

	// AllScopes is what cookie and operator callers have.
	AllScopes = Runtime | Models
)

var scopeNames = []struct {
	s    Scopes
	name string
}{{Runtime, "runtime"}, {Models, "models"}}

// ParseScopes parses a non-empty list of scope names without duplicates.
func ParseScopes(names []string) (Scopes, error) {
	if len(names) == 0 {
		return 0, errors.New("tenant: at least one scope (runtime, models) is required")
	}
	var out Scopes
	for _, n := range names {
		var s Scopes
		for _, e := range scopeNames {
			if e.name == n {
				s = e.s
			}
		}
		if s == 0 {
			return 0, fmt.Errorf("tenant: unknown scope %q (want runtime or models)", n)
		}
		if out&s != 0 {
			return 0, fmt.Errorf("tenant: duplicate scope %q", n)
		}
		out |= s
	}
	return out, nil
}

// Has reports whether every scope in want is present.
func (s Scopes) Has(want Scopes) bool { return s&want == want }

// Names returns the scope names in canonical order (runtime, models).
func (s Scopes) Names() []string {
	out := []string{}
	for _, e := range scopeNames {
		if s&e.s != 0 {
			out = append(out, e.name)
		}
	}
	return out
}

func (s Scopes) String() string { return strings.Join(s.Names(), ",") }

// Principal is the authenticated caller of a request.
type Principal struct {
	OrgID, UserID int64
	KeyID         string // empty for cookie and operator callers
	Scopes        Scopes // Runtime, Models
	Role          Role   // Owner, Member
	Operator      bool
}

// IsKey reports whether the caller authenticated with a platform API key.
func (p Principal) IsKey() bool { return p.KeyID != "" }

type principalKey struct{}

// WithPrincipal returns ctx carrying p.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// FromContext returns the Principal stored by WithPrincipal.
func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}

// OperatorOrg is the reserved org of tasks and sessions created with the operator token.
const OperatorOrg = "operator"

// PersonalOrgName is the name of the org every user gets at sign-up (and that the 0012 migration backfills).
func PersonalOrgName(userID int64) string { return "user-" + strconv.FormatInt(userID, 10) }

var (
	orgNameRe      = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{1,62}$`)
	personalNameRe = regexp.MustCompile(`^user-[0-9]+$`)
)

// ValidateOrgName checks a name for a new org: 2–63 chars of a-z 0-9 . _ -, starting with a letter or digit, and
// not one of the reserved forms (operator, user-<n>).
func ValidateOrgName(name string) error {
	if !orgNameRe.MatchString(name) {
		return errors.New("tenant: org name must be 2–63 characters of a-z, 0-9, '.', '_' or '-', starting with a letter or digit")
	}
	if name == OperatorOrg || personalNameRe.MatchString(name) {
		return fmt.Errorf("tenant: org name %q is reserved", name)
	}
	return nil
}

// IsPersonalOrg reports whether name is the personal org of userID.
func IsPersonalOrg(name string, userID int64) bool { return name == PersonalOrgName(userID) }

// Org is an organization.
type Org struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Disabled  bool      `json:"disabled"`
	CreatedAt time.Time `json:"created_at"`
	Members   int64     `json:"members"`
}

// Membership is a user's place in their org.
type Membership struct {
	OrgID    int64  `json:"org_id"`
	OrgName  string `json:"org"`
	UserID   int64  `json:"-"`
	Username string `json:"username"`
	Role     Role   `json:"role"`
}

// Key is the stored view of a platform API key (never the secret).
type Key struct {
	ID         string     `json:"id"`
	OrgID      int64      `json:"org_id"`
	UserID     int64      `json:"-"`
	Username   string     `json:"username"`
	Name       string     `json:"name"`
	Scopes     Scopes     `json:"-"`
	CreatedAt  time.Time  `json:"created_at"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
	RevokedAt  *time.Time `json:"revoked_at,omitempty"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
}

// NewKey is the input of Store.CreateKey: the ID and the HMAC of the secret are generated by the caller.
type NewKey struct {
	ID         string
	UserID     int64
	Name       string
	SecretHMAC []byte
	Scopes     Scopes
	ExpiresAt  *time.Time
}

// KeyFilter limits ListKeys; zero fields do not filter.
type KeyFilter struct {
	OrgID, UserID int64
}

// Business rejections of Store (HTTP 409 unless noted).
var (
	ErrUserInOtherOrg = errors.New("tenant: user already belongs to another org")
	ErrLastOwner      = errors.New("tenant: an org must keep at least one owner")
	ErrPersonalOrg    = errors.New("tenant: a user cannot leave their personal org")
)

// Store is the tenancy storage. Not-found is persistence.ErrNotFound and duplicates persistence.ErrConflict.
type Store interface {
	// CreateOrg creates an org with a validated name; an existing name is persistence.ErrConflict.
	CreateOrg(ctx context.Context, name string) (Org, error)
	ListOrgs(ctx context.Context) ([]Org, error)
	OrgByName(ctx context.Context, name string) (Org, error)
	// MemberByName returns the membership of a user by normalized (lower-case) username.
	MemberByName(ctx context.Context, usernameKey string) (Membership, error)
	// AddMember puts a user into org with role. A user already in org has their role updated. A user in their
	// personal org (alone) moves; their keys in the old org are revoked in the same transaction. A user in any
	// other org is ErrUserInOtherOrg. Demoting the last owner is ErrLastOwner.
	AddMember(ctx context.Context, orgID int64, usernameKey string, role Role) (Membership, error)
	// RemoveMember returns the user to their personal org (as owner) and revokes their keys in org. A user who is
	// not in org is persistence.ErrNotFound; removing the last owner is ErrLastOwner; org being the user's personal
	// org is ErrPersonalOrg.
	RemoveMember(ctx context.Context, orgID int64, usernameKey string) error
	// CreateKey stores a key for the user in the user's current org.
	CreateKey(ctx context.Context, k NewKey) (Key, error)
	ListKeys(ctx context.Context, f KeyFilter) ([]Key, error)
	GetKey(ctx context.Context, id string) (Key, error)
	// RevokeKey sets revoked_at (idempotent) and notifies apikey_changed.
	RevokeKey(ctx context.Context, id string) (Key, error)
}
