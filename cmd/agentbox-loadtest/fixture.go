package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/account"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/persistence/postgres"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/quota"
	"github.com/wanghr0318-dotcom/deepresearch-sandbox-go-python/internal/tenant"
)

// fixture is the tenancy a scenario runs as: orgs, their users, one models-scoped key per user.
type fixture struct {
	Orgs []orgFix
}

type orgFix struct {
	Name  string
	ID    int64
	Users []userFix
}

type userFix struct {
	Name  string
	ID    int64
	KeyID string
	Key   string `json:"-"`
}

// keys lists every key of the fixture, org by org.
func (f *fixture) keys() []userFix {
	var out []userFix
	for _, o := range f.Orgs {
		out = append(out, o.Users...)
	}
	return out
}

// env is what setup needs: the database (users, orgs, limits, budgets) and the operator API (keys).
type env struct {
	store *postgres.Store
	op    operator
}

// fixturePassword is the password of every load-test user (they never log in; it only has to be valid).
const fixturePassword = "LoadTest-0rg"

// newFixture creates orgs×users users, one org per row with the first user as owner and the rest as members (the
// operator path: no invite), and one models-scoped key per user. Users are created through the store, not
// /auth/register, which allows 5 registrations per minute per IP. Names start with prefix, so runs never collide.
func (e env) newFixture(ctx context.Context, prefix string, orgs, users int) (*fixture, error) {
	hash, err := account.HashPassword(fixturePassword)
	if err != nil {
		return nil, err
	}
	ten := e.store.Tenancy()
	f := &fixture{}
	for o := range orgs {
		of := orgFix{Name: fmt.Sprintf("%s-o%d", prefix, o)}
		for u := range users {
			display, key, err := account.NormalizeUsername(fmt.Sprintf("%s-o%du%d", prefix, o, u))
			if err != nil {
				return nil, err
			}
			usr, err := e.store.CreateUser(ctx, display, key, hash)
			if err != nil {
				return nil, fmt.Errorf("create user %s: %w", display, err)
			}
			if u == 0 {
				org, _, err := ten.CreateOrgWithOwner(ctx, of.Name, key)
				if err != nil {
					return nil, fmt.Errorf("create org %s: %w", of.Name, err)
				}
				of.ID = org.ID
			} else if _, err := ten.AddMember(ctx, of.ID, key, tenant.Member); err != nil {
				return nil, fmt.Errorf("add member %s: %w", display, err)
			}
			var k struct {
				ID  string `json:"id"`
				Key string `json:"key"`
			}
			if err := e.op.do(ctx, "POST", "/v1/keys", map[string]any{"name": "loadtest", "scopes": []string{"models"},
				"username": display}, &k); err != nil {
				return nil, err
			}
			of.Users = append(of.Users, userFix{Name: display, ID: usr.ID, KeyID: k.ID, Key: k.Key})
		}
		f.Orgs = append(f.Orgs, of)
	}
	return f, nil
}

// setKeyRPM sets a key's requests-per-minute limit.
func (e env) setKeyRPM(ctx context.Context, keyID string, rpm int64) error {
	return e.store.Quota().PutLimits(ctx, quota.Target{Level: quota.LevelKey, ID: keyID}, quota.Limits{RPM: &rpm})
}

// setAllLimits sets rpm and tpm (0 = leave unset) at every level of every key of f: the org, the user (scoped to
// the org) and the key. With any limit set, each request takes from the Redis Lua buckets of all three levels.
func (e env) setAllLimits(ctx context.Context, f *fixture, rpm, tpm int64) error {
	if rpm <= 0 && tpm <= 0 {
		return nil
	}
	var l quota.Limits
	if rpm > 0 {
		l.RPM = &rpm
	}
	if tpm > 0 {
		l.TPM = &tpm
	}
	q := e.store.Quota()
	for _, o := range f.Orgs {
		if err := q.PutLimits(ctx, quota.Target{Level: quota.LevelOrg, ID: strconv.FormatInt(o.ID, 10)}, l); err != nil {
			return err
		}
		for _, u := range o.Users {
			if err := q.PutLimits(ctx, quota.Target{Level: quota.LevelUser, ID: quota.UserTargetID(o.ID, u.ID)}, l); err != nil {
				return err
			}
			if err := q.PutLimits(ctx, quota.Target{Level: quota.LevelKey, ID: u.KeyID}, l); err != nil {
				return err
			}
		}
	}
	return nil
}

// setOrgBudget sets an org's monthly budget for the current month.
func (e env) setOrgBudget(ctx context.Context, orgID, limitMicro int64) error {
	_, err := e.store.Quota().PutBudget(ctx, quota.Target{Level: quota.LevelOrg, ID: strconv.FormatInt(orgID, 10)}, time.Time{}, &limitMicro)
	return err
}

// setKeyBudget sets a key's monthly budget for the current month.
func (e env) setKeyBudget(ctx context.Context, keyID string, limitMicro int64) error {
	_, err := e.store.Quota().PutBudget(ctx, quota.Target{Level: quota.LevelKey, ID: keyID}, time.Time{}, &limitMicro)
	return err
}
