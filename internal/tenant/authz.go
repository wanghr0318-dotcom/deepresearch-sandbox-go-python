package tenant

// Authorization rules of the management API (design §5, §8). An API key never manages keys, members, orgs, limits
// or budgets: a leaked key cannot mint new keys or raise its own limits. Those need a cookie session (member or
// owner) or the operator token.

// CanManage reports whether the caller may use management endpoints at all (keys, members, orgs, limits, budgets).
func CanManage(p Principal) bool { return !p.IsKey() }

// CanSeeKey reports whether the caller may list or revoke k: members their own keys, owners every key of their
// org, the operator all keys.
func CanSeeKey(p Principal, k Key) bool {
	switch {
	case !CanManage(p):
		return false
	case p.Operator:
		return true
	case p.Role == Owner && p.OrgID == k.OrgID:
		return true
	}
	return p.UserID == k.UserID && p.OrgID == k.OrgID
}

// KeyListFilter is the widest list the caller may see (ok false for API-key callers).
func KeyListFilter(p Principal) (KeyFilter, bool) {
	switch {
	case !CanManage(p):
		return KeyFilter{}, false
	case p.Operator:
		return KeyFilter{}, true
	case p.Role == Owner:
		return KeyFilter{OrgID: p.OrgID}, true
	}
	return KeyFilter{OrgID: p.OrgID, UserID: p.UserID}, true
}

// CanCreateKeyFor reports whether the caller may create a key for target: cookie users for themselves, the
// operator for anyone.
func CanCreateKeyFor(p Principal, target Membership) bool {
	switch {
	case !CanManage(p):
		return false
	case p.Operator:
		return true
	}
	return p.UserID == target.UserID
}

// CanManageMembers reports whether the caller may remove members of orgID (or revoke its invites): its owners and
// the operator.
func CanManageMembers(p Principal, orgID int64) bool {
	return CanManage(p) && (p.Operator || (p.Role == Owner && p.OrgID == orgID))
}

// CanAddMembers reports whether the caller may move a user into an org directly: the operator only. Users join an
// org only by accepting an invite (their consent).
func CanAddMembers(p Principal) bool { return CanManage(p) && p.Operator }

// CanInvite reports whether the caller may invite users into orgID: its owners, with a user session.
func CanInvite(p Principal, orgID int64) bool {
	return CanManage(p) && !p.Operator && p.Role == Owner && p.OrgID == orgID
}

// CanSeeInvite reports whether the caller may see or delete inv: the invited user (decline) and the owners of its
// org (revoke). Invites are not visible across orgs.
func CanSeeInvite(p Principal, inv Invite) bool {
	return CanManage(p) && !p.Operator && (p.UserID == inv.UserID || (p.Role == Owner && p.OrgID == inv.OrgID))
}

// CanCreateOrg reports whether the caller may create orgs: the operator only.
func CanCreateOrg(p Principal) bool { return CanManage(p) && p.Operator }
