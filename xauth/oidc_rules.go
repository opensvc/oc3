package xauth

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ClaimRule translates the value of an OpenID Connect claim into access and a
// collector team (table auth_oidc_mappings, Claim mappings page).
type ClaimRule struct {
	ID    int64
	Claim string
	Value string
	// AllowAccess: the identities matching the rule may sign in. Once one such
	// rule exists, the others may not.
	AllowAccess bool
	// GroupIDs are the teams the rule grants, GroupRoles their names, in the
	// same order; none when the rule only allows signing in.
	GroupIDs   []int64
	GroupRoles []string
}

// RuleOutcome is what the claim rules make of an identity.
type RuleOutcome struct {
	// AccessRules tells whether at least one rule decides access.
	AccessRules bool
	// Allowed tells whether a rule allowing access matches.
	Allowed bool
	// Granted are the teams the matching rules grant; Managed every team a rule
	// names, which the claims decide.
	GrantedIDs, ManagedIDs     []int64
	GrantedRoles, ManagedRoles []string
}

// MayAccess tells whether the identity may sign in: no rule decides access, or
// one allowing it matches.
func (r RuleOutcome) MayAccess() bool {
	return !r.AccessRules || r.Allowed
}

// MayCreate tells whether an unknown identity may have its account created: a
// rule must explicitly allow its access, so that nobody is created by default.
func (r RuleOutcome) MayCreate() bool {
	return r.AccessRules && r.Allowed
}

// EvaluateClaimRules applies the rules to the claims of an identity.
func EvaluateClaimRules(rules []ClaimRule, claims map[string]any) RuleOutcome {
	var out RuleOutcome
	managed := map[int64]bool{}
	granted := map[int64]bool{}
	for _, rule := range rules {
		match := ClaimMatches(claims, rule.Claim, rule.Value)
		if rule.AllowAccess {
			out.AccessRules = true
			if match {
				out.Allowed = true
			}
		}
		for i, id := range rule.GroupIDs {
			role := rule.GroupRoles[i]
			if !managed[id] {
				managed[id] = true
				out.ManagedIDs = append(out.ManagedIDs, id)
				out.ManagedRoles = append(out.ManagedRoles, role)
			}
			if match && !granted[id] {
				granted[id] = true
				out.GrantedIDs = append(out.GrantedIDs, id)
				out.GrantedRoles = append(out.GrantedRoles, role)
			}
		}
	}
	return out
}

// ClaimMatches tells whether a claim equals the value or, for a list, contains it.
func ClaimMatches(claims map[string]any, name, value string) bool {
	for _, v := range ClaimValues(claims, name) {
		if v == value {
			return true
		}
	}
	return false
}

// ClaimValues returns the values of a claim as text: its own name first, then a
// dotted path into nested claims (realm_access.roles), a list giving one value
// per item. Numbers and booleans are written as JSON writes them.
func ClaimValues(claims map[string]any, name string) []string {
	v, ok := claims[name]
	if !ok && strings.Contains(name, ".") {
		var cur any = claims
		for _, part := range strings.Split(name, ".") {
			m, isMap := cur.(map[string]any)
			if !isMap {
				cur = nil
				break
			}
			cur = m[part]
		}
		v, ok = cur, cur != nil
	}
	if !ok {
		return nil
	}
	if list, isList := v.([]any); isList {
		out := make([]string, 0, len(list))
		for _, item := range list {
			if s, ok := scalarText(item); ok {
				out = append(out, s)
			}
		}
		return out
	}
	if s, ok := scalarText(v); ok {
		return []string{s}
	}
	return nil
}

func scalarText(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case bool:
		return strconv.FormatBool(t), true
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), true
	case int64:
		return strconv.FormatInt(t, 10), true
	}
	return "", false
}

// rulesTTL is how long the rules are kept between two reads of the database, for
// the Bearer requests; a change made through the API is seen at once.
const rulesTTL = 30 * time.Second

type ruleCache struct {
	mu      sync.Mutex
	rules   []ClaimRule
	expires time.Time
}

// queryClaimRules reads the rules with their teams, one row per team granted (or
// one row without team), grouped by rule in ClaimRules. A team that no longer
// exists is left out by the join.
const queryClaimRules = `SELECT m.id, m.claim, m.value, m.allow_access, COALESCE(g.id, 0), COALESCE(g.role, '')
	FROM auth_oidc_mappings m
	LEFT JOIN auth_oidc_mapping_groups mg ON mg.mapping_id = m.id
	LEFT JOIN auth_group g ON g.id = mg.group_id
	ORDER BY m.id, g.role`

// ClaimRules returns the claim rules, read from the database at most every
// rulesTTL.
func (o *OIDC) ClaimRules(ctx context.Context) ([]ClaimRule, error) {
	o.rules.mu.Lock()
	defer o.rules.mu.Unlock()
	if time.Now().Before(o.rules.expires) {
		return o.rules.rules, nil
	}
	if o.db == nil {
		return nil, nil
	}
	rows, err := o.db.QueryContext(ctx, queryClaimRules)
	if err != nil {
		return nil, fmt.Errorf("%w: claim rules: %w", ErrUnavailable, err)
	}
	defer func() { _ = rows.Close() }()
	rules := []ClaimRule{}
	for rows.Next() {
		var (
			r       ClaimRule
			access  sql.NullString
			groupID int64
			role    string
		)
		if err := rows.Scan(&r.ID, &r.Claim, &r.Value, &access, &groupID, &role); err != nil {
			return nil, fmt.Errorf("%w: claim rules: %w", ErrUnavailable, err)
		}
		r.AllowAccess = access.String == "T"
		if n := len(rules); n > 0 && rules[n-1].ID == r.ID {
			r = rules[n-1]
		} else {
			rules = append(rules, r)
		}
		if groupID != 0 {
			last := &rules[len(rules)-1]
			last.GroupIDs = append(last.GroupIDs, groupID)
			last.GroupRoles = append(last.GroupRoles, role)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: claim rules: %w", ErrUnavailable, err)
	}
	o.rules.rules = rules
	o.rules.expires = time.Now().Add(rulesTTL)
	return rules, nil
}

// ForgetClaimRules drops the cached rules: the next sign-in or Bearer request
// reads them again.
func (o *OIDC) ForgetClaimRules() {
	o.rules.mu.Lock()
	defer o.rules.mu.Unlock()
	o.rules.expires = time.Time{}
}

// displayedClaims leaves out of the claims those that only make sense to verify
// the token, keeping those a rule may use.
func displayedClaims(claims map[string]any) map[string]any {
	out := make(map[string]any, len(claims))
	for k, v := range claims {
		switch k {
		case "iss", "aud", "exp", "iat", "nbf", "nonce", "at_hash", "c_hash", "auth_time",
			"acr", "amr", "azp", "sid", "jti", "sub", "typ":
			continue
		}
		out[k] = v
	}
	return out
}
