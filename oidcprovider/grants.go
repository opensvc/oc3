package oidcprovider

import (
	"fmt"
	"path"
	"regexp"
	"slices"
	"strings"
)

// The om3 roles a rule may grant. The internal roles of the daemons (heartbeat,
// join, leave) are not given to users.
var (
	// unscopedRoles hold on the whole cluster.
	unscopedRoles = []string{"root", "prioritizer", "squatter", "blacklistadmin", "scraper"}
	// scopedRoles hold on the namespaces their selector names: a namespace, or a
	// glob pattern as "prod*" or "*".
	scopedRoles = []string{"admin", "operator", "guest"}
)

// namespaceSelector is a namespace name of om3, optionally a glob pattern.
var namespaceSelector = regexp.MustCompile(`^[a-zA-Z0-9*?\[\]_.-]{1,63}$`)

// ParseGrants splits a list of om3 grants, separated by spaces or commas, checks
// each and returns them sorted, without duplicates. The error names the first
// grant refused.
func ParseGrants(s string) ([]string, error) {
	fields := strings.FieldsFunc(s, func(r rune) bool { return r == ' ' || r == ',' || r == '\n' || r == '\t' })
	out := make([]string, 0, len(fields))
	for _, g := range fields {
		if err := checkGrant(g); err != nil {
			return nil, err
		}
		if !slices.Contains(out, g) {
			out = append(out, g)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no grant")
	}
	slices.Sort(out)
	return out, nil
}

func checkGrant(g string) error {
	role, selector, scoped := strings.Cut(g, ":")
	switch {
	case slices.Contains(unscopedRoles, role):
		if scoped {
			return fmt.Errorf("grant %s: the role %s takes no namespace", g, role)
		}
	case slices.Contains(scopedRoles, role):
		if !scoped {
			return nil
		}
		if !namespaceSelector.MatchString(selector) {
			return fmt.Errorf("grant %s: invalid namespace selector %q", g, selector)
		}
		if _, err := path.Match(selector, ""); err != nil {
			return fmt.Errorf("grant %s: invalid namespace pattern %q", g, selector)
		}
	default:
		return fmt.Errorf("grant %s: unknown role %s, expected one of %s", g, role,
			strings.Join(append(slices.Clone(unscopedRoles), scopedRoles...), ", "))
	}
	return nil
}

// MergeGrants returns the union of the grant lists, sorted.
func MergeGrants(lists ...[]string) []string {
	out := []string{}
	for _, l := range lists {
		for _, g := range l {
			if !slices.Contains(out, g) {
				out = append(out, g)
			}
		}
	}
	slices.Sort(out)
	return out
}
