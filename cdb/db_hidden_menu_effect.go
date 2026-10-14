package cdb

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

// The effect of the menu entries a group hides on its members. An entry is
// hidden to a user only when every organizational group of the user hides it
// (HiddenMenuEntriesOf), so a group hiding an entry hides it from none of its
// members whose other groups, their personal group included, show it.

// MenuEffectGroup is a group of a member that keeps an entry shown to them.
type MenuEffectGroup struct {
	ID   int64
	Role string
	// Personal tells the member's own group, user_<id>, as the historical
	// collector creates one for every account.
	Personal bool
}

// MenuEffectMember is a member of the group, and what an entry is to them.
type MenuEffectMember struct {
	UserID int64
	Name   string
	Email  string
	// Hidden tells the member does not see the entry: all their groups hide it.
	Hidden bool
	// KeptBy are the groups of the member showing the entry, when not hidden.
	KeptBy []MenuEffectGroup
}

// MenuEffectEntry is an entry the group hides, and its members.
type MenuEffectEntry struct {
	MenuEntry string
	Members   []MenuEffectMember
}

// GroupHiddenMenuEffect returns, for each menu entry the group hides, its members
// and whether each really loses it, or which of their groups keep it shown. The
// count of members is returned apart: a group hiding nothing has no entry.
func (oDb *DB) GroupHiddenMenuEffect(ctx context.Context, groupID int64) ([]MenuEffectEntry, int, error) {
	hiddenBy := map[int64]map[string]bool{}
	readHidden := func(groupIDs []any) error {
		rows, err := oDb.DB.QueryContext(ctx, `SELECT group_id, menu_entry FROM group_hidden_menu_entries
			WHERE menu_entry IS NOT NULL AND group_id IN (`+Placeholders(len(groupIDs))+`)`, groupIDs...)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var id int64
			var entry string
			if err := rows.Scan(&id, &entry); err != nil {
				return err
			}
			if hiddenBy[id] == nil {
				hiddenBy[id] = map[string]bool{}
			}
			hiddenBy[id][entry] = true
		}
		return rows.Err()
	}
	if err := readHidden([]any{groupID}); err != nil {
		return nil, 0, fmt.Errorf("groupHiddenMenuEffect: %w", err)
	}

	// The members, with their name.
	type member struct {
		MenuEffectMember
		groups []MenuEffectGroup
	}
	var members []*member
	byID := map[int64]*member{}
	rows, err := oDb.DB.QueryContext(ctx, `SELECT u.id, TRIM(CONCAT(COALESCE(u.first_name, ''), ' ', COALESCE(u.last_name, ''))),
		COALESCE(u.email, '') FROM auth_membership am JOIN auth_user u ON u.id = am.user_id
		WHERE am.group_id = ? ORDER BY 2, 3`, groupID)
	if err != nil {
		return nil, 0, fmt.Errorf("groupHiddenMenuEffect members: %w", err)
	}
	for rows.Next() {
		m := &member{}
		if err := rows.Scan(&m.UserID, &m.Name, &m.Email); err != nil {
			_ = rows.Close()
			return nil, 0, fmt.Errorf("groupHiddenMenuEffect members: %w", err)
		}
		members = append(members, m)
		byID[m.UserID] = m
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("groupHiddenMenuEffect members: %w", err)
	}
	if len(hiddenBy[groupID]) == 0 || len(members) == 0 {
		return []MenuEffectEntry{}, len(members), nil
	}

	// Their organizational groups, as HiddenMenuEntriesOf counts them.
	userIDs := make([]any, 0, len(members))
	for _, m := range members {
		userIDs = append(userIDs, m.UserID)
	}
	rows, err = oDb.DB.QueryContext(ctx, `SELECT am.user_id, g.id, g.role FROM auth_membership am
		JOIN auth_group g ON g.id = am.group_id
		WHERE am.user_id IN (`+Placeholders(len(userIDs))+`)
		AND g.role <> 'Everybody' AND COALESCE(g.privilege, '') <> 'T'`, userIDs...)
	if err != nil {
		return nil, 0, fmt.Errorf("groupHiddenMenuEffect groups: %w", err)
	}
	others := map[int64]bool{}
	for rows.Next() {
		var userID int64
		var g MenuEffectGroup
		var role sql.NullString
		if err := rows.Scan(&userID, &g.ID, &role); err != nil {
			_ = rows.Close()
			return nil, 0, fmt.Errorf("groupHiddenMenuEffect groups: %w", err)
		}
		g.Role = role.String
		g.Personal = g.Role == fmt.Sprintf("user_%d", userID)
		if m := byID[userID]; m != nil {
			m.groups = append(m.groups, g)
		}
		if g.ID != groupID {
			others[g.ID] = true
		}
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("groupHiddenMenuEffect groups: %w", err)
	}
	if len(others) > 0 {
		ids := make([]any, 0, len(others))
		for id := range others {
			ids = append(ids, id)
		}
		if err := readHidden(ids); err != nil {
			return nil, 0, fmt.Errorf("groupHiddenMenuEffect: %w", err)
		}
	}

	entries := make([]string, 0, len(hiddenBy[groupID]))
	for entry := range hiddenBy[groupID] {
		entries = append(entries, entry)
	}
	sort.Strings(entries)
	out := make([]MenuEffectEntry, 0, len(entries))
	for _, entry := range entries {
		e := MenuEffectEntry{MenuEntry: entry, Members: make([]MenuEffectMember, 0, len(members))}
		for _, m := range members {
			em := m.MenuEffectMember
			em.KeptBy = []MenuEffectGroup{}
			for _, g := range m.groups {
				if !hiddenBy[g.ID][entry] {
					em.KeptBy = append(em.KeptBy, g)
				}
			}
			sort.Slice(em.KeptBy, func(i, j int) bool {
				return strings.ToLower(em.KeptBy[i].Role) < strings.ToLower(em.KeptBy[j].Role)
			})
			em.Hidden = len(em.KeptBy) == 0
			e.Members = append(e.Members, em)
		}
		out = append(out, e)
	}
	return out, len(members), nil
}
