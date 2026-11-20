package cdb

import (
	"context"
	"fmt"
	"testing"
)

// TestCompUsageVisibility checks the usage of a ruleset and of a moduleset
// names only what the caller may see: the modulesets and rulesets published to
// their groups, the nodes and services of their apps; everything for a
// manager.
func TestCompUsageVisibility(t *testing.T) {
	db := testMariaDB(t,
		"CREATE TABLE comp_moduleset (id int, modset_name varchar(64))",
		"CREATE TABLE comp_moduleset_ruleset (modset_id int, ruleset_id int)",
		"CREATE TABLE comp_moduleset_moduleset (parent_modset_id int, child_modset_id int)",
		"CREATE TABLE comp_moduleset_team_publication (modset_id int, group_id int)",
		"CREATE TABLE comp_rulesets (id int, ruleset_name varchar(64))",
		"CREATE TABLE comp_ruleset_team_publication (ruleset_id int, group_id int)",
		"CREATE TABLE comp_rulesets_rulesets (parent_rset_id int, child_rset_id int)",
		"CREATE TABLE comp_rulesets_nodes (ruleset_id int, node_id char(36))",
		"CREATE TABLE comp_rulesets_services (ruleset_id int, svc_id char(36))",
		"CREATE TABLE nodes (node_id char(36), nodename varchar(64), app varchar(64))",
		"CREATE TABLE services (svc_id char(36), svcname varchar(64), svc_app varchar(64))",
		"CREATE TABLE apps (id int, app varchar(64))",
		"CREATE TABLE apps_responsibles (app_id int, group_id int)",
		"CREATE TABLE auth_group (id int, role varchar(64), privilege char(1) DEFAULT 'F')",
		"INSERT INTO auth_group VALUES (10, 'teamA', 'F'), (20, 'teamB', 'F')",
		"INSERT INTO apps VALUES (1, 'A'), (2, 'B')",
		"INSERT INTO apps_responsibles VALUES (1, 10), (2, 20)",
		"INSERT INTO nodes VALUES ('nA', 'nodeA', 'A'), ('nB', 'nodeB', 'B')",
		"INSERT INTO services VALUES ('sA', 'svcA', 'A'), ('sB', 'svcB', 'B')",
		// ruleset 1, used by modulesets 1 (team A) and 2 (team B), rulesets 2
		// (team A) and 3 (team B), nodes and services of both apps.
		"INSERT INTO comp_rulesets VALUES (1, 'rs'), (2, 'parentA'), (3, 'parentB')",
		"INSERT INTO comp_ruleset_team_publication VALUES (1, 10), (2, 10), (3, 20)",
		"INSERT INTO comp_rulesets_rulesets VALUES (2, 1), (3, 1)",
		"INSERT INTO comp_moduleset VALUES (1, 'msA'), (2, 'msB'), (3, 'child')",
		"INSERT INTO comp_moduleset_team_publication VALUES (1, 10), (2, 20)",
		"INSERT INTO comp_moduleset_ruleset VALUES (1, 1), (2, 1)",
		"INSERT INTO comp_moduleset_moduleset VALUES (1, 3), (2, 3)",
		"INSERT INTO comp_rulesets_nodes VALUES (1, 'nA'), (1, 'nB')",
		"INSERT INTO comp_rulesets_services VALUES (1, 'sA'), (1, 'sB')",
	)
	odb := testDashboardDB(db)
	ctx := context.Background()
	names := func(usage map[string][]map[string]any) string {
		out := map[string][]any{}
		for key, entries := range usage {
			for _, e := range entries {
				for k, v := range e {
					if k != "id" && k != "node_id" && k != "svc_id" {
						out[key] = append(out[key], v)
					}
				}
			}
		}
		return fmt.Sprint(out)
	}
	usage, err := odb.CompRulesetUsage(ctx, 1, []string{"teamA"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if got := names(usage); got != "map[modulesets:[msA] nodes:[nodeA] rulesets:[parentA] services:[svcA]]" {
		t.Errorf("team A: %s", got)
	}
	if usage, err = odb.CompRulesetUsage(ctx, 1, nil, true); err != nil {
		t.Fatal(err)
	}
	if got := names(usage); got != "map[modulesets:[msA msB] nodes:[nodeA nodeB] rulesets:[parentA parentB] services:[svcA svcB]]" {
		t.Errorf("manager: %s", got)
	}
	if usage, err = odb.CompModulesetUsage(ctx, 3, []string{"teamB"}, false); err != nil {
		t.Fatal(err)
	}
	if got := names(usage); got != "map[modulesets:[msB]]" {
		t.Errorf("moduleset usage, team B: %s", got)
	}
}
