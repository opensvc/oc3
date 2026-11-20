package cdb

import (
	"context"
	"fmt"
	"testing"
)

// TestFormsRevisionsVisibility checks the form revisions a user reads: those of
// the forms published to their groups, and those of the requests they take
// part in; all of them for a manager.
func TestFormsRevisionsVisibility(t *testing.T) {
	db := testMariaDB(t,
		"CREATE TABLE forms_revisions (id int AUTO_INCREMENT PRIMARY KEY, form_md5 varchar(32), form_id int)",
		"CREATE TABLE forms_team_publication (form_id int, group_id int)",
		"CREATE TABLE auth_group (id int, role varchar(64), privilege char(1) DEFAULT 'F')",
		"CREATE TABLE auth_user (id int, first_name varchar(64), last_name varchar(64))",
		"CREATE TABLE auth_membership (user_id int, group_id int)",
		"CREATE TABLE forms_store (id int, form_head_id int, form_md5 varchar(32), form_submitter varchar(128), form_assignee varchar(128))",
		"CREATE TABLE workflows (id int, form_head_id int, form_md5 varchar(32), creator varchar(128), last_assignee varchar(128))",
		"INSERT INTO auth_group VALUES (10, 'teamA', 'F'), (20, 'teamB', 'F')",
		"INSERT INTO auth_user VALUES (7, 'Ann', 'Smith')",
		"INSERT INTO auth_membership VALUES (7, 10)",
		// 1 of a form published to team A, 2 of a form of team B, 3 of a form of
		// team B a request of Ann started from, 4 of a step of that request.
		"INSERT INTO forms_revisions (form_md5, form_id) VALUES ('m1', 1), ('m2', 2), ('m3', 3), ('m4', 3)",
		"INSERT INTO forms_team_publication VALUES (1, 10), (2, 20), (3, 20)",
		"INSERT INTO forms_store VALUES (100, NULL, 'm3', 'Ann Smith', 'teamB'), (101, 100, 'm4', 'someone', 'teamB')",
		"INSERT INTO workflows VALUES (1, 100, 'm3', 'Ann Smith', 'teamB')",
	)
	odb := testDashboardDB(db)
	ctx := context.Background()
	ids := func(p ListParams, idOrMD5 *string) string {
		t.Helper()
		p.SelectExprs, p.Props = []string{"forms_revisions.id"}, []string{"id"}
		rows, err := odb.GetFormsRevisions(ctx, idOrMD5, p)
		if err != nil {
			t.Fatal(err)
		}
		var out []any
		for _, r := range rows {
			out = append(out, r["id"])
		}
		return fmt.Sprint(out)
	}
	ann := int64(7)
	if got := ids(ListParams{Groups: []string{"teamA"}, UserID: &ann}, nil); got != "[1 3 4]" {
		t.Errorf("Ann: %s", got)
	}
	if got := ids(ListParams{Groups: []string{"teamA"}}, nil); got != "[1]" {
		t.Errorf("team A, no user: %s", got)
	}
	if got := ids(ListParams{}, nil); got != "[]" {
		t.Errorf("nobody: %s", got)
	}
	if got := ids(ListParams{IsManager: true}, nil); got != "[1 2 3 4]" {
		t.Errorf("manager: %s", got)
	}
	md5 := "m2"
	if got := ids(ListParams{Groups: []string{"teamA"}, UserID: &ann}, &md5); got != "[]" {
		t.Errorf("Ann, form of team B by md5: %s", got)
	}
}
