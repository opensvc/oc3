package cdb

import (
	"context"
	"testing"
)

// TestUserFullNameTaken checks a full name is refused when another user or a
// group bears it, as the requests compare them, whatever the case; the user's
// own name and an empty name are not.
func TestUserFullNameTaken(t *testing.T) {
	db := testMariaDB(t,
		"CREATE TABLE auth_user (id int, first_name varchar(128), last_name varchar(128))"+
			" DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci",
		"CREATE TABLE auth_group (id int, role varchar(255)) DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci",
		"INSERT INTO auth_user VALUES (1, 'Ann', 'Smith'), (2, 'Bob', NULL), (3, NULL, NULL)",
		"INSERT INTO auth_group VALUES (10, 'ops')",
	)
	odb := testDashboardDB(db)
	ctx := context.Background()
	for _, tc := range []struct {
		first, last string
		except      int64
		want        string
	}{
		{"Ann", "Smith", 4, "user"},
		{"ann", "SMITH", 4, "user"},
		{"Ann", "Smith", 1, ""},
		{"Bob", "", 4, "user"},
		{"ops", "", 4, "group"},
		{"Ann", "Smyth", 4, ""},
		{"", "", 4, ""},
	} {
		got, err := odb.UserFullNameTaken(ctx, tc.first, tc.last, tc.except)
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Errorf("%q %q (user %d): %q, want %q", tc.first, tc.last, tc.except, got, tc.want)
		}
	}
}
