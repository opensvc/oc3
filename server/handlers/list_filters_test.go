package serverhandlers

import (
	"reflect"
	"strings"
	"testing"

	"github.com/opensvc/oc3/server"
)

func TestFilterCondition(t *testing.T) {
	cases := []struct {
		expr string
		cond string
		args []any
	}{
		{"dev", "n.name LIKE ?", []any{"%dev%"}},
		{"50%", "n.name LIKE ?", []any{`%50\%%`}},
		{"~^dev", "n.name REGEXP ?", []any{"^dev"}},
		{"in:a,b", "n.name IN (?,?)", []any{"a", "b"}},
		{"gte:8", "n.name >= ?", []any{"8"}},
		{"empty", "(n.name IS NULL OR n.name = '')", nil},
		{"!empty", "(n.name IS NOT NULL AND n.name <> '')", nil},
		{"!dev", "(n.name IS NULL OR NOT (n.name LIKE ?))", []any{"%dev%"}},
		{"!~^dev", "(n.name IS NULL OR NOT (n.name REGEXP ?))", []any{"^dev"}},
		{"!in:a,b", "(n.name IS NULL OR NOT (n.name IN (?,?)))", []any{"a", "b"}},
		{"!eq:a", "(n.name IS NULL OR NOT (n.name = ?))", []any{"a"}},
		// A lone "!" is text, and an inversion is inverted back.
		{"!", "n.name LIKE ?", []any{"%!%"}},
		{"!!dev", "(n.name IS NULL OR NOT ((n.name IS NULL OR NOT (n.name LIKE ?))))", []any{"%dev%"}},
	}
	for _, c := range cases {
		cond, args, err := filterCondition("n.name", "name", c.expr)
		if err != nil {
			t.Errorf("%q: unexpected error %v", c.expr, err)
			continue
		}
		if cond != c.cond {
			t.Errorf("%q: got condition %q, want %q", c.expr, cond, c.cond)
		}
		if !reflect.DeepEqual(args, c.args) {
			t.Errorf("%q: got args %v, want %v", c.expr, args, c.args)
		}
	}
	for _, expr := range []string{"~(", "!~("} {
		if _, _, err := filterCondition("n.name", "name", expr); err == nil {
			t.Errorf("%q: expected an invalid regular expression error", expr)
		}
	}
}

func TestAlertFilters(t *testing.T) {
	raw := server.InQueryFilter{"services.svcname:dev2n1", "alert:down", "dash_severity:gte:3"}
	filters, err := buildFilters(&raw, propsMapping["alert"])
	if err != nil {
		t.Fatal(err)
	}
	if len(filters) != 3 {
		t.Fatalf("filters: %v", filters)
	}
	for i, want := range []string{"COALESCE(NULLIF(services.svcname, ''), nodes.nodename)", "CONCAT(COALESCE(dashboard.dash_fmt, '')", "dashboard.dash_severity"} {
		if !strings.Contains(filters[i].Expr, want) {
			t.Errorf("filter %d: %q does not use %q", i, filters[i].Expr, want)
		}
	}
}
