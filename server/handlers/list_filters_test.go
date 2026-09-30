package serverhandlers

import (
	"reflect"
	"testing"
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
