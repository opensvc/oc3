package serverhandlers

import (
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

func TestCleanConstraintValues(t *testing.T) {
	got, pattern, err := cleanConstraint(server.FieldConstraintModeStrict, []string{" Paris", "Lyon", "", "Paris", "  ", "Lille"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"Paris", "Lyon", "Lille"}; !reflect.DeepEqual(got, want) || pattern != "" {
		t.Errorf("values: got %q and pattern %q, want %q and no pattern", got, pattern, want)
	}

	p := " [A-Z]{3}-[0-9]+ "
	got, pattern, err = cleanConstraint(server.FieldConstraintModeRegex, []string{"ignored"}, &p)
	if err != nil || pattern != "[A-Z]{3}-[0-9]+" || len(got) != 0 {
		t.Errorf("regex: got %q, %q, %v; want the trimmed pattern and no value", got, pattern, err)
	}
	for name, bad := range map[string]string{"no pattern": "  ", "does not compile": "(abc", "too long": strings.Repeat("a", fieldConstraintMaxPattern+1)} {
		if _, _, err := cleanConstraint(server.FieldConstraintModeRegex, nil, &bad); err == nil {
			t.Errorf("regex %s: accepted", name)
		}
	}
	if _, _, err := cleanConstraint(server.FieldConstraintModeRegex, []string{"Paris"}, nil); err == nil {
		t.Error("regex without pattern: accepted")
	}

	refused := map[string]struct {
		mode   server.FieldConstraintMode
		values []string
	}{
		"no value in suggest": {server.FieldConstraintModeSuggest, []string{" ", ""}},
		"no value in strict":  {server.FieldConstraintModeStrict, nil},
		"unknown mode":        {server.FieldConstraintMode("loose"), []string{"Paris"}},
		"free is no mode":     {server.FieldConstraintMode("free"), []string{"Paris"}},
		"value too long":      {server.FieldConstraintModeStrict, []string{strings.Repeat("é", fieldConstraintMaxLength+1)}},
	}
	many := make([]string, fieldConstraintMaxValues+1)
	for i := range many {
		many[i] = strconv.Itoa(i)
	}
	refused["too many values"] = struct {
		mode   server.FieldConstraintMode
		values []string
	}{server.FieldConstraintModeSuggest, many}
	for name, tc := range refused {
		if _, _, err := cleanConstraint(tc.mode, tc.values, nil); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestStrictViolation(t *testing.T) {
	constraints := []cdb.FieldConstraint{
		{Field: "loc_city", Mode: "strict", Values: []string{"Paris", "Lyon"}},
		{Field: "sec_zone", Mode: "suggest", Values: []string{"dmz"}},
		{Field: "loc_rack", Mode: "regex", Pattern: "R[0-9]{2}"},
		{Field: "loc_room", Mode: "regex", Pattern: "(broken"},
	}
	current := map[string]string{"loc_city": "Zanzibar", "loc_rack": "rack 7"}
	cases := map[string]struct {
		fields map[string]any
		wrong  bool
	}{
		"allowed value":            {map[string]any{"loc_city": "Lyon"}, false},
		"cleared":                  {map[string]any{"loc_city": ""}, false},
		"value held kept":          {map[string]any{"loc_city": "Zanzibar"}, false},
		"other value":              {map[string]any{"loc_city": "Nantes"}, true},
		"case differs":             {map[string]any{"loc_city": "paris"}, true},
		"suggest accepts anything": {map[string]any{"sec_zone": "lan"}, false},
		"attribute not set":        {map[string]any{"loc_zip": "75001"}, false},
		"pattern matched":          {map[string]any{"loc_rack": "R11"}, false},
		"pattern matched in part":  {map[string]any{"loc_rack": "xR11"}, true},
		"pattern not matched":      {map[string]any{"loc_rack": "R1"}, true},
		"pattern, value held kept": {map[string]any{"loc_rack": "rack 7"}, false},
		"pattern, cleared":         {map[string]any{"loc_rack": ""}, false},
		"stored pattern broken":    {map[string]any{"loc_room": "A"}, true},
	}
	for name, tc := range cases {
		msg := strictViolation(constraints, tc.fields, current)
		if (msg != "") != tc.wrong {
			t.Errorf("%s: got %q", name, msg)
		}
	}
}

func TestFieldConstraintList(t *testing.T) {
	list := fieldConstraintList(server.FieldConstraintObjectKindNode, []cdb.FieldConstraint{
		{Field: "sec_zone", Mode: "suggest", Values: []string{"dmz"}},
		{Field: "loc_city", Mode: "strict", Values: []string{"Paris"}, UpdatedBy: "a@b"},
		// A row for an attribute no longer constrainable.
		{Field: "team_responsible", Mode: "strict", Values: []string{"x"}},
	})
	// Only the constrained attributes, in the order of constrainableNodeFields: the
	// others are free text, the default.
	if len(list.Data) != 2 || list.Data[0].Field != "sec_zone" || list.Data[1].Field != "loc_city" {
		t.Fatalf("got %+v, want sec_zone then loc_city", list.Data)
	}
	if c := list.Data[1]; c.Mode != server.FieldConstraintModeStrict || !reflect.DeepEqual(c.Values, []string{"Paris"}) || c.UpdatedBy != "a@b" {
		t.Errorf("loc_city: got %+v", c)
	}
	if !reflect.DeepEqual(list.Fields, constrainableNodeFields) {
		t.Errorf("fields: got %q", list.Fields)
	}
	if empty := fieldConstraintList(server.FieldConstraintObjectKindNode, nil); empty.Data == nil || len(empty.Data) != 0 {
		t.Errorf("no constraint: got %+v, want an empty list", empty.Data)
	}
	if isConstrainable(server.FieldConstraintObjectKindNode, "team_responsible") {
		t.Error("team_responsible is chosen among the teams, it cannot be constrained")
	}
}

func TestAuditValues(t *testing.T) {
	if got := auditValues([]string{"Lyon", "Paris"}); got != "Lyon, Paris" {
		t.Errorf("got %q", got)
	}
	long := make([]string, fieldConstraintMaxValues)
	for i := range long {
		long[i] = strings.Repeat("é", fieldConstraintMaxLength)
	}
	if got := auditValues(long); len([]rune(got)) != 1001 || !strings.HasSuffix(got, "…") {
		t.Errorf("got %d characters", len([]rune(got)))
	}
}

func TestConstrainableKinds(t *testing.T) {
	for _, kind := range []server.FieldConstraintObjectKind{
		server.FieldConstraintObjectKindNode, server.FieldConstraintObjectKindService, server.FieldConstraintObjectKindApp,
	} {
		if len(constrainableFields[kind]) == 0 {
			t.Errorf("%s: no constrainable attribute", kind)
		}
		if table := constrainedTables[kind]; table[0] == "" || table[1] == "" {
			t.Errorf("%s: no table to read the values held", kind)
		}
	}
	cases := []struct {
		kind  server.FieldConstraintObjectKind
		field string
		want  bool
	}{
		{server.FieldConstraintObjectKindService, "svc_drpnode", true},
		{server.FieldConstraintObjectKindApp, "app_domain", true},
		// An attribute of another kind.
		{server.FieldConstraintObjectKindService, "loc_city", false},
		{server.FieldConstraintObjectKindNode, "app_domain", false},
		// Chosen among known values elsewhere.
		{server.FieldConstraintObjectKindService, "svc_app", false},
		{server.FieldConstraintObjectKindApp, "app_team_ops", false},
	}
	for _, tc := range cases {
		if got := isConstrainable(tc.kind, tc.field); got != tc.want {
			t.Errorf("%s.%s: got %v, want %v", tc.kind, tc.field, got, tc.want)
		}
	}
	list := fieldConstraintList(server.FieldConstraintObjectKindApp, []cdb.FieldConstraint{{Field: "app_domain", Mode: "suggest", Values: []string{"finance"}}})
	if len(list.Data) != 1 || list.Data[0].ObjectKind != server.FieldConstraintObjectKindApp || !reflect.DeepEqual(list.Fields, []string{"app_domain"}) {
		t.Errorf("app list: got %+v", list)
	}
}
