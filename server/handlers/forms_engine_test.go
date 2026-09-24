package serverhandlers

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestCheckFormCondition(t *testing.T) {
	d := map[string]any{
		"os":    "linux",
		"size":  "12",
		"count": json.Number("3"),
		"blank": "",
		"undef": "undefined",
	}
	cases := []struct {
		cond string
		want bool
	}{
		{"", true},
		{"none", true},
		{"#os == linux", true},
		{"#os == aix", false},
		{"#missing == linux", false},
		{"#count == 3", false}, // a number never equals a string, as in python
		{"#blank == empty", true},
		{"#undef == empty", true},
		{"#missing == empty", true},
		{"#os == empty", false},
		{"#os != aix", true},
		{"#os != linux", false},
		{"#missing != linux", true},
		{"#count != 3", true},
		{"#os != empty", true},
		{"#blank != empty", false},
		{"#os IN aix,linux", true},
		{"#os IN aix,sunos", false},
		{"#missing IN linux", false},
		{"#os NOT IN aix,sunos", true},
		{"#os NOT IN aix,linux", false},
		{"#missing NOT IN linux", true},
		{"#size > 10", true},
		{"#size > 12", false},
		{"#size < 13", true},
		{"#count > 2", true},
		{"#os > 2", false},
	}
	for _, c := range cases {
		got, err := checkFormCondition(c.cond, d)
		if err != nil {
			t.Errorf("%q: unexpected error %s", c.cond, err)
			continue
		}
		if got != c.want {
			t.Errorf("%q: got %v, want %v", c.cond, got, c.want)
		}
	}
	for _, bad := range []any{nil, "os == linux", "#os ~ linux"} {
		if _, err := checkFormCondition(bad, d); err == nil {
			t.Errorf("%v: expected an error", bad)
		}
	}
}

func TestCheckFormConditionsList(t *testing.T) {
	d := map[string]any{"os": "linux", "arch": "x86_64"}
	ok, err := checkFormConditions([]any{"#os == linux", "#arch == x86_64"}, d)
	if err != nil || !ok {
		t.Errorf("all conditions hold: got %v %v", ok, err)
	}
	ok, err = checkFormConditions([]any{"#os == linux", "#arch == ppc"}, d)
	if err != nil || ok {
		t.Errorf("one condition fails: got %v %v", ok, err)
	}
}

func TestFormDereference(t *testing.T) {
	data := map[string]any{
		"name":  "db1",
		"names": "db1,db2",
		"array": map[string]any{"id": json.Number("554")},
	}
	got := formDereference("#names for #name on #array.id", data, "")
	if want := "db1,db2 for db1 on 554"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestFormRestArgs(t *testing.T) {
	data := map[string]any{"id": json.Number("554"), "dg": map[string]any{"id": "2525"}}
	got, err := formRestArgs("/arrays/#id/diskgroups/#dg.id/quotas/", data)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"arrays", "554", "diskgroups", "2525", "quotas"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if _, err := formRestArgs("/arrays/#missing", data); err == nil {
		t.Error("a missing reference must fail")
	}
}

func TestCheckOutputCondition(t *testing.T) {
	d := map[string]any{"os": "linux"}
	if _, err := checkOutputCondition(map[string]any{"Condition": "#os == linux"}, d); err == nil {
		t.Error("a condition on a non-dict output must fail")
	}
	ok, err := checkOutputCondition(map[string]any{"Condition": "#os == linux", "Format": "dict"}, d)
	if err != nil || !ok {
		t.Errorf("got %v %v", ok, err)
	}
	ok, err = checkOutputCondition(map[string]any{}, d)
	if err != nil || !ok {
		t.Errorf("no condition: got %v %v", ok, err)
	}
}

func TestInternalForms(t *testing.T) {
	for id := int64(-1); id >= -10; id-- {
		f, ok := internalFormByID(id)
		if !ok {
			t.Errorf("internal form %d missing", id)
			continue
		}
		if f.Name == "" || len(defMaps(f.Definition, "Outputs")) == 0 && id > -9 {
			t.Errorf("internal form %d: incomplete %+v", id, f)
		}
	}
	if _, ok := internalFormByID(-11); ok {
		t.Error("internal form -11 must not exist")
	}
}
