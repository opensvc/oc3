package serverhandlers

import (
	"strings"
	"testing"
)

func TestCheckReport(t *testing.T) {
	definition := "sections:\n  - title: disks\n    charts: [disk_usage]\n"
	name, err := checkReport("  capacity  ", &definition)
	if err != nil || name != "capacity" {
		t.Errorf("got %q, %v", name, err)
	}
	empty := "  "
	if _, err := checkReport("capacity", &empty); err != nil {
		t.Errorf("empty definition: %v", err)
	}
	if _, err := checkReport("capacity", nil); err != nil {
		t.Errorf("no definition: %v", err)
	}
	broken := "sections: [unclosed\n"
	long := strings.Repeat("x", 101)
	for _, c := range []struct {
		name       string
		definition *string
	}{{"", nil}, {"  ", nil}, {long, nil}, {"capacity", &broken}} {
		if _, err := checkReport(c.name, c.definition); err == nil {
			t.Errorf("%q: expected an error", c.name)
		}
	}
}
