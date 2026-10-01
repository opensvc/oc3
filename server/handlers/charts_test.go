package serverhandlers

import (
	"strings"
	"testing"
)

func TestCheckChart(t *testing.T) {
	definition := "Metrics:\n  - metric_id: 3\n    label: disks\nOptions:\n  stack: true\n"
	name, err := checkChart("  capacity  ", &definition)
	if err != nil || name != "capacity" {
		t.Errorf("got %q, %v", name, err)
	}
	empty := "  "
	if _, err := checkChart("capacity", &empty); err != nil {
		t.Errorf("empty definition: %v", err)
	}
	if _, err := checkChart("capacity", nil); err != nil {
		t.Errorf("no definition: %v", err)
	}
	broken := "sections: [unclosed\n"
	long := strings.Repeat("x", 101)
	for _, c := range []struct {
		name       string
		definition *string
	}{{"", nil}, {"  ", nil}, {long, nil}, {"capacity", &broken}} {
		if _, err := checkChart(c.name, c.definition); err == nil {
			t.Errorf("%q: expected an error", c.name)
		}
	}
}
