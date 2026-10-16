package serverhandlers

import "testing"

func TestParseSLA(t *testing.T) {
	for _, c := range []struct {
		in   string
		want any
		ok   bool
	}{
		{"99.9", 99.9, true},
		{" 99,5 % ", 99.5, true},
		{"100", 100.0, true},
		{"", nil, true},
		{"101", nil, false},
		{"-1", nil, false},
		{"high", nil, false},
	} {
		got, err := parseSLA(c.in)
		if (err == nil) != c.ok || (c.ok && got != c.want) {
			t.Errorf("%q: got %v, %v", c.in, got, err)
		}
	}
}

// The notifications of a service are stored as the collector's "T"/"F", not as
// the 1/0 a Go bool would give, which alertd reads as neither.
func TestServiceFieldsNotifications(t *testing.T) {
	on, off := true, false
	for v, want := range map[*bool]string{&on: "T", &off: "F"} {
		got := serviceBodyFields{SvcNotifications: v}.toFields()["svc_notifications"]
		if got != want {
			t.Errorf("svc_notifications %v: got %#v, want %q", *v, got, want)
		}
	}
	if _, set := (serviceBodyFields{}).toFields()["svc_notifications"]; set {
		t.Error("an absent notifications flag is left alone")
	}
}
