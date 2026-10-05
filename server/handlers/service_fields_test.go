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
