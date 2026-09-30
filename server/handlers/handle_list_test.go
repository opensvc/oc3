package serverhandlers

import "testing"

func TestTotalFromPage(t *testing.T) {
	cases := []struct {
		name                   string
		limit, offset, pageLen int
		total                  int
		known                  bool
	}{
		{"no limit: the page is the list", 0, 0, 120, 120, true},
		{"no limit, offset ignored", 0, 50, 120, 120, true},
		{"partial first page", 50, 0, 12, 12, true},
		{"partial last page", 50, 100, 7, 107, true},
		{"empty list", 50, 0, 0, 0, true},
		{"full page: rows may follow", 50, 0, 50, 0, false},
		{"empty page past the end", 50, 200, 0, 0, false},
	}
	for _, c := range cases {
		total, known := totalFromPage(c.limit, c.offset, c.pageLen)
		if total != c.total || known != c.known {
			t.Errorf("%s: got (%d, %v), want (%d, %v)", c.name, total, known, c.total, c.known)
		}
	}
}
