package serverhandlers

import "testing"

func TestSelectRequest(t *testing.T) {
	for _, request := range []string{
		"SELECT 1",
		"  select os_name, count(*) from nodes group by os_name",
		"WITH t AS (SELECT 1) SELECT * FROM t",
		"-- nodes by os\nSELECT os_name FROM nodes",
		"/* a\nb */ SELECT 1",
		"# comment\n  Select 1",
	} {
		if !selectRequest.MatchString(request) {
			t.Errorf("%q: expected a SELECT", request)
		}
	}
	for _, request := range []string{
		"DROP TABLE nodes",
		"UPDATE nodes SET app = ''",
		"-- SELECT\nDELETE FROM nodes",
		"SELECTED",
		"/* SELECT */ TRUNCATE nodes",
	} {
		if selectRequest.MatchString(request) {
			t.Errorf("%q: expected a refusal", request)
		}
	}
}
