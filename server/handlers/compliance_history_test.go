package serverhandlers

import (
	"testing"

	"github.com/opensvc/oc3/util/git"
)

func TestComplianceVersionsSource(t *testing.T) {
	got := complianceVersions([]git.LogEntry{
		{ID: "a", Body: "- one\n- two\n\nCompliance-Source: designer"},
		{ID: "b", Body: ""},
	})
	if got[0].Source != "designer" || got[0].Body != "- one\n- two" {
		t.Errorf("trailer: %+v", got[0])
	}
	if got[1].Source != "" || got[1].Body != "" {
		t.Errorf("no trailer: %+v", got[1])
	}
}

func TestParseComplianceObject(t *testing.T) {
	if kind, id, ok := parseComplianceObject("ruleset:12"); !ok || kind != "ruleset" || id != 12 {
		t.Errorf("ruleset:12 -> %q %d %v", kind, id, ok)
	}
	for _, bad := range []string{"node:1", "ruleset", "ruleset:x", ""} {
		if _, _, ok := parseComplianceObject(bad); ok {
			t.Errorf("%q parsed", bad)
		}
	}
}

func TestComplianceObjectOf(t *testing.T) {
	content := `{"filtersets":[],"rulesets":[{"id":1,"ruleset_name":"a"},{"id":2,"ruleset_name":"b"}],"modulesets":[]}`
	if got, err := complianceObjectOf(content, "ruleset", 2); err != nil || got != `{"id":2,"ruleset_name":"b"}` {
		t.Errorf("ruleset 2: %q %v", got, err)
	}
	if got, err := complianceObjectOf(content, "moduleset", 2); err != nil || got != "" {
		t.Errorf("moduleset 2: %q %v", got, err)
	}
}
