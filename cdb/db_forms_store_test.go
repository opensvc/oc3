package cdb

import (
	"strings"
	"testing"
)

func TestWorkflowsVisibility(t *testing.T) {
	if cond, args := workflowsVisibility(ListParams{IsManager: true}); cond != "" || args != nil {
		t.Errorf("a manager reads every request: %q %v", cond, args)
	}
	if cond, _ := workflowsVisibility(ListParams{}); cond != "1=0" {
		t.Errorf("no user, no request: %q", cond)
	}
	id := int64(139)
	cond, args := workflowsVisibility(ListParams{UserID: &id})
	if n := strings.Count(cond, "?"); n != len(args) {
		t.Fatalf("%d placeholders for %d arguments", n, len(args))
	}
	for _, part := range []string{"workflows.creator IN", "workflows.last_assignee IN", "step.form_submitter IN", "step.form_assignee IN"} {
		if !strings.Contains(cond, part) {
			t.Errorf("missing %q", part)
		}
	}
	for _, a := range args {
		if a != id {
			t.Errorf("argument %v, want the user id", a)
		}
	}
}
