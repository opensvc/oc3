package serverhandlers

import (
	"reflect"
	"testing"
)

func TestWorkflowNextForms(t *testing.T) {
	parse := func(text string) map[string]any {
		t.Helper()
		v, err := parseFormYaml(text)
		if err != nil {
			t.Fatalf("parse: %s", err)
		}
		m, _ := v.(map[string]any)
		return m
	}
	cases := []struct {
		name string
		yaml string
		want []string
	}{
		{
			name: "next forms of the workflow output",
			yaml: "Outputs:\n- Dest: workflow\n  NextForms: [approval, rejection]\n- Dest: mail\n  NextForms: [ignored]\n",
			want: []string{"approval", "rejection"},
		},
		{
			name: "next forms of the scripts outcomes",
			yaml: "Outputs:\n- Dest: workflow\n  Scripts:\n    Success:\n      NextForms: [delivery]\n    Error:\n      NextForms: [retry]\n",
			want: []string{"delivery", "retry"},
		},
		{
			name: "last step",
			yaml: "Outputs:\n- Dest: workflow\n",
			want: nil,
		},
		{
			name: "no definition",
			yaml: "",
			want: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := workflowNextForms(parse(tc.yaml)); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestStepAssignedTo(t *testing.T) {
	groups := []string{"Everybody", "Manager", "Change Board"}
	cases := []struct {
		name     string
		assignee string
		caller   string
		want     bool
	}{
		{"by full name", "Alice Martin", "Alice Martin", true},
		{"by group", "Change Board", "Alice Martin", true},
		{"name without first name", " Manager", "Bob Durand", true},
		{"caller without first name", " Smith", " Smith", true},
		{"someone else", "Network", "Alice Martin", false},
		{"no assignee", "", "Alice Martin", false},
		{"blank assignee", "  ", " ", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := stepAssignedTo(tc.assignee, tc.caller, groups); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}
