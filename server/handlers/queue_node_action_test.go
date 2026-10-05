package serverhandlers

import "testing"

func TestNodeActionCommand(t *testing.T) {
	for _, c := range []struct {
		action, actionType, want string
	}{
		{"pushasset", "pull", "pushasset"},
		{"freeze", "feed", "freeze --local"},
		{"compliance_check", "pull", "compliance check"},
		{"reboot", "push", "ssh -o StrictHostKeyChecking=no -o CheckHostIP=no -o ForwardX11=no -o ConnectTimeout=5 -o PasswordAuthentication=no opensvc@n1 -- sudo nodemgr reboot"},
	} {
		if got := nodeActionCommand(c.action, c.actionType, "n1"); got != c.want {
			t.Errorf("%s %s: got %q, want %q", c.action, c.actionType, got, c.want)
		}
	}
	if nodeActions["rotate_root_pw"] {
		t.Error("the root password rotation is accepted")
	}
	for action := range nodeActionWords {
		if !nodeActions[action] {
			t.Errorf("%s has a command but is not accepted", action)
		}
	}
}
