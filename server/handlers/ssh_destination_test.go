package serverhandlers

import (
	"strings"
	"testing"
)

// TestValidSSHHost checks the hosts an action may connect to over ssh: a name
// or an address, never something adding arguments to ssh.
func TestValidSSHHost(t *testing.T) {
	for _, host := range []string{"node1", "node1.example.com", "10.0.0.1", "fe80::1%eth0", "2001:db8::1", "a_b-c"} {
		if !validSSHHost(host) {
			t.Errorf("%q refused", host)
		}
	}
	for _, host := range []string{"", "-oProxyCommand=x", "x -oProxyCommand=touch${IFS}/tmp/p", "x;id", "x$(id)", "x`id`",
		"x\tdate", "x\n", "x/y", strings.Repeat("a", 256)} {
		if validSSHHost(host) {
			t.Errorf("%q accepted", host)
		}
	}
	if cmd := nodeActionCommand("pushasset", "push", "node1"); !strings.Contains(cmd, " opensvc@node1 -- sudo nodemgr ") {
		t.Errorf("command %q", cmd)
	}
	if msg := checkNodeConnectTo(map[string]any{"connect_to": "x -oProxyCommand=id"}); msg == "" {
		t.Error("a connect_to adding ssh options is accepted")
	}
	for _, v := range []any{"", "node1.example.com", nil} {
		if msg := checkNodeConnectTo(map[string]any{"connect_to": v}); msg != "" {
			t.Errorf("connect_to %v refused: %s", v, msg)
		}
	}
}
