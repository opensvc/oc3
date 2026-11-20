package serverhandlers

import "regexp"

// sshHostRe is a host the collector connects to over ssh: a name, an IPv4 or
// an IPv6 address (with its zone), with no space nor shell character.
var sshHostRe = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.:%-]*$`)

// validSSHHost tells whether host may follow "opensvc@" in the ssh command of
// an action. The command is split on spaces and run without a shell, but a host
// holding a space adds arguments to ssh, such as -oProxyCommand=…, which ssh
// runs through a shell on the collector; a leading dash is an option.
func validSSHHost(host string) bool {
	return len(host) <= 255 && sshHostRe.MatchString(host)
}
