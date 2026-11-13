package oidcprovider

import (
	"fmt"
	"net"
	"net/url"
	"path"
	"strings"
)

// RedirectKind tells what a redirection URI is for: the end of a sign-in, or
// the return after a sign-out.
type RedirectKind int

const (
	RedirectSignIn RedirectKind = iota
	RedirectSignOut
)

// The paths of the om3 webapp receiving the end of a sign-in, possibly under a
// base path (/ui), and the one of the om CLI, on a loopback address.
var (
	webappCallbackSuffixes = []string{"/auth-callback", "/silent-renew"}
	cliCallbackPath        = "/callback"
)

// ClientHosts is what a redirection URI of a client may name: the host names and
// addresses of the nodes of the cluster, and the patterns of host names the
// configuration allows for every cluster (a virtual address, a reverse proxy).
type ClientHosts struct {
	Hosts    []string
	Patterns []string
}

// CheckRedirectURI tells whether the URI is one the provider may send the
// browser of a user to, with a code or after a sign-out, for this client:
//
//   - https, on a host of the cluster (any port: the listener port, or a
//     proxy's), for the om3 webapp; its sign-in paths ending with /auth-callback
//     or /silent-renew;
//   - http on a loopback address, any port, for the om CLI, which listens there
//     for the end of the sign-in on /callback (RFC 8252).
//
// No fragment, no credentials in the URI.
func CheckRedirectURI(raw string, kind RedirectKind, hosts ClientHosts) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid redirect uri: %w", err)
	}
	if u.Fragment != "" || strings.Contains(raw, "#") {
		return fmt.Errorf("redirect uri %s: a fragment is not allowed", raw)
	}
	if u.User != nil {
		return fmt.Errorf("redirect uri %s: credentials are not allowed", raw)
	}
	if u.Opaque != "" || u.Host == "" {
		return fmt.Errorf("redirect uri %s: an absolute uri is expected", raw)
	}
	hostname := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	switch u.Scheme {
	case "http":
		if !isLoopback(hostname) {
			return fmt.Errorf("redirect uri %s: http is only allowed on a loopback address", raw)
		}
		if kind == RedirectSignIn && u.Path != cliCallbackPath {
			return fmt.Errorf("redirect uri %s: the path on a loopback address must be %s", raw, cliCallbackPath)
		}
		return nil
	case "https":
	default:
		return fmt.Errorf("redirect uri %s: unsupported scheme %s", raw, u.Scheme)
	}
	if !hosts.allow(hostname) {
		return fmt.Errorf("redirect uri %s: the host %s is not a node of the cluster", raw, hostname)
	}
	if kind == RedirectSignIn && !hasCallbackSuffix(u.Path) {
		return fmt.Errorf("redirect uri %s: the path must end with %s", raw, strings.Join(webappCallbackSuffixes, " or "))
	}
	return nil
}

func hasCallbackSuffix(p string) bool {
	for _, s := range webappCallbackSuffixes {
		if strings.HasSuffix(p, s) {
			return true
		}
	}
	return false
}

// routable tells whether a node address may name a webapp in a redirection
// URI: the nodes report every address of their interfaces, the loopback, link
// local and multicast ones included, which are not the node's from the
// browser.
func routable(ip net.IP) bool {
	return !ip.IsLoopback() && !ip.IsLinkLocalUnicast() && !ip.IsMulticast() &&
		!ip.IsInterfaceLocalMulticast() && !ip.IsLinkLocalMulticast() && !ip.IsUnspecified()
}

func isLoopback(hostname string) bool {
	if hostname == "localhost" {
		return true
	}
	ip := net.ParseIP(hostname)
	return ip != nil && ip.IsLoopback()
}

func (h ClientHosts) allow(hostname string) bool {
	if hostname == "" {
		return false
	}
	ip := net.ParseIP(hostname)
	for _, s := range h.Hosts {
		s = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(s)), ".")
		if s == "" {
			continue
		}
		other := net.ParseIP(s)
		if other != nil && !routable(other) {
			continue
		}
		if s == hostname || (ip != nil && other != nil && other.Equal(ip)) {
			return true
		}
	}
	if ip != nil {
		// The patterns are host names.
		return false
	}
	for _, p := range h.Patterns {
		p = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(p)), ".")
		if p == "" {
			continue
		}
		if ok, err := path.Match(p, hostname); err == nil && ok {
			return true
		}
	}
	return false
}
