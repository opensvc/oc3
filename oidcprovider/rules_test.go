package oidcprovider

import (
	"strings"
	"testing"
)

func TestCheckRedirectURI(t *testing.T) {
	hosts := ClientHosts{
		Hosts:    []string{"n1", "n1.example.com.", "10.0.0.1", "fd00::1", "127.0.0.1", "::1", "fe80::1", "224.0.0.1"},
		Patterns: []string{"*.vip.example.com"},
	}
	ok := map[string]RedirectKind{
		"https://n1:1215/auth-callback":            RedirectSignIn,
		"https://N1.example.com/ui/auth-callback":  RedirectSignIn,
		"https://10.0.0.1:1215/silent-renew":       RedirectSignIn,
		"https://[fd00::1]:1215/auth-callback":     RedirectSignIn,
		"https://c1.vip.example.com/auth-callback": RedirectSignIn,
		"http://127.0.0.1:43123/callback":          RedirectSignIn,
		"http://[::1]:43123/callback":              RedirectSignIn,
		"http://localhost:43123/callback":          RedirectSignIn,
		"https://n1:1215/":                         RedirectSignOut,
		"https://n1:1215/ui/":                      RedirectSignOut,
	}
	for uri, kind := range ok {
		if err := CheckRedirectURI(uri, kind, hosts); err != nil {
			t.Errorf("%s refused: %v", uri, err)
		}
	}
	refused := map[string]RedirectKind{
		"https://n2:1215/auth-callback":               RedirectSignIn, // not a node
		"https://n1.evil.com/auth-callback":           RedirectSignIn,
		"https://n1:1215/other":                       RedirectSignIn, // not a callback path
		"https://n1:1215/auth-callback#frag":          RedirectSignIn,
		"https://user:pw@n1/auth-callback":            RedirectSignIn,
		"http://n1:1215/auth-callback":                RedirectSignIn, // http off loopback
		"http://127.0.0.1:43123/other":                RedirectSignIn,
		"javascript:alert(1)":                         RedirectSignIn,
		"/auth-callback":                              RedirectSignIn,
		"https://10.0.0.2/auth-callback":              RedirectSignIn,
		"https://127.0.0.1:1215/auth-callback":        RedirectSignIn, // a node address, not routable
		"https://[::1]:1215/auth-callback":            RedirectSignIn,
		"https://[fe80::1]:1215/auth-callback":        RedirectSignIn,
		"https://224.0.0.1/auth-callback":             RedirectSignIn,
		"https://vip.example.com/auth-callback":       RedirectSignIn, // the pattern wants a label
		"https://10.0.0.1.vip.example.com.evil/x":     RedirectSignOut,
		"https://evil.com/?https://n1/auth-callback":  RedirectSignOut,
		"https://n1.example.com.evil.com/":            RedirectSignOut,
		"https://n1:1215/auth-callback\\@evil.com/xx": RedirectSignIn,
	}
	for uri, kind := range refused {
		if err := CheckRedirectURI(uri, kind, hosts); err == nil {
			t.Errorf("%s accepted", uri)
		}
	}
}

func TestParseGrants(t *testing.T) {
	got, err := ParseGrants(" guest:* root,admin:prod  guest:* operator ")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, " ") != "admin:prod guest:* operator root" {
		t.Errorf("grants %v", got)
	}
	for _, s := range []string{"", "  ", "god", "root:prod", "heartbeat", "join", "admin:", "admin:a b", "guest:[", "admin:prod/x"} {
		if _, err := ParseGrants(s); err == nil {
			t.Errorf("%q accepted", s)
		}
	}
}

func TestMergeGrants(t *testing.T) {
	got := MergeGrants([]string{"guest:*", "admin:prod"}, nil, []string{"admin:prod", "root"})
	if strings.Join(got, " ") != "admin:prod guest:* root" {
		t.Errorf("merged %v", got)
	}
	if got := MergeGrants(); got == nil || len(got) != 0 {
		t.Errorf("merge of nothing %v", got)
	}
}
