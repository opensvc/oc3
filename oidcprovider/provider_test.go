package oidcprovider

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/go-redis/redis/v8"
)

func newTestProvider(t *testing.T) (*Provider, *miniredis.Miniredis) {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ks, err := NewKeySet(k)
	if err != nil {
		t.Fatal(err)
	}
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	p, err := New(Config{Issuer: "https://collector.example.com/api/oidc/"}, ks, rdb)
	if err != nil {
		t.Fatal(err)
	}
	return p, mr
}

func TestNewDefaults(t *testing.T) {
	p, _ := newTestProvider(t)
	cfg := p.Config()
	if cfg.Issuer != "https://collector.example.com/api/oidc" {
		t.Errorf("issuer %q, the trailing slash is to be trimmed", cfg.Issuer)
	}
	if cfg.UIURL != "https://collector.example.com" {
		t.Errorf("ui url %q, the origin of the issuer expected", cfg.UIURL)
	}
	if cfg.AccessTokenTTL != 10*time.Minute || cfg.RefreshIdleTimeout != time.Hour || cfg.RefreshMaxLifetime != 12*time.Hour {
		t.Errorf("unexpected defaults %+v", cfg)
	}
	for _, issuer := range []string{"", "collector", "ftp://x/y", "https://x/y?a=b"} {
		if _, err := New(Config{Issuer: issuer}, p.keys, nil); err == nil {
			t.Errorf("issuer %q accepted", issuer)
		}
	}
}

func TestDiscovery(t *testing.T) {
	p, _ := newTestProvider(t)
	d := p.Discovery()
	if d["issuer"] != "https://collector.example.com/api/oidc" ||
		d["token_endpoint"] != "https://collector.example.com/api/oidc/token" ||
		d["jwks_uri"] != "https://collector.example.com/api/oidc/jwks" {
		t.Errorf("unexpected discovery %v", d)
	}
	if algs := d["id_token_signing_alg_values_supported"].([]string); len(algs) != 1 || algs[0] != "ES256" {
		t.Errorf("algs %v", algs)
	}
}

func TestLoadKeys(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, block *pem.Block) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, pem.EncodeToMemory(block), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	rk, _ := rsa.GenerateKey(rand.Reader, 2048)
	ek, _ := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	small, _ := rsa.GenerateKey(rand.Reader, 1024)
	pkcs8, _ := x509.MarshalPKCS8PrivateKey(rk)
	sec1, _ := x509.MarshalECPrivateKey(ek)
	rsaPath := write("rsa.pem", &pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8})
	ecPath := write("ec.pem", &pem.Block{Type: "EC PRIVATE KEY", Bytes: sec1})
	smallPath := write("small.pem", &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(small)})

	ks, err := LoadKeys([]string{rsaPath, ecPath})
	if err != nil {
		t.Fatal(err)
	}
	if ks.Algorithm() != "RS256" {
		t.Errorf("the first key signs: alg %s", ks.Algorithm())
	}
	jwks := ks.JWKS()
	if len(jwks.Keys) != 2 || jwks.Keys[1].Algorithm != "ES384" {
		t.Errorf("both keys are to be published: %+v", jwks.Keys)
	}
	for _, k := range jwks.Keys {
		if !k.IsPublic() || k.KeyID == "" {
			t.Errorf("a public key with a kid expected: %+v", k)
		}
	}
	if _, err := LoadKeys([]string{smallPath}); err == nil {
		t.Error("a 1024 bits RSA key accepted")
	}
	if _, err := LoadKeys([]string{rsaPath, rsaPath}); err == nil {
		t.Error("the same key twice accepted")
	}
	if _, err := LoadKeys(nil); err == nil {
		t.Error("no key accepted")
	}
}

func TestPKCE(t *testing.T) {
	verifier := strings.Repeat("a", 43)
	sum := sha256.Sum256([]byte(verifier))
	challenge := base64.RawURLEncoding.EncodeToString(sum[:])
	if !ValidChallenge(challenge) {
		t.Error("valid challenge refused")
	}
	if !VerifyPKCE(challenge, verifier) {
		t.Error("matching verifier refused")
	}
	if VerifyPKCE(challenge, strings.Repeat("b", 43)) {
		t.Error("other verifier accepted")
	}
	if VerifyPKCE(challenge, "short") {
		t.Error("short verifier accepted")
	}
	if ValidChallenge("plain") || ValidChallenge(strings.Repeat("!", 43)) {
		t.Error("invalid challenge accepted")
	}
}

func TestRequestAndCode(t *testing.T) {
	p, mr := newTestProvider(t)
	ctx := context.Background()
	r := &AuthRequest{ClientID: "c1", RedirectURI: "https://n1:1215/auth-callback", State: "s"}
	if err := p.SaveRequest(ctx, r); err != nil {
		t.Fatal(err)
	}
	if r.ID == "" {
		t.Fatal("no request id")
	}
	// Stored by the hash of its id: the id itself is not a key.
	for _, k := range mr.Keys() {
		if strings.Contains(k, r.ID) {
			t.Errorf("the request id is in the key %s", k)
		}
	}
	got, err := p.LoadRequest(ctx, r.ID)
	if err != nil || got.ClientID != "c1" || got.ID != r.ID {
		t.Fatalf("load: %+v %v", got, err)
	}
	if _, err := p.ConsumeRequest(ctx, r.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ConsumeRequest(ctx, r.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("a request consumed twice: %v", err)
	}

	code, err := p.IssueCode(ctx, AuthCode{ClientID: "c1", UserID: 7})
	if err != nil {
		t.Fatal(err)
	}
	ac, err := p.ConsumeCode(ctx, code)
	if err != nil || ac.UserID != 7 {
		t.Fatalf("code: %+v %v", ac, err)
	}
	if _, err := p.ConsumeCode(ctx, code); !errors.Is(err, ErrNotFound) {
		t.Errorf("a code used twice: %v", err)
	}
	code, _ = p.IssueCode(ctx, AuthCode{ClientID: "c1"})
	mr.FastForward(CodeTTL + time.Second)
	if _, err := p.ConsumeCode(ctx, code); !errors.Is(err, ErrNotFound) {
		t.Errorf("an expired code accepted: %v", err)
	}
}

func TestIssueAndParseTokens(t *testing.T) {
	p, _ := newTestProvider(t)
	auth := time.Now().Add(-time.Minute)
	id := Identity{UserID: 42, Email: "jo@example.com", Name: "Jo"}
	tokens, err := p.IssueTokens("cluster-1", id, "openid email opensvc:om3", "n0nce", auth, []string{"admin:prod", "guest:*"})
	if err != nil {
		t.Fatal(err)
	}
	if tokens.AccessToken == "" || tokens.IDToken == "" || tokens.ExpiresIn != 600 {
		t.Fatalf("tokens %+v", tokens)
	}

	// What om3 checks: the signature by the jwks, iss, aud = the client id,
	// exp, and the grants in entitlements.
	tok, err := jwt.ParseSigned(tokens.AccessToken, []jose.SignatureAlgorithm{jose.ES256})
	if err != nil {
		t.Fatal(err)
	}
	kid := tok.Headers[0].KeyID
	jwks := p.Keys().JWKS()
	keys := jwks.Key(kid)
	if len(keys) != 1 {
		t.Fatalf("kid %s not in the jwks", kid)
	}
	var claims struct {
		jwt.Claims
		PreferredUsername string   `json:"preferred_username"`
		Entitlements      []string `json:"entitlements"`
	}
	if err := tok.Claims(keys[0].Key, &claims); err != nil {
		t.Fatal(err)
	}
	if err := claims.Validate(jwt.Expected{Issuer: "https://collector.example.com/api/oidc", AnyAudience: jwt.Audience{"cluster-1"}}); err != nil {
		t.Errorf("access token claims: %v", err)
	}
	if claims.Subject != "42" || claims.PreferredUsername != "jo@example.com" ||
		strings.Join(claims.Entitlements, " ") != "admin:prod guest:*" {
		t.Errorf("unexpected claims %+v", claims)
	}

	at, err := p.ParseToken(tokens.AccessToken, false)
	if err != nil || at.ClientID != "cluster-1" {
		t.Errorf("parse access token: %+v %v", at, err)
	}
	it, err := p.ParseToken(tokens.IDToken, false)
	if err != nil || it.Nonce != "n0nce" || it.Email != "jo@example.com" || it.ClientID != "" {
		t.Errorf("parse id token: %+v %v", it, err)
	}

	// Without openid, no id token; without email, no email in it.
	tokens, _ = p.IssueTokens("cluster-1", id, "offline_access", "", auth, nil)
	if tokens.IDToken != "" {
		t.Error("an id token without the openid scope")
	}
	tokens, _ = p.IssueTokens("cluster-1", id, "openid", "", auth, nil)
	if it, _ := p.ParseToken(tokens.IDToken, false); it == nil || it.Email != "" {
		t.Errorf("an email without the email scope: %+v", it)
	}

	// Expired: refused, but for an id_token_hint.
	p.now = func() time.Time { return time.Now().Add(-time.Hour) }
	tokens, _ = p.IssueTokens("cluster-1", id, "openid", "", auth, nil)
	p.now = time.Now
	if _, err := p.ParseToken(tokens.IDToken, false); err == nil {
		t.Error("an expired token accepted")
	}
	if _, err := p.ParseToken(tokens.IDToken, true); err != nil {
		t.Errorf("an expired id_token_hint refused: %v", err)
	}

	// Signed by another provider: refused.
	other, _ := newTestProvider(t)
	tokens, _ = other.IssueTokens("cluster-1", id, "openid", "", auth, nil)
	if _, err := p.ParseToken(tokens.AccessToken, false); err == nil {
		t.Error("a token of another key accepted")
	}
}

func TestRedirects(t *testing.T) {
	r := &AuthRequest{RedirectURI: "https://n1:1215/auth-callback?x=1", State: "st"}
	u, _ := url.Parse(RedirectWithCode(r, "c0de", "https://iss"))
	q := u.Query()
	if q.Get("code") != "c0de" || q.Get("state") != "st" || q.Get("x") != "1" || q.Get("iss") != "https://iss" {
		t.Errorf("redirect with code %s", u)
	}
	u, _ = url.Parse(RedirectWithError(&AuthRequest{RedirectURI: "https://n1/auth-callback"}, "access_denied", "no", ""))
	if u.Query().Get("error") != "access_denied" || u.Query().Has("state") || u.Query().Has("iss") {
		t.Errorf("redirect with error %s", u)
	}
	if got := RedirectWithState("https://n1/", ""); got != "https://n1/" {
		t.Errorf("redirect with no state %s", got)
	}
}

func TestFilterScope(t *testing.T) {
	if got := FilterScope("openid profile opensvc:om3:root openid offline_access foo"); got != "openid profile offline_access" {
		t.Errorf("filtered scope %q", got)
	}
}
