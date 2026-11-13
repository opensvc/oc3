// Package oidcprovider is the collector as the OpenID Connect provider of the
// om3 clusters: their webapps and the om CLI sign their users in with the
// collector, which hands tokens naming the cluster as their audience and
// carrying the om3 grants the user holds on it.
//
// Every cluster the collector knows is a public client (no secret: the webapp
// runs in a browser, the CLI on any machine), its cluster id the client id. The
// authorization code flow with PKCE (S256) is the only one. The redirection
// URIs are those of the nodes of the cluster, as the collector knows them.
//
// The authorization endpoint hands the request to the collector SPA, where the
// user signs in as usual (password or the collector's own provider); the SPA
// then approves the request through the API, which gives the code.
package oidcprovider

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
	"github.com/go-redis/redis/v8"
)

const (
	// RequestTTL is the time a user has to sign in and approve a request.
	RequestTTL = 10 * time.Minute
	// CodeTTL is the time a client has to trade its code for tokens.
	CodeTTL = time.Minute

	keyPrefix = "oc3:oidcp:"
)

// Scopes are the scopes the provider supports. opensvc:om3 is the one the om3
// clients ask for the grants, which every token carries anyway.
var Scopes = []string{"openid", "profile", "email", "offline_access", "opensvc:om3"}

// ErrNotFound is returned for a request or a code unknown, expired or used.
var ErrNotFound = errors.New("not found or expired")

// Config is the configuration of the provider.
type Config struct {
	// Issuer is the URL of the provider, as the clients reach it: the API URL
	// followed by /oidc.
	Issuer string
	// UIURL is the URL of the collector SPA, where a user signs in and
	// approves a request.
	UIURL string
	// AccessTokenTTL is the lifetime of the access and id tokens.
	AccessTokenTTL time.Duration
	// RefreshIdleTimeout is the lifetime of a refresh token, renewed with it.
	RefreshIdleTimeout time.Duration
	// RefreshMaxLifetime bounds the renewals: past it from the sign-in, the
	// user signs in again.
	RefreshMaxLifetime time.Duration
	// RedirectHostPatterns are host name patterns (*.example.com) allowed in
	// the redirection URIs of every cluster, beside the nodes of the cluster.
	RedirectHostPatterns []string
}

// Provider issues the codes and tokens.
type Provider struct {
	cfg   Config
	keys  *KeySet
	redis *redis.Client
	now   func() time.Time
}

// New returns the provider.
func New(cfg Config, keys *KeySet, rdb *redis.Client) (*Provider, error) {
	u, err := url.Parse(cfg.Issuer)
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") {
		return nil, fmt.Errorf("invalid issuer %q: an absolute http(s) url is expected", cfg.Issuer)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("invalid issuer %q: no query nor fragment allowed", cfg.Issuer)
	}
	cfg.Issuer = strings.TrimSuffix(cfg.Issuer, "/")
	if cfg.UIURL == "" {
		cfg.UIURL = u.Scheme + "://" + u.Host
	}
	cfg.UIURL = strings.TrimSuffix(cfg.UIURL, "/")
	if cfg.AccessTokenTTL <= 0 {
		cfg.AccessTokenTTL = 10 * time.Minute
	}
	if cfg.RefreshIdleTimeout <= 0 {
		cfg.RefreshIdleTimeout = time.Hour
	}
	if cfg.RefreshMaxLifetime <= 0 {
		cfg.RefreshMaxLifetime = 12 * time.Hour
	}
	if keys == nil {
		return nil, errors.New("no signing key")
	}
	return &Provider{cfg: cfg, keys: keys, redis: rdb, now: time.Now}, nil
}

// Config returns the configuration, defaults applied.
func (p *Provider) Config() Config { return p.cfg }

// Keys returns the signing keys.
func (p *Provider) Keys() *KeySet { return p.keys }

// Discovery is the document of /.well-known/openid-configuration.
func (p *Provider) Discovery() map[string]any {
	iss := p.cfg.Issuer
	return map[string]any{
		"issuer":                                         iss,
		"authorization_endpoint":                         iss + "/authorize",
		"token_endpoint":                                 iss + "/token",
		"userinfo_endpoint":                              iss + "/userinfo",
		"jwks_uri":                                       iss + "/jwks",
		"revocation_endpoint":                            iss + "/revoke",
		"end_session_endpoint":                           iss + "/logout",
		"scopes_supported":                               Scopes,
		"response_types_supported":                       []string{"code"},
		"response_modes_supported":                       []string{"query"},
		"grant_types_supported":                          []string{"authorization_code", "refresh_token"},
		"subject_types_supported":                        []string{"public"},
		"id_token_signing_alg_values_supported":          []string{p.keys.Algorithm()},
		"token_endpoint_auth_methods_supported":          []string{"none"},
		"code_challenge_methods_supported":               []string{"S256"},
		"authorization_response_iss_parameter_supported": true,
		"claims_supported": []string{
			"sub", "iss", "aud", "exp", "iat", "auth_time", "nonce",
			"name", "email", "preferred_username", "entitlements",
		},
	}
}

// AuthRequest is an authorization request waiting for the user to approve it.
type AuthRequest struct {
	ID                  string    `json:"-"`
	ClientID            string    `json:"client_id"`
	RedirectURI         string    `json:"redirect_uri"`
	Scope               string    `json:"scope"`
	State               string    `json:"state,omitempty"`
	Nonce               string    `json:"nonce,omitempty"`
	CodeChallenge       string    `json:"code_challenge"`
	CodeChallengeMethod string    `json:"code_challenge_method"`
	Prompt              string    `json:"prompt,omitempty"`
	Created             time.Time `json:"created"`
}

// HasPrompt tells whether the client asked the prompt value (none, login,
// consent).
func (r AuthRequest) HasPrompt(v string) bool {
	for _, s := range strings.Fields(r.Prompt) {
		if s == v {
			return true
		}
	}
	return false
}

// AuthCode is what a code stands for.
type AuthCode struct {
	ClientID      string    `json:"client_id"`
	RedirectURI   string    `json:"redirect_uri"`
	Scope         string    `json:"scope"`
	Nonce         string    `json:"nonce,omitempty"`
	CodeChallenge string    `json:"code_challenge"`
	UserID        int64     `json:"user_id"`
	AuthTime      time.Time `json:"auth_time"`
}

// RandomToken returns n random bytes, base64url encoded.
func RandomToken(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// HashToken is the sha256 of a token, hex encoded: what is stored of it.
func HashToken(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func (p *Provider) key(kind, id string) string { return keyPrefix + kind + ":" + HashToken(id) }

// SaveRequest stores the request until the user approves it, and gives it its
// id.
func (p *Provider) SaveRequest(ctx context.Context, r *AuthRequest) error {
	id, err := RandomToken(24)
	if err != nil {
		return err
	}
	r.ID = id
	r.Created = p.now()
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return p.redis.Set(ctx, p.key("req", id), b, RequestTTL).Err()
}

// LoadRequest returns the request, ErrNotFound when it expired or was used.
func (p *Provider) LoadRequest(ctx context.Context, id string) (*AuthRequest, error) {
	b, err := p.redis.Get(ctx, p.key("req", id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}
	var r AuthRequest
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	r.ID = id
	return &r, nil
}

// ConsumeRequest returns the request and forgets it: a request is approved or
// denied once.
func (p *Provider) ConsumeRequest(ctx context.Context, id string) (*AuthRequest, error) {
	b, err := p.redis.GetDel(ctx, p.key("req", id)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}
	var r AuthRequest
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, err
	}
	r.ID = id
	return &r, nil
}

// IssueCode stores what the code stands for and returns the code.
func (p *Provider) IssueCode(ctx context.Context, c AuthCode) (string, error) {
	code, err := RandomToken(32)
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(c)
	if err != nil {
		return "", err
	}
	if err := p.redis.Set(ctx, p.key("code", code), b, CodeTTL).Err(); err != nil {
		return "", err
	}
	return code, nil
}

// ConsumeCode returns what the code stands for, once.
func (p *Provider) ConsumeCode(ctx context.Context, code string) (*AuthCode, error) {
	b, err := p.redis.GetDel(ctx, p.key("code", code)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}
	var c AuthCode
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	return &c, nil
}

// VerifyPKCE tells whether the verifier is the one of the S256 challenge.
func VerifyPKCE(challenge, verifier string) bool {
	// RFC 7636: 43 to 128 characters of the unreserved set.
	if len(verifier) < 43 || len(verifier) > 128 {
		return false
	}
	sum := sha256.Sum256([]byte(verifier))
	want := base64.RawURLEncoding.EncodeToString(sum[:])
	return subtle.ConstantTimeCompare([]byte(want), []byte(challenge)) == 1
}

// ValidChallenge tells whether a code_challenge is a S256 one: 43 base64url
// characters.
func ValidChallenge(s string) bool {
	if len(s) != 43 {
		return false
	}
	_, err := base64.RawURLEncoding.DecodeString(s)
	return err == nil
}

// Identity is the user a token is about.
type Identity struct {
	UserID int64
	Email  string
	Name   string
}

// TokenClaims are the claims of the access and id tokens.
type TokenClaims struct {
	jwt.Claims
	AuthTime          *jwt.NumericDate `json:"auth_time,omitempty"`
	Nonce             string           `json:"nonce,omitempty"`
	ClientID          string           `json:"client_id,omitempty"`
	Scope             string           `json:"scope,omitempty"`
	Name              string           `json:"name,omitempty"`
	Email             string           `json:"email,omitempty"`
	PreferredUsername string           `json:"preferred_username,omitempty"`
	Entitlements      []string         `json:"entitlements,omitempty"`
}

// Tokens are what the token endpoint hands.
type Tokens struct {
	AccessToken string
	IDToken     string
	ExpiresIn   int
}

// IssueTokens signs the access token of the user for the client, carrying the
// grants, and the id token when the scope has openid.
func (p *Provider) IssueTokens(clientID string, id Identity, scope, nonce string, authTime time.Time, entitlements []string) (Tokens, error) {
	now := p.now()
	exp := now.Add(p.cfg.AccessTokenTTL)
	jti, err := RandomToken(16)
	if err != nil {
		return Tokens{}, err
	}
	base := jwt.Claims{
		Issuer:   p.cfg.Issuer,
		Subject:  strconv.FormatInt(id.UserID, 10),
		Audience: jwt.Audience{clientID},
		IssuedAt: jwt.NewNumericDate(now),
		Expiry:   jwt.NewNumericDate(exp),
	}
	at := TokenClaims{
		Claims:            base,
		AuthTime:          jwt.NewNumericDate(authTime),
		ClientID:          clientID,
		Scope:             scope,
		Name:              id.Name,
		Email:             id.Email,
		PreferredUsername: id.Email,
		Entitlements:      entitlements,
	}
	at.ID = jti
	if at.Entitlements == nil {
		at.Entitlements = []string{}
	}
	out := Tokens{ExpiresIn: int(p.cfg.AccessTokenTTL.Seconds())}
	if out.AccessToken, err = jwt.Signed(p.keys.signer).Claims(at).Serialize(); err != nil {
		return Tokens{}, fmt.Errorf("sign the access token: %w", err)
	}
	if !HasScope(scope, "openid") {
		return out, nil
	}
	it := TokenClaims{
		Claims:            base,
		AuthTime:          jwt.NewNumericDate(authTime),
		Nonce:             nonce,
		Name:              id.Name,
		PreferredUsername: id.Email,
		Entitlements:      at.Entitlements,
	}
	if HasScope(scope, "email") {
		it.Email = id.Email
	}
	if out.IDToken, err = jwt.Signed(p.keys.signer).Claims(it).Serialize(); err != nil {
		return Tokens{}, fmt.Errorf("sign the id token: %w", err)
	}
	return out, nil
}

// HasScope tells whether the space separated scope holds s.
func HasScope(scope, s string) bool {
	for _, v := range strings.Fields(scope) {
		if v == s {
			return true
		}
	}
	return false
}

// FilterScope keeps the supported scopes of the requested ones, in their order.
func FilterScope(scope string) string {
	out := []string{}
	for _, s := range strings.Fields(scope) {
		for _, supported := range Scopes {
			if s == supported && !HasScope(strings.Join(out, " "), s) {
				out = append(out, s)
			}
		}
	}
	return strings.Join(out, " ")
}

// ParseToken verifies a token the provider signed, with one of its keys, and
// returns its claims. With expiredOK, an expired token is accepted (the
// id_token_hint of a sign-out).
func (p *Provider) ParseToken(raw string, expiredOK bool) (*TokenClaims, error) {
	algs := []jose.SignatureAlgorithm{jose.RS256, jose.ES256, jose.ES384}
	tok, err := jwt.ParseSigned(raw, algs)
	if err != nil {
		return nil, err
	}
	if len(tok.Headers) != 1 {
		return nil, errors.New("one signature expected")
	}
	keys := p.keys.public.Key(tok.Headers[0].KeyID)
	if len(keys) == 0 {
		return nil, errors.New("unknown signing key")
	}
	var c TokenClaims
	if err := tok.Claims(keys[0].Key, &c); err != nil {
		return nil, err
	}
	expected := jwt.Expected{Issuer: p.cfg.Issuer}
	at := p.now()
	if expiredOK && c.IssuedAt != nil {
		at = c.IssuedAt.Time()
	}
	expected.Time = at
	if err := c.ValidateWithLeeway(expected, time.Minute); err != nil {
		return nil, err
	}
	return &c, nil
}

// SignInURL is the page of the SPA where the user approves the request.
func (p *Provider) SignInURL(requestID string) string {
	return p.cfg.UIURL + "/oidc/authorize?request=" + url.QueryEscape(requestID)
}

// RedirectWithCode is the redirection URI of the request, with the code.
func RedirectWithCode(r *AuthRequest, code, issuer string) string {
	return redirectWith(r.RedirectURI, url.Values{"code": {code}, "state": {r.State}, "iss": {issuer}})
}

// RedirectWithError is the redirection URI of the request, with an error.
func RedirectWithError(r *AuthRequest, code, description, issuer string) string {
	return redirectWith(r.RedirectURI, url.Values{
		"error": {code}, "error_description": {description}, "state": {r.State}, "iss": {issuer},
	})
}

// RedirectWithState is the URI with the state of the client, if any: the
// return after a sign-out.
func RedirectWithState(raw, state string) string {
	return redirectWith(raw, url.Values{"state": {state}})
}

func redirectWith(raw string, v url.Values) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	q := u.Query()
	for k, values := range v {
		if len(values) == 0 || values[0] == "" {
			continue
		}
		q.Set(k, values[0])
	}
	u.RawQuery = q.Encode()
	return u.String()
}
