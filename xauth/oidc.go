package xauth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/go-redis/redis/v8"
	"golang.org/x/oauth2"

	"github.com/opensvc/oc3/util/logkey"
)

// OIDCConfig is the configuration of the OpenID Connect sign-in, under
// server.oidc. oc3 is a confidential client using the authorization code flow
// with PKCE, and keeps the tokens on the server side: the browser only holds an
// opaque session cookie (Backend for Frontend). It is also a resource server
// accepting the provider's JWT access tokens as Bearer credentials.
type OIDCConfig struct {
	Enable bool
	// Issuer is the exact issuer identifier, as the discovery document gives it.
	Issuer       string
	ClientID     string
	ClientSecret string
	// RedirectURL is the callback, /api/auth/callback on the origin of the SPA.
	RedirectURL string
	// PostLogoutRedirectURL is where the provider sends the user back after
	// signing out; empty, the provider's own page stays.
	PostLogoutRedirectURL string
	Scopes                []string
	// DisplayName names the provider on the sign-in screen.
	DisplayName string
	// APIAudience is the audience the Bearer access tokens must carry; the client
	// id by default, which is what providers like authentik put in theirs.
	APIAudience string
	// LinkByVerifiedEmail links an unknown identity to the account with the same
	// email, when the provider says the email is verified.
	LinkByVerifiedEmail bool
	// AutoCreateUsers creates an account for an unknown identity whose email no
	// account uses yet, when a claim rule allows its access (ClaimRules): nobody
	// is created while no rule decides access.
	AutoCreateUsers bool
	IdleTimeout     time.Duration
	MaxLifetime     time.Duration
	// CookieSecure sets the Secure attribute and the __Host- prefix on the cookies.
	// False only for a development origin served over plain http.
	CookieSecure bool
}

// signingAlgs is the allowlist of signature algorithms of the tokens: asymmetric
// only, never "none" nor an HMAC that could be keyed with a public key.
var signingAlgs = []string{
	oidc.RS256, oidc.RS384, oidc.RS512,
	oidc.PS256, oidc.PS384, oidc.PS512,
	oidc.ES256, oidc.ES384, oidc.ES512,
	oidc.EdDSA,
}

const (
	loginTTL      = 10 * time.Minute
	redisPrefix   = "oc3:oidc:"
	touchInterval = time.Minute
)

// OIDC is the OpenID Connect sign-in and the store of the sessions it opens.
type OIDC struct {
	cfg    OIDCConfig
	redis  *redis.Client
	db     *sql.DB
	origin string

	mu         sync.RWMutex
	ready      bool
	oauth      oauth2.Config
	idToken    *oidc.IDTokenVerifier
	access     *oidc.IDTokenVerifier
	logout     *oidc.IDTokenVerifier
	endSession string

	bearerCache *bearerCache
	rules       ruleCache
}

// ReadSecretFile returns the trimmed content of a file holding a secret.
func ReadSecretFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

// NewOIDC checks the configuration and starts discovering the provider in the
// background: an unreachable provider must not keep the server, and the nodes
// feeding it, from starting. Until discovery succeeds, the sign-in answers that it
// is unavailable.
func NewOIDC(ctx context.Context, cfg OIDCConfig, rdb *redis.Client, db *sql.DB) (*OIDC, error) {
	if cfg.Issuer == "" || cfg.ClientID == "" || cfg.RedirectURL == "" {
		return nil, errors.New("oidc: issuer, client_id and redirect_url are mandatory")
	}
	if cfg.ClientSecret == "" {
		return nil, errors.New("oidc: the client secret is mandatory (client_secret_file or OC3_SERVER_OIDC_CLIENT_SECRET)")
	}
	redirect, err := url.Parse(cfg.RedirectURL)
	if err != nil || redirect.Host == "" || (redirect.Scheme != "https" && redirect.Scheme != "http") {
		return nil, fmt.Errorf("oidc: invalid redirect_url %q", cfg.RedirectURL)
	}
	if redirect.Scheme == "https" && !cfg.CookieSecure {
		return nil, errors.New("oidc: session.cookie_secure cannot be false with an https redirect_url")
	}
	if redirect.Scheme == "http" && cfg.CookieSecure && redirect.Hostname() != "localhost" {
		return nil, errors.New("oidc: an http redirect_url needs session.cookie_secure false (development only)")
	}
	if len(cfg.Scopes) == 0 {
		cfg.Scopes = []string{oidc.ScopeOpenID, "email", "profile"}
	}
	if !slices.Contains(cfg.Scopes, oidc.ScopeOpenID) {
		cfg.Scopes = append([]string{oidc.ScopeOpenID}, cfg.Scopes...)
	}
	if cfg.APIAudience == "" {
		cfg.APIAudience = cfg.ClientID
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = time.Hour
	}
	if cfg.MaxLifetime <= 0 {
		cfg.MaxLifetime = 12 * time.Hour
	}
	if cfg.DisplayName == "" {
		cfg.DisplayName = "OpenID Connect"
	}
	o := &OIDC{
		cfg:         cfg,
		redis:       rdb,
		db:          db,
		origin:      redirect.Scheme + "://" + redirect.Host,
		bearerCache: newBearerCache(4096),
	}
	go o.discover(ctx)
	return o, nil
}

// discover reads the discovery document until it succeeds, backing off up to a
// minute between attempts. go-oidc refuses a document whose issuer is not exactly
// the configured one.
func (o *OIDC) discover(ctx context.Context) {
	delay := 2 * time.Second
	for {
		err := o.discoverOnce(ctx)
		if err == nil {
			slog.Info("oidc: provider discovered", "issuer", o.cfg.Issuer)
			return
		}
		slog.Error("oidc: cannot discover the provider, retrying", "issuer", o.cfg.Issuer, "retry_in", delay.String(), logkey.Error, err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		delay = min(delay*2, time.Minute)
	}
}

func (o *OIDC) discoverOnce(ctx context.Context) error {
	provider, err := oidc.NewProvider(ctx, o.cfg.Issuer)
	if err != nil {
		return err
	}
	var extra struct {
		EndSession string `json:"end_session_endpoint"`
	}
	if err := provider.Claims(&extra); err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.oauth = oauth2.Config{
		ClientID:     o.cfg.ClientID,
		ClientSecret: o.cfg.ClientSecret,
		Endpoint:     provider.Endpoint(),
		RedirectURL:  o.cfg.RedirectURL,
		Scopes:       o.cfg.Scopes,
	}
	o.idToken = provider.Verifier(&oidc.Config{ClientID: o.cfg.ClientID, SupportedSigningAlgs: signingAlgs})
	o.access = provider.Verifier(&oidc.Config{ClientID: o.cfg.APIAudience, SupportedSigningAlgs: signingAlgs})
	// A logout token may carry no exp: its freshness is checked on iat instead.
	o.logout = provider.Verifier(&oidc.Config{ClientID: o.cfg.ClientID, SupportedSigningAlgs: signingAlgs, SkipExpiryCheck: true})
	o.endSession = extra.EndSession
	o.ready = true
	return nil
}

// Ready reports whether the provider was discovered.
func (o *OIDC) Ready() bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.ready
}

// Config returns the configuration in effect, defaults applied.
func (o *OIDC) Config() OIDCConfig { return o.cfg }

// Origin is the scheme and host of the SPA, the only origin allowed to make
// requests authenticated by the session cookie.
func (o *OIDC) Origin() string { return o.origin }

func (o *OIDC) verifiers() (oauth2.Config, *oidc.IDTokenVerifier, *oidc.IDTokenVerifier, *oidc.IDTokenVerifier, string) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.oauth, o.idToken, o.access, o.logout, o.endSession
}

// ErrNotReady is returned while the provider has not been discovered.
var ErrNotReady = errors.New("oidc: the provider is not reachable yet")

// randomToken returns 32 random bytes, base64url encoded without padding.
func randomToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// hashKey is the Redis key of a secret: the secret itself, a session or login id
// that a cookie carries, never appears in Redis.
func hashKey(kind, secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return redisPrefix + kind + ":" + hex.EncodeToString(sum[:])
}

func equalStrings(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// SafeReturnTo keeps a return path only when it stays on the origin: a relative
// path starting with a single slash, without scheme nor host. Anything else
// becomes "/", so that the sign-in cannot be used as an open redirect.
func SafeReturnTo(s string) string {
	if s == "" || !strings.HasPrefix(s, "/") || strings.HasPrefix(s, "//") || strings.HasPrefix(s, "/\\") {
		return "/"
	}
	if strings.ContainsAny(s, "\\\r\n\t") {
		return "/"
	}
	u, err := url.Parse(s)
	if err != nil || u.Scheme != "" || u.Host != "" || u.User != nil {
		return "/"
	}
	return s
}
