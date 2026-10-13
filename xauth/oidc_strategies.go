package xauth

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shaj13/go-guardian/v2/auth"
)

// XAuthSource tells how a user request was authenticated: AuthSourceSession for
// the session cookie of an OIDC sign-in, AuthSourceBearer for an access token,
// AuthSourceBasic for a password. Requests made with the session cookie are the
// only ones exposed to cross-site request forgery.
const (
	XAuthSource       = "auth_source"
	AuthSourceSession = "session"
	AuthSourceBearer  = "bearer"
	AuthSourceBasic   = "basic"
)

const (
	queryUserByID = `SELECT auth_user.email, auth_user.registration_key FROM auth_user WHERE auth_user.id = ?`

	queryRolesByUserID = `SELECT auth_group.role FROM auth_membership
		JOIN auth_group ON auth_group.id = auth_membership.group_id
		WHERE auth_membership.user_id = ?`

	queryUserByIdentity = `SELECT auth_user.id, auth_user.email, auth_user.registration_key
		FROM auth_user_identities
		JOIN auth_user ON auth_user.id = auth_user_identities.user_id
		WHERE auth_user_identities.issuer = ? AND auth_user_identities.subject = ?`
)

var errNoSession = errors.New("no session")

// userInfo builds the identity of an account, its roles read from the database at
// every request as the Basic strategy does: a withdrawn privilege applies at once.
func userInfo(ctx context.Context, db *sql.DB, userID int64, email, source string, roles []string) auth.Info {
	ext := make(auth.Extensions)
	id := strconv.FormatInt(userID, 10)
	ext.Set(XUserID, id)
	ext.Set(XUserEmail, email)
	ext.Set(XAuthSource, source)
	return auth.NewUserInfo(email, id, roles, ext)
}

func rolesOf(ctx context.Context, db *sql.DB, userID int64) ([]string, error) {
	rows, err := db.QueryContext(ctx, queryRolesByUserID, userID)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	defer func() { _ = rows.Close() }()
	roles := []string{}
	for rows.Next() {
		var role sql.NullString
		if err := rows.Scan(&role); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
		}
		if role.Valid {
			roles = append(roles, role.String)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	return roles, nil
}

// lockedKey reports whether web2py would refuse the account a sign-in.
func lockedKey(key sql.NullString) bool {
	return strings.TrimSpace(key.String) != ""
}

type oidcSession struct {
	o  *OIDC
	db *sql.DB
}

// NewOIDCSession is the strategy of the session cookie set at the end of an OIDC
// sign-in. The account is read again at every request: a locked or deleted
// account ends its sessions.
func NewOIDCSession(o *OIDC, db *sql.DB) auth.Strategy {
	return &oidcSession{o: o, db: db}
}

func (s *oidcSession) Authenticate(ctx context.Context, r *http.Request) (auth.Info, error) {
	cookie, err := r.Cookie(s.o.SessionCookieName())
	if err != nil || cookie.Value == "" {
		return nil, errNoSession
	}
	sess, err := s.o.LoadSession(ctx, cookie.Value)
	if err != nil {
		return nil, err
	}
	if sess == nil {
		return nil, errors.New("session expired")
	}
	var (
		email sql.NullString
		key   sql.NullString
	)
	err = s.db.QueryRowContext(ctx, queryUserByID, sess.UserID).Scan(&email, &key)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && lockedKey(key)) {
		_, _ = s.o.DeleteSession(ctx, cookie.Value)
		return nil, errors.New("account removed or locked")
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	roles, err := rolesOf(ctx, s.db, sess.UserID)
	if err != nil {
		return nil, err
	}
	return userInfo(ctx, s.db, sess.UserID, email.String, AuthSourceSession, roles), nil
}

// verifiedToken is an access token already verified, kept until it expires.
type verifiedToken struct {
	issuer  string
	subject string
	claims  map[string]any
	expires time.Time
}

// bearerCache keeps the verified access tokens, keyed by their hash, so that a
// script calling the API in a loop does not have its token checked every time.
type bearerCache struct {
	mu   sync.Mutex
	max  int
	byID map[string]verifiedToken
}

func newBearerCache(max int) *bearerCache {
	return &bearerCache{max: max, byID: make(map[string]verifiedToken)}
}

func (c *bearerCache) get(key string) (verifiedToken, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.byID[key]
	if ok && !time.Now().Before(t.expires) {
		delete(c.byID, key)
		return verifiedToken{}, false
	}
	return t, ok
}

func (c *bearerCache) put(key string, t verifiedToken) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.byID) >= c.max {
		now := time.Now()
		for k, v := range c.byID {
			if !now.Before(v.expires) {
				delete(c.byID, k)
			}
		}
		if len(c.byID) >= c.max {
			c.byID = make(map[string]verifiedToken)
		}
	}
	c.byID[key] = t
}

// VerifyAccessToken checks a JWT access token: signature by a key of the
// provider's JWKS with an asymmetric algorithm, issuer, audience (APIAudience),
// expiry. ID tokens of other clients carry another audience and are refused.
func (o *OIDC) VerifyAccessToken(ctx context.Context, raw string) (*verifiedToken, error) {
	sum := sha256.Sum256([]byte(raw))
	key := hex.EncodeToString(sum[:])
	if t, ok := o.bearerCache.get(key); ok {
		return &t, nil
	}
	if !o.Ready() {
		return nil, ErrNotReady
	}
	_, _, verifier, _, _ := o.verifiers()
	token, err := verifier.Verify(ctx, raw)
	if err != nil {
		return nil, err
	}
	var claims map[string]any
	if err := token.Claims(&claims); err != nil {
		return nil, err
	}
	if token.Subject == "" {
		return nil, errors.New("no sub claim")
	}
	t := verifiedToken{
		issuer:  token.Issuer,
		subject: token.Subject,
		claims:  claims,
		expires: token.Expiry,
	}
	o.bearerCache.put(key, t)
	return &t, nil
}

type oidcBearer struct {
	o  *OIDC
	db *sql.DB
}

// NewOIDCBearer is the strategy of the provider's access tokens, for scripts and
// automations. The token must belong to an identity already linked to an account:
// a Bearer request never creates nor links one. The claim rules apply to its
// claims: access, and the teams they manage, the others coming from the database.
func NewOIDCBearer(o *OIDC, db *sql.DB) auth.Strategy {
	return &oidcBearer{o: o, db: db}
}

func (b *oidcBearer) Authenticate(ctx context.Context, r *http.Request) (auth.Info, error) {
	header := r.Header.Get("Authorization")
	scheme, raw, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(raw) == "" {
		return nil, errors.New("no bearer token")
	}
	token, err := b.o.VerifyAccessToken(ctx, strings.TrimSpace(raw))
	if err != nil {
		if errors.Is(err, ErrNotReady) {
			return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
		}
		return nil, errors.New("invalid bearer token")
	}
	var (
		userID int64
		email  sql.NullString
		key    sql.NullString
	)
	err = b.db.QueryRowContext(ctx, queryUserByIdentity, token.issuer, token.subject).Scan(&userID, &email, &key)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, errors.New("bearer token of an identity unknown to the collector")
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	if lockedKey(key) {
		return nil, errors.New("account locked")
	}
	rules, err := b.o.ClaimRules(ctx)
	if err != nil {
		return nil, err
	}
	outcome := EvaluateClaimRules(rules, token.claims)
	if !outcome.MayAccess() {
		return nil, errors.New("bearer token of an identity no claim rule allows")
	}
	roles, err := rolesOf(ctx, b.db, userID)
	if err != nil {
		return nil, err
	}
	// The teams a rule names follow the token's claims, without writing to the
	// database: the others are those of the account.
	if len(outcome.ManagedRoles) > 0 {
		kept := roles[:0]
		for _, role := range roles {
			if !contains(outcome.ManagedRoles, role) {
				kept = append(kept, role)
			}
		}
		roles = append(kept, outcome.GrantedRoles...)
	}
	return userInfo(ctx, b.db, userID, email.String, AuthSourceBearer, roles), nil
}

func contains(list []string, s string) bool {
	for _, item := range list {
		if item == s {
			return true
		}
	}
	return false
}

// AccessTokenClaims returns the claims of a verified access token that a rule may
// use.
func (o *OIDC) AccessTokenClaims(ctx context.Context, raw string) (map[string]any, error) {
	t, err := o.VerifyAccessToken(ctx, raw)
	if err != nil {
		return nil, err
	}
	return displayedClaims(t.claims), nil
}
