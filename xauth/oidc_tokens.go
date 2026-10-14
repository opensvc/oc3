package xauth

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"golang.org/x/oauth2"
)

// The tokens of a sign-in: the provider's access and refresh tokens of the user,
// kept with their session so that oc3 can call, on the user's behalf, a service
// that requires them, such as the AI agent. They live in the session record,
// encrypted with the key of OIDCConfig.TokenKeyFile, never reach the browser, and
// end with the session. Without a key, they are not kept.

// ErrNoToken tells a session without a usable access token: no key configured,
// no token kept at sign-in, or a refresh the provider refused. The user has to
// sign in again.
var ErrNoToken = errors.New("oidc: no access token for this session; sign in again")

// refreshMargin is how long before its expiry an access token is refreshed.
const refreshMargin = time.Minute

// sessionTokens is the clear form of Session.Tokens.
type sessionTokens struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token,omitempty"`
	Expiry       time.Time `json:"expiry"`
}

// LoadTokenKey reads the key encrypting the session tokens: the base64 of 32
// random bytes, alone in a file that must not be readable by its group or by
// others.
func LoadTokenKey(path string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("oidc token key: %w", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("oidc token key %s: mode %04o lets its group or others read it, use 0600", path, info.Mode().Perm())
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("oidc token key: %w", err)
	}
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(b)))
	if err != nil || len(key) != 32 {
		return nil, errors.New("oidc token key: the file must hold the base64 of 32 bytes")
	}
	return key, nil
}

// KeepsTokens tells whether the sessions keep the provider's tokens.
func (o *OIDC) KeepsTokens() bool { return len(o.cfg.TokenKey) == 32 }

// tokensAAD binds the encrypted tokens to the identity of the session: a token
// blob copied into another user's session does not open.
func tokensAAD(s *Session) []byte {
	return []byte("oc3-oidc-tokens:v1:" + s.Issuer + "\x00" + s.Subject)
}

func (o *OIDC) sealTokens(s *Session, t sessionTokens) (string, error) {
	block, err := aes.NewCipher(o.cfg.TokenKey)
	if err != nil {
		return "", err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	plain, err := json.Marshal(t)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(aead.Seal(nonce, nonce, plain, tokensAAD(s))), nil
}

func (o *OIDC) openTokens(s *Session) (*sessionTokens, error) {
	if s.Tokens == "" || !o.KeepsTokens() {
		return nil, ErrNoToken
	}
	sealed, err := base64.StdEncoding.DecodeString(s.Tokens)
	if err != nil {
		return nil, ErrNoToken
	}
	block, err := aes.NewCipher(o.cfg.TokenKey)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(sealed) < aead.NonceSize() {
		return nil, ErrNoToken
	}
	plain, err := aead.Open(nil, sealed[:aead.NonceSize()], sealed[aead.NonceSize():], tokensAAD(s))
	if err != nil {
		// Another key, or another session's tokens.
		return nil, ErrNoToken
	}
	var t sessionTokens
	if err := json.Unmarshal(plain, &t); err != nil || t.AccessToken == "" {
		return nil, ErrNoToken
	}
	return &t, nil
}

// Token is the provider's token of a sign-in, for CreateSession to keep.
func (c *Claims) Token() *oauth2.Token { return c.token }

// WithToken sets the provider's token a new session keeps (CreateSession).
func (s *Session) WithToken(t *oauth2.Token) { s.token = t }

// AccessToken returns a valid access token of the user of a session, refreshed
// with the refresh token when it expires within refreshMargin. Concurrent requests
// of one session refresh once: the others wait for the new token, the provider
// rotating refresh tokens. The session is not extended.
func (o *OIDC) AccessToken(ctx context.Context, sessionID string) (string, error) {
	if !o.KeepsTokens() {
		return "", ErrNoToken
	}
	key := hashKey("session", sessionID)
	lock := hashKey("refresh", sessionID)
	deadline := time.Now().Add(10 * time.Second)
	for {
		s, err := o.readSession(ctx, key)
		if err != nil {
			return "", err
		}
		if s == nil {
			return "", ErrNoToken
		}
		t, err := o.openTokens(s)
		if err != nil {
			return "", err
		}
		if time.Until(t.Expiry) > refreshMargin {
			return t.AccessToken, nil
		}
		if t.RefreshToken == "" {
			return "", ErrNoToken
		}
		got, err := o.redis.SetNX(ctx, lock, "1", 15*time.Second).Result()
		if err != nil {
			return "", fmt.Errorf("%w: oidc refresh lock: %w", ErrUnavailable, err)
		}
		if !got {
			// Another request refreshes: read its result shortly.
			if time.Now().After(deadline) {
				return "", ErrNoToken
			}
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(200 * time.Millisecond):
			}
			continue
		}
		access, err := o.refresh(ctx, key, s, t)
		_ = o.redis.Del(context.WithoutCancel(ctx), lock).Err()
		return access, err
	}
}

// readSession reads a session record without extending it.
func (o *OIDC) readSession(ctx context.Context, key string) (*Session, error) {
	b, err := o.redis.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: oidc session: %w", ErrUnavailable, err)
	}
	var s Session
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, nil
	}
	if !time.Now().Before(s.Expires) {
		return nil, nil
	}
	return &s, nil
}

// refresh asks the provider for a new access token and writes it into the
// session, its expiry in the store kept.
func (o *OIDC) refresh(ctx context.Context, key string, s *Session, t *sessionTokens) (string, error) {
	if !o.Ready() {
		return "", fmt.Errorf("%w: %w", ErrUnavailable, ErrNotReady)
	}
	oauthCfg, _, _, _, _ := o.verifiers()
	fresh, err := oauthCfg.TokenSource(ctx, &oauth2.Token{RefreshToken: t.RefreshToken, Expiry: time.Unix(1, 0)}).Token()
	if err != nil {
		// Revoked, expired, or the user disabled at the provider: the tokens are
		// dropped, and the session goes on without them.
		s.Tokens = ""
		if b, err := json.Marshal(s); err == nil {
			_ = o.redis.SetArgs(ctx, key, b, redis.SetArgs{KeepTTL: true}).Err()
		}
		return "", ErrNoToken
	}
	next := sessionTokens{AccessToken: fresh.AccessToken, RefreshToken: fresh.RefreshToken, Expiry: fresh.Expiry}
	if next.RefreshToken == "" {
		next.RefreshToken = t.RefreshToken
	}
	sealed, err := o.sealTokens(s, next)
	if err != nil {
		return "", err
	}
	s.Tokens = sealed
	b, err := json.Marshal(s)
	if err != nil {
		return "", err
	}
	if err := o.redis.SetArgs(ctx, key, b, redis.SetArgs{KeepTTL: true}).Err(); err != nil {
		return "", fmt.Errorf("%w: oidc session: %w", ErrUnavailable, err)
	}
	return next.AccessToken, nil
}
