package serverhandlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/oidcprovider"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// The endpoints of the collector as the OpenID Connect provider of the om3
// clusters. They are the protocol's, not the API's: public (each checks what
// it needs), out of the OpenAPI document, and open to the scripts of every
// origin, the om3 webapps being served by the nodes. No cookie is read: the
// user signs in on the SPA, which approves the request through the API.

// RegisterOIDCProvider adds the endpoints of the provider under prefix, the
// path of its issuer URL.
func (a *Api) RegisterOIDCProvider(e *echo.Echo, prefix string) {
	g := e.Group(prefix, oidcProviderCORS)
	routes := map[string][]string{
		"/.well-known/openid-configuration": {http.MethodGet},
		"/jwks":                             {http.MethodGet},
		"/authorize":                        {http.MethodGet},
		"/token":                            {http.MethodPost},
		"/userinfo":                         {http.MethodGet, http.MethodPost},
		"/revoke":                           {http.MethodPost},
		"/logout":                           {http.MethodGet},
	}
	handlers := map[string]echo.HandlerFunc{
		"/.well-known/openid-configuration": a.oidcProviderDiscovery,
		"/jwks":                             a.oidcProviderJWKS,
		"/authorize":                        a.oidcProviderAuthorize,
		"/token":                            a.oidcProviderToken,
		"/userinfo":                         a.oidcProviderUserinfo,
		"/revoke":                           a.oidcProviderRevoke,
		"/logout":                           a.oidcProviderLogout,
	}
	for path, methods := range routes {
		for _, m := range methods {
			g.Add(m, path, handlers[path])
		}
		g.OPTIONS(path, func(c echo.Context) error { return c.NoContent(http.StatusNoContent) })
	}
}

// oidcProviderCORS lets the scripts of any origin call the endpoints, without
// credentials: the om3 webapp reads the discovery document, trades its code
// and renews its tokens from the origin of a node.
func oidcProviderCORS(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		h := c.Response().Header()
		h.Set("Access-Control-Allow-Origin", "*")
		if c.Request().Method == http.MethodOptions {
			h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
			h.Set("Access-Control-Max-Age", "600")
		}
		return next(c)
	}
}

// oauthError is the error answer of the token, userinfo and revocation
// endpoints (RFC 6749 5.2).
func oauthError(c echo.Context, status int, code, description string) error {
	noStore(c)
	if status == http.StatusUnauthorized {
		c.Response().Header().Set("WWW-Authenticate", fmt.Sprintf("Bearer error=%q", code))
	}
	return c.JSON(status, map[string]string{"error": code, "error_description": description})
}

func (a *Api) oidcProviderDiscovery(c echo.Context) error {
	c.Response().Header().Set("Cache-Control", "public, max-age=300")
	return c.JSON(http.StatusOK, a.OIDCProvider.Discovery())
}

func (a *Api) oidcProviderJWKS(c echo.Context) error {
	c.Response().Header().Set("Cache-Control", "public, max-age=300")
	return c.JSON(http.StatusOK, a.OIDCProvider.Keys().JWKS())
}

// oidcProviderClient returns the cluster of the client id and the hosts its
// redirection URIs may name; a nil client when the cluster is unknown.
func (a *Api) oidcProviderClient(ctx context.Context, clientID string) (*cdb.OIDCClient, oidcprovider.ClientHosts, error) {
	hosts := oidcprovider.ClientHosts{Patterns: a.OIDCProvider.Config().RedirectHostPatterns}
	if clientID == "" {
		return nil, hosts, nil
	}
	client, err := a.ODB.OIDCClientByID(ctx, clientID)
	if err != nil || client == nil {
		return nil, hosts, err
	}
	if hosts.Hosts, err = a.ODB.OIDCClientHosts(ctx, clientID); err != nil {
		return nil, hosts, err
	}
	return client, hosts, nil
}

// oidcProviderEntitlements returns the om3 grants of the user on the cluster.
// A rule holding a grant that is not one any more (the rules are checked when
// written) is skipped and logged.
func (a *Api) oidcProviderEntitlements(ctx context.Context, log *slog.Logger, userID int64, clusterID string) ([]string, error) {
	lists, err := a.ODB.OIDCUserGrants(ctx, userID, clusterID)
	if err != nil {
		return nil, err
	}
	parsed := make([][]string, 0, len(lists))
	for _, s := range lists {
		l, err := oidcprovider.ParseGrants(s)
		if err != nil {
			log.Warn("skip an invalid om3 grant rule", logkey.Error, err)
			continue
		}
		parsed = append(parsed, l)
	}
	return oidcprovider.MergeGrants(parsed...), nil
}

// oidcProviderAuthorize handles GET <issuer>/authorize: it checks the client
// and its redirection URI, then hands the request to the SPA, where the user
// signs in and approves it. Until the redirection URI is known to be the
// cluster's, an error is shown here, never sent there.
func (a *Api) oidcProviderAuthorize(c echo.Context) error {
	log := echolog.GetLogHandler(c, "oidcProviderAuthorize")
	ctx := c.Request().Context()
	noStore(c)
	q := c.QueryParams()
	clientID := q.Get("client_id")
	redirectURI := q.Get("redirect_uri")
	client, hosts, err := a.oidcProviderClient(ctx, clientID)
	if err != nil {
		log.Error("cannot read the client", logkey.Error, err)
		return c.String(http.StatusInternalServerError, "cannot read the client")
	}
	if client == nil {
		return c.String(http.StatusBadRequest, "unknown client_id: the client id of an om3 cluster is its cluster id")
	}
	if redirectURI == "" {
		return c.String(http.StatusBadRequest, "redirect_uri is mandatory")
	}
	if err := oidcprovider.CheckRedirectURI(redirectURI, oidcprovider.RedirectSignIn, hosts); err != nil {
		log.Warn("refuse a redirect uri", "client_id", clientID, logkey.Error, err)
		return c.String(http.StatusBadRequest, err.Error())
	}
	r := &oidcprovider.AuthRequest{
		ClientID:            clientID,
		RedirectURI:         redirectURI,
		Scope:               oidcprovider.FilterScope(q.Get("scope")),
		State:               q.Get("state"),
		Nonce:               q.Get("nonce"),
		CodeChallenge:       q.Get("code_challenge"),
		CodeChallengeMethod: q.Get("code_challenge_method"),
		Prompt:              q.Get("prompt"),
	}
	issuer := a.OIDCProvider.Config().Issuer
	fail := func(code, description string) error {
		return c.Redirect(http.StatusFound, oidcprovider.RedirectWithError(r, code, description, issuer))
	}
	switch {
	case q.Get("response_type") != "code":
		return fail("unsupported_response_type", "the code flow is the only one")
	case q.Get("response_mode") != "" && q.Get("response_mode") != "query":
		return fail("invalid_request", "the query response mode is the only one")
	case q.Get("request") != "" || q.Get("request_uri") != "":
		return fail("request_not_supported", "request objects are not supported")
	case r.CodeChallengeMethod != "S256" || !oidcprovider.ValidChallenge(r.CodeChallenge):
		return fail("invalid_request", "PKCE with the S256 method is required")
	case len(r.State) > 1024 || len(r.Nonce) > 1024:
		return fail("invalid_request", "state or nonce too long")
	}
	if err := a.OIDCProvider.SaveRequest(ctx, r); err != nil {
		log.Error("cannot save the request", logkey.Error, err)
		return fail("temporarily_unavailable", "cannot save the request")
	}
	return c.Redirect(http.StatusFound, a.OIDCProvider.SignInURL(r.ID))
}

// oidcProviderToken handles POST <issuer>/token: the code traded for tokens,
// or a refresh token for new ones.
func (a *Api) oidcProviderToken(c echo.Context) error {
	log := echolog.GetLogHandler(c, "oidcProviderToken")
	ctx := c.Request().Context()
	if err := c.Request().ParseForm(); err != nil {
		return oauthError(c, http.StatusBadRequest, "invalid_request", "invalid form")
	}
	form := c.Request().PostForm
	if form.Get("client_secret") != "" || c.Request().Header.Get("Authorization") != "" {
		return oauthError(c, http.StatusUnauthorized, "invalid_client", "the om3 clusters are public clients, without secret")
	}
	switch form.Get("grant_type") {
	case "authorization_code":
		return a.oidcProviderTokenFromCode(c, log, ctx, form.Get("client_id"), form.Get("code"), form.Get("redirect_uri"), form.Get("code_verifier"))
	case "refresh_token":
		return a.oidcProviderTokenFromRefresh(c, log, ctx, form.Get("client_id"), form.Get("refresh_token"))
	case "":
		return oauthError(c, http.StatusBadRequest, "invalid_request", "grant_type is mandatory")
	default:
		return oauthError(c, http.StatusBadRequest, "unsupported_grant_type", "authorization_code and refresh_token are the grant types supported")
	}
}

func (a *Api) oidcProviderTokenFromCode(c echo.Context, log *slog.Logger, ctx context.Context, clientID, code, redirectURI, verifier string) error {
	if code == "" || clientID == "" {
		return oauthError(c, http.StatusBadRequest, "invalid_request", "code and client_id are mandatory")
	}
	ac, err := a.OIDCProvider.ConsumeCode(ctx, code)
	if errors.Is(err, oidcprovider.ErrNotFound) {
		return oauthError(c, http.StatusBadRequest, "invalid_grant", "unknown, expired or used code")
	} else if err != nil {
		log.Error("cannot read the code", logkey.Error, err)
		return oauthError(c, http.StatusServiceUnavailable, "temporarily_unavailable", "cannot read the code")
	}
	switch {
	case ac.ClientID != clientID:
		return oauthError(c, http.StatusBadRequest, "invalid_grant", "the code was issued to another client")
	case ac.RedirectURI != redirectURI:
		return oauthError(c, http.StatusBadRequest, "invalid_grant", "redirect_uri differs from the one of the authorization request")
	case !oidcprovider.VerifyPKCE(ac.CodeChallenge, verifier):
		return oauthError(c, http.StatusBadRequest, "invalid_grant", "code_verifier does not match the code challenge")
	}
	return a.oidcProviderIssue(c, log, ctx, ac.UserID, clientID, ac.Scope, ac.Nonce, ac.AuthTime, "")
}

func (a *Api) oidcProviderTokenFromRefresh(c echo.Context, log *slog.Logger, ctx context.Context, clientID, raw string) error {
	if raw == "" || clientID == "" {
		return oauthError(c, http.StatusBadRequest, "invalid_request", "refresh_token and client_id are mandatory")
	}
	t, err := a.ODB.OIDCRefreshTokenByHash(ctx, oidcprovider.HashToken(raw))
	if err != nil {
		log.Error("cannot read the refresh token", logkey.Error, err)
		return oauthError(c, http.StatusServiceUnavailable, "temporarily_unavailable", "cannot read the refresh token")
	}
	if t == nil || t.Revoked {
		return oauthError(c, http.StatusBadRequest, "invalid_grant", "unknown or revoked refresh token")
	}
	if t.ClientID != clientID {
		return oauthError(c, http.StatusBadRequest, "invalid_grant", "the refresh token was issued to another client")
	}
	cfg := a.OIDCProvider.Config()
	now := time.Now()
	if now.After(t.Expires) || now.After(t.AuthTime.Add(cfg.RefreshMaxLifetime)) {
		return oauthError(c, http.StatusBadRequest, "invalid_grant", "expired refresh token, sign in again")
	}
	fresh, err := a.ODB.UseOIDCRefreshToken(ctx, t.ID)
	if err != nil {
		log.Error("cannot use the refresh token", logkey.Error, err)
		return oauthError(c, http.StatusServiceUnavailable, "temporarily_unavailable", "cannot use the refresh token")
	}
	if !fresh {
		// Used before: a copy of it is in other hands, or the client lost the
		// answer. Either way the family is no longer to be trusted.
		log.Warn("refresh token used again, revoke its family", "client_id", clientID, "user_id", t.UserID)
		if err := a.ODB.RevokeOIDCRefreshFamily(ctx, t.Family); err != nil {
			log.Error("cannot revoke the refresh token family", logkey.Error, err)
		}
		return oauthError(c, http.StatusBadRequest, "invalid_grant", "refresh token already used, sign in again")
	}
	return a.oidcProviderIssue(c, log, ctx, t.UserID, clientID, t.Scope, "", t.AuthTime, t.Family)
}

// oidcProviderIssue answers the token request with the tokens of the user for
// the client, its grants read again, and a new refresh token of the family
// (a new family when empty).
func (a *Api) oidcProviderIssue(c echo.Context, log *slog.Logger, ctx context.Context, userID int64, clientID, scope, nonce string, authTime time.Time, family string) error {
	revoke := func() {
		if family != "" {
			if err := a.ODB.RevokeOIDCRefreshFamily(ctx, family); err != nil {
				log.Error("cannot revoke the refresh token family", logkey.Error, err)
			}
		}
	}
	user, err := a.ODB.OIDCUserByID(ctx, userID)
	if err != nil {
		log.Error("cannot read the user", logkey.Error, err)
		return oauthError(c, http.StatusServiceUnavailable, "temporarily_unavailable", "cannot read the user")
	}
	if user == nil || user.Locked {
		revoke()
		return oauthError(c, http.StatusBadRequest, "invalid_grant", "the account is gone or locked")
	}
	client, err := a.ODB.OIDCClientByID(ctx, clientID)
	if err != nil {
		log.Error("cannot read the client", logkey.Error, err)
		return oauthError(c, http.StatusServiceUnavailable, "temporarily_unavailable", "cannot read the client")
	}
	if client == nil {
		revoke()
		return oauthError(c, http.StatusBadRequest, "invalid_grant", "the cluster is no longer known to the collector")
	}
	grants, err := a.oidcProviderEntitlements(ctx, log, userID, clientID)
	if err != nil {
		log.Error("cannot read the grants", logkey.Error, err)
		return oauthError(c, http.StatusServiceUnavailable, "temporarily_unavailable", "cannot read the grants")
	}
	if len(grants) == 0 {
		revoke()
		return oauthError(c, http.StatusBadRequest, "invalid_grant", "no om3 grant on this cluster any more")
	}
	tokens, err := a.OIDCProvider.IssueTokens(clientID,
		oidcprovider.Identity{UserID: user.ID, Email: user.Email, Name: user.Name}, scope, nonce, authTime, grants)
	if err != nil {
		log.Error("cannot sign the tokens", logkey.Error, err)
		return oauthError(c, http.StatusInternalServerError, "server_error", "cannot sign the tokens")
	}
	refresh, err := oidcprovider.RandomToken(32)
	if err != nil {
		return oauthError(c, http.StatusInternalServerError, "server_error", "cannot make the refresh token")
	}
	if family == "" {
		if family, err = oidcprovider.RandomToken(24); err != nil {
			return oauthError(c, http.StatusInternalServerError, "server_error", "cannot make the refresh token")
		}
		family = family[:32]
		// A new sign-in: the occasion to forget the tokens expired long ago.
		if err := a.ODB.PurgeOIDCRefreshTokens(ctx); err != nil {
			log.Warn("cannot purge the expired refresh tokens", logkey.Error, err)
		}
	}
	cfg := a.OIDCProvider.Config()
	expires := time.Now().Add(cfg.RefreshIdleTimeout)
	if limit := authTime.Add(cfg.RefreshMaxLifetime); expires.After(limit) {
		expires = limit
	}
	if err := a.ODB.InsertOIDCRefreshToken(ctx, oidcprovider.HashToken(refresh), cdb.OIDCRefreshToken{
		Family: family, UserID: user.ID, ClientID: clientID, Scope: scope, AuthTime: authTime, Expires: expires,
	}); err != nil {
		log.Error("cannot store the refresh token", logkey.Error, err)
		return oauthError(c, http.StatusServiceUnavailable, "temporarily_unavailable", "cannot store the refresh token")
	}
	noStore(c)
	resp := map[string]any{
		"access_token":  tokens.AccessToken,
		"token_type":    "Bearer",
		"expires_in":    tokens.ExpiresIn,
		"refresh_token": refresh,
		"scope":         scope,
	}
	if tokens.IDToken != "" {
		resp["id_token"] = tokens.IDToken
	}
	return c.JSON(http.StatusOK, resp)
}

// oidcProviderUserinfo handles <issuer>/userinfo: the claims of the user an
// access token of the provider is about.
func (a *Api) oidcProviderUserinfo(c echo.Context) error {
	scheme, raw, _ := strings.Cut(c.Request().Header.Get("Authorization"), " ")
	if !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(raw) == "" {
		return oauthError(c, http.StatusUnauthorized, "invalid_token", "a Bearer access token is required")
	}
	claims, err := a.OIDCProvider.ParseToken(strings.TrimSpace(raw), false)
	if err != nil || claims.ClientID == "" {
		return oauthError(c, http.StatusUnauthorized, "invalid_token", "invalid or expired access token")
	}
	noStore(c)
	out := map[string]any{
		"sub":                claims.Subject,
		"preferred_username": claims.PreferredUsername,
		"entitlements":       claims.Entitlements,
	}
	if claims.Name != "" && oidcprovider.HasScope(claims.Scope, "profile") {
		out["name"] = claims.Name
	}
	if oidcprovider.HasScope(claims.Scope, "email") {
		out["email"] = claims.Email
	}
	return c.JSON(http.StatusOK, out)
}

// oidcProviderRevoke handles POST <issuer>/revoke (RFC 7009): a refresh token
// revokes its family. The access tokens live until they expire.
func (a *Api) oidcProviderRevoke(c echo.Context) error {
	log := echolog.GetLogHandler(c, "oidcProviderRevoke")
	ctx := c.Request().Context()
	if err := c.Request().ParseForm(); err != nil {
		return oauthError(c, http.StatusBadRequest, "invalid_request", "invalid form")
	}
	raw := c.Request().PostForm.Get("token")
	if raw == "" {
		return oauthError(c, http.StatusBadRequest, "invalid_request", "token is mandatory")
	}
	t, err := a.ODB.OIDCRefreshTokenByHash(ctx, oidcprovider.HashToken(raw))
	if err != nil {
		log.Error("cannot read the refresh token", logkey.Error, err)
		return oauthError(c, http.StatusServiceUnavailable, "temporarily_unavailable", "cannot read the refresh token")
	}
	if t != nil {
		if err := a.ODB.RevokeOIDCRefreshFamily(ctx, t.Family); err != nil {
			log.Error("cannot revoke the refresh token family", logkey.Error, err)
			return oauthError(c, http.StatusServiceUnavailable, "temporarily_unavailable", "cannot revoke the token")
		}
	}
	noStore(c)
	return c.NoContent(http.StatusOK)
}

// oidcProviderLogout handles GET <issuer>/logout, the end of session of an om3
// webapp: the refresh tokens of the user for the cluster are revoked, and the
// browser goes back to the post_logout_redirect_uri when it is one of the
// cluster's, else to the collector. The collector session stays: signing out
// of a cluster is not signing out of the collector.
func (a *Api) oidcProviderLogout(c echo.Context) error {
	log := echolog.GetLogHandler(c, "oidcProviderLogout")
	ctx := c.Request().Context()
	noStore(c)
	q := c.QueryParams()
	clientID := q.Get("client_id")
	if hint := q.Get("id_token_hint"); hint != "" {
		if claims, err := a.OIDCProvider.ParseToken(hint, true); err == nil && len(claims.Audience) == 1 {
			if clientID == "" {
				clientID = claims.Audience[0]
			}
			if clientID == claims.Audience[0] {
				var userID int64
				if _, err := fmt.Sscan(claims.Subject, &userID); err == nil {
					if err := a.ODB.RevokeOIDCRefreshTokens(ctx, userID, clientID); err != nil {
						log.Error("cannot revoke the refresh tokens", logkey.Error, err)
					}
				}
			}
		}
	}
	back := a.OIDCProvider.Config().UIURL + "/"
	if target := q.Get("post_logout_redirect_uri"); target != "" {
		client, hosts, err := a.oidcProviderClient(ctx, clientID)
		if err != nil {
			log.Error("cannot read the client", logkey.Error, err)
		} else if client != nil && oidcprovider.CheckRedirectURI(target, oidcprovider.RedirectSignOut, hosts) == nil {
			back = oidcprovider.RedirectWithState(target, q.Get("state"))
		}
	}
	return c.Redirect(http.StatusFound, back)
}
