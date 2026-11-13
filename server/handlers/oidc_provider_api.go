package serverhandlers

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/oidcprovider"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetOidcProvider handles GET /oidc_provider.
func (a *Api) GetOidcProvider(c echo.Context) error {
	if a.OIDCProvider == nil {
		return c.JSON(http.StatusOK, server.OidcProviderInfo{Enabled: false})
	}
	issuer := a.OIDCProvider.Config().Issuer
	return c.JSON(http.StatusOK, server.OidcProviderInfo{Enabled: true, Issuer: &issuer})
}

func (a *Api) requireOIDCProvider(c echo.Context) error {
	if a.OIDCProvider == nil {
		return denyRequest(c, http.StatusNotFound, "the collector is not configured as an OpenID Connect provider")
	}
	return nil
}

// oidcProviderSignedInUser returns the id of the user who signed in and
// approves a request, refusing the requests made as another user: a token is
// for the user it is about, and only they may ask for it.
func oidcProviderSignedInUser(c echo.Context) (int64, error) {
	if !IsAuthByUser(c) {
		return 0, denyRequest(c, http.StatusUnauthorized, "user authentication required")
	}
	if IsImpersonating(c) {
		return 0, denyRequest(c, http.StatusForbidden, "a cluster sign-in cannot be approved while impersonating")
	}
	id := authUserID(c)
	if id == nil {
		return 0, denyRequest(c, http.StatusForbidden, "no collector account")
	}
	return *id, nil
}

// GetOidcProviderRequest handles GET /oidc_provider/requests/{request_id}.
func (a *Api) GetOidcProviderRequest(c echo.Context, requestId server.InPathOidcProviderRequestID) error {
	log := echolog.GetLogHandler(c, "GetOidcProviderRequest")
	ctx := c.Request().Context()
	noStore(c)
	if err := a.requireOIDCProvider(c); err != nil {
		return err
	}
	userID, err := oidcProviderSignedInUser(c)
	if err != nil {
		return err
	}
	r, err := a.OIDCProvider.LoadRequest(ctx, requestId)
	if errors.Is(err, oidcprovider.ErrNotFound) {
		return JSONProblem(c, http.StatusNotFound, "unknown or expired sign-in request, sign in again from the cluster")
	} else if err != nil {
		log.Error("cannot read the request", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot read the request")
	}
	client, err := a.ODB.OIDCClientByID(ctx, r.ClientID)
	if err != nil {
		log.Error("cannot read the client", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot read the client")
	}
	if client == nil {
		return JSONProblem(c, http.StatusNotFound, "the cluster is no longer known to the collector")
	}
	user, err := a.ODB.OIDCUserByID(ctx, userID)
	if err != nil || user == nil {
		log.Error("cannot read the user", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot read the user")
	}
	grants, err := a.oidcProviderEntitlements(ctx, log, userID, r.ClientID)
	if err != nil {
		log.Error("cannot read the grants", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot read the grants")
	}
	return c.JSON(http.StatusOK, server.OidcProviderRequest{
		RequestId:   requestId,
		ClientId:    r.ClientID,
		ClusterName: client.ClusterName,
		RedirectUri: r.RedirectURI,
		Scope:       r.Scope,
		Prompt:      r.Prompt,
		User:        user.Email,
		Grants:      grants,
	})
}

// PostOidcProviderRequestApprove handles POST /oidc_provider/requests/{request_id}/approve.
func (a *Api) PostOidcProviderRequestApprove(c echo.Context, requestId server.InPathOidcProviderRequestID) error {
	log := echolog.GetLogHandler(c, "PostOidcProviderRequestApprove")
	ctx := c.Request().Context()
	noStore(c)
	if err := a.requireOIDCProvider(c); err != nil {
		return err
	}
	userID, err := oidcProviderSignedInUser(c)
	if err != nil {
		return err
	}
	r, err := a.OIDCProvider.LoadRequest(ctx, requestId)
	if errors.Is(err, oidcprovider.ErrNotFound) {
		return JSONProblem(c, http.StatusNotFound, "unknown or expired sign-in request, sign in again from the cluster")
	} else if err != nil {
		log.Error("cannot read the request", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot read the request")
	}
	client, err := a.ODB.OIDCClientByID(ctx, r.ClientID)
	if err != nil {
		log.Error("cannot read the client", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot read the client")
	}
	if client == nil {
		return JSONProblem(c, http.StatusNotFound, "the cluster is no longer known to the collector")
	}
	user, err := a.ODB.OIDCUserByID(ctx, userID)
	if err != nil {
		log.Error("cannot read the user", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot read the user")
	}
	if user == nil || user.Locked {
		return JSONProblem(c, http.StatusForbidden, "the account is locked")
	}
	grants, err := a.oidcProviderEntitlements(ctx, log, userID, r.ClientID)
	if err != nil {
		log.Error("cannot read the grants", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot read the grants")
	}
	if len(grants) == 0 {
		return JSONProblemf(c, http.StatusForbidden, "you have no om3 grant on the cluster %s", client.ClusterName)
	}
	// Consumed once checked: of two approvals, one gets a code.
	if r, err = a.OIDCProvider.ConsumeRequest(ctx, requestId); errors.Is(err, oidcprovider.ErrNotFound) {
		return JSONProblem(c, http.StatusNotFound, "unknown or expired sign-in request, sign in again from the cluster")
	} else if err != nil {
		log.Error("cannot read the request", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot read the request")
	}
	code, err := a.OIDCProvider.IssueCode(ctx, oidcprovider.AuthCode{
		ClientID:      r.ClientID,
		RedirectURI:   r.RedirectURI,
		Scope:         r.Scope,
		Nonce:         r.Nonce,
		CodeChallenge: r.CodeChallenge,
		UserID:        userID,
		AuthTime:      time.Now(),
	})
	if err != nil {
		log.Error("cannot issue the code", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot issue the code")
	}
	if err := a.ODB.Log(ctx, cdb.LogEntry{
		Action: "oidc_provider.signin",
		User:   user.Email,
		Fmt:    "signed in to the cluster %(cluster)s with the om3 grants %(grants)s",
		Dict:   map[string]any{"cluster": client.ClusterName, "cluster_id": client.ClusterID, "grants": strings.Join(grants, " ")},
		Level:  "info",
	}); err != nil {
		log.Error("cannot write audit log", logkey.Error, err)
	}
	return c.JSON(http.StatusOK, server.OidcProviderRedirect{
		RedirectTo: oidcprovider.RedirectWithCode(r, code, a.OIDCProvider.Config().Issuer),
	})
}

// PostOidcProviderRequestDeny handles POST /oidc_provider/requests/{request_id}/deny.
func (a *Api) PostOidcProviderRequestDeny(c echo.Context, requestId server.InPathOidcProviderRequestID) error {
	log := echolog.GetLogHandler(c, "PostOidcProviderRequestDeny")
	ctx := c.Request().Context()
	noStore(c)
	if err := a.requireOIDCProvider(c); err != nil {
		return err
	}
	if !IsAuthByUser(c) {
		return denyRequest(c, http.StatusUnauthorized, "user authentication required")
	}
	r, err := a.OIDCProvider.ConsumeRequest(ctx, requestId)
	if errors.Is(err, oidcprovider.ErrNotFound) {
		return JSONProblem(c, http.StatusNotFound, "unknown or expired sign-in request")
	} else if err != nil {
		log.Error("cannot read the request", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot read the request")
	}
	return c.JSON(http.StatusOK, server.OidcProviderRedirect{
		RedirectTo: oidcprovider.RedirectWithError(r, "access_denied", "the user denied the sign-in", a.OIDCProvider.Config().Issuer),
	})
}

func oidcGrantRuleResponse(r cdb.OIDCGrantRule) server.OidcProviderGrant {
	return server.OidcProviderGrant{
		Id:          int(r.ID),
		GroupId:     int(r.GroupID),
		GroupRole:   r.GroupRole,
		ClusterId:   r.ClusterID,
		ClusterName: r.ClusterName,
		Grants:      r.Grants,
		Author:      r.Author,
		Updated:     r.Updated,
	}
}

// GetOidcProviderGrants handles GET /oidc_provider/grants.
func (a *Api) GetOidcProviderGrants(c echo.Context) error {
	log := echolog.GetLogHandler(c, "GetOidcProviderGrants")
	if err := requireManager(c); err != nil {
		return err
	}
	rules, err := a.ODB.OIDCGrantRules(c.Request().Context())
	if err != nil {
		log.Error("cannot read the rules", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot read the rules")
	}
	out := server.OidcProviderGrantList{Data: make([]server.OidcProviderGrant, 0, len(rules))}
	for _, r := range rules {
		out.Data = append(out.Data, oidcGrantRuleResponse(r))
	}
	return c.JSON(http.StatusOK, out)
}

type oidcGrantRuleWrite struct {
	groupID     int64
	groupRole   string
	clusterID   string
	clusterName string
	grants      string
}

// checkOidcGrantRule validates a rule as sent by the client. The error is meant
// for the client; the status says which.
func (a *Api) checkOidcGrantRule(ctx context.Context, body server.OidcProviderGrantInput, exceptID int64) (oidcGrantRuleWrite, int, error) {
	w := oidcGrantRuleWrite{groupID: int64(body.GroupId)}
	role, found, err := a.ODB.GroupRoleByID(ctx, w.groupID)
	if err != nil {
		return w, http.StatusInternalServerError, err
	}
	if !found {
		return w, http.StatusBadRequest, errors.New("team not found")
	}
	w.groupRole = role
	if body.ClusterId != nil {
		w.clusterID = strings.TrimSpace(*body.ClusterId)
	}
	if w.clusterID != "" {
		client, err := a.ODB.OIDCClientByID(ctx, w.clusterID)
		if err != nil {
			return w, http.StatusInternalServerError, err
		}
		if client == nil {
			return w, http.StatusBadRequest, errors.New("cluster not found")
		}
		w.clusterName = client.ClusterName
	}
	grants, err := oidcprovider.ParseGrants(body.Grants)
	if err != nil {
		return w, http.StatusBadRequest, err
	}
	w.grants = strings.Join(grants, " ")
	other, err := a.ODB.OIDCGrantRuleDuplicate(ctx, w.groupID, w.clusterID, exceptID)
	if err != nil {
		return w, http.StatusInternalServerError, err
	}
	if other != 0 {
		return w, http.StatusConflict, errors.New("a rule of the same team and cluster already exists, change it instead")
	}
	return w, 0, nil
}

func oidcGrantRuleScope(w oidcGrantRuleWrite) string {
	if w.clusterID == "" {
		return "every cluster"
	}
	return "the cluster " + w.clusterName
}

func (a *Api) writeOidcGrantRule(c echo.Context, name string, id int64) error {
	log := echolog.GetLogHandler(c, name)
	ctx := c.Request().Context()
	if err := requireManager(c); err != nil {
		return err
	}
	if id != 0 {
		current, err := a.ODB.OIDCGrantRuleByID(ctx, id)
		if err != nil {
			log.Error("cannot read the rule", logkey.Error, err)
			return JSONProblem(c, http.StatusInternalServerError, "cannot read the rule")
		}
		if current == nil {
			return JSONProblemf(c, http.StatusNotFound, "om3 grant rule %d not found", id)
		}
	}
	var body server.OidcProviderGrantInput
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	w, status, err := a.checkOidcGrantRule(ctx, body, id)
	if err != nil {
		if status == http.StatusInternalServerError {
			log.Error("cannot check the rule", logkey.Error, err)
			return JSONProblem(c, status, "cannot check the rule")
		}
		return JSONProblem(c, status, err.Error())
	}
	author, _ := c.Get(XUserEmail).(string)
	action, verb := "oidc_provider.grant.change", "changed"
	if err := a.inTx(ctx, log, func(tx *cdb.DB) error {
		var err error
		if id == 0 {
			action, verb = "oidc_provider.grant.create", "added"
			id, err = tx.InsertOIDCGrantRule(ctx, w.groupID, w.clusterID, w.grants, author)
		} else {
			err = tx.UpdateOIDCGrantRule(ctx, id, w.groupID, w.clusterID, w.grants, author)
		}
		if err != nil {
			return err
		}
		if err := tx.Log(ctx, cdb.LogEntry{
			Action: action,
			User:   author,
			Fmt:    verb + " the om3 grant rule giving %(grants)s to %(group)s on %(scope)s",
			Dict:   map[string]any{"grants": w.grants, "group": w.groupRole, "scope": oidcGrantRuleScope(w)},
			Level:  "info",
		}); err != nil {
			log.Error("cannot write audit log", logkey.Error, err)
		}
		return nil
	}); err != nil {
		log.Error("cannot write the rule", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot write the rule")
	}
	rule, err := a.ODB.OIDCGrantRuleByID(ctx, id)
	if err != nil || rule == nil {
		log.Error("cannot read the rule", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot read the rule")
	}
	return c.JSON(http.StatusOK, oidcGrantRuleResponse(*rule))
}

// PostOidcProviderGrants handles POST /oidc_provider/grants.
func (a *Api) PostOidcProviderGrants(c echo.Context) error {
	return a.writeOidcGrantRule(c, "PostOidcProviderGrants", 0)
}

// PostOidcProviderGrant handles POST /oidc_provider/grants/{grant_id}.
func (a *Api) PostOidcProviderGrant(c echo.Context, grantId server.InPathOidcProviderGrantID) error {
	if grantId <= 0 {
		return JSONProblemf(c, http.StatusNotFound, "om3 grant rule %d not found", grantId)
	}
	return a.writeOidcGrantRule(c, "PostOidcProviderGrant", int64(grantId))
}

// DeleteOidcProviderGrant handles DELETE /oidc_provider/grants/{grant_id}.
func (a *Api) DeleteOidcProviderGrant(c echo.Context, grantId server.InPathOidcProviderGrantID) error {
	log := echolog.GetLogHandler(c, "DeleteOidcProviderGrant")
	ctx := c.Request().Context()
	if err := requireManager(c); err != nil {
		return err
	}
	current, err := a.ODB.OIDCGrantRuleByID(ctx, int64(grantId))
	if err != nil {
		log.Error("cannot read the rule", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot read the rule")
	}
	if current == nil {
		return JSONProblemf(c, http.StatusNotFound, "om3 grant rule %d not found", grantId)
	}
	author, _ := c.Get(XUserEmail).(string)
	scope := "every cluster"
	if current.ClusterID != "" {
		scope = "the cluster " + current.ClusterName
	}
	if err := a.inTx(ctx, log, func(tx *cdb.DB) error {
		if err := tx.DeleteOIDCGrantRule(ctx, int64(grantId)); err != nil {
			return err
		}
		if err := tx.Log(ctx, cdb.LogEntry{
			Action: "oidc_provider.grant.delete",
			User:   author,
			Fmt:    "deleted the om3 grant rule giving %(grants)s to %(group)s on %(scope)s",
			Dict:   map[string]any{"grants": current.Grants, "group": current.GroupRole, "scope": scope},
			Level:  "info",
		}); err != nil {
			log.Error("cannot write audit log", logkey.Error, err)
		}
		return nil
	}); err != nil {
		log.Error("cannot delete the rule", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot delete the rule")
	}
	return c.NoContent(http.StatusNoContent)
}
