package serverhandlers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
	"github.com/opensvc/oc3/xauth"
)

// requireManager refuses the request of anyone but a Manager: the claim rules may
// grant any team, Manager included.
func requireManager(c echo.Context) error {
	if !IsAuthByUser(c) {
		return denyRequest(c, http.StatusUnauthorized, "user authentication required")
	}
	if !IsManager(c) {
		return denyRequest(c, http.StatusForbidden, "Manager privilege required")
	}
	return nil
}

// GetOidcMappings handles GET /oidc_mappings.
func (a *Api) GetOidcMappings(c echo.Context, params server.GetOidcMappingsParams) error {
	if err := requireManager(c); err != nil {
		return err
	}
	return a.handleList(c, "GetOidcMappings", "oidc_mapping", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetOIDCMappings(ctx, p)
	})
}

// GetOidcMapping handles GET /oidc_mappings/{mapping_id}.
func (a *Api) GetOidcMapping(c echo.Context, mappingId int, params server.GetOidcMappingParams) error {
	if err := requireManager(c); err != nil {
		return err
	}
	return a.getOidcMapping(c, "GetOidcMapping", mappingId, params.Props)
}

func (a *Api) getOidcMapping(c echo.Context, name string, id int, props *server.InQueryProps) error {
	return a.handleItem(c, name, "oidc_mapping", "mapping_id", strconv.Itoa(id), listEndpointParams{props: props},
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetOIDCMapping(ctx, strconv.Itoa(id), p)
		})
}

// checkOidcMapping validates a rule as sent by the client, and returns it with the
// names of the teams it grants. The error is meant for the client; the status says
// which.
func (a *Api) checkOidcMapping(ctx context.Context, body server.OidcMappingInput) (cdb.OIDCMappingWrite, []string, int, error) {
	m := cdb.OIDCMappingWrite{
		Claim:       strings.TrimSpace(body.Claim),
		Value:       strings.TrimSpace(body.Value),
		AllowAccess: body.AllowAccess != nil && *body.AllowAccess,
	}
	switch {
	case m.Claim == "":
		return m, nil, http.StatusBadRequest, errors.New("claim is mandatory")
	case utf8.RuneCountInString(m.Claim) > 128:
		return m, nil, http.StatusBadRequest, errors.New("claim is longer than 128 characters")
	case m.Value == "":
		return m, nil, http.StatusBadRequest, errors.New("value is mandatory")
	case utf8.RuneCountInString(m.Value) > 255:
		return m, nil, http.StatusBadRequest, errors.New("value is longer than 255 characters")
	}
	roles := []string{}
	if body.GroupIds != nil {
		seen := map[int64]bool{}
		for _, raw := range *body.GroupIds {
			id := int64(raw)
			if seen[id] {
				continue
			}
			seen[id] = true
			role, found, err := a.ODB.GroupRoleByID(ctx, id)
			if err != nil {
				return m, nil, http.StatusInternalServerError, err
			}
			if !found {
				return m, nil, http.StatusBadRequest, fmt.Errorf("team %d not found", id)
			}
			// The teams a rule names follow the claims: an account not matching would
			// lose them, Everybody or its own private team included.
			if role == "Everybody" || strings.HasPrefix(role, "user_") {
				return m, nil, http.StatusBadRequest, fmt.Errorf("the team %s cannot be granted by a claim rule", role)
			}
			m.GroupIDs = append(m.GroupIDs, id)
			roles = append(roles, role)
		}
	}
	if !m.AllowAccess && len(m.GroupIDs) == 0 {
		return m, nil, http.StatusBadRequest, errors.New("a rule must allow signing in, grant teams, or both")
	}
	return m, roles, 0, nil
}

func mappingEffect(m cdb.OIDCMappingWrite, roles []string) string {
	parts := []string{}
	if m.AllowAccess {
		parts = append(parts, "allows signing in")
	}
	if len(roles) > 0 {
		parts = append(parts, "grants "+strings.Join(roles, ", "))
	}
	return strings.Join(parts, " and ")
}

// PostOidcMappings handles POST /oidc_mappings.
func (a *Api) PostOidcMappings(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostOidcMappings")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	if err := requireManager(c); err != nil {
		return err
	}
	var body server.PostOidcMappingsJSONRequestBody
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	m, roles, status, err := a.checkOidcMapping(ctx, body)
	if err != nil {
		if status == http.StatusInternalServerError {
			log.Error("cannot check the rule", logkey.Error, err)
			return JSONProblem(c, status, "cannot check the rule")
		}
		return JSONProblem(c, status, err.Error())
	}
	if other, dup, err := a.ODB.OIDCMappingDuplicate(ctx, m.Claim, m.Value, 0); err != nil {
		log.Error("cannot check duplicates", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot check duplicates")
	} else if dup {
		return JSONProblemf(c, http.StatusConflict, "a rule on the same claim and value already exists, add the teams to it: %d", other)
	}
	m.Author, _ = c.Get(XUserEmail).(string)

	var id int64
	if err := a.inTx(ctx, log, func(tx *cdb.DB) error {
		var err error
		if id, err = tx.InsertOIDCMapping(ctx, m); err != nil {
			return err
		}
		return a.logMapping(ctx, tx, log, "oidc_mapping.create",
			"added the claim rule %(claim)s = %(value)s, which %(effect)s", m, roles)
	}); err != nil {
		log.Error("cannot create the rule", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot create the rule")
	}
	a.afterMappingChange(ctx, log)
	return a.getOidcMapping(c, "PostOidcMappings", int(id), nil)
}

// PostOidcMapping handles POST /oidc_mappings/{mapping_id}.
func (a *Api) PostOidcMapping(c echo.Context, mappingId int) error {
	log := echolog.GetLogHandler(c, "PostOidcMapping")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	if err := requireManager(c); err != nil {
		return err
	}
	current, err := a.ODB.OIDCMappingByID(ctx, int64(mappingId))
	if err != nil {
		log.Error("cannot read the rule", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot read the rule")
	}
	if current == nil {
		return JSONProblemf(c, http.StatusNotFound, "claim rule %d not found", mappingId)
	}
	var body server.PostOidcMappingJSONRequestBody
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	m, roles, status, err := a.checkOidcMapping(ctx, body)
	if err != nil {
		if status == http.StatusInternalServerError {
			log.Error("cannot check the rule", logkey.Error, err)
			return JSONProblem(c, status, "cannot check the rule")
		}
		return JSONProblem(c, status, err.Error())
	}
	if other, dup, err := a.ODB.OIDCMappingDuplicate(ctx, m.Claim, m.Value, int64(mappingId)); err != nil {
		log.Error("cannot check duplicates", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot check duplicates")
	} else if dup {
		return JSONProblemf(c, http.StatusConflict, "a rule on the same claim and value already exists, add the teams to it: %d", other)
	}
	m.Author, _ = c.Get(XUserEmail).(string)
	if err := a.inTx(ctx, log, func(tx *cdb.DB) error {
		if err := tx.UpdateOIDCMapping(ctx, int64(mappingId), m); err != nil {
			return err
		}
		return a.logMapping(ctx, tx, log, "oidc_mapping.change",
			"changed the claim rule to %(claim)s = %(value)s, which %(effect)s", m, roles)
	}); err != nil {
		log.Error("cannot change the rule", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot change the rule")
	}
	a.afterMappingChange(ctx, log)
	return a.getOidcMapping(c, "PostOidcMapping", mappingId, nil)
}

// DeleteOidcMapping handles DELETE /oidc_mappings/{mapping_id}.
func (a *Api) DeleteOidcMapping(c echo.Context, mappingId int) error {
	log := echolog.GetLogHandler(c, "DeleteOidcMapping")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	if err := requireManager(c); err != nil {
		return err
	}
	current, err := a.ODB.OIDCMappingByID(ctx, int64(mappingId))
	if err != nil {
		log.Error("cannot read the rule", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot read the rule")
	}
	if current == nil {
		return JSONProblemf(c, http.StatusNotFound, "claim rule %d not found", mappingId)
	}
	m := cdb.OIDCMappingWrite{Claim: current.Claim, Value: current.Value, AllowAccess: current.AllowAccess, GroupIDs: current.GroupIDs}
	m.Author, _ = c.Get(XUserEmail).(string)
	if err := a.inTx(ctx, log, func(tx *cdb.DB) error {
		if err := tx.DeleteOIDCMapping(ctx, int64(mappingId)); err != nil {
			return err
		}
		return a.logMapping(ctx, tx, log, "oidc_mapping.delete",
			"deleted the claim rule %(claim)s = %(value)s, which %(effect)s", m, current.GroupRoles)
	}); err != nil {
		log.Error("cannot delete the rule", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, "cannot delete the rule")
	}
	a.afterMappingChange(ctx, log)
	return c.JSON(http.StatusOK, map[string]string{"info": "claim rule deleted"})
}

func (a *Api) inTx(ctx context.Context, log *slog.Logger, fn func(tx *cdb.DB) error) error {
	tx, markSuccess, endTx, err := a.ODB.BeginTxWithControl(ctx, log, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer endTx()
	if err := fn(tx); err != nil {
		return err
	}
	markSuccess()
	return nil
}

func (a *Api) logMapping(ctx context.Context, tx *cdb.DB, log *slog.Logger, action, format string, m cdb.OIDCMappingWrite, roles []string) error {
	if err := tx.Log(ctx, cdb.LogEntry{
		Action: action,
		User:   m.Author,
		Fmt:    format,
		Dict:   map[string]any{"claim": m.Claim, "value": m.Value, "effect": mappingEffect(m, roles)},
		Level:  "info",
	}); err != nil {
		log.Error("cannot write audit log", logkey.Error, err)
	}
	return nil
}

// afterMappingChange makes the next sign-in and Bearer request read the rules
// again, and announces the change to the views on display.
func (a *Api) afterMappingChange(ctx context.Context, log *slog.Logger) {
	if a.OIDC != nil {
		a.OIDC.ForgetClaimRules()
	}
	if err := a.ODB.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}
}

// GetAuthClaims handles GET /auth/claims: the claims of the request's OpenID
// Connect sign-in that a rule may use.
func (a *Api) GetAuthClaims(c echo.Context) error {
	noStore(c)
	out := map[string]any{"source": "", "claims": map[string]any{}}
	user := UserInfoFromContext(c)
	if user == nil {
		return c.JSON(http.StatusOK, out)
	}
	source := user.GetExtensions().Get(xauth.XAuthSource)
	out["source"] = source
	if a.OIDC == nil {
		return c.JSON(http.StatusOK, out)
	}
	ctx := c.Request().Context()
	switch source {
	case xauth.AuthSourceSession:
		if cookie, err := c.Cookie(a.OIDC.SessionCookieName()); err == nil {
			if sess, err := a.OIDC.LoadSession(ctx, cookie.Value); err == nil && sess != nil && sess.Claims != nil {
				out["claims"] = sess.Claims
			}
		}
	case xauth.AuthSourceBearer:
		_, raw, _ := strings.Cut(c.Request().Header.Get("Authorization"), " ")
		if claims, err := a.OIDC.AccessTokenClaims(ctx, strings.TrimSpace(raw)); err == nil {
			out["claims"] = claims
		}
	}
	return c.JSON(http.StatusOK, out)
}
