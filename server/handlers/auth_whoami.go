package serverhandlers

import (
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/shaj13/go-guardian/v2/auth"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
	"github.com/opensvc/oc3/xauth"
)

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// signInTime reads an RFC 3339 extension of the sign-in, nil when absent.
func signInTime(ext auth.Extensions, key string) *time.Time {
	t, err := time.Parse(time.RFC3339, ext.Get(key))
	if err != nil {
		return nil
	}
	return &t
}

// GetAuthWhoami handles GET /auth/whoami: who the request is authenticated as,
// and how. The extensions of the sign-in are never copied as they are: those of
// a password sign-in hold the password hash.
func (a *Api) GetAuthWhoami(c echo.Context) error {
	log := echolog.GetLogHandler(c, "GetAuthWhoami")
	ctx := c.Request().Context()
	c.Response().Header().Set("Cache-Control", "no-store")
	user := UserInfoFromContext(c)
	if user == nil {
		return JSONProblemf(c, http.StatusUnauthorized, "not authenticated")
	}
	signedIn := RealUserInfo(c)
	signInExt := signedIn.GetExtensions()
	resp := server.Whoami{Privileges: []string{}, Groups: []string{}}

	if IsAuthByNode(c) {
		resp.AuthSource = "node"
		nodeID, _ := c.Get(XNodeID).(string)
		nodename, _ := c.Get(XNodename).(string)
		clusterID, _ := c.Get(XClusterID).(string)
		app, _ := c.Get(XApp).(string)
		resp.Node = &struct {
			App       *string `json:"app,omitempty"`
			ClusterId *string `json:"cluster_id,omitempty"`
			NodeId    string  `json:"node_id"`
			Nodename  string  `json:"nodename"`
		}{App: optional(app), ClusterId: optional(clusterID), NodeId: nodeID, Nodename: nodename}
		return c.JSON(http.StatusOK, resp)
	}

	source := signInExt.Get(xauth.XAuthSource)
	if source == "" {
		source = xauth.AuthSourceBasic
	}
	resp.AuthSource = server.WhoamiAuthSource(source)
	resp.Issuer = optional(signInExt.Get(xauth.XIssuer))
	resp.Subject = optional(signInExt.Get(xauth.XSubject))
	resp.Expires = signInTime(signInExt, xauth.XExpires)
	resp.IdleExpires = signInTime(signInExt, xauth.XIdleExpires)

	if id := authUserID(c); id != nil {
		uid := int(*id)
		resp.UserId = &uid
		name, email, err := a.ODB.UserName(ctx, *id)
		if err != nil {
			log.Error("cannot read the user", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot read the user")
		}
		resp.Name = optional(strings.TrimSpace(name))
		resp.Email = optional(email)
	}

	roles := UserGroupsFromContext(c)
	privileged, err := a.ODB.PrivilegeRoles(ctx, roles)
	if err != nil {
		log.Error("cannot read the groups", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the groups")
	}
	for _, role := range roles {
		if privileged[role] {
			resp.Privileges = append(resp.Privileges, role)
		} else {
			resp.Groups = append(resp.Groups, role)
		}
	}
	sort.Strings(resp.Privileges)
	sort.Strings(resp.Groups)

	if IsImpersonating(c) {
		id, err := strconv.ParseInt(signInExt.Get(xauth.XUserID), 10, 64)
		if err == nil {
			name, email, err := a.ODB.UserName(ctx, id)
			if err != nil {
				log.Error("cannot read the impersonator", logkey.Error, err)
				return JSONProblemf(c, http.StatusInternalServerError, "cannot read the impersonator")
			}
			resp.Impersonator = &struct {
				Email  string `json:"email"`
				Name   string `json:"name"`
				UserId int    `json:"user_id"`
			}{Email: email, Name: strings.TrimSpace(name), UserId: int(id)}
		}
	}
	return c.JSON(http.StatusOK, resp)
}
