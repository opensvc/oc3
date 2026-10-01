package serverhandlers

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"slices"

	"github.com/labstack/echo/v4"
	"github.com/shaj13/go-guardian/v2/auth"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
	"github.com/opensvc/oc3/xauth"
)

const (
	// ImpersonateHeader carries the auth_user.id a request is made as.
	ImpersonateHeader = "OC3-Impersonate"
	// ImpersonatedByHeader answers a request made as another user with the email
	// of the user who really signed in.
	ImpersonatedByHeader = "OC3-Impersonated-By"
	// ImpersonationRefusedHeader marks a refusal due to the impersonation itself,
	// so that the client can stop impersonating rather than retry.
	ImpersonationRefusedHeader = "OC3-Impersonation-Refused"

	XImpersonator = "XImpersonator"
)

// ImpersonateMiddleware lets a Manager act as another user, as the impersonation
// of the historical collector does (/init/default/user/impersonate), which
// required the Impersonate group instead: a request carrying the
// ImpersonateHeader is handled with the identity, the groups and so the visibility of that user. The
// collector being stateless here, the header is checked on every request, and
// the identity of the user who really signed in stays available to the handlers
// (RealUserInfo) and in the server log.
//
// It must run after AuthMiddleware.
func ImpersonateMiddleware(db *sql.DB) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			ident := c.Request().Header.Get(ImpersonateHeader)
			if ident == "" {
				return next(c)
			}
			refuse := func(code int, format string, args ...any) error {
				c.Response().Header().Set(ImpersonationRefusedHeader, "true")
				return JSONProblemf(c, code, format, args...)
			}
			signedIn := UserInfoFromContext(c)
			if signedIn == nil || !IsAuthByUser(c) {
				return refuse(http.StatusForbidden, "only a signed-in user can impersonate")
			}
			if !IsManager(c) {
				return refuse(http.StatusForbidden, "impersonating requires the Manager privilege")
			}
			if _, ok := xauth.ParseUserID(ident); !ok {
				return refuse(http.StatusBadRequest, "%s must be a user id", ImpersonateHeader)
			}
			if ident == signedIn.GetExtensions().Get(xauth.XUserID) {
				return next(c)
			}
			target, err := xauth.LoadUserInfo(c.Request().Context(), db, ident)
			switch {
			case errors.Is(err, xauth.ErrUnknownUser):
				return refuse(http.StatusForbidden, "user %s does not exist", ident)
			case err != nil:
				echolog.GetLog(c).Error("cannot load the impersonated user", logkey.Error, err)
				return JSONProblem(c, http.StatusServiceUnavailable, "authentication is unavailable, the database cannot be reached; retry later")
			}
			setImpersonated(c, signedIn, target)
			return next(c)
		}
	}
}

// setImpersonated replaces the identity of the request by target, keeping signedIn
// as the impersonator.
func setImpersonated(c echo.Context, signedIn, target auth.Info) {
	c.Set(XImpersonator, signedIn)
	c.Set("user", target)
	c.Set("groups", target.GetGroups())
	c.Set(XUserEmail, target.GetExtensions().Get(xauth.XUserEmail))
	c.Response().Header().Set(ImpersonatedByHeader, signedIn.GetUserName())
	echolog.GetLog(c).Debug("impersonated request", "impersonator", signedIn.GetUserName(), "user", target.GetUserName())
}

// RealUserInfo returns the user who really signed in: the impersonator when the
// request is made as another user, the authenticated user otherwise.
func RealUserInfo(c echo.Context) auth.Info {
	if signedIn, ok := c.Get(XImpersonator).(auth.Info); ok {
		return signedIn
	}
	return UserInfoFromContext(c)
}

// IsImpersonating reports whether the request is made as another user.
func IsImpersonating(c echo.Context) bool {
	_, ok := c.Get(XImpersonator).(auth.Info)
	return ok
}

// GetImpersonation handles GET /impersonation: whether the user who signed in
// holds the Manager privilege, which impersonating requires, and who the request
// is made as.
func (a *Api) GetImpersonation(c echo.Context) error {
	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	signedIn := RealUserInfo(c)
	resp := server.ImpersonationStatus{
		Allowed: slices.Contains(signedIn.GetGroups(), "Manager"),
	}
	if IsImpersonating(c) {
		target := UserInfoFromContext(c)
		id, _ := xauth.ParseUserID(target.GetID())
		resp.Impersonating = &server.Impersonation{UserId: id, Email: target.GetUserName(), Groups: target.GetGroups()}
	}
	return c.JSON(http.StatusOK, resp)
}

// PostUserImpersonate handles POST /users/{user_id}/impersonate: checks that the
// caller may impersonate that user and logs the start of the impersonation, as
// the historical collector logs "User %(id)s is impersonating %(other_id)s". The
// requests made as that user then carry the OC3-Impersonate header.
func (a *Api) PostUserImpersonate(c echo.Context, userId string) error {
	log := echolog.GetLogHandler(c, "PostUserImpersonate")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	if IsImpersonating(c) {
		return JSONProblemf(c, http.StatusConflict, "stop impersonating before impersonating another user")
	}
	if !IsManager(c) {
		return JSONProblemf(c, http.StatusForbidden, "impersonating requires the Manager privilege")
	}
	signedIn := UserInfoFromContext(c)
	target, err := xauth.LoadUserInfo(ctx, a.ODB.DB, userId)
	switch {
	case errors.Is(err, xauth.ErrUnknownUser):
		return JSONProblemf(c, http.StatusNotFound, "user %s does not exist", userId)
	case err != nil:
		log.Error("cannot load the user to impersonate", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot load the user to impersonate")
	}
	if target.GetID() == signedIn.GetID() {
		return JSONProblemf(c, http.StatusBadRequest, "a user cannot impersonate themselves")
	}

	if logErr := a.ODB.Log(ctx, cdb.LogEntry{
		Action: "user.impersonate",
		User:   signedIn.GetUserName(),
		Fmt:    "User %(id)s is impersonating %(other_id)s",
		Dict:   map[string]any{"id": signedIn.GetUserName(), "other_id": target.GetUserName()},
		Level:  "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	id, _ := xauth.ParseUserID(target.GetID())
	return c.JSON(http.StatusOK, server.Impersonation{
		UserId: id,
		Email:  target.GetUserName(),
		Groups: target.GetGroups(),
	})
}
