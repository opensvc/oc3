package serverhandlers

import (
	"context"
	"net/http"
	"unicode/utf8"

	"github.com/labstack/echo/v4"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
	"github.com/opensvc/oc3/xauth"
)

// minResetPasswordLength is the shortest password accepted when a user changes
// their own, counted in characters rather than bytes. It is stricter than the
// length required at user creation.
const minResetPasswordLength = 12

// PostUserSelfPassword handles POST /users/self/password: the caller changes
// their own password, proving they know the current one, as the change_password
// form of the historical collector does. The new one must be at least
// minResetPasswordLength characters long, and is stored as a web2py hash, so
// that both collectors accept it. The change is logged, without the password.
func (a *Api) PostUserSelfPassword(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostUserSelfPassword")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	userID := authUserID(c)
	if userID == nil {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	if IsImpersonating(c) {
		return JSONProblemf(c, http.StatusForbidden, "a password cannot be changed while impersonating")
	}
	if identityManagedByProvider(c) {
		return JSONProblemf(c, http.StatusForbidden, "your password is managed by your identity provider: change it there")
	}

	var body server.PostUserSelfPasswordJSONRequestBody
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if utf8.RuneCountInString(body.NewPassword) < minResetPasswordLength {
		return JSONProblemf(c, http.StatusBadRequest, "the new password must be at least %d characters long", minResetPasswordLength)
	}
	if body.NewPassword == body.CurrentPassword {
		return JSONProblemf(c, http.StatusBadRequest, "the new password must differ from the current one")
	}

	hmacKey := viper.GetString("w2p_hmac")
	stored, found, err := a.ODB.UserPasswordHash(ctx, *userID)
	if err != nil {
		log.Error("cannot read password", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the current password")
	}
	if !found || !xauth.VerifyWeb2pyPassword(body.CurrentPassword, stored, hmacKey) {
		return JSONProblemf(c, http.StatusForbidden, "the current password is incorrect")
	}

	hash, err := xauth.HashWeb2pyPassword(body.NewPassword, hmacKey)
	if err != nil {
		log.Error("cannot hash password", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot hash password")
	}
	if err := a.ODB.SetUserPassword(ctx, *userID, hash); err != nil {
		log.Error("cannot store password", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot store the new password")
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := a.ODB.Log(ctx, cdb.LogEntry{
		Action: "user.change",
		User:   userEmail,
		Fmt:    "change user %(email)s: password",
		Dict:   map[string]any{"email": userEmail},
		Level:  "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}
	log.Info("password changed")
	return c.JSON(http.StatusOK, map[string]string{"info": "password changed"})
}
