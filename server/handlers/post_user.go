package serverhandlers

import (
	"context"
	"net/http"
	"net/mail"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// The widths of the auth_user columns.
const (
	maxUserNameLength  = 128
	maxUserEmailLength = 512
)

// PostUser handles POST /users/{user_id}: the caller changes their own first
// name, last name or email, as the profile form of the historical collector
// allows any signed-in user to. Another user's account is refused: changing it
// is the UserManager's, not ported yet. The email is the sign-in name, unique
// among users; the change is logged with the fields changed.
func (a *Api) PostUser(c echo.Context, userId string) error {
	log := echolog.GetLogHandler(c, "PostUser")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	odb := a.ODB

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	selfID := authUserID(c)
	if selfID == nil {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	callerEmail, _ := c.Get(XUserEmail).(string)
	if userId != "self" && userId != strconv.FormatInt(*selfID, 10) && userId != callerEmail {
		return JSONProblemf(c, http.StatusForbidden, "only your own account can be changed")
	}

	var body server.PostUserJSONRequestBody
	if err := c.Bind(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	current, found, err := odb.GetUserIdentity(ctx, *selfID)
	if err != nil {
		log.Error("cannot read user", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the user")
	}
	if !found {
		return JSONProblemf(c, http.StatusNotFound, "user %d not found", *selfID)
	}

	next := current
	var changed []string
	// setName applies a name given in the body, false when it is too long.
	setName := func(field string, value *string, target *string) bool {
		if value == nil {
			return true
		}
		v := strings.TrimSpace(*value)
		if utf8.RuneCountInString(v) > maxUserNameLength {
			return false
		}
		if v != *target {
			*target = v
			changed = append(changed, field)
		}
		return true
	}
	for _, name := range []struct {
		field  string
		value  *string
		target *string
	}{
		{"first_name", body.FirstName, &next.FirstName},
		{"last_name", body.LastName, &next.LastName},
	} {
		if !setName(name.field, name.value, name.target) {
			return JSONProblemf(c, http.StatusBadRequest, "%s must be at most %d characters long", name.field, maxUserNameLength)
		}
	}
	if body.Email != nil {
		email := strings.TrimSpace(*body.Email)
		if addr, err := mail.ParseAddress(email); err != nil || addr.Address != email {
			return JSONProblemf(c, http.StatusBadRequest, "email must be a valid address, got %q", *body.Email)
		}
		if utf8.RuneCountInString(email) > maxUserEmailLength {
			return JSONProblemf(c, http.StatusBadRequest, "email must be at most %d characters long", maxUserEmailLength)
		}
		if !strings.EqualFold(email, current.Email) {
			otherID, taken, err := odb.UserIDByEmail(ctx, email)
			if err != nil {
				log.Error("cannot check email", logkey.Error, err)
				return JSONProblemf(c, http.StatusInternalServerError, "cannot check email")
			}
			if taken && otherID != *selfID {
				return JSONProblemf(c, http.StatusConflict, "another user already has the email %s", email)
			}
		}
		if email != current.Email {
			next.Email = email
			changed = append(changed, "email")
		}
	}

	if len(changed) > 0 {
		if err := odb.SetUserIdentity(ctx, *selfID, next); err != nil {
			log.Error("cannot store user", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot store the user")
		}
		format := "change user %(email)s: %(fields)s"
		dict := map[string]any{"email": current.Email, "fields": strings.Join(changed, ", ")}
		if next.Email != current.Email {
			format = "change user %(email)s: %(fields)s, email now %(new_email)s"
			dict["new_email"] = next.Email
		}
		if logErr := odb.Log(ctx, cdb.LogEntry{
			Action: "user.change",
			User:   callerEmail,
			Fmt:    format,
			Dict:   dict,
			Level:  "info",
		}); logErr != nil {
			log.Error("cannot write audit log", logkey.Error, logErr)
		}
		if err := odb.Session.NotifyChanges(ctx); err != nil {
			log.Error("cannot notify changes", logkey.Error, err)
		}
		log.Info("user changed", "fields", strings.Join(changed, ","))
	}

	id := strconv.FormatInt(*selfID, 10)
	return a.handleItem(c, "PostUser", "user", "user_id", id, listEndpointParams{props: &userCreatedProps, withUserID: true},
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return odb.GetUser(ctx, id, p)
		})
}
