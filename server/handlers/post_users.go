package serverhandlers

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/mail"
	"strconv"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
	"github.com/opensvc/oc3/xauth"
)

// minPasswordLength is the shortest password accepted at user creation.
const minPasswordLength = 8

// userCreatedProps are the properties returned for a created user: never the
// password, the reset key nor the registration id.
var userCreatedProps = server.InQueryProps("id,email,username,first_name,last_name,phone_work")

func IsUserManager(c echo.Context) bool {
	return IsManager(c) || HasGroup(c, "UserManager")
}

func requireUserManager(c echo.Context) error {
	if !IsAuthByUser(c) {
		return denyRequest(c, http.StatusUnauthorized, "user authentication required")
	}
	if !IsUserManager(c) {
		return denyRequest(c, http.StatusForbidden, "UserManager privilege required")
	}
	return nil
}

func optionalString(value *string) *string {
	if value == nil {
		return nil
	}
	s := strings.TrimSpace(*value)
	if s == "" {
		return nil
	}
	return &s
}

// PostUsers handles POST /users: create a user, as the historical collector's
// rest_post_users did, except that an existing email or username is refused
// instead of silently updated.
func (a *Api) PostUsers(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostUsers")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if err := requireUserManager(c); err != nil {
		return err
	}

	var body server.PostUsersJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	email := strings.TrimSpace(body.Email)
	if addr, err := mail.ParseAddress(email); err != nil || addr.Address != email {
		return JSONProblemf(c, http.StatusBadRequest, "email must be a valid address, got %q", body.Email)
	}
	insert := cdb.UserInsert{
		Email:     email,
		Username:  optionalString(body.Username),
		FirstName: optionalString(body.FirstName),
		LastName:  optionalString(body.LastName),
		PhoneWork: optionalString(body.PhoneWork),
	}

	if otherID, taken, err := odb.UserIDByEmail(ctx, email); err != nil {
		log.Error("cannot check email", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check email")
	} else if taken {
		return JSONProblemf(c, http.StatusConflict, "a user with email %s already exists: %d", email, otherID)
	}
	if insert.Username != nil {
		if otherID, taken, err := odb.UserIDByUsername(ctx, *insert.Username); err != nil {
			log.Error("cannot check username", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot check username")
		} else if taken {
			return JSONProblemf(c, http.StatusConflict, "a user named %s already exists: %d", *insert.Username, otherID)
		}
	}

	// Without a password the account exists but cannot sign in, as in the
	// historical collector.
	if body.Password != nil && *body.Password != "" {
		if len(*body.Password) < minPasswordLength {
			return JSONProblemf(c, http.StatusBadRequest, "password must be at least %d characters long", minPasswordLength)
		}
		hash, err := xauth.HashWeb2pyPassword(*body.Password, viper.GetString("w2p_hmac"))
		if err != nil {
			log.Error("cannot hash password", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot hash password")
		}
		insert.PasswordHash = &hash
	}

	log.Info("called", "email", email)

	var userID int64
	if err := func() error {
		tx, markSuccess, endTx, err := odb.BeginTxWithControl(ctx, log, &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("cannot start transaction: %w", err)
		}
		defer endTx()
		if userID, err = tx.InsertUserWithPrivateGroup(ctx, insert); err != nil {
			return err
		}
		userEmail, _ := c.Get(XUserEmail).(string)
		if logErr := tx.Log(ctx, cdb.LogEntry{
			Action: "user.create",
			User:   userEmail,
			Fmt:    "add user %(email)s",
			Dict:   map[string]any{"email": email},
			Level:  "info",
		}); logErr != nil {
			log.Error("cannot write audit log", logkey.Error, logErr)
		}
		markSuccess()
		return nil
	}(); err != nil {
		log.Error("cannot create user", "email", email, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot create user")
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	id := strconv.FormatInt(userID, 10)
	return a.handleItem(c, "PostUsers", "user", "user_id", id, listEndpointParams{props: &userCreatedProps, withUserID: true},
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return odb.GetUser(ctx, id, p)
		})
}
