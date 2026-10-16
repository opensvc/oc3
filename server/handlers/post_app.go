package serverhandlers

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// appFieldMaxLen is the size of the app, app_domain and app_team_ops columns.
const appFieldMaxLen = 64

// appProblem is the refusal of a change of an application code.
type appProblem struct {
	status int
	text   string
}

// checkAppUpdate validates the changes of an application code, trimming the
// name, and returns the refusal, or nil. The name may be neither empty, nor too
// long, nor the name of another code, compared as the column collation does;
// the ops team, when set, is an organizational group.
func checkAppUpdate(ctx context.Context, odb *cdb.DB, app *cdb.App, body *server.PostAppJSONRequestBody) *appProblem {
	refuse := func(status int, format string, args ...any) *appProblem {
		return &appProblem{status: status, text: fmt.Sprintf(format, args...)}
	}
	if body.App != nil {
		name := strings.TrimSpace(*body.App)
		switch {
		case name == "":
			return refuse(http.StatusBadRequest, "the application code may not be empty")
		case utf8.RuneCountInString(name) > appFieldMaxLen:
			return refuse(http.StatusBadRequest, "the application code may not exceed %d characters", appFieldMaxLen)
		}
		otherID, taken, err := odb.AppIDByName(ctx, name)
		if err != nil {
			return refuse(http.StatusInternalServerError, "cannot check app existence")
		}
		if taken && otherID != app.ID {
			return refuse(http.StatusConflict, "app %s already exists", name)
		}
		body.App = &name
	}
	if body.AppDomain != nil && utf8.RuneCountInString(*body.AppDomain) > appFieldMaxLen {
		return refuse(http.StatusBadRequest, "the domain may not exceed %d characters", appFieldMaxLen)
	}
	if body.AppTeamOps != nil && *body.AppTeamOps != "" {
		ok, err := odb.OrgGroupExists(ctx, *body.AppTeamOps)
		if err != nil {
			return refuse(http.StatusInternalServerError, "cannot check the ops team")
		}
		if !ok {
			return refuse(http.StatusBadRequest, "the ops team %s is not an organizational group", *body.AppTeamOps)
		}
	}
	return nil
}

// PostApp handles POST /apps/{app_id}: it changes the code, description, domain
// or ops team of an application code, renaming it on its nodes and services too.
// The caller is an AppManager responsible for the app.
func (a *Api) PostApp(c echo.Context, appId string) error {
	log := echolog.GetLogHandler(c, "PostApp")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}

	// An AppManager, as the historical collector checks it, then responsible
	// for the app below; a Manager is both.
	isManager := IsManager(c)
	if !IsAppManager(c) {
		return JSONProblemf(c, http.StatusForbidden, "AppManager privilege required")
	}

	var body server.PostAppJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	log.Info("called", "app_id", appId)

	odb := cdb.New(a.DB)
	odb.CreateSession(a.Ev)

	app, err := odb.GetApp(ctx, appId, nil, true)
	if err != nil {
		log.Error("cannot get app", "app_id", appId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get app")
	}
	if app == nil {
		return JSONProblemf(c, http.StatusNotFound, "app %s not found", appId)
	}

	responsible, err := odb.AppResponsible(ctx, appId, UserGroupsFromContext(c), isManager, "")
	if err != nil {
		log.Error("cannot check app responsibility", "app_id", appId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check app responsibility")
	}
	if !responsible {
		return JSONProblemf(c, http.StatusForbidden, "you are not responsible for this app")
	}

	if problem := checkAppUpdate(ctx, odb, app, &body); problem != nil {
		return JSONProblem(c, problem.status, problem.text)
	}
	if body.AppDomain != nil {
		if msg, err := checkConstraints(c, ctx, odb, server.FieldConstraintObjectKindApp, strconv.FormatInt(app.ID, 10), map[string]any{"app_domain": *body.AppDomain}); err != nil {
			log.Error("cannot check the field constraints", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot check the field constraints")
		} else if msg != "" {
			return JSONProblem(c, http.StatusBadRequest, msg)
		}
	}

	fields := cdb.UpdateAppFields{
		App:         body.App,
		Description: body.Description,
		AppDomain:   body.AppDomain,
		AppTeamOps:  body.AppTeamOps,
	}

	// Committed before the change is announced and read back.
	if problem := func() *appProblem {
		tx, markSuccess, endTx, err := odb.BeginTxWithControl(ctx, log, &sql.TxOptions{})
		if err != nil {
			log.Error("cannot start transaction", logkey.Error, err)
			return &appProblem{http.StatusInternalServerError, "cannot update app"}
		}
		defer endTx()

		if err := tx.UpdateApp(ctx, app.ID, fields); err != nil {
			log.Error("cannot update app", "app_id", appId, logkey.Error, err)
			return &appProblem{http.StatusInternalServerError, "cannot update app"}
		}

		// If the app code is renamed, update nodes and services references
		if body.App != nil && *body.App != app.App {
			if err := tx.UpdateNodesApp(ctx, app.App, *body.App); err != nil {
				log.Error("cannot update nodes app", logkey.Error, err)
				return &appProblem{http.StatusInternalServerError, "cannot update nodes app reference"}
			}
			if err := tx.UpdateServicesApp(ctx, app.App, *body.App); err != nil {
				log.Error("cannot update services app", logkey.Error, err)
				return &appProblem{http.StatusInternalServerError, "cannot update services app reference"}
			}
		}

		userEmail, _ := c.Get(XUserEmail).(string)
		if err := tx.Log(ctx, cdb.LogEntry{
			Action: "apps.change",
			User:   userEmail,
			Fmt:    "app %(app)s changed",
			Dict: map[string]any{
				"app": app.App,
			},
			Level: "info",
		}); err != nil {
			log.Error("cannot write audit log", logkey.Error, err)
			return &appProblem{http.StatusInternalServerError, "cannot write audit log"}
		}

		markSuccess()
		return nil
	}(); problem != nil {
		return JSONProblem(c, problem.status, problem.text)
	}

	if err := odb.Session.NotifyTableChangeWithData(ctx, "apps", map[string]any{"id": app.ID}); err != nil {
		log.Error("cannot notify apps change", logkey.Error, err)
	}

	updated, err := odb.GetApp(ctx, strconv.FormatInt(app.ID, 10), nil, true)
	if err != nil || updated == nil {
		return JSONProblemf(c, http.StatusInternalServerError, "cannot fetch updated app")
	}
	return c.JSON(http.StatusOK, updated)
}
