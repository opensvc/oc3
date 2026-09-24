package serverhandlers

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

func requireObsManager(c echo.Context) error {
	if !IsAuthByUser(c) {
		return denyRequest(c, http.StatusUnauthorized, "user authentication required")
	}
	if !IsObsManager(c) {
		return denyRequest(c, http.StatusForbidden, "ObsManager privilege required")
	}
	return nil
}

// PostObsolescenceSetting handles POST /obsolescence/settings/{id}: modify an obsolescence setting.
func (a *Api) PostObsolescenceSetting(c echo.Context, id string) error {
	log := echolog.GetLogHandler(c, "PostObsolescenceSetting")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if err := requireObsManager(c); err != nil {
		return err
	}

	var body server.PostObsolescenceSettingJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	return a.postObsolescenceSettingUpdate(c, ctx, log, id, body.ObsWarnDate, body.ObsAlertDate)
}

func (a *Api) postObsolescenceSettingUpdate(c echo.Context, ctx context.Context, log *slog.Logger, id string, obsWarnDate, obsAlertDate *string) error {
	odb := a.ODB

	log.Info("called", "id", id)

	row, err := odb.GetObsolescenceSettingRow(ctx, id)
	if err != nil {
		log.Error("cannot get obsolescence setting", "id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get obsolescence setting")
	}
	if row == nil {
		return JSONProblemf(c, http.StatusNotFound, "obsolescence setting %s not found", id)
	}

	if _, err := parseObsDate(obsWarnDate); err != nil {
		return JSONProblemf(c, http.StatusBadRequest, "invalid obs_warn_date: %s", err)
	}
	if _, err := parseObsDate(obsAlertDate); err != nil {
		return JSONProblemf(c, http.StatusBadRequest, "invalid obs_alert_date: %s", err)
	}

	changes := []string{}
	if obsWarnDate != nil {
		changes = append(changes, fmt.Sprintf("obs_warn_date: %s => %s", row.ObsWarnDate.String, *obsWarnDate))
	}
	if obsAlertDate != nil {
		changes = append(changes, fmt.Sprintf("obs_alert_date: %s => %s", row.ObsAlertDate.String, *obsAlertDate))
	}

	fields := cdb.UpdateObsolescenceSettingFields{
		ObsWarnDate:  obsWarnDate,
		ObsAlertDate: obsAlertDate,
	}
	userEmail, _ := c.Get(XUserEmail).(string)
	if err := odb.UpdateObsolescenceSetting(ctx, row.ID, fields, userEmail); err != nil {
		log.Error("cannot update obsolescence setting", "id", row.ID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot update obsolescence setting")
	}

	// The nodes take the setting's dates as now stored, both at once.
	if obsWarnDate != nil || obsAlertDate != nil {
		if err := odb.SyncNodeObsolescenceDates(ctx, row.ID, row.ObsType); err != nil {
			log.Error("cannot update node obsolescence dates", "id", row.ID, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot update node obsolescence dates")
		}
	}
	if obsWarnDate != nil {
		if err := odb.DeleteDashObsWithout(ctx, row.ObsName, row.ObsType, "warn"); err != nil {
			log.Error("cannot clean up obsolescence dashboard alerts", "id", row.ID, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot clean up obsolescence dashboard alerts")
		}
	}
	if obsAlertDate != nil {
		if err := odb.DeleteDashObsWithout(ctx, row.ObsName, row.ObsType, "alert"); err != nil {
			log.Error("cannot clean up obsolescence dashboard alerts", "id", row.ID, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot clean up obsolescence dashboard alerts")
		}
	}

	switch row.ObsType {
	case "os":
		if err := odb.UpdateDashObsOSWarnForName(ctx, row.ObsName); err != nil {
			log.Error("cannot update os obsolescence warning dashboard", "id", row.ID, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot update obsolescence dashboard")
		}
		if err := odb.UpdateDashObsOSAlertForName(ctx, row.ObsName); err != nil {
			log.Error("cannot update os obsolescence alert dashboard", "id", row.ID, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot update obsolescence dashboard")
		}
	case "hw":
		if err := odb.UpdateDashObsHWAlertForName(ctx, row.ObsName); err != nil {
			log.Error("cannot update hw obsolescence alert dashboard", "id", row.ID, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot update obsolescence dashboard")
		}
		if err := odb.UpdateDashObsHWWarnForName(ctx, row.ObsName); err != nil {
			log.Error("cannot update hw obsolescence warning dashboard", "id", row.ID, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot update obsolescence dashboard")
		}
	}

	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "obsolescence.setting.change",
		User:   userEmail,
		Fmt:    "Obsolescence setting %(obs_type)s:%(obs_name)s change: %(data)s",
		Dict: map[string]any{
			"obs_type": row.ObsType,
			"obs_name": row.ObsName,
			"data":     strings.Join(changes, ", "),
		},
		Level: "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	idStr := strconv.Itoa(row.ID)
	return a.handleItem(c, "PostObsolescenceSetting", "obsolescence", "id", idStr, listEndpointParams{},
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return odb.GetObsolescenceSetting(ctx, idStr, p)
		})
}

// obsDateLayouts are the date forms accepted for obsolescence dates: a calendar
// date, or the "YYYY-MM-DD hh:mm:ss" form the settings are listed with.
var obsDateLayouts = []string{"2006-01-02", "2006-01-02 15:04:05"}

// parseObsDate validates an obsolescence date from a request body. An empty string
// clears the date and maps to NULL: the column is a datetime, which rejects ”.
func parseObsDate(value *string) (sql.NullString, error) {
	if value == nil || *value == "" {
		return sql.NullString{}, nil
	}
	for _, layout := range obsDateLayouts {
		if _, err := time.Parse(layout, *value); err == nil {
			return sql.NullString{String: *value, Valid: true}, nil
		}
	}
	return sql.NullString{}, fmt.Errorf("%q is not a YYYY-MM-DD or YYYY-MM-DD hh:mm:ss date", *value)
}
