package serverhandlers

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteObsolescenceSetting handles DELETE /obsolescence/settings/{id}
func (a *Api) DeleteObsolescenceSetting(c echo.Context, id string) error {
	log := echolog.GetLogHandler(c, "DeleteObsolescenceSetting")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if err := requireObsManager(c); err != nil {
		return err
	}

	log.Info("called", "id", id)

	return a.deleteObsolescenceSettingByID(c, log, ctx, id)
}

func (a *Api) deleteObsolescenceSettingByID(c echo.Context, log *slog.Logger, ctx context.Context, id string) error {
	odb := a.ODB

	row, err := odb.GetObsolescenceSettingRow(ctx, id)
	if err != nil {
		log.Error("cannot get obsolescence setting", "id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get obsolescence setting")
	}
	if row == nil {
		return c.JSON(http.StatusOK, map[string]string{
			"error": "Obsolescence setting " + id + " not found",
		})
	}

	if err := odb.DeleteObsolescenceSetting(ctx, row.ID); err != nil {
		log.Error("cannot delete obsolescence setting", "id", row.ID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete obsolescence setting")
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "obsolescence.setting.delete",
		User:   userEmail,
		Fmt:    "Obsolescence setting %(obs_type)s:%(obs_name)s deleted",
		Dict: map[string]any{
			"obs_type": row.ObsType,
			"obs_name": row.ObsName,
		},
		Level: "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		"info": "Obsolescence setting " + row.ObsType + ":" + row.ObsName + " deleted",
	})
}
