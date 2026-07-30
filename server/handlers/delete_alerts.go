package serverhandlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

var alertBulkFilterKeys = []string{
	"id", "dash_type", "dash_created", "dash_env", "dash_md5", "dash_severity",
	"dash_updated", "node_id", "svc_id", "dash_fmt", "dash_dict", "dash_instance",
}

// DeleteAlerts handles DELETE /alerts
func (a *Api) DeleteAlerts(c echo.Context) error {
	log := echolog.GetLogHandler(c, "DeleteAlerts")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	var body map[string]any
	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	var cols []string
	var vals []any
	for _, k := range alertBulkFilterKeys {
		if v, ok := body[k]; ok {
			cols = append(cols, k)
			vals = append(vals, fmt.Sprintf("%v", v))
		}
	}

	id, found, err := odb.FindAlertIDByCriteria(ctx, cols, vals)
	if err != nil {
		log.Error("cannot find alert", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot find alert")
	}
	if !found {
		return c.JSON(http.StatusOK, map[string]string{
			"info": fmt.Sprintf("no alert matching %v", body),
		})
	}

	return a.deleteAlertByID(c, "DeleteAlerts", strconv.FormatInt(id, 10))
}
