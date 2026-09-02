package serverhandlers

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// PostAlerts handles POST /alerts (create or update an alert).
func (a *Api) PostAlerts(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostAlerts")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) && !IsAuthByNode(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "authentication required")
	}

	var body map[string]any
	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	if IsAuthByNode(c) {
		callerNodeID, _ := c.Get(XNodeID).(string)
		body["node_id"] = callerNodeID
	} else if !IsAlertsManager(c) {
		return JSONProblemf(c, http.StatusForbidden, "user has no AlertsManager privilege")
	}

	dashType, ok := bodyString(body, "dash_type")
	if !ok || dashType == "" {
		return JSONProblemf(c, http.StatusBadRequest, "'dash_type' is mandatory")
	}
	nodeID, _ := bodyString(body, "node_id")
	svcID, _ := bodyString(body, "svc_id")
	dashInstance, _ := bodyString(body, "dash_instance")

	envVal, _, err := a.resolveAlertEnv(ctx, body)
	if err != nil {
		log.Error("cannot resolve alert env", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve alert env")
	}

	fmtStr, _ := bodyString(body, "dash_fmt")
	dictMap, dictJSON, dictPresent := alertDict(body)
	if !dictPresent {
		dictMap = map[string]any{}
		dictJSON = "{}"
	}
	if _, ok := formatNamedTemplate(fmtStr, dictMap); !ok {
		return JSONProblemf(c, http.StatusBadRequest, "incompatible 'dash_fmt' and 'dash_dict'")
	}

	var severity int64
	if sev, ok := body["dash_severity"]; ok && isNonEmpty(sev) {
		severity = clampInt(toInt64(sev), 0, 4)
	} else if base, ok := body["base_severity"]; ok {
		s := clampInt(toInt64(base), 0, 3)
		if strings.Contains(envVal, "PRD") && s < 3 {
			s++
		}
		severity = s
	} else {
		severity = 1
	}

	id, found, err := odb.FindAlertIDByKey(ctx, dashType, nodeID, svcID, dashInstance)
	if err != nil {
		log.Error("cannot lookup alert", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup alert")
	}

	if found {
		if err := odb.UpsertUpdateAlert(ctx, id, fmtStr, dictJSON, envVal, int(severity)); err != nil {
			log.Error("cannot update alert", "alert_id", id, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot update alert")
		}
		logAlertChange(c, odb, ctx, log, "dashboard.change", "update alert %(data)s", body)
	} else {
		fields := map[string]any{
			"dash_type":     dashType,
			"dash_instance": dashInstance,
			"node_id":       nodeID,
			"svc_id":        svcID,
			"dash_fmt":      fmtStr,
			"dash_dict":     dictJSON,
			"dash_severity": severity,
		}
		if env, ok := bodyString(body, "dash_env"); ok {
			fields["dash_env"] = env
		}
		id, err = odb.InsertAlert(ctx, fields)
		if err != nil {
			log.Error("cannot create alert", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot create alert")
		}
		logAlertChange(c, odb, ctx, log, "dashboard.create", "create alert %(data)s", body)
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return fetchAndReturnAlert(c, odb, ctx, id, "cannot fetch alert")
}
