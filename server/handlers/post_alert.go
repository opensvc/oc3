package serverhandlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// PostAlert handles POST /alerts/{id} (update an alert).
func (a *Api) PostAlert(c echo.Context, id string) error {
	log := echolog.GetLogHandler(c, "PostAlert")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) && !IsAuthByNode(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "authentication required")
	}
	if !IsAlertsManager(c) {
		return JSONProblemf(c, http.StatusForbidden, "user has no AlertsManager privilege")
	}

	alertID, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return JSONProblemf(c, http.StatusNotFound, "alert %s does not exist", id)
	}

	curEnv, curFmt, curDict, found, err := odb.GetAlertForUpdate(ctx, alertID)
	if err != nil {
		log.Error("cannot lookup alert", "alert_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup alert %s", id)
	}
	if !found {
		return JSONProblemf(c, http.StatusNotFound, "alert %s does not exist", id)
	}

	var body map[string]any
	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	// Resolve env from svc_id/node_id in the body, falling back to the existing
	// alert env.
	envVal, envResolved, err := a.resolveAlertEnv(ctx, body)
	if err != nil {
		log.Error("cannot resolve alert env", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve alert env")
	}
	if !envResolved {
		envVal = curEnv
	}

	fmtStr := curFmt
	if v, ok := bodyString(body, "dash_fmt"); ok {
		fmtStr = v
	}
	dictMap, dictJSON, dictPresent := alertDict(body)
	if !dictPresent {
		dictMap = parseJSONMap(curDict)
	}
	if _, ok := formatNamedTemplate(fmtStr, dictMap); !ok {
		return JSONProblemf(c, http.StatusBadRequest, "incompatible 'dash_fmt' and 'dash_dict'")
	}

	fields := map[string]any{
		"dash_env": envVal,
		"dash_md5": "",
	}
	for _, k := range []string{"dash_type", "dash_instance", "svc_id", "node_id", "dash_fmt"} {
		if v, ok := bodyString(body, k); ok {
			fields[k] = v
		}
	}
	if dictPresent {
		fields["dash_dict"] = dictJSON
	}
	if sev, ok := body["dash_severity"]; ok && isNonEmpty(sev) {
		fields["dash_severity"] = clampInt(toInt64(sev), 0, 4)
	} else if base, ok := body["base_severity"]; ok {
		s := clampInt(toInt64(base), 0, 3)
		if strings.Contains(envVal, "PRD") && s < 4 {
			s++
		}
		fields["dash_severity"] = s
	}

	if err := odb.UpdateAlertFields(ctx, alertID, fields); err != nil {
		log.Error("cannot update alert", "alert_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot update alert %s", id)
	}

	logAlertChange(c, odb, ctx, log, "dashboard.change", "update alert %(data)s", body)

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return fetchAndReturnAlert(c, odb, ctx, alertID, "cannot fetch updated alert")
}

func (a *Api) resolveAlertEnv(ctx context.Context, body map[string]any) (string, bool, error) {
	if svcID, ok := bodyString(body, "svc_id"); ok && svcID != "" {
		env, found, err := a.ODB.AlertSvcEnv(ctx, svcID)
		if err != nil {
			return "", false, err
		}
		if found {
			return env, true, nil
		}
	}
	if nodeID, ok := bodyString(body, "node_id"); ok && nodeID != "" {
		env, found, err := a.ODB.AlertNodeEnv(ctx, nodeID)
		if err != nil {
			return "", false, err
		}
		if found {
			return env, true, nil
		}
	}
	return "", false, nil
}

func logAlertChange(c echo.Context, odb *cdb.DB, ctx context.Context, log *slog.Logger, action, format string, body map[string]any) {
	userEmail, _ := c.Get(XUserEmail).(string)
	entry := cdb.LogEntry{
		Action: action,
		User:   userEmail,
		Fmt:    format,
		Dict:   map[string]any{"data": "alert"},
		Level:  "info",
	}
	if nodeID, ok := bodyString(body, "node_id"); ok {
		if parsed, err := uuid.Parse(nodeID); err == nil {
			entry.NodeID = &parsed
		}
	}
	if svcID, ok := bodyString(body, "svc_id"); ok {
		if parsed, err := uuid.Parse(svcID); err == nil {
			entry.SvcID = &parsed
		}
	}
	if logErr := odb.Log(ctx, entry); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}
}

func fetchAndReturnAlert(c echo.Context, odb *cdb.DB, ctx context.Context, id int64, errMsg string) error {
	mapping := propsMapping["alert"]
	props := defaultProps(mapping)
	hasAlert := slices.Contains(props, "alert")
	fetchProps := props
	if hasAlert {
		fetchProps = ensureProps(stripProp(fetchProps, "alert"), "dash_fmt", "dash_dict")
	}
	selectExprs, err := buildSelectClause(fetchProps, mapping)
	if err != nil {
		return JSONProblemf(c, http.StatusInternalServerError, "%s", errMsg)
	}
	rows, err := odb.GetAlert(ctx, strconv.FormatInt(id, 10), cdb.ListParams{
		Props:       fetchProps,
		SelectExprs: selectExprs,
		TypeHints:   buildTypeHints(fetchProps, mapping),
	})
	if err != nil || len(rows) == 0 {
		return JSONProblemf(c, http.StatusInternalServerError, "%s", errMsg)
	}
	if hasAlert {
		mangleAlerts(rows, props)
	}
	return c.JSON(http.StatusOK, rows[0])
}

func bodyString(body map[string]any, key string) (string, bool) {
	v, ok := body[key]
	if !ok {
		return "", false
	}
	return fmt.Sprintf("%v", v), true
}

func alertDict(body map[string]any) (dict map[string]any, jsonStr string, present bool) {
	v, ok := body["dash_dict"]
	if !ok {
		return nil, "", false
	}
	switch x := v.(type) {
	case string:
		return parseJSONMap(x), x, true
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return map[string]any{}, "{}", true
		}
		return parseJSONMap(string(b)), string(b), true
	}
}

func parseJSONMap(s string) map[string]any {
	if s == "" {
		return map[string]any{}
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return map[string]any{}
	}
	return m
}

func isNonEmpty(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case float64:
		return x != 0
	case string:
		return x != ""
	default:
		return true
	}
}

func clampInt(v, lo, hi int64) int64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
