package serverhandlers

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// PostFilterset handles POST /filtersets/{filterset_id}: modify a filterset's properties.
func (a *Api) PostFilterset(c echo.Context, filtersetId string) error {
	log := echolog.GetLogHandler(c, "PostFilterset")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	if !IsManager(c) {
		return JSONProblemf(c, http.StatusForbidden, "CompManager privilege required")
	}

	var body server.PostFiltersetJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	log.Info("called", "filterset_id", filtersetId)

	id, found, err := odb.FiltersetID(ctx, filtersetId)
	if err != nil {
		log.Error("cannot resolve filterset", "filterset_id", filtersetId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve filterset")
	}
	if !found {
		return JSONProblemf(c, http.StatusNotFound, "filterset %s not found", filtersetId)
	}

	row, err := odb.GetFiltersetRow(ctx, id)
	if err != nil {
		log.Error("cannot get filterset", "filterset_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get filterset")
	}
	if row == nil {
		return JSONProblemf(c, http.StatusNotFound, "filterset %d not found", id)
	}

	getFilterset := func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return odb.GetFilterset(ctx, strconv.Itoa(id), p)
	}

	// No updatable field provided: return the filterset unchanged.
	if body.FsetName == nil && body.FsetStats == nil {
		return a.handleItem(c, "PostFilterset", "filterset", "id", strconv.Itoa(id), listEndpointParams{}, getFilterset)
	}

	fields := cdb.UpdateFiltersetFields{
		FsetName:  body.FsetName,
		FsetStats: body.FsetStats,
	}

	changes := []string{}
	if body.FsetName != nil {
		changes = append(changes, fmt.Sprintf("fset_name: %s => %s", row.FsetName, *body.FsetName))
	}
	if body.FsetStats != nil {
		changes = append(changes, fmt.Sprintf("fset_stats: %s => %s", row.FsetStats, *body.FsetStats))
	}

	userEmail, _ := c.Get(XUserEmail).(string)

	markSuccess, endTx, err := odb.BeginTxWithControl(ctx, log, &sql.TxOptions{})
	if err != nil {
		log.Error("cannot start transaction", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot update filterset")
	}
	defer endTx()

	if err := odb.UpdateFilterset(ctx, id, fields); err != nil {
		log.Error("cannot update filterset", "filterset_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot update filterset")
	}

	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "filterset.change",
		User:   userEmail,
		Fmt:    "change filterset %(data)s",
		Dict:   map[string]any{"data": strings.Join(changes, ", ")},
		Level:  "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	markSuccess()

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return a.handleItem(c, "PostFilterset", "filterset", "id", strconv.Itoa(id), listEndpointParams{}, getFilterset)
}
