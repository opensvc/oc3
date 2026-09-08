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

// PostFilter handles POST /filters/{filter_id}: modify a filter properties.
func (a *Api) PostFilter(c echo.Context, filterId string) error {
	log := echolog.GetLogHandler(c, "PostFilter")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if err := requireCompManager(c); err != nil {
		return err
	}

	var body server.PostFilterJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	log.Info("called", "filter_id", filterId)

	id, found, err := odb.FilterID(ctx, filterId)
	if err != nil {
		log.Error("cannot resolve filter", "filter_id", filterId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve filter")
	}
	if !found {
		return JSONProblemf(c, http.StatusNotFound, "filter %s not found", filterId)
	}

	row, err := odb.GetFilterRow(ctx, id)
	if err != nil {
		log.Error("cannot get filter", "filter_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get filter")
	}
	if row == nil {
		return JSONProblemf(c, http.StatusNotFound, "filter %d not found", id)
	}

	fields := cdb.UpdateFilterFields{
		FTable: body.FTable,
		FField: body.FField,
		FOp:    body.FOp,
		FValue: body.FValue,
		FLabel: body.FLabel,
	}

	changes := []string{}
	if body.FTable != nil {
		changes = append(changes, fmt.Sprintf("f_table: %s => %s", row.FTable, *body.FTable))
	}
	if body.FField != nil {
		changes = append(changes, fmt.Sprintf("f_field: %s => %s", row.FField, *body.FField))
	}
	if body.FOp != nil {
		changes = append(changes, fmt.Sprintf("f_op: %s => %s", row.FOp, *body.FOp))
	}
	if body.FValue != nil {
		changes = append(changes, fmt.Sprintf("f_value: %s => %s", row.FValue, *body.FValue))
	}
	if body.FLabel != nil {
		changes = append(changes, fmt.Sprintf("f_label: %s => %s", row.FLabel, *body.FLabel))
	}

	userEmail, _ := c.Get(XUserEmail).(string)

	markSuccess, endTx, err := odb.BeginTxWithControl(ctx, log, &sql.TxOptions{})
	if err != nil {
		log.Error("cannot start transaction", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot update filter")
	}
	defer endTx()

	if err := odb.UpdateFilter(ctx, id, fields, userEmail); err != nil {
		log.Error("cannot update filter", "filter_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot update filter")
	}

	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "filter.change",
		User:   userEmail,
		Fmt:    "change filter %(data)s",
		Dict:   map[string]any{"data": strings.Join(changes, ", ")},
		Level:  "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	markSuccess()

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return a.handleItem(c, "PostFilter", "filter", "id", strconv.Itoa(id), listEndpointParams{},
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return odb.GetFilter(ctx, strconv.Itoa(id), p)
		})
}
