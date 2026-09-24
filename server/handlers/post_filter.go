package serverhandlers

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"slices"
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

	// f_label and f_cksum are generated columns, computed from the definition.
	if body.FLabel != nil {
		return JSONProblemf(c, http.StatusBadRequest, "f_label is computed from the filter definition and cannot be set")
	}

	// Validate the definition the filter will have once modified, with the same
	// rules as POST /filters: a filter on an unknown table, column or operator would
	// break the filtersets it is attached to.
	fTable, fField, fOp, fValue := row.FTable, row.FField, row.FOp, row.FValue
	if body.FTable != nil {
		fTable = *body.FTable
	}
	if body.FField != nil {
		fField = *body.FField
	}
	if body.FOp != nil {
		fOp = strings.ToUpper(string(*body.FOp))
	}
	if body.FValue != nil {
		fValue = *body.FValue
	}
	switch {
	case fTable == "":
		return JSONProblemf(c, http.StatusBadRequest, "f_table is mandatory")
	case fField == "":
		return JSONProblemf(c, http.StatusBadRequest, "f_field is mandatory")
	case fOp == "":
		return JSONProblemf(c, http.StatusBadRequest, "f_op is mandatory")
	case fValue == "":
		return JSONProblemf(c, http.StatusBadRequest, "f_value is mandatory")
	}
	if !slices.Contains(filterTables, fTable) {
		return JSONProblemf(c, http.StatusBadRequest, "f_table must be one of %s", strings.Join(filterTables, ", "))
	}
	if !slices.Contains(filterOperators, fOp) {
		return JSONProblemf(c, http.StatusBadRequest, "f_op must be one of %s", strings.Join(filterOperators, ", "))
	}
	columnFound, err := odb.ColumnExists(ctx, fTable, fField)
	if err != nil {
		log.Error("cannot check filter field", "f_table", fTable, "f_field", fField, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check filter field")
	}
	if !columnFound {
		return JSONProblemf(c, http.StatusBadRequest, "field not found in model's table")
	}
	existingID, exists, err := odb.FilterByDefinition(ctx, fTable, fField, fOp, fValue)
	if err != nil {
		log.Error("cannot check filter existence", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check filter existence")
	}
	if exists && existingID != id {
		return JSONProblemf(c, http.StatusConflict, "a filter with the same definition already exists: %d", existingID)
	}

	fields := cdb.UpdateFilterFields{
		FTable: body.FTable,
		FField: body.FField,
		FValue: body.FValue,
	}
	if body.FOp != nil {
		fields.FOp = &fOp
	}

	changes := []string{}
	if body.FTable != nil {
		changes = append(changes, fmt.Sprintf("f_table: %s => %s", row.FTable, *body.FTable))
	}
	if body.FField != nil {
		changes = append(changes, fmt.Sprintf("f_field: %s => %s", row.FField, *body.FField))
	}
	if body.FOp != nil {
		changes = append(changes, fmt.Sprintf("f_op: %s => %s", row.FOp, fOp))
	}
	if body.FValue != nil {
		changes = append(changes, fmt.Sprintf("f_value: %s => %s", row.FValue, *body.FValue))
	}

	userEmail, _ := c.Get(XUserEmail).(string)

	// The transaction ends before the modified filter is read back: a read done
	// while it is still open, on another connection, returns the previous values.
	if err := func() error {
		tx, markSuccess, endTx, err := odb.BeginTxWithControl(ctx, log, &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("cannot start transaction: %w", err)
		}
		defer endTx()
		if err := tx.UpdateFilter(ctx, id, fields, userEmail); err != nil {
			return err
		}
		if logErr := tx.Log(ctx, cdb.LogEntry{
			Action: "filter.change",
			User:   userEmail,
			Fmt:    "change filter %(data)s",
			Dict:   map[string]any{"data": strings.Join(changes, ", ")},
			Level:  "info",
		}); logErr != nil {
			log.Error("cannot write audit log", logkey.Error, logErr)
		}
		markSuccess()
		return nil
	}(); err != nil {
		log.Error("cannot update filter", "filter_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot update filter")
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return a.handleItem(c, "PostFilter", "filter", "id", strconv.Itoa(id), listEndpointParams{},
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return odb.GetFilter(ctx, strconv.Itoa(id), p)
		})
}
