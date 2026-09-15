package serverhandlers

import (
	"context"
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

var filterTables = []string{
	"nodes",
	"node_ip",
	"services",
	"svcmon",
	"resmon",
	"apps",
	"node_hba",
	"diskinfo",
	"svcdisks",
	"v_comp_moduleset_attachments",
	"v_tags",
	"packages",
}

var filterOperators = []string{"=", "LIKE", ">", ">=", "<", "<=", "IN"}

// PostFilters handles POST /filters
func (a *Api) PostFilters(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostFilters")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if err := requireCompManager(c); err != nil {
		return err
	}

	var body server.PostFiltersJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	fTable := body.FTable
	fField := body.FField
	fOp := strings.ToUpper(string(body.FOp))
	fValue := body.FValue

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

	log.Info("called", "f_table", fTable, "f_field", fField, "f_op", fOp, "f_value", fValue)

	found, err := odb.ColumnExists(ctx, fTable, fField)
	if err != nil {
		log.Error("cannot check filter field", "f_table", fTable, "f_field", fField, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check filter field")
	}
	if !found {
		return JSONProblemf(c, http.StatusBadRequest, "field not found in model's table")
	}

	existingID, exists, err := odb.FilterByDefinition(ctx, fTable, fField, fOp, fValue)
	if err != nil {
		log.Error("cannot check filter existence", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check filter existence")
	}
	if exists {
		return JSONProblemf(c, http.StatusConflict, "a filter with the same definition already exists: %d", existingID)
	}

	userEmail, _ := c.Get(XUserEmail).(string)

	id, err := odb.InsertFilter(ctx, fTable, fField, fOp, fValue, userEmail)
	if err != nil {
		log.Error("cannot create filter", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot create filter")
	}

	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "filter.create",
		User:   userEmail,
		Fmt:    "added filter %(t)s.%(f)s %(o)s %(val)s",
		Dict: map[string]any{
			"t":   fTable,
			"f":   fField,
			"o":   fOp,
			"val": fValue,
		},
		Level: "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return a.handleItem(c, "PostFilters", "filter", "id", strconv.Itoa(id), listEndpointParams{},
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return odb.GetFilter(ctx, strconv.Itoa(id), p)
		})
}
