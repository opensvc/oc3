package serverhandlers

import (
	"context"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// PostFiltersets handles POST /filtersets
func (a *Api) PostFiltersets(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostFiltersets")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if err := requireCompManager(c); err != nil {
		return err
	}

	var body server.PostFiltersetsJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	if body.Id != nil && *body.Id != "" {
		log.Info("called", "filterset_id", *body.Id)
		return a.postFiltersetUpdate(c, log, ctx, *body.Id, nil, body.FsetStats)
	}

	if body.FsetName == nil || *body.FsetName == "" {
		return JSONProblemf(c, http.StatusBadRequest, "the fset_name property is mandatory")
	}
	fsetName := *body.FsetName

	log.Info("called", "fset_name", fsetName)

	existingID, exists, err := odb.FiltersetByName(ctx, fsetName)
	if err != nil {
		log.Error("cannot check filterset existence", "fset_name", fsetName, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check filterset existence")
	}
	if exists {
		return a.postFiltersetUpdate(c, log, ctx, strconv.Itoa(existingID), nil, body.FsetStats)
	}

	fsetStats := "F"
	if body.FsetStats != nil {
		fsetStats = *body.FsetStats
	}

	userEmail, _ := c.Get(XUserEmail).(string)

	id, err := odb.InsertFilterset(ctx, fsetName, fsetStats, userEmail)
	if err != nil {
		log.Error("cannot create filterset", "fset_name", fsetName, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot create filterset")
	}

	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "filterset.create",
		User:   userEmail,
		Fmt:    "added filterset %(name)s",
		Dict:   map[string]any{"name": fsetName},
		Level:  "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return a.handleItem(c, "PostFiltersets", "filterset", "id", strconv.Itoa(id), listEndpointParams{},
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return odb.GetFilterset(ctx, strconv.Itoa(id), p)
		})
}
