package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// PostFiltersetsFiltersets handles POST /filtersets_filtersets
func (a *Api) PostFiltersetsFiltersets(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostFiltersetsFiltersets")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if err := requireCompManager(c); err != nil {
		return err
	}

	var body server.PostFiltersetsFiltersetsJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if body.ParentFsetId == "" {
		return JSONProblemf(c, http.StatusBadRequest, "The 'parent_fset_id' key is mandatory")
	}
	if body.ChildFsetId == "" {
		return JSONProblemf(c, http.StatusBadRequest, "The 'child_fset_id' key is mandatory")
	}

	log.Info("called", "parent_fset_id", body.ParentFsetId, "child_fset_id", body.ChildFsetId)

	return a.postFiltersetFilterset(c, log, ctx, body.ParentFsetId, body.ChildFsetId, body.FLogOp, body.FOrder)
}
