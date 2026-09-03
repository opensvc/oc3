package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteTagsNodes handles DELETE /tags/nodes
func (a *Api) DeleteTagsNodes(c echo.Context) error {
	log := echolog.GetLogHandler(c, "DeleteTagsNodes")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	var body server.DeleteTagsNodesJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if body.NodeId == "" {
		return JSONProblemf(c, http.StatusBadRequest, "the 'node_id' key is mandatory")
	}
	if body.TagId == "" {
		return JSONProblemf(c, http.StatusBadRequest, "the 'tag_id' key is mandatory")
	}

	log.Info("called", logkey.TagID, body.TagId, logkey.NodeID, body.NodeId)

	tag, err := a.resolveTagByKey(c, log, ctx, body.TagId)
	if err != nil {
		return err
	}

	return a.detachTagNode(c, log, ctx, tag, body.NodeId)
}
