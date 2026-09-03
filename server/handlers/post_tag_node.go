package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// PostTagNode handles POST /tags/{tag_id}/nodes/{node_id}: attach a tag to a node.
func (a *Api) PostTagNode(c echo.Context, tagIdParam int, nodeId string) error {
	log := echolog.GetLogHandler(c, "PostTagNode")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	log.Info("called", logkey.TagID, tagIdParam, logkey.NodeID, nodeId)

	var body server.PostTagNodeJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	tag, err := a.resolveTagByRecordID(c, log, ctx, tagIdParam)
	if err != nil {
		return err
	}

	return a.attachTagNode(c, log, ctx, tag, nodeId, body.TagAttachData)
}
