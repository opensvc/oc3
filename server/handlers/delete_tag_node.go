package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteTagNode handles DELETE /tags/{tag_id}/nodes/{node_id}: detach a tag from a node.
func (a *Api) DeleteTagNode(c echo.Context, tagIdParam int, nodeId string) error {
	log := echolog.GetLogHandler(c, "DeleteTagNode")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	log.Info("called", logkey.TagID, tagIdParam, logkey.NodeID, nodeId)

	tag, err := a.resolveTagByRecordID(c, log, ctx, tagIdParam)
	if err != nil {
		return err
	}

	return a.detachTagNode(c, log, ctx, tag, nodeId)
}
