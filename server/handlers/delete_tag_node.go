package serverhandlers

import (
	"context"
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteTagNode handles DELETE /tags/{tag_id}/nodes/{node_id}: detach a tag from a node.
func (a *Api) DeleteTagNode(c echo.Context, tagIdParam int, nodeId string) error {
	log := echolog.GetLogHandler(c, "DeleteTagNode")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	log.Info("called", logkey.TagID, tagIdParam, logkey.NodeID, nodeId)

	tags, err := odb.GetTags(ctx, &tagIdParam, 0, 0)
	if err != nil {
		log.Error("cannot get tag", logkey.TagID, tagIdParam, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get tag")
	}
	if len(tags) == 0 {
		return JSONProblemf(c, http.StatusNotFound, "tag %d not found", tagIdParam)
	}
	tag := tags[0]

	responsible, err := odb.NodeResponsible(ctx, nodeId, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		log.Error("cannot check if user is responsible for the node", logkey.NodeID, nodeId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check if user is responsible for node %s", nodeId)
	}
	if !responsible {
		log.Info("user is not responsible for this node", logkey.NodeID, nodeId)
		return JSONProblemf(c, http.StatusForbidden, "user is not responsible for node %s", nodeId)
	}

	n, err := odb.DetachNodeTag(ctx, nodeId, tag.TagID)
	if err != nil {
		log.Error("cannot detach tag from node", logkey.NodeID, nodeId, logkey.TagID, tagIdParam, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot detach tag from node")
	}
	if n == 0 {
		return c.JSON(http.StatusOK, map[string]string{"info": "tag already detached"})
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	logEntry := cdb.LogEntry{
		Action: "node.tag",
		User:   userEmail,
		Fmt:    "tag '%(tag_name)s' detached",
		Dict:   map[string]any{"tag_name": tag.TagName},
		Level:  "info",
	}
	if parsed, err := uuid.Parse(nodeId); err == nil {
		logEntry.NodeID = &parsed
	}
	if logErr := odb.Log(ctx, logEntry); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return c.JSON(http.StatusOK, map[string]string{"info": "tag detached"})
}
