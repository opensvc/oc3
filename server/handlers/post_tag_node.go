package serverhandlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// PostTagNode handles POST /tags/{tag_id}/nodes/{node_id}: attach a tag to a node.
func (a *Api) PostTagNode(c echo.Context, tagIdParam int, nodeId string) error {
	log := echolog.GetLogHandler(c, "PostTagNode")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	log.Info("called", logkey.TagID, tagIdParam, logkey.NodeID, nodeId)

	var body server.PostTagNodeJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

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

	current, err := odb.GetNodeTagAttachment(ctx, nodeId, tag.TagID)
	if err != nil {
		log.Error("cannot get node tag attachment", logkey.NodeID, nodeId, logkey.TagID, tagIdParam, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get node tag attachment")
	}

	allowed, err := odb.NodeTagAttachAllowed(ctx, nodeId, tag.TagName)
	if err != nil {
		log.Error("cannot check tag compatibility", logkey.NodeID, nodeId, logkey.TagID, tagIdParam, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check tag compatibility")
	}
	if !allowed {
		return JSONProblemf(c, http.StatusConflict, "tag '%s' is not compatible with other tags attached to node '%s'", tag.TagName, nodeId)
	}

	var info, logFmt string
	switch {
	case current != nil:
		if body.TagAttachData != nil && *body.TagAttachData != current.TagAttachData {
			if err := odb.UpdateNodeTagAttachData(ctx, nodeId, tag.TagID, *body.TagAttachData); err != nil {
				log.Error("cannot update node tag attach data", logkey.NodeID, nodeId, logkey.TagID, tagIdParam, logkey.Error, err)
				return JSONProblemf(c, http.StatusInternalServerError, "cannot update node tag attach data")
			}
			info = fmt.Sprintf("node '%s' tag '%s' attach data updated", nodeId, tag.TagName)
			logFmt = "node '%(node_id)s' tag '%(tag_name)s' attach data updated"
		} else {
			return c.JSON(http.StatusOK, map[string]string{
				"info": fmt.Sprintf("tag '%s' already attached to node '%s'", tag.TagName, nodeId),
			})
		}
	default:
		if err := odb.AttachNodeTag(ctx, nodeId, tag.TagID); err != nil {
			log.Error("cannot attach tag to node", logkey.NodeID, nodeId, logkey.TagID, tagIdParam, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot attach tag to node")
		}
		info = fmt.Sprintf("node '%s' tag '%s' attached", nodeId, tag.TagName)
		logFmt = "node '%(node_id)s' tag '%(tag_name)s' attached"
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	logEntry := cdb.LogEntry{
		Action: "node.tag",
		User:   userEmail,
		Fmt:    logFmt,
		Dict:   map[string]any{"node_id": nodeId, "tag_name": tag.TagName},
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

	return c.JSON(http.StatusOK, map[string]string{"info": info})
}
