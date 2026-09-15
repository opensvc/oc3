package serverhandlers

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// PostLogs handles POST /logs: create a 'message' log event.
func (a *Api) PostLogs(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostLogs")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	var body server.PostLogsJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	// log_fmt is mandatory and must not be empty.
	if body.LogFmt == "" {
		return JSONProblemf(c, http.StatusBadRequest, "empty log event discarded")
	}

	level := "info"
	if body.LogLevel != nil && *body.LogLevel != "" {
		level = *body.LogLevel
	}

	dict := "{}"
	if body.LogDict != nil {
		b, err := json.Marshal(*body.LogDict)
		if err != nil {
			return JSONProblem(c, http.StatusBadRequest, err.Error())
		}
		dict = string(b)
	}

	msg := cdb.LogMessage{
		Fmt:   body.LogFmt,
		Dict:  dict,
		Level: level,
	}

	switch {
	case IsAuthByNode(c):
		nodeID, _ := c.Get(XNodeID).(string)
		if nodeID != "" {
			id := nodeID
			msg.NodeID = &id
		}
		msg.User = "agent"
	case IsAuthByUser(c):
		userEmail, _ := c.Get(XUserEmail).(string)
		msg.User = userEmail
		// the user must be responsible for the referenced svc or node
		if body.SvcId != nil && *body.SvcId != "" {
			responsible, err := odb.ServiceResponsible(ctx, *body.SvcId, UserGroupsFromContext(c), IsManager(c))
			if err != nil {
				log.Error("cannot check service responsibility", "svc_id", *body.SvcId, logkey.Error, err)
				return JSONProblemf(c, http.StatusInternalServerError, "cannot check service responsibility")
			}
			if !responsible {
				return JSONProblemf(c, http.StatusForbidden, "user is not responsible for service %s", *body.SvcId)
			}
			id := *body.SvcId
			msg.SvcID = &id
		} else if body.NodeId != nil && *body.NodeId != "" {
			responsible, err := odb.NodeResponsible(ctx, *body.NodeId, UserGroupsFromContext(c), IsManager(c))
			if err != nil {
				log.Error("cannot check node responsibility", logkey.NodeID, *body.NodeId, logkey.Error, err)
				return JSONProblemf(c, http.StatusInternalServerError, "cannot check node responsibility")
			}
			if !responsible {
				return JSONProblemf(c, http.StatusForbidden, "user is not responsible for node %s", *body.NodeId)
			}
			id := *body.NodeId
			msg.NodeID = &id
		}
	default:
		return JSONProblemf(c, http.StatusBadRequest, "unknown log sender")
	}

	logID, err := odb.InsertLogMessage(ctx, msg)
	if err != nil {
		log.Error("cannot insert log event", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot insert log event")
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	idStr := strconv.FormatInt(logID, 10)
	return a.handleItem(c, "PostLogs", "log_event", "id", idStr, listEndpointParams{},
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return odb.GetLog(ctx, idStr, p)
		})
}
