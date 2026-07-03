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

// PostTagService handles POST /tags/{tag_id}/services/{svc_id}: attach a tag to a service.
func (a *Api) PostTagService(c echo.Context, tagIdParam int, svcId string) error {
	log := echolog.GetLogHandler(c, "PostTagService")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	log.Info("called", logkey.TagID, tagIdParam, "svc_id", svcId)

	var body server.PostTagServiceJSONRequestBody
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

	svc, err := odb.ServiceBySvcIDOrName(ctx, svcId)
	if err != nil {
		log.Error("cannot resolve service", "svc_id", svcId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve service %s", svcId)
	}
	if svc == nil {
		return JSONProblemf(c, http.StatusNotFound, "service %s not found", svcId)
	}

	responsible, err := odb.ServiceResponsible(ctx, svc.SvcID, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		log.Error("cannot check if user is responsible for the service", "svc_id", svc.SvcID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check if user is responsible for service %s", svc.SvcID)
	}
	if !responsible {
		log.Info("user is not responsible for this service", "svc_id", svc.SvcID)
		return JSONProblemf(c, http.StatusForbidden, "user is not responsible for service %s", svc.SvcID)
	}

	current, err := odb.GetServiceTagAttachment(ctx, svc.SvcID, tag.TagID)
	if err != nil {
		log.Error("cannot get service tag attachment", "svc_id", svc.SvcID, logkey.TagID, tagIdParam, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get service tag attachment")
	}

	allowed, err := odb.ServiceTagAttachAllowed(ctx, svc.SvcID, tag.TagName)
	if err != nil {
		log.Error("cannot check tag compatibility", "svc_id", svc.SvcID, logkey.TagID, tagIdParam, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check tag compatibility")
	}
	if !allowed {
		return JSONProblemf(c, http.StatusConflict, "tag '%s' is not compatible with other tags attached to service '%s'", tag.TagName, svc.SvcID)
	}

	var info, logFmt string
	switch {
	case current != nil:
		if body.TagAttachData != nil && *body.TagAttachData != current.TagAttachData {
			if err := odb.UpdateServiceTagAttachData(ctx, svc.SvcID, tag.TagID, *body.TagAttachData); err != nil {
				log.Error("cannot update service tag attach data", "svc_id", svc.SvcID, logkey.TagID, tagIdParam, logkey.Error, err)
				return JSONProblemf(c, http.StatusInternalServerError, "cannot update service tag attach data")
			}
			info = fmt.Sprintf("service '%s' tag '%s' attach data updated", svc.SvcID, tag.TagName)
			logFmt = "service '%(svc_id)s' tag '%(tag_name)s' attach data updated"
		} else {
			return c.JSON(http.StatusOK, map[string]string{
				"info": fmt.Sprintf("tag '%s' already attached to service '%s'", tag.TagName, svc.SvcID),
			})
		}
	default:
		if err := odb.AttachServiceTag(ctx, svc.SvcID, tag.TagID); err != nil {
			log.Error("cannot attach tag to service", "svc_id", svc.SvcID, logkey.TagID, tagIdParam, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot attach tag to service")
		}
		info = fmt.Sprintf("service '%s' tag '%s' attached", svc.SvcID, tag.TagName)
		logFmt = "service '%(svc_id)s' tag '%(tag_name)s' attached"
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	logEntry := cdb.LogEntry{
		Action: "service.tag",
		User:   userEmail,
		Fmt:    logFmt,
		Dict:   map[string]any{"svc_id": svc.SvcID, "tag_name": tag.TagName},
		Level:  "info",
	}
	if parsed, err := uuid.Parse(svc.SvcID); err == nil {
		logEntry.SvcID = &parsed
	}
	if logErr := odb.Log(ctx, logEntry); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return c.JSON(http.StatusOK, map[string]string{"info": info})
}
