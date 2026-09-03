package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// PostTag handles POST /tags/{tag_id} update a tag's properties.
func (a *Api) PostTag(c echo.Context, tagIdParam int) error {
	log := echolog.GetLogHandler(c, "PostTag")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if err := requireTagManager(c); err != nil {
		return err
	}

	var body server.PostTagJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	log.Info("called", logkey.TagID, tagIdParam)

	tags, err := odb.GetTags(ctx, &tagIdParam, 0, 0)
	if err != nil {
		log.Error("cannot get tag", logkey.TagID, tagIdParam, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get tag")
	}
	if len(tags) == 0 {
		return JSONProblemf(c, http.StatusNotFound, "tag %d not found", tagIdParam)
	}
	tag := tags[0]

	fields := cdb.UpdateTagFields{
		TagName:    body.TagName,
		TagExclude: body.TagExclude,
		TagData:    body.TagData,
	}
	if _, err := odb.UpdateTag(ctx, tagIdParam, fields); err != nil {
		log.Error("cannot update tag", logkey.TagID, tagIdParam, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot update tag")
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "tag.change",
		User:   userEmail,
		Fmt:    "change tag %(tag_name)s: %(data)s",
		Dict: map[string]any{
			"tag_name": tag.TagName,
			"data":     fields,
		},
		Level: "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return a.handleGetTags(c, &tagIdParam, ListQueryParameters{Props: defaultProps(propsMapping["tag"])})
}
