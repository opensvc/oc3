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
func (a *Api) PostTag(c echo.Context, tagIdParam string) error {
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

	tag, err := a.resolveTagByKey(c, log, ctx, tagIdParam)
	if err != nil {
		return err
	}

	fields := cdb.UpdateTagFields{
		TagName:    body.TagName,
		TagExclude: body.TagExclude,
		TagData:    body.TagData,
	}
	if _, err := odb.UpdateTag(ctx, tag.ID, fields); err != nil {
		log.Error("cannot update tag", logkey.TagID, tag.ID, logkey.Error, err)
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

	return a.handleGetTags(c, &tag.ID, ListQueryParameters{Props: defaultProps(propsMapping["tag"])}, nil)
}
