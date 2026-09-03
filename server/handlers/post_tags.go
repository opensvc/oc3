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

// PostTags handles POST /tags
func (a *Api) PostTags(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostTags")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if err := requireTagManager(c); err != nil {
		return err
	}

	var body server.PostTagsJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if body.TagName == "" {
		return JSONProblemf(c, http.StatusBadRequest, "the tag_name property is mandatory")
	}

	log.Info("called", "tag_name", body.TagName)

	existing, err := odb.TagByName(ctx, body.TagName)
	if err != nil {
		log.Error("cannot check tag existence", "tag_name", body.TagName, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check tag existence")
	}
	if existing != nil {
		return JSONProblemf(c, http.StatusConflict, "tag '%s' already exists", body.TagName)
	}

	tag, err := odb.InsertTag(ctx, body.TagName, body.TagExclude, body.TagData)
	if err != nil {
		log.Error("cannot create tag", "tag_name", body.TagName, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot create tag")
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "tag.create",
		User:   userEmail,
		Fmt:    "tag '%(tag_name)s' created",
		Dict:   map[string]any{"tag_name": tag.TagName},
		Level:  "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	a.notifyChanges(log, ctx)

	return a.handleGetTags(c, &tag.ID, ListQueryParameters{Props: defaultProps(propsMapping["tag"])})
}
