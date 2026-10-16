package serverhandlers

import (
	"context"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// tagFieldMaxLen is the size of the tag_name and tag_exclude columns.
const tagFieldMaxLen = 128

// PostTag handles POST /tags/{tag_id}: it updates the name, the exclusion
// pattern or the data of a tag. A name is trimmed, and refused when empty, too
// long or carried by another tag; an exclusion pattern, when too long or not a
// regular expression the database accepts.
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

	if body.TagName != nil {
		name := strings.TrimSpace(*body.TagName)
		switch {
		case name == "":
			return JSONProblemf(c, http.StatusBadRequest, "the tag name may not be empty")
		case utf8.RuneCountInString(name) > tagFieldMaxLen:
			return JSONProblemf(c, http.StatusBadRequest, "the tag name may not exceed %d characters", tagFieldMaxLen)
		}
		// tag_name is unique: refused here with its reason rather than by the
		// database as an internal error.
		other, err := odb.TagByName(ctx, name)
		if err != nil {
			log.Error("cannot check tag existence", "tag_name", name, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot check tag existence")
		}
		if other != nil && other.ID != tag.ID {
			return JSONProblemf(c, http.StatusConflict, "tag '%s' already exists", name)
		}
		body.TagName = &name
	}
	if body.TagExclude != nil && *body.TagExclude != "" {
		if utf8.RuneCountInString(*body.TagExclude) > tagFieldMaxLen {
			return JSONProblemf(c, http.StatusBadRequest, "the exclusion pattern may not exceed %d characters", tagFieldMaxLen)
		}
		refusal, err := odb.CheckTagExclude(ctx, *body.TagExclude)
		if err != nil {
			log.Error("cannot check the exclusion pattern", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot check the exclusion pattern")
		}
		if refusal != nil {
			return JSONProblemf(c, http.StatusBadRequest, "invalid exclusion pattern: %s", refusal)
		}
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
