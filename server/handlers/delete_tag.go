package serverhandlers

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteTag handles DELETE /tags/{tag_id}: it deletes the tag and its
// attachments to nodes and services.
func (a *Api) DeleteTag(c echo.Context, tagIdParam int) error {
	log := echolog.GetLogHandler(c, "DeleteTag")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	if !IsManager(c) {
		return JSONProblemf(c, http.StatusForbidden, "TagManager privilege required")
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

	markSuccess, endTx, err := odb.BeginTxWithControl(ctx, log, &sql.TxOptions{})
	if err != nil {
		log.Error("cannot start transaction", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete tag")
	}
	defer endTx()

	res, err := odb.DeleteTagCascade(ctx, tagIdParam, tag.TagID)
	if err != nil {
		log.Error("cannot delete tag", logkey.TagID, tagIdParam, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete tag %s", tag.TagName)
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "tag.delete",
		User:   userEmail,
		Fmt:    "tag '%(tag_name)s' deleted",
		Dict:   map[string]any{"tag_name": tag.TagName},
		Level:  "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot write audit log")
	}

	markSuccess()

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	info := []string{
		fmt.Sprintf("%d node attachments deleted", res.NodeAttachments),
		fmt.Sprintf("%d service attachments deleted", res.SvcAttachments),
	}
	if res.TagDeleted {
		info = append(info, fmt.Sprintf("tag '%s' deleted", tag.TagName))
	} else {
		info = append(info, fmt.Sprintf("tag '%s' not deleted", tag.TagName))
	}

	return c.JSON(http.StatusOK, map[string]string{"info": strings.Join(info, ", ")})
}
