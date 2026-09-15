package serverhandlers

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
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
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if err := requireTagManager(c); err != nil {
		return err
	}

	log.Info("called", logkey.TagID, tagIdParam)

	tag, err := a.resolveTagByRecordID(c, log, ctx, tagIdParam)
	if err != nil {
		return err
	}

	return a.deleteTagCascade(c, log, ctx, tag)
}

// deleteTagCascade deletes the tag and its node and service attachments.
func (a *Api) deleteTagCascade(c echo.Context, log *slog.Logger, ctx context.Context, tag *cdb.Tag) error {
	odb := a.ODB

	tx, markSuccess, endTx, err := odb.BeginTxWithControl(ctx, log, &sql.TxOptions{})
	if err != nil {
		log.Error("cannot start transaction", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete tag")
	}
	defer endTx()

	res, err := tx.DeleteTagCascade(ctx, tag.ID, tag.TagID)
	if err != nil {
		log.Error("cannot delete tag", logkey.TagID, tag.ID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete tag %s", tag.TagName)
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := tx.Log(ctx, cdb.LogEntry{
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

	a.notifyChanges(log, ctx)

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

func requireTagManager(c echo.Context) error {
	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	if !IsTagManager(c) {
		return JSONProblemf(c, http.StatusForbidden, "TagManager privilege required")
	}
	return nil
}
