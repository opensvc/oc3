package serverhandlers

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/safe"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// safeAccess is who asks for safe files, see cdb.SafeAccess.
func safeAccess(c echo.Context) cdb.SafeAccess {
	a := cdb.SafeAccess{Manager: IsManager(c), Groups: UserGroupsFromContext(c)}
	if IsAuthByNode(c) {
		a.NodeID, _ = c.Get(XNodeID).(string)
		a.Groups = nil
		return a
	}
	a.UserID = authUserID(c)
	return a
}

// safeFile finds a file by its id or stored name and checks the caller may read
// it, or manage it, with the messages of lib_safe_check_file_publication() and
// lib_safe_check_file_responsible().
func (a *Api) safeFile(c echo.Context, ref string, manage bool) (*cdb.SafeFile, error) {
	ctx := c.Request().Context()
	log := echolog.GetLogHandler(c, "safe")
	f, err := a.ODB.SafeFileByRef(ctx, ref)
	if err != nil {
		return nil, httpInternal(log, "cannot read the safe file", err)
	}
	if f == nil {
		return nil, httpErrorf(http.StatusNotFound, "safe file %s does not exist", ref)
	}
	allowed, err := a.ODB.SafeFileAllowed(ctx, f.ID, safeAccess(c), manage)
	if err != nil {
		return nil, httpInternal(log, "cannot check the safe file access", err)
	}
	if !allowed {
		who := "you are"
		if IsAuthByNode(c) {
			who = "this node is"
		}
		what := "access"
		if manage {
			what = "manage"
		}
		return nil, httpErrorf(http.StatusForbidden, "%s not authorized to %s this file", who, what)
	}
	return f, nil
}

// checkSafeUploader requires the SafeUploader privilege to store a file, except
// for a node, as the historical collector does.
func checkSafeUploader(c echo.Context) error {
	if IsAuthByNode(c) || IsManager(c) || HasGroup(c, "SafeUploader") {
		return nil
	}
	return httpErrorf(http.StatusForbidden, "the SafeUploader privilege is required")
}

// safeAudit writes an entry of the audit log: the user, or the node, acting on a
// safe file. The content of a file is never logged.
func (a *Api) safeAudit(c echo.Context, action, format string, dict map[string]any) {
	ctx := c.Request().Context()
	entry := cdb.LogEntry{Action: action, Fmt: format, Dict: dict, Level: "info"}
	if IsAuthByNode(c) {
		nodeID, _ := c.Get(XNodeID).(string)
		nodename, _ := c.Get(XNodename).(string)
		entry.User = nodename
		if parsed, err := uuid.Parse(nodeID); err == nil {
			entry.NodeID = &parsed
		}
	} else {
		entry.User, _ = c.Get(XUserEmail).(string)
	}
	if err := a.ODB.Log(ctx, entry); err != nil {
		echolog.GetLogHandler(c, "safe").Error("cannot write audit log", logkey.Error, err)
	}
}

func (a *Api) safeChanged(c echo.Context, tables ...string) {
	ctx := c.Request().Context()
	for _, table := range tables {
		a.ODB.Session.SetChanges(table)
	}
	if err := a.ODB.Session.NotifyChanges(ctx); err != nil {
		echolog.GetLogHandler(c, "safe").Error("cannot notify changes", logkey.Error, err)
	}
}

// safeList answers a list of safe files, read with the caller's access.
func (a *Api) safeList(c echo.Context, name string, ref *string, historyOf *int64, p listEndpointParams) error {
	access := safeAccess(c)
	return a.handleList(c, name, "safe", p, func(ctx context.Context, lp cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetSafeFiles(ctx, ref, historyOf, access, lp)
	})
}

// safeFileResponse answers a file just written, as the historical handlers
// answer with rest_get_safe_file().
func (a *Api) safeFileResponse(c echo.Context, id int64) error {
	ref := strconv.FormatInt(id, 10)
	return a.safeList(c, "safe", &ref, nil, listEndpointParams{})
}

// GetSafeFiles handles GET /safe.
func (a *Api) GetSafeFiles(c echo.Context, params server.GetSafeFilesParams) error {
	return a.safeList(c, "GetSafeFiles", nil, nil, listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter,
	})
}

// GetSafeFile handles GET /safe/{file_id}: empty when the caller may not see it.
func (a *Api) GetSafeFile(c echo.Context, fileId string, params server.GetSafeFileParams) error {
	return a.safeList(c, "GetSafeFile", &fileId, nil, listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter,
	})
}

// GetSafeFileHistory handles GET /safe/{file_id}/history.
func (a *Api) GetSafeFileHistory(c echo.Context, fileId string, params server.GetSafeFileHistoryParams) error {
	f, err := a.ODB.SafeFileByRef(c.Request().Context(), fileId)
	if err != nil {
		return httpProblem(c, httpInternal(echolog.GetLogHandler(c, "GetSafeFileHistory"), "cannot read the safe file", err))
	}
	if f == nil {
		return JSONProblemf(c, http.StatusNotFound, "safe file %s does not exist", fileId)
	}
	return a.safeList(c, "GetSafeFileHistory", nil, &f.ID, listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter,
	})
}

// readUpload reads the "file" part of a multipart request, within the size the
// safe accepts: the name to tell it by (the "name" part, else the file name), the
// name of the uploaded file, which its stored name encodes as web2py does, and its
// content.
func (a *Api) readUpload(c echo.Context) (string, string, []byte, error) {
	limit := a.Safe.MaxSize
	if limit > 0 {
		// The other parts and the encoding take some room beside the file.
		c.Request().Body = http.MaxBytesReader(c.Response(), c.Request().Body, limit+1024*1024)
	}
	header, err := c.FormFile("file")
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return "", "", nil, httpErrorf(http.StatusRequestEntityTooLarge, "the file is larger than the %d bytes allowed", limit)
		}
		return "", "", nil, httpErrorf(http.StatusBadRequest, "a multipart \"file\" part is expected: %s", err)
	}
	part, err := header.Open()
	if err != nil {
		return "", "", nil, httpErrorf(http.StatusBadRequest, "cannot read the uploaded file: %s", err)
	}
	defer func() { _ = part.Close() }()
	reader := io.Reader(part)
	if limit > 0 {
		reader = io.LimitReader(part, limit+1)
	}
	content, err := io.ReadAll(reader)
	if err != nil {
		return "", "", nil, httpErrorf(http.StatusBadRequest, "cannot read the uploaded file: %s", err)
	}
	if limit > 0 && int64(len(content)) > limit {
		return "", "", nil, httpErrorf(http.StatusRequestEntityTooLarge, "the file is larger than the %d bytes allowed", limit)
	}
	filename := filepath.Base(header.Filename)
	name := strings.TrimSpace(c.FormValue("name"))
	if name == "" {
		name = filename
	}
	return name, filename, content, nil
}

func uploadedFrom(c echo.Context) string {
	return c.RealIP()
}

func safeUploader(c echo.Context) *int64 {
	if IsAuthByNode(c) {
		return nil
	}
	return authUserID(c)
}

// PostSafeUpload handles POST /safe/upload.
func (a *Api) PostSafeUpload(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostSafeUpload")
	ctx := c.Request().Context()
	if err := checkSafeUploader(c); err != nil {
		return httpProblem(c, err)
	}
	name, filename, plaintext, err := a.readUpload(c)
	if err != nil {
		return httpProblem(c, err)
	}
	content, err := a.Safe.Put(filename, plaintext)
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot store the safe file", err))
	}
	content.Name, content.Uploader, content.UploadedFrom = name, safeUploader(c), uploadedFrom(c)
	groupID, err := a.ODB.SafeDefaultGroupID(ctx, safeAccess(c))
	if err != nil {
		_ = a.Safe.Store.Remove(content.UUID)
		return httpProblem(c, httpInternal(log, "cannot read the default group", err))
	}
	id, err := a.ODB.InsertSafeFile(ctx, content, groupID)
	if err != nil {
		_ = a.Safe.Store.Remove(content.UUID)
		return httpProblem(c, httpInternal(log, "cannot record the safe file", err))
	}
	a.safeAudit(c, "safe.upload", "file %(uuid)s uploaded (%(name)s, %(size)d bytes, encrypted: %(encrypted)s)", map[string]any{
		"id": id, "uuid": content.UUID, "name": name, "size": content.Size, "encrypted": strconv.FormatBool(content.EncVersion != nil),
	})
	a.safeChanged(c, "safe", "safe_team_publication", "safe_team_responsible")
	return a.safeFileResponse(c, id)
}

// PostSafeFile handles POST /safe: an empty file reference.
func (a *Api) PostSafeFile(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostSafeFile")
	ctx := c.Request().Context()
	if err := checkSafeUploader(c); err != nil {
		return httpProblem(c, err)
	}
	var body server.PostSafeFileJSONBody
	if c.Request().ContentLength != 0 {
		if err := c.Bind(&body); err != nil {
			return JSONProblemf(c, http.StatusBadRequest, "invalid body: %s", err)
		}
	}
	name := ""
	if body.Name != nil {
		name = strings.TrimSpace(*body.Name)
	}
	groupID, err := a.ODB.SafeDefaultGroupID(ctx, safeAccess(c))
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the default group", err))
	}
	id, err := a.ODB.InsertSafeFile(ctx, cdb.SafeFileContent{Name: name, Uploader: safeUploader(c), UploadedFrom: uploadedFrom(c)}, groupID)
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot record the safe file", err))
	}
	a.safeAudit(c, "safe.add", "file %(id)d added (%(name)s)", map[string]any{"id": id, "name": name})
	a.safeChanged(c, "safe", "safe_team_publication", "safe_team_responsible")
	return a.safeFileResponse(c, id)
}

// PostSafeFileUpload handles POST /safe/{file_id}/upload: a new version.
func (a *Api) PostSafeFileUpload(c echo.Context, fileId string) error {
	log := echolog.GetLogHandler(c, "PostSafeFileUpload")
	ctx := c.Request().Context()
	if err := checkSafeUploader(c); err != nil {
		return httpProblem(c, err)
	}
	f, err := a.safeFile(c, fileId, true)
	if err != nil {
		return httpProblem(c, err)
	}
	name, filename, plaintext, err := a.readUpload(c)
	if err != nil {
		return httpProblem(c, err)
	}
	// The form name, when given, renames the file; the uploaded file's name does not.
	if strings.TrimSpace(c.FormValue("name")) == "" && f.Name != "" {
		name = f.Name
	}
	content, err := a.Safe.Put(filename, plaintext)
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot store the safe file", err))
	}
	content.Name, content.Uploader, content.UploadedFrom = name, safeUploader(c), uploadedFrom(c)
	if _, err := a.ODB.UpdateSafeFileContent(ctx, f.ID, content); err != nil {
		_ = a.Safe.Store.Remove(content.UUID)
		return httpProblem(c, httpInternal(log, "cannot record the new version", err))
	}
	a.safeAudit(c, "safe.upload", "file %(id)d new version %(uuid)s uploaded (%(name)s, %(size)d bytes, encrypted: %(encrypted)s)", map[string]any{
		"id": f.ID, "uuid": content.UUID, "name": name, "size": content.Size, "encrypted": strconv.FormatBool(content.EncVersion != nil),
	})
	a.safeChanged(c, "safe", "safe_log", "safe_team_publication", "safe_team_responsible")
	return a.safeFileResponse(c, f.ID)
}

// PostSafeFileName handles POST /safe/{file_id}: a rename.
func (a *Api) PostSafeFileName(c echo.Context, fileId string) error {
	log := echolog.GetLogHandler(c, "PostSafeFileName")
	f, err := a.safeFile(c, fileId, true)
	if err != nil {
		return httpProblem(c, err)
	}
	var body server.PostSafeFileNameJSONBody
	if err := c.Bind(&body); err != nil || strings.TrimSpace(body.Name) == "" {
		return JSONProblemf(c, http.StatusBadRequest, "a name is expected")
	}
	name := strings.TrimSpace(body.Name)
	if err := a.ODB.SetSafeFileName(c.Request().Context(), f.ID, name); err != nil {
		return httpProblem(c, httpInternal(log, "cannot rename the safe file", err))
	}
	a.safeAudit(c, "safe.change", "file %(id)d renamed from %(from)s to %(to)s", map[string]any{"id": f.ID, "from": f.Name, "to": name})
	a.safeChanged(c, "safe")
	return a.safeFileResponse(c, f.ID)
}

// DeleteSafeFile handles DELETE /safe/{file_id}.
func (a *Api) DeleteSafeFile(c echo.Context, fileId string) error {
	log := echolog.GetLogHandler(c, "DeleteSafeFile")
	ctx := c.Request().Context()
	f, err := a.safeFile(c, fileId, true)
	if err != nil {
		return httpProblem(c, err)
	}
	if err := a.ODB.DeleteSafeFile(ctx, f.ID); err != nil {
		return httpProblem(c, httpInternal(log, "cannot delete the safe file", err))
	}
	// The content goes too, unless another row holds the same stored file.
	if f.UUID != "" {
		if shared, err := a.ODB.SafeFileUsedElsewhere(ctx, f.ID, f.UUID); err != nil {
			log.Error("cannot check whether the file is shared", logkey.Error, err)
		} else if !shared {
			if err := a.Safe.Store.Remove(f.UUID); err != nil {
				log.Error("cannot remove the safe file content", logkey.Error, err)
			}
		}
	}
	a.safeAudit(c, "safe.delete", "file %(uuid)s deleted", map[string]any{"id": f.ID, "uuid": f.UUID})
	a.safeChanged(c, "safe", "safe_log", "safe_team_publication", "safe_team_responsible")
	return c.JSON(http.StatusOK, server.SafeInfoResponse{Info: fmt.Sprintf("file %s deleted", f.UUID)})
}

// safeContent reads a file's plaintext for the caller, with the messages of the
// historical collector for a compromised file.
func (a *Api) safeContent(c echo.Context, log *slog.Logger, f *cdb.SafeFile) ([]byte, error) {
	if f.UUID == "" {
		return nil, httpErrorf(http.StatusNotFound, "safe file %d has no content yet", f.ID)
	}
	plaintext, err := a.Safe.Get(f)
	switch {
	case err == nil:
		return plaintext, nil
	case errors.Is(err, safe.ErrCompromised):
		log.Error("safe file compromised", "id", f.ID, "uuid", f.UUID)
		return nil, httpErrorf(http.StatusForbidden, "the file is compromised: its content does not match the expected md5 %s", f.MD5)
	case errors.Is(err, safe.ErrOpen), errors.Is(err, safe.ErrSealedWithoutKey):
		log.Error("safe file cannot be decrypted", "id", f.ID, "uuid", f.UUID, logkey.Error, err)
		return nil, httpErrorf(http.StatusForbidden, "the file is compromised: it cannot be decrypted")
	default:
		return nil, httpInternal(log, "cannot read the safe file", err)
	}
}

// GetSafeFileDownload handles GET /safe/{file_id}/download.
func (a *Api) GetSafeFileDownload(c echo.Context, fileId string, params server.GetSafeFileDownloadParams) error {
	log := echolog.GetLogHandler(c, "GetSafeFileDownload")
	f, err := a.safeFile(c, fileId, false)
	if err != nil {
		return httpProblem(c, err)
	}
	if params.Md5 != nil && *params.Md5 != "" && *params.Md5 == f.MD5 {
		return c.NoContent(http.StatusNoContent)
	}
	plaintext, err := a.safeContent(c, log, f)
	if err != nil {
		return httpProblem(c, err)
	}
	a.safeAudit(c, "safe.download", "file %(uuid)s downloaded", map[string]any{"id": f.ID, "uuid": f.UUID})
	filename := safe.OriginalName(f.UUID)
	if filename == "" {
		filename = f.UUID
	}
	h := c.Response().Header()
	h.Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": filename}))
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	return c.Blob(http.StatusOK, "application/octet-stream", plaintext)
}

// previewable tells the text files a preview shows, as lib_safe_preview()
// accepts text/plain, logs and yaml.
func previewable(name string, content []byte) (string, bool) {
	ext := strings.ToLower(filepath.Ext(name))
	contentType := mime.TypeByExtension(ext)
	switch ext {
	case ".log", ".yaml", ".yml", ".conf", ".cfg", ".ini", ".txt", ".json", ".pem", ".crt", ".csr", ".key", ".pub":
		contentType = "text/plain"
	}
	if !strings.HasPrefix(contentType, "text/") || !utf8.Valid(content) {
		return contentType, false
	}
	return contentType, true
}

// GetSafeFilePreview handles GET /safe/{file_id}/preview.
func (a *Api) GetSafeFilePreview(c echo.Context, fileId string) error {
	log := echolog.GetLogHandler(c, "GetSafeFilePreview")
	f, err := a.safeFile(c, fileId, false)
	if err != nil {
		return httpProblem(c, err)
	}
	plaintext, err := a.safeContent(c, log, f)
	if err != nil {
		return httpProblem(c, err)
	}
	contentType, ok := previewable(f.UUID, plaintext)
	if !ok {
		return JSONProblemf(c, http.StatusBadRequest, "the file content type is %s, not text", contentType)
	}
	a.safeAudit(c, "safe.preview", "file %(uuid)s previewed", map[string]any{"id": f.ID, "uuid": f.UUID})
	c.Response().Header().Set("Cache-Control", "no-store")
	var resp server.SafeFilePreviewResponse
	resp.Data.ContentType = contentType
	resp.Data.Data = base64.StdEncoding.EncodeToString(plaintext)
	return c.JSON(http.StatusOK, resp)
}

// GetSafeFileUsage handles GET /safe/{file_id}/usage.
func (a *Api) GetSafeFileUsage(c echo.Context, fileId string) error {
	log := echolog.GetLogHandler(c, "GetSafeFileUsage")
	f, err := a.safeFile(c, fileId, false)
	if err != nil {
		return httpProblem(c, err)
	}
	usage, err := a.ODB.GetSafeFileUsage(c.Request().Context(), f, safeAccess(c))
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the file usage", err))
	}
	return c.JSON(http.StatusOK, map[string]any{"data": usage})
}

// GetSafeFileAmIResponsible handles GET /safe/{file_id}/am_i_responsible.
func (a *Api) GetSafeFileAmIResponsible(c echo.Context, fileId string) error {
	_, err := a.safeFile(c, fileId, true)
	var he *httpError
	switch {
	case err == nil:
		return c.JSON(http.StatusOK, map[string]any{"data": true})
	case errors.As(err, &he) && he.status == http.StatusForbidden:
		return c.JSON(http.StatusOK, map[string]any{"data": false})
	}
	return httpProblem(c, err)
}

func (a *Api) safeGroups(c echo.Context, fileId string, responsible bool) error {
	f, err := a.safeFile(c, fileId, false)
	if err != nil {
		return httpProblem(c, err)
	}
	groups, err := a.ODB.SafeFileGroups(c.Request().Context(), f.ID, responsible)
	if err != nil {
		return httpProblem(c, httpInternal(echolog.GetLogHandler(c, "safe"), "cannot read the file groups", err))
	}
	return c.JSON(http.StatusOK, map[string]any{"data": groups})
}

func (a *Api) safeGroupChange(c echo.Context, fileId, group string, responsible, add bool) error {
	log := echolog.GetLogHandler(c, "safe")
	ctx := c.Request().Context()
	f, err := a.safeFile(c, fileId, true)
	if err != nil {
		return httpProblem(c, err)
	}
	g, found, err := a.ODB.AuthGroupByIDOrRole(ctx, group)
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot read the group", err))
	}
	if !found || g == nil {
		return JSONProblemf(c, http.StatusNotFound, "group %s does not exist", group)
	}
	kind, verb, table := "publication", "published to", "safe_team_publication"
	if responsible {
		kind, verb, table = "responsible", "made the responsibility of", "safe_team_responsible"
	}
	var changed bool
	if add {
		changed, err = a.ODB.AddSafeFileGroup(ctx, f.ID, g.ID, responsible)
	} else {
		changed, err = a.ODB.RemoveSafeFileGroup(ctx, f.ID, g.ID, responsible)
	}
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot change the file groups", err))
	}
	if !changed {
		if add {
			return c.JSON(http.StatusOK, server.SafeInfoResponse{Info: fmt.Sprintf("file %s is already %s group %s", f.UUID, verb, g.Role)})
		}
		return c.JSON(http.StatusOK, server.SafeInfoResponse{Info: fmt.Sprintf("file %s is not %s group %s", f.UUID, verb, g.Role)})
	}
	action, format := "safe."+kind+".create", "file %(uuid)s "+verb+" group %(group)s"
	if !add {
		action, format = "safe."+kind+".delete", "file %(uuid)s no longer "+verb+" group %(group)s"
	}
	dict := map[string]any{"id": f.ID, "uuid": f.UUID, "group": g.Role}
	a.safeAudit(c, action, format, dict)
	a.safeChanged(c, table)
	text := format
	for k, v := range dict {
		text = strings.ReplaceAll(text, "%("+k+")s", fmt.Sprint(v))
	}
	return c.JSON(http.StatusOK, server.SafeInfoResponse{Info: text})
}

// GetSafeFilePublications handles GET /safe/{file_id}/publications.
func (a *Api) GetSafeFilePublications(c echo.Context, fileId string) error {
	return a.safeGroups(c, fileId, false)
}

// PostSafeFilePublication handles POST /safe/{file_id}/publications/{group}.
func (a *Api) PostSafeFilePublication(c echo.Context, fileId string, group string) error {
	return a.safeGroupChange(c, fileId, group, false, true)
}

// DeleteSafeFilePublication handles DELETE /safe/{file_id}/publications/{group}.
func (a *Api) DeleteSafeFilePublication(c echo.Context, fileId string, group string) error {
	return a.safeGroupChange(c, fileId, group, false, false)
}

// GetSafeFileResponsibles handles GET /safe/{file_id}/responsibles.
func (a *Api) GetSafeFileResponsibles(c echo.Context, fileId string) error {
	return a.safeGroups(c, fileId, true)
}

// PostSafeFileResponsible handles POST /safe/{file_id}/responsibles/{group}.
func (a *Api) PostSafeFileResponsible(c echo.Context, fileId string, group string) error {
	return a.safeGroupChange(c, fileId, group, true, true)
}

// DeleteSafeFileResponsible handles DELETE /safe/{file_id}/responsibles/{group}.
func (a *Api) DeleteSafeFileResponsible(c echo.Context, fileId string, group string) error {
	return a.safeGroupChange(c, fileId, group, true, false)
}
