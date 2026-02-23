package feederhandlers

import (
	"archive/tar"
	"encoding/json"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/feeder"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"

	"github.com/spf13/viper"
)

type sysreportData struct {
	NeedCommit bool     `json:"need_commit"`
	Deleted    []string `json:"deleted"`
	NodeID     string   `json:"node_id"`
}

func (a *Api) PostNodeSysReport(ctx echo.Context) error {
	log := echolog.GetLogHandler(ctx, "PostNodeSysReport")
	file, err := ctx.FormFile("file")
	if err != nil {
		return JSONProblemf(ctx, http.StatusBadRequest, "FormFile: %s", err)
	}

	var payload feeder.SysReport
	if values, ok := ctx.Request().MultipartForm.Value["deleted"]; ok {
		payload.Deleted = values
	}
	payload.File.InitFromMultipart(file)

	uploadDir := viper.GetString("scheduler.directories.uploads")
	sysreportDir := filepath.Join(uploadDir, "sysreport")
	if err := os.MkdirAll(sysreportDir, 0755); err != nil {
		log.Error("can't create sysreport dir", logkey.Error, err)
		return JSONProblem(ctx, http.StatusInternalServerError, "can't create sysreport dir")
	}

	nodeID := ctx.Get(XNodeID).(string)

	needCommit := false
	needCommit = sendSysreportDelete(log, payload.Deleted, sysreportDir, nodeID) || needCommit
	needCommit = sendSysreportArchive(log, payload, file.Filename, sysreportDir, nodeID) || needCommit

	// TODO: Add metric PostNodeSysReport File size

	v := sysreportData{
		NeedCommit: needCommit,
		Deleted:    payload.Deleted,
		NodeID:     nodeID,
	}
	if b, err := json.Marshal(v); err != nil {
		log.Error("Marshal", logkey.Error, err)
		return JSONProblem(ctx, http.StatusInternalServerError, "unexpected marshall error")
	} else if err := a.Redis.RPush(ctx.Request().Context(), cachekeys.FeedSysreportQ, string(b)).Err(); err != nil {
		log.Error("RPush FeedSysreportQ", logkey.Error, err)
		return JSONProblem(ctx, http.StatusInternalServerError, "unexpected internal feed queue error")
	}

	return ctx.JSON(http.StatusAccepted, "sysreport accepted")
}

func sendSysreportDelete(l *slog.Logger, deleted []string, sysreportDir string, nodeID string) bool {
	if len(deleted) == 0 {
		return false
	}
	nodeDir := filepath.Join(sysreportDir, nodeID)
	for _, fpath := range deleted {
		fpath = strings.TrimSpace(fpath)
		var relpath string
		if filepath.IsAbs(fpath) {
			relpath = "file" + fpath
		} else {
			relpath = filepath.Join("file", fpath)
		}
		pathToDelete := filepath.Join(nodeDir, relpath)
		if err := os.Remove(pathToDelete); err != nil && !os.IsNotExist(err) {
			l.Warn("sendSysreportDelete", logkey.Error, err)
		}
	}
	return true
}

func sendSysreportArchive(l *slog.Logger, payload feeder.SysReport, filename string, sysreportDir string, nodeID string) bool {
	if filename == "" {
		return false
	}
	fPath := filepath.Join(sysreportDir, filename)

	if !strings.HasSuffix(fPath, ".tar") {
		return false
	}

	reader, err := payload.File.Reader()
	if err != nil {
		l.Error("sendSysreportArchive", logkey.Error, err)
		return false
	}
	defer reader.Close()

	tr := tar.NewReader(reader)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			l.Error("sendSysreportArchive", logkey.Error, err)
			return false
		}
		idx := strings.Index(header.Name, "/")
		if idx == -1 {
			continue
		}
		targetName := nodeID + header.Name[idx:]
		targetPath := filepath.Join(sysreportDir, targetName)

		if info, err := os.Stat(targetPath); err == nil {
			// enable write
			_ = os.Chmod(targetPath, info.Mode()|0200)
		}

		if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
			l.Error("sendSysreportArchive", logkey.Error, err)
			return false
		}

		outFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, fs.FileMode(header.Mode))
		if err != nil {
			l.Error("sendSysreportArchive OpenFile", logkey.Error, err)
			return false
		} else {
			defer func() {
				_ = outFile.Close()
			}()
		}

		if _, err := io.Copy(outFile, tr); err != nil {
			l.Error("sendSysreportArchive Copy", logkey.Error, err)
			return false
		}

		if info, err := os.Stat(targetPath); err == nil {
			// restore read only
			_ = os.Chmod(targetPath, info.Mode()|0400)
		}

	}
	return true
}
