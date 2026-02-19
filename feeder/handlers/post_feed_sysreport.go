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
	"github.com/spf13/viper"
)

type sysreportData struct {
	NeedCommit bool     `json:"need_commit"`
	Deleted    []string `json:"deleted"`
	NodeID     string   `json:"node_id"`
}

func (a *Api) PostNodeSysReport(ctx echo.Context) error {
	file, err := ctx.FormFile("file")
	if err != nil {
		return JSONProblem(ctx, http.StatusBadRequest, "Missing file", err.Error())
	}

	var payload feeder.SysReport
	if values, ok := ctx.Request().MultipartForm.Value["deleted"]; ok {
		payload.Deleted = values
	}
	payload.File.InitFromMultipart(file)

	sysreportDir := viper.GetString("sysreport.dir")
	if err := os.MkdirAll(sysreportDir, 0755); err != nil {
		return JSONProblem(ctx, http.StatusInternalServerError, "can't create sysreport dir", err.Error())
	}

	nodeID := ctx.Get(XNodeID).(string)

	needCommit := false
	needCommit = sendSysreportDelete(payload.Deleted, sysreportDir, nodeID) || needCommit
	needCommit = sendSysreportArchive(payload, file.Filename, sysreportDir, nodeID) || needCommit

	slog.Info("PostNodeSysReport", "size", payload.File.FileSize(), "node_id", nodeID)

	v := sysreportData{
		NeedCommit: needCommit,
		Deleted:    payload.Deleted,
		NodeID:     nodeID,
	}
	if b, err := json.Marshal(v); err != nil {
		slog.Error("PostNodeSysReport", "marshal", err)
	} else if err := a.Redis.RPush(ctx.Request().Context(), cachekeys.FeedSysreportQ, string(b)).Err(); err != nil {
		slog.Error("PostNodeSysReport", "rpush", err)
	}

	return ctx.JSON(http.StatusAccepted, "sysreport accepted")
}

func sendSysreportDelete(deleted []string, sysreportDir string, nodeID string) bool {
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
			slog.Debug("sendSysreportDelete", "path", pathToDelete, "error", err)
		}
	}
	return true
}

func sendSysreportArchive(payload feeder.SysReport, filename string, sysreportDir string, nodeID string) bool {
	if filename == "" {
		return false
	}
	fPath := filepath.Join(sysreportDir, filename)

	if !strings.HasSuffix(fPath, ".tar") {
		return false
	}

	reader, err := payload.File.Reader()
	if err != nil {
		slog.Error("sendSysreportArchive", "error", err)
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
			slog.Error("sendSysreportArchive", "error", err)
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
			slog.Error("sendSysreportArchive", "error", err)
			return false
		}

		outFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, fs.FileMode(header.Mode))
		if err != nil {
			slog.Error("sendSysreportArchive", "error", err)
			return false
		}
		if _, err := io.Copy(outFile, tr); err != nil {
			outFile.Close()
			slog.Error("sendSysreportArchive", "error", err)
			return false
		}
		outFile.Close()

		if info, err := os.Stat(targetPath); err == nil {
			// restore read only
			_ = os.Chmod(targetPath, info.Mode()|0400)
		}

	}
	return true
}
