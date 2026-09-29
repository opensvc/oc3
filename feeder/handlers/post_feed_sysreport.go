package feederhandlers

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"

	"github.com/spf13/viper"
)

type sysreportData struct {
	NeedCommit bool     `json:"need_commit"`
	Deleted    []string `json:"deleted"`
	NodeID     string   `json:"node_id"`
}

// PostNodeSysReport stores the files and command outputs a node tracks, as
// the tree <uploads>/sysreport/<node id>/{file,cmd}/..., and queues the
// commit of the tree the scheduler versions.
//
// The form carries the tar of what changed, the tracked files deleted, and
// the full flag of a report holding everything the node tracks. A report of
// deletions only has no tar.
//
// Every path is confined to the tree of the node: a member or a deleted path
// climbing out of it with ".." lands inside it, as rooted there.
func (a *Api) PostNodeSysReport(ctx echo.Context) error {
	log := echolog.GetLogHandler(ctx, "PostNodeSysReport")
	nodeID, ok := ctx.Get(XNodeID).(string)
	if !ok || nodeID == "" {
		return JSONNodeAuthProblem(ctx)
	}
	form, err := ctx.MultipartForm()
	if err != nil {
		return JSONProblemf(ctx, http.StatusBadRequest, "multipart form: %s", err)
	}
	deleted := form.Value["deleted"]
	full := false
	if l := form.Value["full"]; len(l) > 0 {
		if full, err = strconv.ParseBool(l[0]); err != nil {
			return JSONProblemf(ctx, http.StatusBadRequest, "full: %s", err)
		}
	}
	var file *multipart.FileHeader
	if l := form.File["file"]; len(l) > 0 {
		file = l[0]
	}
	if file == nil && full {
		return JSONProblem(ctx, http.StatusBadRequest, "a full report needs its archive")
	}

	uploadDir := viper.GetString("scheduler.directories.uploads")
	nodeDir := filepath.Join(uploadDir, "sysreport", nodeID)
	if err := os.MkdirAll(nodeDir, 0755); err != nil {
		log.Error("can't create sysreport dir", logkey.Error, err)
		return JSONProblem(ctx, http.StatusInternalServerError, "can't create sysreport dir")
	}

	needCommit := sysreportDelete(log, deleted, nodeDir)
	if file != nil {
		written, err := sysreportExtract(file, nodeDir)
		if err != nil {
			log.Error("sysreportExtract", logkey.Error, err)
			return JSONProblemf(ctx, http.StatusBadRequest, "archive: %s", err)
		}
		if len(written) > 0 {
			needCommit = true
		}
		if full {
			if n := sysreportRemoveOthers(log, nodeDir, written); n > 0 {
				needCommit = true
			}
		}
	}

	v := sysreportData{
		NeedCommit: needCommit,
		Deleted:    deleted,
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

// confine returns the path rel names inside base, rel rooted at base: a rel
// climbing with ".." stops at base, and never leaves it.
func confine(base, rel string) string {
	return filepath.Join(base, filepath.Clean("/"+rel))
}

// sysreportDelete removes the files of the node tree the node deleted,
// named by their path on the node, and says whether it was given any.
func sysreportDelete(l *slog.Logger, deleted []string, nodeDir string) bool {
	if len(deleted) == 0 {
		return false
	}
	fileDir := filepath.Join(nodeDir, "file")
	for _, fpath := range deleted {
		fpath = strings.TrimSpace(fpath)
		if fpath == "" {
			continue
		}
		if err := os.Remove(confine(fileDir, fpath)); err != nil && !os.IsNotExist(err) {
			l.Warn("sysreportDelete", logkey.Error, err)
		}
	}
	return true
}

// sysreportExtract writes the regular files of the archive in the node tree,
// the first component of their name, the node name, replaced by the tree,
// and returns the paths written. Each file is closed once written.
func sysreportExtract(file *multipart.FileHeader, nodeDir string) (map[string]bool, error) {
	reader, err := file.Open()
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return sysreportExtractReader(reader, nodeDir)
}

func sysreportExtractReader(reader io.Reader, nodeDir string) (map[string]bool, error) {
	written := make(map[string]bool)
	tr := tar.NewReader(reader)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return written, err
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}
		_, rel, ok := strings.Cut(header.Name, "/")
		if !ok || rel == "" {
			continue
		}
		targetPath := confine(nodeDir, rel)
		if err := writeSysreportFile(targetPath, fs.FileMode(header.Mode).Perm(), tr); err != nil {
			return written, err
		}
		written[targetPath] = true
	}
	return written, nil
}

func writeSysreportFile(targetPath string, mode fs.FileMode, r io.Reader) error {
	if info, err := os.Stat(targetPath); err == nil {
		// enable write
		_ = os.Chmod(targetPath, info.Mode()|0200)
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
		return err
	}
	f, err := os.OpenFile(targetPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, mode|0600)
	if err != nil {
		return fmt.Errorf("open %s: %w", targetPath, err)
	}
	if _, err := io.Copy(f, r); err != nil {
		_ = f.Close()
		return fmt.Errorf("write %s: %w", targetPath, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", targetPath, err)
	}
	return os.Chmod(targetPath, mode|0400)
}

// sysreportRemoveOthers removes the files of the node tree a full report does
// not hold, and returns how many it removed. The git history of the tree is
// left alone.
func sysreportRemoveOthers(l *slog.Logger, nodeDir string, written map[string]bool) int {
	n := 0
	for _, sub := range []string{"file", "cmd"} {
		root := filepath.Join(nodeDir, sub)
		_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if d.IsDir() || written[path] {
				return nil
			}
			if err := os.Remove(path); err != nil {
				l.Warn("sysreportRemoveOthers", logkey.Error, err)
				return nil
			}
			n++
			return nil
		})
	}
	return n
}
